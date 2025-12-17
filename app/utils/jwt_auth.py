"""
JWT Authentication Utility

Implements JWKS-based JWT verification according to VERIFY.md specification.
"""

import os
import json
import jwt
import threading
import time
from functools import wraps
from typing import Optional, Dict, Any
from flask import request, jsonify
import requests
from datetime import datetime


class JWTAuthenticator:
    """
    JWT Authentication Handler
    
    Fetches JWKS from Identity Service and verifies JWTs.
    """
    
    def __init__(self, identity_service_url: str, ttl_minutes: int = 10):
        """
        Initialize JWT Authenticator
        
        Args:
            identity_service_url: Base URL of S04 Identity Service
            ttl_minutes: TTL for JWKS cache in minutes
        """
        self.identity_service_url = identity_service_url.rstrip('/')
        self.jwks_url = f"{self.identity_service_url}/.well-known/jwks.json"
        self.ttl_seconds = ttl_minutes * 60
        
        # JWKS cache
        self.jwks_cache: Optional[Dict[str, Any]] = None
        self.jwks_last_fetched: Optional[float] = None
        self.jwks_lock = threading.Lock()
        
        # Start background refresh thread
        self.refresh_thread = threading.Thread(target=self._refresh_jwks_loop, daemon=True)
        self.refresh_thread.start()
        
        # Initial fetch
        self._fetch_jwks()
    
    def _fetch_jwks(self) -> bool:
        """
        Fetch JWKS from Identity Service
        
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"[JWTAuth] Fetching JWKS from {self.jwks_url}")
            response = requests.get(self.jwks_url, timeout=10)
            response.raise_for_status()
            
            jwks_data = response.json()
            
            # Validate JWKS structure
            required_fields = ['kid', 'kty', 'alg', 'public_key', 'use']
            for field in required_fields:
                if field not in jwks_data:
                    print(f"[JWTAuth] Invalid JWKS: missing field '{field}'")
                    return False
            
            # Validate values
            if jwks_data['kty'] not in ['RSA']:
                print(f"[JWTAuth] Invalid key type: {jwks_data['kty']}")
                return False
            
            if jwks_data['alg'] not in ['RS256']:
                print(f"[JWTAuth] Invalid algorithm: {jwks_data['alg']}")
                return False
            
            if jwks_data['use'] != 'sig':
                print(f"[JWTAuth] Invalid use: {jwks_data['use']}")
                return False
            
            with self.jwks_lock:
                self.jwks_cache = jwks_data
                self.jwks_last_fetched = time.time()
            
            print(f"[JWTAuth] JWKS fetched successfully, kid={jwks_data['kid']}")
            return True
            
        except Exception as e:
            print(f"[JWTAuth] Error fetching JWKS: {e}")
            return False
    
    def _refresh_jwks_loop(self):
        """Background thread to refresh JWKS periodically"""
        while True:
            try:
                time.sleep(self.ttl_seconds)
                
                # Check if TTL expired
                with self.jwks_lock:
                    if self.jwks_last_fetched:
                        elapsed = time.time() - self.jwks_last_fetched
                        if elapsed >= self.ttl_seconds:
                            print("[JWTAuth] JWKS TTL expired, refreshing...")
                            self._fetch_jwks()
                
            except Exception as e:
                print(f"[JWTAuth] Error in refresh loop: {e}")
    
    def verify_jwt(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Verify JWT according to VERIFY.md specification
        
        Args:
            token: JWT token string
        
        Returns:
            Decoded payload if valid, None otherwise
        """
        try:
            # Step 1: Parse JWT Header (without verification)
            unverified_header = jwt.get_unverified_header(token)
            
            alg = unverified_header.get('alg')
            kid = unverified_header.get('kid')
            
            # Check algorithm
            if alg != 'RS256':
                print(f"[JWTAuth] Invalid algorithm: {alg}")
                return None
            
            # Check kid presence
            if not kid:
                print("[JWTAuth] Missing kid in JWT header")
                return None
            
            # Step 2: Resolve Public Key
            with self.jwks_lock:
                if not self.jwks_cache:
                    print("[JWTAuth] JWKS cache is empty")
                    return None
                
                # Check if kid matches
                if self.jwks_cache['kid'] != kid:
                    print(f"[JWTAuth] Unknown kid: {kid}")
                    return None
                
                public_key_pem = self.jwks_cache['public_key']
            
            # Step 3: Verify Signature
            try:
                # Decode and verify JWT
                payload = jwt.decode(
                    token,
                    public_key_pem,
                    algorithms=['RS256'],
                    options={
                        'verify_signature': True,
                        'verify_exp': True,
                        'verify_iat': True,
                        'require': ['exp', 'iat', 'sub', 'full_name', 'email']
                    }
                )
            except jwt.ExpiredSignatureError:
                print("[JWTAuth] JWT expired")
                return None
            except jwt.InvalidTokenError as e:
                print(f"[JWTAuth] JWT verification failed: {e}")
                return None
            
            # Step 4: Validate Claims
            # Check required claims exist
            required_claims = ['sub', 'full_name', 'email']
            for claim in required_claims:
                if claim not in payload:
                    print(f"[JWTAuth] Missing required claim: {claim}")
                    return None
            
            # Ensure permissions is array (default to empty if not present)
            if 'permissions' not in payload:
                payload['permissions'] = []
            elif not isinstance(payload['permissions'], list):
                print(f"[JWTAuth] Invalid permissions type")
                return None
            
            print(f"[JWTAuth] JWT verified successfully for user {payload['sub']}")
            return payload
            
        except Exception as e:
            print(f"[JWTAuth] Error verifying JWT: {e}")
            return None


# Global authenticator instance
_authenticator: Optional[JWTAuthenticator] = None


def init_jwt_auth(identity_service_url: str, ttl_minutes: int = 10):
    """
    Initialize JWT authentication system
    
    Args:
        identity_service_url: Base URL of S04 Identity Service
        ttl_minutes: TTL for JWKS cache in minutes
    """
    global _authenticator
    _authenticator = JWTAuthenticator(identity_service_url, ttl_minutes)


def get_authenticator() -> Optional[JWTAuthenticator]:
    """Get the global authenticator instance"""
    return _authenticator


def jwt_required(f):
    """
    Decorator to require JWT authentication for Flask routes
    
    Extracts JWT from Authorization header, verifies it, and injects
    the payload into request context as request.jwt_payload.
    
    Returns HTTP 401 if authentication fails.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            return jsonify({
                'status': 'error',
                'message': 'Missing Authorization header'
            }), 401
        
        # Extract Bearer token
        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return jsonify({
                'status': 'error',
                'message': 'Invalid Authorization header format'
            }), 401
        
        token = parts[1]
        
        # Verify JWT
        authenticator = get_authenticator()
        if not authenticator:
            print("[JWTAuth] Authenticator not initialized")
            return jsonify({
                'status': 'error',
                'message': 'Authentication system not available'
            }), 500
        
        payload = authenticator.verify_jwt(token)
        if not payload:
            return jsonify({
                'status': 'error',
                'message': 'Invalid or expired token'
            }), 401
        
        # Inject payload into request context
        request.jwt_payload = payload
        
        return f(*args, **kwargs)
    
    return decorated_function


def jwt_optional(f):
    """
    Decorator to optionally extract JWT authentication for Flask routes
    
    If JWT is present and valid, injects payload into request.jwt_payload.
    If JWT is missing or invalid, continues without authentication.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0].lower() == 'bearer':
                token = parts[1]
                authenticator = get_authenticator()
                if authenticator:
                    payload = authenticator.verify_jwt(token)
                    if payload:
                        request.jwt_payload = payload
        
        # Continue regardless of authentication status
        return f(*args, **kwargs)
    
    return decorated_function


def require_permission(permission: str):
    """
    Decorator to require specific permission
    
    Must be used after @jwt_required decorator.
    
    Args:
        permission: Required permission string
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            payload = getattr(request, 'jwt_payload', None)
            
            if not payload:
                return jsonify({
                    'status': 'error',
                    'message': 'Authentication required'
                }), 401
            
            permissions = payload.get('permissions', [])
            if permission not in permissions:
                return jsonify({
                    'status': 'error',
                    'message': f'Permission denied: {permission} required'
                }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator
