import json
import os
import threading
import uuid
from datetime import datetime
from typing import Any, Optional
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_cors import CORS
from dotenv import load_dotenv
from bson import ObjectId

from .MessageQueueService import MessageQueueService
from ..collections import conversations_collection, messages_collection, reviews_collection
from ..utils.jwt_auth import init_jwt_auth, jwt_required, jwt_optional

# Load environment variables
load_dotenv()


class ConsultationService:
    """
    S01 - Consultation Service
    
    Main service handling customer consultations, managing conversations,
    coordinating with AI agents, human agents, and other microservices.
    """
    
    def __init__(self) -> None:
        # Flask app setup
        self.app = Flask(__name__)
        CORS(self.app)
        self.app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
        
        # SocketIO setup
        self.socketio = SocketIO(
            self.app,
            cors_allowed_origins="*",
            async_mode='threading'
        )
        
        # RabbitMQ configuration
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        self.mq_service = MessageQueueService(self.rabbitmq_url)
        self.mq_lock = threading.Lock()
        
        # Queue configurations
        # A02 - S02 AI Agent
        self.a02_request_queue = os.getenv("A02_REQUEST_QUEUE", "s01_s02_requests")
        self.a02_response_queue = os.getenv("A02_RESPONSE_QUEUE", "s01_s02_responses")
        
        # A36 - S19 Conversation Analysis
        self.a36a_queue = os.getenv("A36A_QUEUE_NAME", "s01_s19_events")
        self.a36b_queue = os.getenv("A36B_QUEUE_NAME", "s19_s01_events")
        
        # A35 - S18 Partner Selection
        self.a35a_queue = os.getenv("A35A_QUEUE_NAME", "s01_s18_events")
        self.a35b_queue = os.getenv("A35B_QUEUE_NAME", "s18_s01_events")
        
        # A10 - S13 Partner Consultation  
        self.a10a_queue = os.getenv("A10A_QUEUE_NAME", "s01_s13_events")
        self.a10b_queue = os.getenv("A10B_QUEUE_NAME", "s13_s01_events")
        
        # A38 - S20 Speech Service
        self.a38_request_queue = os.getenv("A38_REQUEST_QUEUE", "s01_s20_requests")
        self.a38_response_queue = os.getenv("A38_RESPONSE_QUEUE", "s01_s20_responses")
        
        # Threading
        self.num_threads = int(os.getenv("S01_NUM_THREADS", "4"))
        self.threads: list[threading.Thread] = []
        
        # Pending requests for request-response patterns
        self.pending_requests: dict[str, dict] = {}
        self.pending_requests_lock = threading.Lock()
        
        # Active Socket.IO sessions
        self.active_sessions: dict[str, dict] = {}  # {session_id: {conversation_id, customer_id, ...}}
        self.sessions_lock = threading.Lock()
        
        # Initialize JWT authentication
        identity_service_url = os.getenv('IDENTITY_SERVICE_URL', 'http://localhost:5004')
        jwks_ttl_minutes = int(os.getenv('JWKS_TTL_IN_MINUTES', '10'))
        init_jwt_auth(identity_service_url, jwks_ttl_minutes)
        print(f"[S01] JWT authentication initialized with Identity Service: {identity_service_url}")
        
        # Register HTTP routes
        self._register_http_routes()
        
        # Register SocketIO handlers
        self._register_socketio_handlers()
    
    def start(self):
        """Start the service"""
        print("[S01] Starting Consultation Service...")
        
        # Start RabbitMQ consumer threads
        self.threads.append(
            threading.Thread(target=self._consume_a02_responses, daemon=True)
        )
        self.threads.append(
            threading.Thread(target=self._consume_a36b_events, daemon=True)
        )
        self.threads.append(
            threading.Thread(target=self._consume_a35b_events, daemon=True)
        )
        self.threads.append(
            threading.Thread(target=self._consume_a10b_events, daemon=True)
        )
        self.threads.append(
            threading.Thread(target=self._consume_a38_responses, daemon=True)
        )
        
        for t in self.threads:
            t.start()
        
        print(f"[S01] Service started with {len(self.threads)} RabbitMQ consumer threads")
        
        # Run Flask-SocketIO server
        host = os.getenv('FLASK_HOST', '0.0.0.0')
        port = int(os.getenv('FLASK_PORT', '5000'))
        print(f"[S01] Starting Flask-SocketIO server on {host}:{port}")
        self.socketio.run(self.app, host=host, port=port, allow_unsafe_werkzeug=True)
    
    # ===== HTTP Routes =====
    
    def _register_http_routes(self):
        """Register HTTP API routes"""
        
        @self.app.route('/api/v1/conversations', methods=['POST'])
        @jwt_required
        def create_conversation():
            """Create a new conversation"""
            try:
                # Extract customer_id from JWT token
                jwt_payload = request.jwt_payload
                customer_id = jwt_payload['sub']
                
                data = request.json or {}
                title = data.get('title', 'New Conversation')
                
                conversation = {
                    'title': title,
                    'customer_id': customer_id,
                    'status': 'AI_AGENT_TEXTING',
                    'partner_id': None,
                    'customer_satisfaction': 5,
                    'summary': '',
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
                
                result = conversations_collection.insert_one(conversation)
                conversation['id'] = str(result.inserted_id)
                del conversation['_id']
                
                return jsonify({
                    'status': 'success',
                    'data': self._serialize_conversation(conversation)
                }), 201
            except Exception as e:
                print(f"[S01] Error creating conversation: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/v1/conversations/<conversation_id>', methods=['GET'])
        @jwt_required
        def get_conversation(conversation_id):
            """Get conversation details"""
            try:
                # Extract customer_id from JWT
                jwt_payload = request.jwt_payload
                customer_id = jwt_payload['sub']
                
                conversation = conversations_collection.find_one({
                    '_id': ObjectId(conversation_id),
                    'customer_id': customer_id  # Ensure user can only access their own conversations
                })
                if not conversation:
                    return jsonify({'status': 'error', 'message': 'Conversation not found'}), 404
                
                return jsonify({
                    'status': 'success',
                    'data': self._serialize_conversation(conversation)
                })
            except Exception as e:
                print(f"[S01] Error getting conversation: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/v1/conversations', methods=['GET'])
        @jwt_required
        def list_conversations():
            """List all conversations for a customer"""
            try:
                # Extract customer_id from JWT
                jwt_payload = request.jwt_payload
                customer_id = jwt_payload['sub']
                
                conversations = list(conversations_collection.find(
                    {'customer_id': customer_id}
                ).sort('updated_at', -1))
                
                return jsonify({
                    'status': 'success',
                    'data': [self._serialize_conversation(c) for c in conversations]
                })
            except Exception as e:
                print(f"[S01] Error listing conversations: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/v1/conversations/<conversation_id>/messages', methods=['GET'])
        @jwt_required
        def get_messages(conversation_id):
            """Get all messages in a conversation"""
            try:
                # Extract customer_id from JWT and verify ownership
                jwt_payload = request.jwt_payload
                customer_id = jwt_payload['sub']
                
                # Check conversation ownership
                conversation = conversations_collection.find_one({
                    '_id': ObjectId(conversation_id),
                    'customer_id': customer_id
                })
                if not conversation:
                    return jsonify({'status': 'error', 'message': 'Conversation not found'}), 404
                
                messages = list(messages_collection.find(
                    {'conversation_id': conversation_id}
                ).sort('created_at', 1))
                
                return jsonify({
                    'status': 'success',
                    'data': [self._serialize_message(m) for m in messages]
                })
            except Exception as e:
                print(f"[S01] Error getting messages: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/v1/conversations/<conversation_id>/reviews', methods=['POST'])
        @jwt_required
        def create_review(conversation_id):
            """Create a review for a conversation"""
            try:
                # Extract customer_id from JWT and verify ownership
                jwt_payload = request.jwt_payload
                customer_id = jwt_payload['sub']
                
                # Check conversation ownership
                conversation = conversations_collection.find_one({
                    '_id': ObjectId(conversation_id),
                    'customer_id': customer_id
                })
                if not conversation:
                    return jsonify({'status': 'error', 'message': 'Conversation not found'}), 404
                
                data = request.json or {}
                rating = data.get('rating')
                comment = data.get('comment', '')
                
                if not rating or rating < 1 or rating > 5:
                    return jsonify({'status': 'error', 'message': 'Invalid rating'}), 400
                
                # Check if review already exists
                existing = reviews_collection.find_one({'conversation_id': conversation_id})
                if existing:
                    return jsonify({'status': 'error', 'message': 'Review already exists'}), 400
                
                review = {
                    'conversation_id': conversation_id,
                    'rating': rating,
                    'comment': comment
                }
                
                reviews_collection.insert_one(review)
                
                # Update customer_satisfaction in conversation
                conversations_collection.update_one(
                    {'_id': ObjectId(conversation_id)},
                    {'$set': {'customer_satisfaction': rating, 'updated_at': datetime.utcnow()}}
                )
                
                # Notify S19 via A36a event rating_changed
                self._send_rating_changed_event(conversation_id, None, rating)
                
                return jsonify({'status': 'success'}), 201
            except Exception as e:
                print(f"[S01] Error creating review: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
    
    # ===== SocketIO Handlers =====
    
    def _register_socketio_handlers(self):
        """Register SocketIO event handlers"""
        
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection with JWT authentication"""
            # Extract JWT from query parameters or headers
            token = None
            
            # Try to get from query parameters
            if 'token' in request.args:
                token = request.args.get('token')
            # Try to get from Authorization header
            elif 'Authorization' in request.headers:
                auth_header = request.headers.get('Authorization')
                parts = auth_header.split()
                if len(parts) == 2 and parts[0].lower() == 'bearer':
                    token = parts[1]
            
            if not token:
                print(f"[S01] Client connection rejected: No token provided")
                return False  # Reject connection
            
            # Verify JWT
            from ..utils.jwt_auth import get_authenticator
            authenticator = get_authenticator()
            if not authenticator:
                print(f"[S01] Client connection rejected: Auth system not available")
                return False
            
            payload = authenticator.verify_jwt(token)
            if not payload:
                print(f"[S01] Client connection rejected: Invalid token")
                return False
            
            # Store JWT payload in session
            with self.sessions_lock:
                self.active_sessions[request.sid] = {
                    'jwt_payload': payload,
                    'customer_id': payload['sub'],
                    'full_name': payload['full_name'],
                    'email': payload['email']
                }
            
            print(f"[S01] Client connected: {request.sid} (user: {payload['sub']})")
            emit('connected', {
                'message': 'Connected to Consultation Service',
                'user': {
                    'id': payload['sub'],
                    'full_name': payload['full_name'],
                    'email': payload['email']
                }
            })
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            print(f"[S01] Client disconnected: {request.sid}")
            # Clean up active session
            with self.sessions_lock:
                if request.sid in self.active_sessions:
                    del self.active_sessions[request.sid]
        
        @self.socketio.on('join_conversation')
        def handle_join_conversation(data):
            """Join a conversation room"""
            # Get session info
            with self.sessions_lock:
                session = self.active_sessions.get(request.sid)
            
            if not session:
                emit('error', {'message': 'Not authenticated'})
                return
            
            customer_id = session['customer_id']
            conversation_id = data.get('conversation_id')
            
            if not conversation_id:
                emit('error', {'message': 'conversation_id required'})
                return
            
            # Verify conversation ownership
            conversation = conversations_collection.find_one({
                '_id': ObjectId(conversation_id),
                'customer_id': customer_id
            })
            if not conversation:
                emit('error', {'message': 'Conversation not found or access denied'})
                return
            
            join_room(conversation_id)
            
            # Update session with conversation_id
            with self.sessions_lock:
                self.active_sessions[request.sid]['conversation_id'] = conversation_id
            
            print(f"[S01] Client {request.sid} (user: {customer_id}) joined conversation {conversation_id}")
            emit('joined', {'conversation_id': conversation_id})
        
        @self.socketio.on('send_message')
        def handle_send_message(data):
            """Handle incoming message from customer"""
            # Get session info
            with self.sessions_lock:
                session = self.active_sessions.get(request.sid)
            
            if not session:
                emit('error', {'message': 'Not authenticated'})
                return
            
            customer_id = session['customer_id']
            customer_name = session['full_name']
            
            conversation_id = data.get('conversation_id')
            content = data.get('content')
            
            if not conversation_id or not content:
                emit('error', {'message': 'conversation_id and content required'})
                return
            
            # Get conversation and verify ownership
            conversation = conversations_collection.find_one({
                '_id': ObjectId(conversation_id),
                'customer_id': customer_id
            })
            if not conversation:
                emit('error', {'message': 'Conversation not found or access denied'})
                return
            
            status = conversation.get('status')
            
            # Save customer message
            message = {
                'conversation_id': conversation_id,
                'sender_type': 'CUSTOMER',
                'sender_id': customer_id,
                'sender_name': customer_name,
                'content': content,
                'emotion': 'Neutral',
                'created_at': datetime.utcnow()
            }
            result = messages_collection.insert_one(message)
            message['id'] = str(result.inserted_id)
            
            # Route based on conversation status
            if status == 'AI_AGENT_TEXTING':
                self._handle_ai_agent_text_message(conversation, message)
            elif status == 'FORWARDING':
                self._handle_forwarding_message(conversation, message)
            elif status == 'HUMAN_AGENT_TEXTING':
                self._handle_human_agent_text_message(conversation, message)
            else:
                emit('error', {'message': f'Invalid conversation status: {status}'})
    
    # ===== Flow Handlers =====
    
    def _handle_ai_agent_text_message(self, conversation, customer_message):
        """Flow 1: AI Agent Handling (Text)"""
        conversation_id = str(conversation['_id'])
        
        # Send to S19 for analysis
        self._send_new_message_to_s19(customer_message, conversation)
        
        # Send to S02 for AI response
        def ai_response_thread():
            try:
                request_id = str(uuid.uuid4())
                
                # Prepare A02 request
                a02_request = {
                    'id': request_id,
                    'method': 'generate_response',
                    'params': {
                        'conversation_id': conversation_id,
                        'message': customer_message['content'],
                        'conversation_history': self._get_conversation_history(conversation_id)
                    }
                }
                
                # Setup response handler
                response_event = threading.Event()
                response_data = {'result': None}
                
                with self.pending_requests_lock:
                    self.pending_requests[request_id] = {
                        'event': response_event,
                        'data': response_data,
                        'conversation_id': conversation_id
                    }
                
                # Send request
                with self.mq_lock:
                    mq = self.mq_service.clone()
                mq.declare_queue(self.a02_request_queue)
                mq.publish_message(self.a02_request_queue, a02_request)
                
                # Wait for response
                if not response_event.wait(timeout=60):
                    print(f"[S01] Timeout waiting for A02 response")
                    self.socketio.emit('error', {'message': 'AI Agent timeout'}, room=conversation_id)
                    return
                
                result = response_data['result']
                if not result:
                    return
                
                if result.get('status') == 'error':
                    content = result.get('content', '')
                    if content == 'FORWARD':
                        # AI can't answer, switch to FORWARDING
                        self._switch_to_forwarding_status(conversation_id)
                    else:
                        self.socketio.emit('error', {'message': content}, room=conversation_id)
                else:
                    # Stream AI response
                    ai_response_text = result.get('content', {}).get('response', '')
                    self._stream_ai_response(conversation_id, ai_response_text)
                    
                    # Save AI message
                    self._save_ai_message(conversation_id, ai_response_text)
            
            except Exception as e:
                print(f"[S01] Error in AI response thread: {e}")
                self.socketio.emit('error', {'message': str(e)}, room=conversation_id)
        
        # Run in separate thread
        threading.Thread(target=ai_response_thread, daemon=True).start()
    
    def _handle_forwarding_message(self, conversation, customer_message):
        """Flow 3: Forwarding to Partner"""
        conversation_id = str(conversation['_id'])
        
        # Send notification message
        notification_msg = {
            'conversation_id': conversation_id,
            'sender_type': 'AI_AGENT',
            'sender_id': None,
            'sender_name': None,
            'content': 'Chúng tôi xin lỗi vì đã đem đến trải nghiệm không tốt cho Quý khách. Quý khách hãy chờ trong giây lát để được chuyển tiếp tới tư vấn viên phù hợp.',
            'emotion': 'Neutral',
            'created_at': datetime.utcnow()
        }
        messages_collection.insert_one(notification_msg)
        self.socketio.emit('new_message', self._serialize_message(notification_msg), room=conversation_id)
        
        # Request partner selection from S18
        def partner_selection_thread():
            try:
                # Send A35a event need_forwarding
                event_msg = {
                    'event': 'need_forwarding',
                    'content': {
                        'conversation': {
                            'id': conversation_id,
                            'customer_id': conversation.get('customer_id'),
                            'status': conversation.get('status'),
                            'partner_id': conversation.get('partner_id'),
                            'customer_satisfaction': conversation.get('customer_satisfaction'),
                            'summary': conversation.get('summary', '')
                        }
                    }
                }
                
                with self.mq_lock:
                    mq = self.mq_service.clone()
                mq.declare_queue(self.a35a_queue)
                mq.publish_message(self.a35a_queue, event_msg)
                
                print(f"[S01] Sent need_forwarding event to S18 for conversation {conversation_id}")
            
            except Exception as e:
                print(f"[S01] Error in partner selection: {e}")
        
        threading.Thread(target=partner_selection_thread, daemon=True).start()
    
    def _handle_human_agent_text_message(self, conversation, customer_message):
        """Flow 4: Human Agent Handling (Text)"""
        conversation_id = str(conversation['_id'])
        partner_id = conversation.get('partner_id')
        
        # Forward to S13 via A10a
        event_msg = {
            'event': 'new_message',
            'content': {
                'conversation_id': conversation_id,
                'partner_id': partner_id,
                'message': {
                    'id': customer_message.get('id'),
                    'sender_type': customer_message.get('sender_type'),
                    'content': customer_message.get('content'),
                    'created_at': customer_message.get('created_at').isoformat()
                }
            }
        }
        
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a10a_queue)
        mq.publish_message(self.a10a_queue, event_msg)
        
        print(f"[S01] Forwarded message to S13 for conversation {conversation_id}")
    
    # ===== RabbitMQ Consumers =====
    
    def _consume_a02_responses(self):
        """Consumer for A02 responses from S02"""
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a02_response_queue)
        mq.register_callback(self.a02_response_queue, self._handle_a02_response)
        mq.start_consuming()
    
    def _handle_a02_response(self, message: dict):
        """Handle A02 response from S02 AI Agent"""
        request_id = message.get('id')
        result = message.get('result')
        
        with self.pending_requests_lock:
            pending = self.pending_requests.get(request_id)
            if not pending:
                return
            
            pending['data']['result'] = result
            pending['event'].set()
            del self.pending_requests[request_id]
    
    def _consume_a36b_events(self):
        """Consumer for A36b events from S19"""
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a36b_queue)
        mq.register_callback(self.a36b_queue, self._handle_a36b_event)
        mq.start_consuming()
    
    def _handle_a36b_event(self, message: dict):
        """Handle A36b events from S19 Conversation Analysis"""
        event = message.get('event')
        content = message.get('content', {})
        
        if event == 'update_message_emotion':
            message_id = content.get('message_id')
            emotion = content.get('emotion')
            messages_collection.update_one(
                {'_id': ObjectId(message_id)},
                {'$set': {'emotion': emotion}}
            )
            print(f"[S01] Updated message {message_id} emotion to {emotion}")
        
        elif event == 'update_conversation_summary':
            conversation_id = content.get('conversation_id')
            summary = content.get('summary')
            conversations_collection.update_one(
                {'_id': ObjectId(conversation_id)},
                {'$set': {'summary': summary, 'updated_at': datetime.utcnow()}}
            )
            print(f"[S01] Updated conversation {conversation_id} summary")
        
        elif event == 'update_customer_satisfaction':
            conversation_id = content.get('conversation_id')
            satisfaction = content.get('customer_satisfaction')
            conversations_collection.update_one(
                {'_id': ObjectId(conversation_id)},
                {'$set': {'customer_satisfaction': satisfaction, 'updated_at': datetime.utcnow()}}
            )
            print(f"[S01] Updated conversation {conversation_id} satisfaction to {satisfaction}")
            
            # Check if satisfaction is 1, switch to FORWARDING
            if satisfaction == 1:
                self._switch_to_forwarding_status(conversation_id)
    
    def _consume_a35b_events(self):
        """Consumer for A35b events from S18"""
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a35b_queue)
        mq.register_callback(self.a35b_queue, self._handle_a35b_event)
        mq.start_consuming()
    
    def _handle_a35b_event(self, message: dict):
        """Handle A35b events from S18 Partner Selection"""
        event = message.get('event')
        content = message.get('content', {})
        
        if event == 'forwarded_partner_selection_finished':
            conversation_id = content.get('conversation_id')
            status = content.get('status')
            selection = content.get('selection')
            
            if status == 'success' and selection:
                partner_id = selection.get('partner_id')
                partner_name = selection.get('partner_name')
                
                # Update conversation with partner_id
                conversations_collection.update_one(
                    {'_id': ObjectId(conversation_id)},
                    {'$set': {'partner_id': partner_id, 'updated_at': datetime.utcnow()}}
                )
                
                # Send consultation_request to S13 via A10a
                self._send_consultation_request_to_partner(conversation_id, partner_id)
            else:
                # Selection failed, revert to AI_AGENT_TEXTING
                print(f"[S01] Partner selection failed for {conversation_id}")
                self._revert_to_ai_agent(conversation_id)
    
    def _consume_a10b_events(self):
        """Consumer for A10b events from S13"""
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a10b_queue)
        mq.register_callback(self.a10b_queue, self._handle_a10b_event)
        mq.start_consuming()
    
    def _handle_a10b_event(self, message: dict):
        """Handle A10b events from S13 Partner Consultation"""
        event = message.get('event')
        content = message.get('content', {})
        
        if event == 'consultation_response':
            conversation_id = content.get('conversation_id')
            response_status = content.get('status')
            
            if response_status == 'accepted':
                # Update status to HUMAN_AGENT_TEXTING
                conversations_collection.update_one(
                    {'_id': ObjectId(conversation_id)},
                    {'$set': {'status': 'HUMAN_AGENT_TEXTING', 'updated_at': datetime.utcnow()}}
                )
                self.socketio.emit('status_switch', {'status': 'HUMAN_AGENT_TEXTING'}, room=conversation_id)
                print(f"[S01] Conversation {conversation_id} switched to HUMAN_AGENT_TEXTING")
            else:
                # Rejected, revert to AI_AGENT_TEXTING
                self._revert_to_ai_agent(conversation_id)
        
        elif event == 'new_message':
            # Human agent sent a message
            conversation_id = content.get('conversation_id')
            msg_content = content.get('message', {})
            
            # Save message
            message = {
                'conversation_id': conversation_id,
                'sender_type': 'HUMAN_AGENT',
                'sender_id': msg_content.get('sender_id'),
                'sender_name': msg_content.get('sender_name'),
                'content': msg_content.get('content'),
                'emotion': 'Neutral',
                'created_at': datetime.utcnow()
            }
            messages_collection.insert_one(message)
            
            # Send to frontend
            self.socketio.emit('new_message', self._serialize_message(message), room=conversation_id)
    
    def _consume_a38_responses(self):
        """Consumer for A38 responses from S20"""
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a38_response_queue)
        mq.register_callback(self.a38_response_queue, self._handle_a38_response)
        mq.start_consuming()
    
    def _handle_a38_response(self, message: dict):
        """Handle A38 response from S20 Speech Service"""
        # TODO: Implement speech service integration
        pass
    
    # ===== Helper Methods =====
    
    def _switch_to_forwarding_status(self, conversation_id: str):
        """Switch conversation status to FORWARDING"""
        conversations_collection.update_one(
            {'_id': ObjectId(conversation_id)},
            {'$set': {'status': 'FORWARDING', 'updated_at': datetime.utcnow()}}
        )
        self.socketio.emit('status_switch', {'status': 'FORWARDING'}, room=conversation_id)
        print(f"[S01] Conversation {conversation_id} switched to FORWARDING")
    
    def _revert_to_ai_agent(self, conversation_id: str):
        """Revert conversation status to AI_AGENT_TEXTING"""
        conversations_collection.update_one(
            {'_id': ObjectId(conversation_id)},
            {'$set': {'status': 'AI_AGENT_TEXTING', 'updated_at': datetime.utcnow()}}
        )
        
        # Send notification
        msg = {
            'conversation_id': conversation_id,
            'sender_type': 'AI_AGENT',
            'content': 'Không thể kết nối với tư vấn viên. AI Agent sẽ tiếp tục hỗ trợ bạn.',
            'emotion': 'Neutral',
            'created_at': datetime.utcnow()
        }
        messages_collection.insert_one(msg)
        self.socketio.emit('new_message', self._serialize_message(msg), room=conversation_id)
        self.socketio.emit('status_switch', {'status': 'AI_AGENT_TEXTING'}, room=conversation_id)
    
    def _stream_ai_response(self, conversation_id: str, response_text: str):
        """Stream AI response in chunks"""
        self.socketio.emit('text_start', {}, room=conversation_id)
        
        # Stream in chunks of 50 characters
        chunk_size = 50
        for i in range(0, len(response_text), chunk_size):
            chunk = response_text[i:i+chunk_size]
            self.socketio.emit('text_chunk', {'chunk': chunk}, room=conversation_id)
        
        self.socketio.emit('text_stop', {}, room=conversation_id)
    
    def _save_ai_message(self, conversation_id: str, content: str):
        """Save AI agent message"""
        message = {
            'conversation_id': conversation_id,
            'sender_type': 'AI_AGENT',
            'sender_id': None,
            'sender_name': None,
            'content': content,
            'emotion': 'Neutral',
            'created_at': datetime.utcnow()
        }
        messages_collection.insert_one(message)
    
    def _send_new_message_to_s19(self, message: dict, conversation: dict):
        """Send new_message event to S19 via A36a"""
        event_msg = {
            'event': 'new_message',
            'content': {
                'message': {
                    'id': message.get('id'),
                    'sender_type': message.get('sender_type'),
                    'sender_id': message.get('sender_id'),
                    'content': message.get('content')
                },
                'conversation': {
                    'id': str(conversation['_id']),
                    'customer_satisfaction': conversation.get('customer_satisfaction'),
                    'summary': conversation.get('summary', '')
                }
            }
        }
        
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a36a_queue)
        mq.publish_message(self.a36a_queue, event_msg)
    
    def _send_rating_changed_event(self, conversation_id: str, old_rating: Optional[int], new_rating: int):
        """Send rating_changed event to S19 via A36a"""
        event_msg = {
            'event': 'rating_changed',
            'content': {
                'conversation_id': conversation_id,
                'rating': {
                    'old': old_rating,
                    'new': new_rating
                }
            }
        }
        
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a36a_queue)
        mq.publish_message(self.a36a_queue, event_msg)
    
    def _send_consultation_request_to_partner(self, conversation_id: str, partner_id: str):
        """Send consultation_request to S13 via A10a"""
        conversation = conversations_collection.find_one({'_id': ObjectId(conversation_id)})
        messages = list(messages_collection.find({'conversation_id': conversation_id}).sort('created_at', 1))
        
        event_msg = {
            'event': 'consultation_request',
            'content': {
                'conversation_id': conversation_id,
                'partner_id': partner_id,
                'customer_id': conversation.get('customer_id'),
                'summary': conversation.get('summary', ''),
                'messages': [self._serialize_message(m) for m in messages]
            }
        }
        
        with self.mq_lock:
            mq = self.mq_service.clone()
        mq.declare_queue(self.a10a_queue)
        mq.publish_message(self.a10a_queue, event_msg)
    
    def _get_conversation_history(self, conversation_id: str) -> list:
        """Get conversation history for AI context"""
        messages = list(messages_collection.find(
            {'conversation_id': conversation_id}
        ).sort('created_at', 1).limit(20))
        
        return [
            {
                'role': 'user' if m.get('sender_type') == 'CUSTOMER' else 'assistant',
                'content': m.get('content')
            }
            for m in messages
        ]
    
    def _serialize_conversation(self, conv: dict) -> dict:
        """Serialize conversation for JSON response"""
        if '_id' in conv:
            conv['id'] = str(conv['_id'])
            del conv['_id']
        if 'created_at' in conv and isinstance(conv['created_at'], datetime):
            conv['created_at'] = conv['created_at'].isoformat()
        if 'updated_at' in conv and isinstance(conv['updated_at'], datetime):
            conv['updated_at'] = conv['updated_at'].isoformat()
        return conv
    
    def _serialize_message(self, msg: dict) -> dict:
        """Serialize message for JSON response"""
        if '_id' in msg:
            msg['id'] = str(msg['_id'])
            del msg['_id']
        if 'created_at' in msg and isinstance(msg['created_at'], datetime):
            msg['created_at'] = msg['created_at'].isoformat()
        return msg
