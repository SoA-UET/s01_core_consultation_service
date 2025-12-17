# NOTE: S01 uses Flask-SocketIO, not Flask-RESTX
# HTTP API is defined in ConsultationService.py

from flask import Blueprint

v1 = Blueprint("v1", __name__, url_prefix="/api/v1")

# No API namespaces needed - S01 handles routes directly
