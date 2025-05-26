import jwt
import secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify
from config import SECRET_KEY, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS

# Removed: from db import SessionLocal
# Removed: from models import User

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Extract token from Authorization header
        if "Authorization" in request.headers:
            auth_header = request.headers["Authorization"]
            parts = auth_header.split(" ")
            if len(parts) == 2 and parts[0].lower() == "bearer":
                token = parts[1]
        
        if not token:
            return jsonify({
                "error": "Access token is missing",
                "code": "TOKEN_MISSING"
            }), 401
        
        try:
            # Decode and validate token
            data = jwt.decode(
                token, 
                SECRET_KEY, 
                algorithms=["HS256"],
                options={"require": ["exp", "iat", "sub"]}  # Require essential claims
            )
            
            # Validate token type
            if data.get("type") != "access":
                return jsonify({
                    "error": "Invalid token type",
                    "code": "INVALID_TOKEN_TYPE"
                }), 401
            
            # Set user info for the request
            request.user = data.get("sub") # user_id
            request.user_roles = data.get("roles", []) # roles from token
            
        except jwt.ExpiredSignatureError:
            return jsonify({
                "error": "Token has expired",
                "code": "TOKEN_EXPIRED"
            }), 401
        except jwt.InvalidTokenError as e:
            return jsonify({
                "error": "Invalid token",
                "code": "INVALID_TOKEN",
                "details": str(e)
            }), 401
        except Exception as e:
            return jsonify({
                "error": "Token validation failed",
                "code": "TOKEN_VALIDATION_ERROR"
            }), 401
            
        return f(*args, **kwargs)
    return decorated

def generate_tokens(user_id: str, roles: list = None):
    """Generate both access and refresh tokens"""
    if not user_id or not isinstance(user_id, str):
        raise ValueError("Invalid user ID")
    
    # IMPORT HERE to break circular dependency
    from db import SessionLocal
    from models import User

    # If roles are not explicitly provided, fetch them from the database
    if roles is None:
        db = SessionLocal()
        try:
            user = db.query(User).filter_by(username=user_id).first()
            if user:
                roles = user.roles
            else:
                roles = [] # Default to no roles if user not found (shouldn't happen after login)
        finally:
            db.close()
    
    current_time = datetime.utcnow()
    
    # Generate unique token ID for revocation tracking
    jti = secrets.token_urlsafe(32)
    
    # Access token payload
    access_payload = {
        "sub": user_id,  # Subject (user ID)
        "type": "access",
        "roles": roles,
        "iat": current_time,  # Issued at
        "exp": current_time + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "jti": jti  # JWT ID for revocation
    }
    
    # Refresh token payload
    refresh_payload = {
        "sub": user_id,
        "type": "refresh", 
        "iat": current_time,
        "exp": current_time + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        "jti": secrets.token_urlsafe(32)
    }
    
    access_token = jwt.encode(access_payload, SECRET_KEY, algorithm="HS256")
    refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm="HS256")
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,  # seconds
        "expires_at": access_payload["exp"].isoformat()
    }

def refresh_access_token(refresh_token: str):
    """Generate new access token from refresh token"""
    # IMPORT HERE to break circular dependency
    from db import SessionLocal
    from models import User

    db = SessionLocal() # For fetching roles
    try:
        # Decode refresh token
        payload = jwt.decode(
            refresh_token,
            SECRET_KEY,
            algorithms=["HS256"],
            options={"require": ["exp", "iat", "sub"]}
        )
        
        # Validate token type
        if payload.get("type") != "refresh":
            raise jwt.InvalidTokenError("Invalid refresh token type")
        
        user_id = payload.get("sub")
        
        # Fetch current roles from database for the new access token
        user = db.query(User).filter_by(username=user_id).first()
        if not user or not user.is_active:
            raise jwt.InvalidTokenError("User not found or inactive")
        
        return generate_tokens(user_id, roles=user.roles) # Pass roles from DB
        
    except jwt.ExpiredSignatureError:
        raise jwt.ExpiredSignatureError("Refresh token has expired")
    except jwt.InvalidTokenError as e:
        raise jwt.InvalidTokenError(f"Invalid refresh token: {str(e)}")
    finally:
        db.close()

def validate_token_format(token: str) -> bool:
    """Validate JWT token format without decoding"""
    if not token or not isinstance(token, str):
        return False
    
    parts = token.split('.')
    return len(parts) == 3 and all(part for part in parts)

def decode_token_safely(token: str) -> dict:
    """Decode token without verification for inspection"""
    try:
        return jwt.decode(token, options={"verify_signature": False})
    except Exception:
        return {}
