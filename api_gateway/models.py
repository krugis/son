from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON
from sqlalchemy.ext.mutable import MutableList # For mutable JSON lists
from db import Base
import datetime

class Route(Base):
    __tablename__ = "routes"
    id = Column(Integer, primary_key=True)
    path = Column(String, nullable=False)
    method = Column(String, default="GET", nullable=False)
    service_url = Column(String, nullable=False)

    # Adding a unique constraint at the database level for path and method
    __table_args__ = (
        {"unique_together": ("path", "method")},
    )

class RequestLog(Base):
    __tablename__ = "request_logs"
    id = Column(Integer, primary_key=True)
    path = Column(String)
    method = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    user = Column(String)
    status = Column(Integer)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    # Store roles as a JSON array
    roles = Column(MutableList.as_mutable(JSON), default=[])
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f"<User(username='{self.username}', roles='{self.roles}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "roles": self.roles,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat()
        }
