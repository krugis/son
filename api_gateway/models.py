from sqlalchemy import Column, Integer, String, DateTime, Text
from db import Base
import datetime

class Route(Base):
    __tablename__ = "routes"
    id = Column(Integer, primary_key=True)
    path = Column(String, unique=True, nullable=False)
    method = Column(String, default="GET")
    service_url = Column(String, nullable=False)

class RequestLog(Base):
    __tablename__ = "request_logs"
    id = Column(Integer, primary_key=True)
    path = Column(String)
    method = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    user = Column(String)
    status = Column(Integer)
