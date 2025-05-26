from sqlalchemy import Column, Integer, String, Boolean, JSON
from sqlalchemy.ext.mutable import MutableDict
from db import Base

class Policy(Base):
    __tablename__ = "policies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False, index=True)
    condition = Column(String, nullable=False) # Store condition as a string
    action = Column(MutableDict.as_mutable(JSON), nullable=False) # Store action as JSON
    enabled = Column(Boolean, default=True)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "condition": self.condition,
            "action": self.action,
            "enabled": self.enabled
        }
