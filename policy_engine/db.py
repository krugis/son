from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import POLICY_DB_URL

# Create the SQLAlchemy engine
engine = create_engine(POLICY_DB_URL)

# Create a SessionLocal class to get database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for declarative models
Base = declarative_base()
