from pydantic import BaseModel
from sqlalchemy import Column, Integer, JSON
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class RecordRequest(BaseModel):
    name: str
    metadata: dict

class RecordJson(Base):
    __tablename__ = "records"
    id = Column(Integer, primary_key=True)
    record_metadata = Column(JSON)

