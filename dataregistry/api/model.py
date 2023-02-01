from pydantic import BaseModel


class RecordRequest(BaseModel):
    name: str
    metadata: dict
