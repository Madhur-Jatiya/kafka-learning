from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4


class Order(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    order_id: int
    customer_id: int
    amount: float = Field(gt=0)
    status: Literal["PLACED", "CONFIRMED", "CANCELLED"] = "PLACED"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_kafka_message(self) -> str:
        return self.model_dump_json()
