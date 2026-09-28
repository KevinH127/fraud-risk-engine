from datetime import datetime
from pydantic import BaseModel
from typing import Optional

class Transaction(BaseModel):
    transaction_id: str
    card_id: str
    amount: float
    merchant_id: str
    merchant_category_code: int
    latitude: float
    longitude: float
    timestamp: datetime
    is_fraud: bool = False
    fraud_type: Optional[str] = None

    def to_dict(self) -> dict:
        data = self.model_dump()
        data["timestamp"] = self.timestamp.isoformat()
        return data

    