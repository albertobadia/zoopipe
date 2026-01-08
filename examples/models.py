from pydantic import BaseModel, ConfigDict


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


class SalesRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")
    order_id: str
    customer_id: str
    amount: float
    date: str


class TransactionSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    timestamp: str
