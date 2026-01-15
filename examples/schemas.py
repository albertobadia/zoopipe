from pydantic import BaseModel, ConfigDict


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str
