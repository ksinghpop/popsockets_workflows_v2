from fastapi import APIRouter, Depends, Response, HTTPException
from pydantic import BaseModel
from app.db import db
from bson import ObjectId
from typing import List
from app.utils.auth import (
    create_user,
    login_user,
    logout_user,
    get_current_user,
    hash_password
)

router = APIRouter()

class LoginInput(BaseModel):
    username: str
    password: str

class RegisterInput(BaseModel):
    username: str
    password: str
    role: str

class UserOut(BaseModel):
    id: str
    username: str
    role: str

class UpdateUserInput(BaseModel):
    username: str
    role: str

class UpdatePasswordInput(BaseModel):
    user_id: str
    new_password: str


@router.post("/login")
async def login(data: LoginInput, response: Response):
    return await login_user(response, data.username, data.password)

@router.get("/me")
async def get_me(current_user=Depends(get_current_user)):
    """
    Return the current authenticated user's info.
    """
    return {"user": current_user}

@router.post("/logout")
async def logout(response: Response):
    return logout_user(response)

@router.post("/register")
async def register(data: RegisterInput, current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only admins can create users")
    await create_user(data.username, data.password, data.role)
    return {"message": "User created"}

# @router.post("/register")
# async def register(data: RegisterInput):
#     await create_user(data.username, data.password, data.role)
#     return {"message": "User created"}

# List all users (admin only)
@router.get("/users", response_model=List[UserOut])
async def list_users(current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only admins can list users")
    users = db.users.find()
    return [
        {
            "id": str(u["_id"]),
            "username": u["username"],
            "role": u["role"]
        }
        for u in users
    ]

# Get single user detail (admin only)
@router.get("/users/{user_id}", response_model=UserOut)
async def get_user(user_id: str, current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only admins can get user detail")
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "id": str(user["_id"]),
        "username": user["username"],
        "role": user["role"]
    }

# Update a user's username or role (admin only)
@router.put("/users/{user_id}")
async def update_user(user_id: str, data: UpdateUserInput, current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only admins can update users")
    res = db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"username": data.username, "role": data.role}}
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "User updated"}

# Update a user's password (admin only)
@router.post("/users/update_password")
async def update_password(data: UpdatePasswordInput, current_user=Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Only admins can reset passwords")
    hashed_pw = hash_password(data.new_password)
    res = db.users.update_one(
        {"_id": ObjectId(data.user_id)},
        {"$set": {"hashed_password": hashed_pw}}
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "Password updated"}