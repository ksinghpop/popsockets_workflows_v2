from passlib.context import CryptContext
from jose import jwt, JWTError
import datetime as dt
from fastapi import Request, HTTPException, Response
from dotenv import load_dotenv
from app.db import db  # Your MongoDB client
import os

load_dotenv()

# Environment
SECRET_KEY = os.environ.get("SECRET_KEY", "supersecret")
ALGORITHM = os.environ.get("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", "480"))

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Hash a password
def hash_password(password: str):
    return pwd_context.hash(password)

# Verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Create JWT token
def create_access_token(data: dict, expires_delta: dt.timedelta = None):
    to_encode = data.copy()
    expire = dt.datetime.now(dt.timezone.utc) + (expires_delta or dt.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Decode JWT token
def decode_access_token(token: str):
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

# Authenticate a user by username + password
async def authenticate_user(username: str, password: str):
    user = db.users.find_one({"username": username})
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user

# Create a user with role
async def create_user(username: str, password: str, role: str):
    hashed_pw = hash_password(password)
    db.users.insert_one({
        "username": username,
        "hashed_password": hashed_pw,
        "role": role
    })

# Get current user from session cookie
async def get_current_user(request: Request):
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = decode_access_token(token)
        username: str = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return {
        "username": user["username"],
        "role": user["role"]
    }

# Login: validate credentials, set session cookie
async def login_user(response: Response, username: str, password: str):
    user = await authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token({"sub": user["username"]})
    response.set_cookie(
        key="session",
        value=token,
        httponly=True,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        samesite="lax",
        secure=False  # Set True if serving HTTPS
    )
    return {"message": "Logged in successfully"}

# Logout: clear session cookie
def logout_user(response: Response):
    response.delete_cookie("session")
    return {"message": "Logged out"}
