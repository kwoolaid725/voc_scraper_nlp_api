from fastapi import FastAPI, Response, status, HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from typing import Optional, List
from fastapi.templating import Jinja2Templates


router = APIRouter(
    prefix="/items",
    tags=["items"],
)

@router.get("/{item_name}")
async def get_item():
    return {"message": "Hello World"}

@router.post("/{item_name}")
async def get_item():
    return {"message": "Hello World"}

@router.put("/{item_name}")
async def get_item():
    return {"message": "Hello World"}

@router.delete("/{item_name}")
async def get_item():
    return {"message": "Hello World"}

