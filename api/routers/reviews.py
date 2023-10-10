from fastapi import FastAPI, Response, status, HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from typing import Optional, List

from .. import models, schemas
from ..database import  get_db
from fastapi.templating import Jinja2Templates



router = APIRouter(
    prefix="/reviews",
    tags=["reviews"],
)

templates = Jinja2Templates(directory="api/templates")


