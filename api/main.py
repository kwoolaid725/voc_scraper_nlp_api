import pathlib
from fastapi import FastAPI, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from .database import engine
from . import models
# from .routers import
from pydantic_settings import BaseSettings
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse


models.Base.metadata.create_all(bind=engine)
BASE_DIR = pathlib.Path(__file__).resolve().parent #app/
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI()

templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    context = {"request": request}
    return templates.TemplateResponse("home.html", context)


@app.post("/")
async def root():
    return  {"message": "Hello World"}

@app.put("/")
async def root():
    return  {"message": "Hello World"}

@app.delete("/")
async def root():
    return {"message": "Hello World"}




'''
input:
    retail
    product
    url
'''

'''
output:
    raw data
    superset connection
    processed data - sentiment analysis with highlights html
    stars pie chart
    most common words for positive and negative reviews bar chart
'''