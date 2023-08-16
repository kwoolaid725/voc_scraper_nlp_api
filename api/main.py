from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/")
async def root():
    return  {"message": "Hello World"}

@app.put("/")
async def root():
    return  {"message": "Hello World"}

@app.delete("/")
async def root():
    return {"message": "Hello World"}


@app.get("/items/{item_name}")
async def get_item():
    return  {"message": "Hello World"}

@app.post("/items/{item_name}")
async def get_item():
    return  {"message": "Hello World"}

@app.put("/items/{item_name}")
async def get_item():
    return  {"message": "Hello World"}

@app.delete("/items/{item_name}")
async def get_item():
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