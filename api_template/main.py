from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/greet", response_class=HTMLResponse)
async def greet_user(request: Request, name: str = Form(...)):
    greeting = f"Hello, {name}!"
    return HTMLResponse(f"<h1>{greeting}</h1>")

@app.get("/template2", response_class=HTMLResponse)
async def another_template(request: Request):
    return templates.TemplateResponse("template2.html", {"request": request})