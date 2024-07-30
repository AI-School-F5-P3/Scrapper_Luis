from fastapi import FastAPI, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from scraper import QuoteScraper
import models
import schedule
import time
from threading import Thread

Base.metadata.create_all(bind=engine)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
async def startup_event():
    def run_scraper():
        scraper = QuoteScraper(["https://quotes.toscrape.com", "https://www.goodreads.com/quotes"])
        scraper.update_database()

    def run_schedule():
        while True:
            schedule.run_pending()
            time.sleep(1)

    schedule.every(24).hours.do(run_scraper)
    Thread(target=run_schedule, daemon=True).start()

@app.get("/")
async def read_root(request: Request, db: Session = Depends(get_db)):
    quotes = db.query(models.Quote).all()
    return templates.TemplateResponse("index.html", {"request": request, "quotes": quotes})
