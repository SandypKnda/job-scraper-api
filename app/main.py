from fastapi import FastAPI
from app.scraper import run_scraper

app = FastAPI()

@app.get("/scrape")
def scrape_jobs():
    result = run_scraper()
    return {"message": f"{len(result)} new jobs found", "data": result}
