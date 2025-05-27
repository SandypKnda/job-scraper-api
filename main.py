from fastapi import FastAPI
from scraper import scrape_jobs
from db import connect_to_db, save_job

app = FastAPI()

@app.get("/")
def root():
    return {"status": "Scraper running."}

@app.post("/scrape")
def run_scraper():
    session = connect_to_db()
    jobs = scrape_jobs()
    new_jobs = []

    for job in jobs:
        if save_job(session, job):
            new_jobs.append(job)

    return {"total": len(jobs), "new": len(new_jobs)}
