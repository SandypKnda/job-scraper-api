from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.scraper import run_scraper
import traceback

app = FastAPI()

@app.get("/scrape")
def scrape_jobs():
    try:
        result = run_scraper()
        return {
            "message": f"{len(result)} new jobs found",
            "data": result
        }
    except Exception:
        print("Exception during job scraping:")
        print(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": "Something went wrong during job scraping"}
        )

