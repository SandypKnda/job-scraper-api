from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from app.scraper import run_scraper
import traceback

app = FastAPI()

router = APIRouter()

@router.get("/cleanup-jobs")
def cleanup_jobs_collection():
    try:
        collection = connect_astra()
        docs = collection.find()
        count = 0

        for doc in docs:
            if isinstance(doc, str):
                print(f"🧨 Deleting string-only doc: {doc}")
                collection.delete_many({})  # Nuke all bad data — optional
                count += 1
            elif not isinstance(doc, dict):
                print(f"🧨 Deleting unexpected type: {type(doc)}")
                if "_id" in doc:
                    collection.delete_one({"_id": doc["_id"]})
                    count += 1

        return {"message": f"Deleted {count} bad documents"}
    except Exception as e:
        return {"error": str(e)}

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

