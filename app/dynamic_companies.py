from serpapi import GoogleSearch
from urllib.parse import urlparse
from app.utils import connect_astra
import os
import httpx
from uuid import uuid4
from datetime import datetime

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")  # set in Render env vars
RAPIDAPI_HOST = "jsearch.p.rapidapi.com"

def get_data_engineering_job_sources(location="United States", limit=20):
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }
    params = {
        "query": "Data Engineer",
        "page": "1",
        "num_pages": "1",
        "employment_types": "FULLTIME",
        "location": location
    }

    sources = set()

    try:
        response = httpx.get(url, headers=headers, params=params, timeout=10.0)
        data = response.json()
        for job in data.get("data", []):
            employer = job.get("employer_name")
            job_url = job.get("job_apply_link") or job.get("job_google_link") or job.get("job_offer_link")
            if employer and job_url:
                sources.add((employer, job_url.split("/")[2]))  # Extract domain from URL
    except Exception as e:
        print("🔴 Error fetching JSearch data:", e)

    print(f"✅ Found {len(sources)} sources")
    return dict(sources)


def save_discovered_companies_to_db():
    try:
        session = connect_astra()
        companies = get_data_engineering_job_sources()

        insert_stmt = session.prepare("""
            INSERT INTO jobs (id, company, url, discovered_at) 
            VALUES (uuid(), ?, ?, toTimestamp(now()))
        """)
        count = 0
        for name, domain in companies.items():
            session.execute(insert_stmt, [name, domain])
            count += 1

        print(f"✅ Saved {count} discovered companies to DB.")
        return count

    except Exception as e:
        print(f"🔴 Error saving companies to DB: {e}")
        return 0
