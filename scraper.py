from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from uuid import uuid4
from datetime import datetime
import os

app = FastAPI()

ASTRA_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")
ASTRA_TOKEN = os.getenv("ASTRA_DB_API_TOKEN")
ASTRA_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")

@app.get("/scrape-jobs")
def scrape_jobs():
    headers = {"X-Cassandra-Token": ASTRA_TOKEN}
    read_url = f"{ASTRA_ENDPOINT}/api/rest/v2/keyspaces/{ASTRA_KEYSPACE}/job_postings"

    res = requests.get(read_url, headers=headers)
    data = res.json().get("data", [])
    scraped = []

    for job in data:
        if job.get("title") != "Company Careers":
            continue

        resp = requests.get(job["url"], headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            if not text:
                continue

            if any(role in text.lower() for role in ["data engineer", "data scientist"]):
                if "remote" in text.lower():
                    continue
                if "onsite" not in text.lower() and "hybrid" not in text.lower():
                    continue

                href = link['href']
                if not href.startswith("http"):
                    href = f"https://{job['url'].split('/')[2]}{href}"

                new_job = {
                    "id": str(uuid4()),
                    "title": text,
                    "company": job["company"],
                    "location": "United States",
                    "url": href,
                    "source": job["url"],
                    "timestamp": datetime.utcnow().isoformat()
                }

                res = requests.post(read_url, headers={**headers, "Content-Type": "application/json"}, json=new_job)
                if res.status_code == 201:
                    scraped.append(text)

    return {"jobs_scraped": scraped}
