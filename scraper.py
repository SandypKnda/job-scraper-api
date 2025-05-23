from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4
from datetime import datetime
import os

app = FastAPI()

# Connect to Astra DB
def get_session():
    cloud_config = {'secure_connect_bundle': './secure-connect-your-db'}
    auth_provider = PlainTextAuthProvider(
        os.getenv("ASTRA_DB_CLIENT_ID"),
        os.getenv("ASTRA_DB_CLIENT_SECRET")
    )
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    return cluster.connect(os.getenv("ASTRA_DB_KEYSPACE"))

session = get_session()

@app.get("/scrape-jobs")
def scrape_jobs():
    jobs = []
    rows = session.execute("SELECT company, url FROM job_postings WHERE title='Company Careers'")
    
    for row in rows:
        response = requests.get(row.url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            if not title:
                continue

            if any(role in title.lower() for role in ["data engineer", "data scientist"]):
                job_url = link['href']
                if not job_url.startswith("http"):
                    job_url = f"https://{row.url.split('/')[2]}{job_url}"
                
                # Fake location parsing logic — update per site
                location = "United States"
                if "remote" in title.lower():
                    continue  # Skip remote
                if "onsite" not in title.lower() and "hybrid" not in title.lower():
                    continue  # Only want onsite/hybrid

                job = {
                    "id": uuid4(),
                    "title": title,
                    "company": row.company,
                    "location": location,
                    "url": job_url,
                    "source": row.url,
                    "timestamp": datetime.utcnow()
                }

                # Insert job into Astra
                session.execute("""
                    INSERT INTO job_postings (id, title, company, location, url, source, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, tuple(job.values()))

                jobs.append(job)

    return {"jobs_found": len(jobs)}
