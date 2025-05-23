from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import uuid

app = FastAPI()

job_sources = [
    "https://careers.microsoft.com/us/en/search-results",
    "https://www.netflixjobs.com/search?q=data%20engineer",
    "https://careers.airbnb.com/positions/"
]

def is_aggregator(url):
    return any(domain in url for domain in ['linkedin.com', 'glassdoor.com', 'indeed.com'])

@app.get("/scrape")
def scrape():
    all_jobs = []
    for site in job_sources:
        response = requests.get(site)
        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.find_all("a", href=True):
            job_url = link['href']
            if is_aggregator(job_url):
                continue
            job = {
                "id": str(uuid.uuid4()),
                "title": link.get_text(strip=True)[:100],
                "url": job_url if job_url.startswith("http") else f"https://{site.split('/')[2]}{job_url}",
                "source": site,
                "company": site.split('.')[1],
                "location": "United States",
                "timestamp": datetime.utcnow().isoformat()
            }
            all_jobs.append(job)
    return {"jobs": all_jobs}
@app.get("/")
def read_root():
    return {"message": "FastAPI job scraper is running"}
