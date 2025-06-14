import os
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from app.utils import hash_url, send_email, connect_astra, save_if_new
from fastapi import FastAPI

app = FastAPI()

def fetch_company_urls_from_db():
    session = connect_astra()
    rows = session.execute("SELECT company, url FROM job_sources")
    return [(row.company, row.url) for row in rows]

def scrape_page(url):
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                print(f"Non-200 from {url}: {resp.status_code}")
                return None
            return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Fetch failed for {url}: {e}")
        return None

def run_scraper():
    db_session = connect_astra()
    new_jobs = []

    for company, url in fetch_company_urls_from_db():
        soup = scrape_page(url)
        if not soup:
            continue

        for a in soup.find_all("a", href=True):
            try:
                text = a.text.strip().lower()
                href = a["href"]
                if not text or any(x in href for x in ["linkedin", "glassdoor", "indeed"]):
                    continue
                if "data engineer" in text:
                    if "remote" in text or not any(x in text for x in ["onsite", "hybrid"]):
                        continue
                    job_url = href if href.startswith("http") else url + href
                    title = a.text.strip()
                    job_id = hash_url(job_url)
                    inserted = save_if_new(db_session, job_id, job_url, title, company)
                    if inserted:
                        new_jobs.append((title, job_url))
            except Exception as e:
                print(f"[WARN] Error parsing link: {e}")
                continue

    if new_jobs:
        send_email(new_jobs)
    return new_jobs
