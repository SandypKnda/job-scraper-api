import os
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from app.utils import hash_url, send_email, connect_astra, save_if_new
from fastapi import FastAPI

app = FastAPI()

# Sample companies with dedicated job pages
COMPANY_CAREER_PAGES = {
    "Airbnb": "https://careers.airbnb.com/positions/",
    "Stripe": "https://stripe.com/jobs/search?country=US&team=engineering",
    "Snowflake": "https://careers.snowflake.com/us/en",
}

def scrape_page(url):
    with httpx.Client(timeout=10.0) as client:
        try:
            r = client.get(url)
            soup = BeautifulSoup(r.text, "html.parser")
            return soup
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

def run_scraper():
    db_session = connect_astra()
    new_jobs = []

    for company, url in COMPANY_CAREER_PAGES.items():
        soup = scrape_page(url)
        if not soup:
            continue

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(x in href for x in ["linkedin", "glassdoor", "indeed"]):
                continue
            if "data engineer" in a.text.lower():
                job_url = href if href.startswith("http") else url + href
                title = a.text.strip()
                job_id = hash_url(job_url)
                inserted = save_if_new(db_session, job_id, job_url, title, company)
                if inserted:
                    new_jobs.append((title, job_url))

    if new_jobs:
        send_email(new_jobs)
    return new_jobs
