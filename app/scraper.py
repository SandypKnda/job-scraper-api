import os
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from app.utils import hash_url, send_email, connect_astra, save_if_new
from fastapi import FastAPI
import traceback

app = FastAPI()

COMPANY_CAREER_PAGES = {
    "Airbnb": "https://careers.airbnb.com/positions/",
    "Stripe": "https://stripe.com/jobs/search?country=US&team=engineering",
    "Snowflake": "https://careers.snowflake.com/us/en",
}

def scrape_page(url):
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            return soup
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def run_scraper():
    try:
        db_session = connect_astra()
        new_jobs = []

        for company, url in COMPANY_CAREER_PAGES.items():
            print(f"Scraping jobs for {company} from {url}")
            soup = scrape_page(url)
            if not soup:
                print(f"Failed to scrape {url}")
                continue

            for a in soup.find_all("a", href=True):
                href = a["href"]
                # Skip unwanted links
                if any(x in href.lower() for x in ["linkedin", "glassdoor", "indeed"]):
                    continue
                if "data engineer" in a.text.lower():
                    # Compose absolute job URL carefully
                    job_url = href if href.startswith("http") else os.path.join(url, href.lstrip("/"))
                    title = a.text.strip()
                    job_id = hash_url(job_url)

                    try:
                        inserted = save_if_new(db_session, job_id, job_url, title, company)
                        if inserted:
                            new_jobs.append((title, job_url))
                    except Exception:
                        print(f"Failed to save job {title} - {job_url}")
                        print(traceback.format_exc())
                        continue

        if new_jobs:
            try:
                send_email(new_jobs)
            except Exception:
                print("Failed to send email notification")
                print(traceback.format_exc())

        return new_jobs

    except Exception:
        print("Exception in run_scraper:")
        print(traceback.format_exc())
        return []
