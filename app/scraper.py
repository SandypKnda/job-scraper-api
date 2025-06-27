import os
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from app.utils import hash_url, send_email, connect_astra, save_if_new
from fastapi import FastAPI
from app.dynamic_companies import get_data_engineering_job_sources


#from app.dynamic_companies import get_company_domains_from_serpapi

app = FastAPI()

def cleanup_invalid_jobs():
    db = connect_astra()
    collection = db.collection("jobs")
    docs = collection.find()

    deleted = 0
    for doc in docs:
        if isinstance(doc, str):
            try:
                collection.delete_one({"_id": doc})  # string docs may not have _id
                deleted += 1
            except:
                pass  # silently skip
        elif not isinstance(doc, dict):
            deleted += 1
            collection.delete_one({"_id": doc["_id"]})

    print(f"🧹 Deleted {deleted} invalid job entries.")

if __name__ == "__main__":
    cleanup_invalid_jobs()


def load_discovered_domains(collection):
    try:
        rows = collection.find()
        documents = rows.get("data", {}).get("documents", [])
        company_pages = {}
        for row in documents:
            if not isinstance(row, dict):
                print(f"⚠️ Skipped unexpected row type: {type(row)} → {row}")
                try:
                    print("🔍 Parsed version:", json.loads(row))
                except Exception as e:
                    print(f"❌ Could not parse row: {e}")
                continue

            name = row.get("company", "Unknown")
            domain = row.get("url", "")

            if domain:
                if not domain.startswith("http"):
                    domain = "https://" + domain
                company_pages[name] = domain

        print(f"✅ Loaded {len(company_pages)} valid company domains")
        return company_pages
    except Exception as e:
        print(f"🔴 Error loading domains from DB: {e}")
        print(rows)
        return {}

def scrape_page(url):
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                print(f"✅ Successfully fetched {resp.url}")
            else:
                print(f"Non-200 from {url}: {resp.status_code}")
                for r in resp.history:
                    print(f"🔁 Redirected from {r.url} → {r.status_code}")
                return None
            return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Fetch failed for {url}: {e}")
        return None

def run_scraper():
    try:
        collection = connect_astra()
        if not collection:
            print("❌ Failed to connect to Astra collection")
            return []
        company_pages = load_discovered_domains(collection)
        if not company_pages:
            print("⚠️ No company domains found in DB.")
            return []

        new_jobs = []

        for company, url in company_pages.items():
            print(f"🔍 Scraping jobs from: {company} ({url})")
            soup = scrape_page(url)
            if not soup:
                continue

            base_url = url  # ✅ Added to fix undefined variable issue

            print(f"🔍 Parsing job links from: {url}")
            for a in soup.find_all("a", href=True):
                text = a.text.strip()
                href = a["href"]

                print(f"🔗 {text} → {href}")  # ✅ Log all links

                # Skipping known aggregator sites
                if any(x in href for x in ["linkedin", "glassdoor", "indeed", "ziprecruiter", "dice", "jobot"]):
                    continue

                # ✅ Loosened filter for testing
                if "engineer" in text.lower():  # Change back to "data engineer" later
                    job_url = href if href.startswith("http") else base_url.rstrip("/") + "/" + href.lstrip("/")
                    title = text
                    job_id = hash_url(job_url)

                    print(f"🆕 Found potential job: {title} → {job_url}")  # ✅ Debug log

                    inserted = save_if_new(collection, job_id, job_url, title, company)
                    if inserted:
                        print(f"✅ Inserted: {title}")
                        new_jobs.append((title, job_url))
                    else:
                        print(f"⏭️ Skipped (duplicate?): {title}")

        if new_jobs:
            send_email(new_jobs)
        return new_jobs

    except Exception as e:
        print(f"[run_scraper ERROR] {e}")  # Key print for debugging
        return []

