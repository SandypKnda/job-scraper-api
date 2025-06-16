from serpapi import GoogleSearch
from urllib.parse import urlparse
from app.utils import connect_astra
import os
from uuid import uuid4
from datetime import datetime

def get_company_domains_from_serpapi():
    print("🔍 Calling SerpAPI for dynamic companies...")
    search = GoogleSearch({
        "q": "data engineer site:jobs.*.com OR site:careers.*.com",
        "location": "United States",
        "hl": "en",
        "gl": "us",
        "api_key": os.getenv("SERPAPI_API_KEY")
    })
    results = search.get_dict()
    print(f"🔎 SERPAPI Response: {results.keys()}")

    results = search.get_dict()
    domains = set()

    for result in results.get("jobs_results", []):
        for link in result.get("related_links", []):
            url = link.get("link", "")
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain and not any(x in domain for x in ["linkedin", "indeed", "glassdoor"]):
                domains.add((domain.split(".")[0].capitalize(), url))
   
    print(f"✅ Found {len(domains)} domains: {list(domains)}")
    return sorted(domains)


def save_discovered_companies_to_db():
    session = connect_astra()
    companies = get_company_domains_from_serpapi()
    for company, url in companies:
        try:
            session.execute(
                "INSERT INTO job_sources (id, company, url, discovered_at) VALUES (%s, %s, %s, toTimestamp(now()))",
                [str(uuid4()), company, url]
            )
        except Exception as e:
            print(f"Error inserting {company}: {e}")
