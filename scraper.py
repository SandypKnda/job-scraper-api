import os
from datetime import datetime
from serpapi import GoogleSearch
from dynamic_companies import get_company_domains_from_serpapi

def scrape_jobs():
    companies = get_company_domains_from_serpapi()

    all_jobs = []
    for domain in companies:
        search = GoogleSearch({
            "q": f"data engineer site:{domain}",
            "location": "United States",
            "hl": "en",
            "gl": "us",
            "api_key": os.getenv("SERPAPI_API_KEY")
        })

        results = search.get_dict()
        for result in results.get("jobs_results", []):
            job = {
                "title": result.get("title"),
                "company": result.get("company_name"),
                "location": result.get("location"),
                "url": result.get("related_links", [{}])[0].get("link", ""),
                "source": domain,
                "date_posted": datetime.utcnow()
            }
            all_jobs.append(job)

    return all_jobs
