import os
from serpapi import GoogleSearch
from urllib.parse import urlparse

def get_company_domains_from_serpapi():
    search = GoogleSearch({
        "q": "data engineer site:jobs.*.com OR site:careers.*.com",
        "location": "United States",
        "hl": "en",
        "gl": "us",
        "api_key": os.getenv("SERPAPI_API_KEY")
    })

    results = search.get_dict()
    domains = set()

    for result in results.get("jobs_results", []):
        links = result.get("related_links", [])
        for link in links:
            parsed = urlparse(link.get("link", ""))
            domain = parsed.netloc
            if domain and all(x not in domain for x in ["linkedin", "indeed", "glassdoor"]):
                domains.add(domain)

    return sorted(domains)
