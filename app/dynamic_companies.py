import os
from serpapi import GoogleSearch
from urllib.parse import urlparse

def get_company_domains_from_serpapi():
    try:
        search = GoogleSearch({
            "q": "data engineer site:jobs.*.com OR site:careers.*.com",
            "location": "United States",
            "hl": "en",
            "gl": "us",
            "api_key": os.getenv("SERPAPI_API_KEY"),
            "num": "100",  # Ask for 100 results
        })

        results = search.get_dict()
        domains = set()

        for result in results.get("jobs_results", []):
            link = result.get("link", "")
            parsed = urlparse(link)
            domain = parsed.netloc
            if domain and all(x not in domain for x in ["linkedin", "glassdoor", "indeed"]):
                domains.add(domain)

        return sorted(domains)
    except Exception as e:
        print(f"SerpAPI error: {e}")
        return []

