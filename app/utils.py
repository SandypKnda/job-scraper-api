import os
import hashlib
import traceback
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from astrapy.db import AstraDB

def hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def connect_astra():
    """
    Connect to Astra DB using Data API (no secure bundle required).
    This works perfectly on cloud environments like Render.
    """
    
    try:
        # Get environment variables from Render secrets or .env
        token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        keyspace = os.getenv("ASTRA_DB_KEYSPACE")  # Your Cassandra keyspace/namespace

        if not token or not api_endpoint or not keyspace:
            print("❌ Missing environment variables:")
            print(f"  ASTRA_DB_APPLICATION_TOKEN: {'✅' if token else '❌'}")
            print(f"  ASTRA_DB_API_ENDPOINT: {'✅' if api_endpoint else '❌'}")
            print(f"  ASTRA_DB_KEYSPACE: {'✅' if keyspace else '❌'}")
            raise ValueError("Missing Astra DB credentials or endpoint environment variables")

        # Initialize AstraDB client
        db = AstraDB(
            token=token,
            api_endpoint=api_endpoint,
            namespace=keyspace
        )

    except Exception:
        print("Error connecting to Astra DB:")
        print(traceback.format_exc())
        return None

def save_if_new(db, job_id, url, title, company):
    try:
        collection = db.collection("job_postings")
        existing = collection.find_one({"_id": job_id})
        if existing:
            return False
        collection.insert_one({
            "_id": job_id,
            "url": url,
            "title": title,
            "company": company,
            "scraped_at": {"$date": {"$numberLong": "0"}}  # placeholder timestamp
        })
        return True
    except Exception:
        print(f"Error saving job {title} ({url}):")
        print(traceback.format_exc())
        return False

def send_email(job_list):
    if not job_list:
        print("No jobs to email, skipping send_email.")
        return
    content = "\n\n".join([f"{title}\n{url}" for title, url in job_list])
    message = Mail(
        from_email=os.getenv("FROM_EMAIL"),
        to_emails=os.getenv("ALERT_EMAIL"),
        subject="🧠 New Data Engineer Jobs Found!",
        plain_text_content=content,
    )
    try:
        sg_api_key = os.getenv("SENDGRID_API_KEY")
        if not sg_api_key:
            raise ValueError("SENDGRID_API_KEY environment variable not set")
        sg = SendGridAPIClient(sg_api_key)
        sg.send(message)
        print("Job alert email sent successfully.")
    except Exception as e:
        print(f"SendGrid error: {e}")
        print(traceback.format_exc())
