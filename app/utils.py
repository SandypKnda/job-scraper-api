import os
import hashlib
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def connect_astra():
    try:
        #bundle_path = os.getenv("ASTRA_DB_BUNDLE_PATH", "secure-connect-your-db.zip")
        cloud_config = {
            'secure_connect_bundle': '/opt/render/project/src/secure-connect-job-scraper.zip'
        }
        client_id = os.getenv("ASTRA_DB_CLIENT_ID")
        client_secret = os.getenv("ASTRA_DB_CLIENT_SECRET")
        keyspace = os.getenv("ASTRA_DB_KEYSPACE")

        if not all([client_id, client_secret, keyspace]):
            raise ValueError("One or more Astra DB env vars (CLIENT_ID, CLIENT_SECRET, KEYSPACE) are missing")

        auth_provider = PlainTextAuthProvider(client_id, client_secret)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(keyspace)
        return session
    except Exception:
        print("Error connecting to Astra DB:")
        print(traceback.format_exc())
        return None

def save_if_new(session, job_id, url, title, company):
    if session is None:
        print("No valid DB session available for saving jobs")
        return False
    try:
        stmt = session.prepare("SELECT id FROM job_postings WHERE id=?")
        res = session.execute(stmt, [job_id])
        if res.one():
            return False
        insert_stmt = session.prepare("""
            INSERT INTO job_postings (id, url, title, company, scraped_at)
            VALUES (?, ?, ?, ?, toTimestamp(now()))
        """)
        session.execute(insert_stmt, [job_id, url, title, company])
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
