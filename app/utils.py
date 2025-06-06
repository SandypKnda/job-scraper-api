import os
import hashlib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def connect_astra():
    cloud_config = {
        'secure_connect_bundle': f'secure-connect-your-db.zip'  # Upload on Render
    }
    auth_provider = PlainTextAuthProvider("token", os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(os.getenv("ASTRA_DB_KEYSPACE"))
    return session

def save_if_new(session, job_id, url, title, company):
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

def send_email(job_list):
    content = "\n".join([f"{title}\n{url}" for title, url in job_list])
    message = Mail(
        from_email=os.getenv("FROM_EMAIL"),
        to_emails=os.getenv("ALERT_EMAIL"),
        subject="🧠 New Data Engineer Jobs Found!",
        plain_text_content=content,
    )
    try:
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        sg.send(message)
    except Exception as e:
        print(f"SendGrid error: {e}")
