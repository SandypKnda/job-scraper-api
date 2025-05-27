from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import datetime
import hashlib
import os

SECURE_BUNDLE_PATH = os.getenv("ASTRA_DB_BUNDLE_PATH")

def connect_to_db():
    cloud_config= {
        'secure_connect_bundle': SECURE_BUNDLE_PATH
    }
    auth_provider = PlainTextAuthProvider(
        os.getenv("ASTRA_DB_CLIENT_ID"), os.getenv("ASTRA_DB_CLIENT_SECRET"))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace("job_scraper")
    return session

def save_job(session, job):
    url_hash = hashlib.sha256(job["url"].encode()).hexdigest()
    existing = session.execute("SELECT * FROM job_postings WHERE hash=%s", [url_hash])
    if existing.one():
        return False

    session.execute("""
        INSERT INTO job_postings (id, title, company, location, url, source, date_posted, scraped_at, hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        job["title"],
        job["company"],
        job["location"],
        job["url"],
        job["source"],
        job["date_posted"],
        datetime.datetime.utcnow(),
        url_hash
    ))
    return True
