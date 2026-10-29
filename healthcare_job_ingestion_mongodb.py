"""JSearch -> MySQL healthcare job ingestion pipeline."""

from __future__ import annotations

import hashlib
import html
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

import pymysql
import requests
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://api.openwebninja.com/jsearch/search-v2"

ROLES = (
    "nurse",
    "doctor",
    "midwife",
    "dentist",
    "medical laboratory scientist",
    "administrative assistant",
)

LOCATIONS = (
    ("Abuja", "Nigeria"),
    ("Lagos", "Nigeria"),
    ("Enugu", "Nigeria"),
    ("Kano", "Nigeria"),
    ("Nairobi", "Kenya"),
    ("Cairo", "Egypt"),
    ("Johannesburg", "South Africa"),
    ("Accra", "Ghana"),
    ("Dar es Salaam", "Tanzania"),
    ("Kampala", "Uganda"),
    ("Addis Ababa", "Ethiopia"),
    ("Kigali", "Rwanda"),
)

COUNTRY_NAMES = {
    "AO": "Angola",
    "BF": "Burkina Faso",
    "BJ": "Benin",
    "BW": "Botswana",
    "CD": "DR Congo",
    "CG": "Congo",
    "CI": "Côte d'Ivoire",
    "CM": "Cameroon",
    "DJ": "Djibouti",
    "DZ": "Algeria",
    "EG": "Egypt",
    "ER": "Eritrea",
    "ET": "Ethiopia",
    "GA": "Gabon",
    "GH": "Ghana",
    "GM": "Gambia",
    "GN": "Guinea",
    "KE": "Kenya",
    "LR": "Liberia",
    "LY": "Libya",
    "MA": "Morocco",
    "ML": "Mali",
    "MU": "Mauritius",
    "MW": "Malawi",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NG": "Nigeria",
    "RW": "Rwanda",
    "SD": "Sudan",
    "SL": "Sierra Leone",
    "SN": "Senegal",
    "SO": "Somalia",
    "SS": "South Sudan",
    "TG": "Togo",
    "TN": "Tunisia",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

EMPLOYMENT_TYPES = {
    "CONTRACT": "Contract",
    "CONTRACTOR": "Contract",
    "FULL_TIME": "Full-time",
    "FULL-TIME": "Full-time",
    "FULLTIME": "Full-time",
    "INTERN": "Internship",
    "INTERNSHIP": "Internship",
    "OTHER": "Other",
    "PART_TIME": "Part-time",
    "PART-TIME": "Part-time",
    "PARTTIME": "Part-time",
    "TEMPORARY": "Temporary",
    "TEMPORARY_PART_TIME": "Temporary",
    "VOLUNTEER": "Volunteer",
}

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")

COLUMNS = (
    "job_id",
    "job_title",
    "employer_name",
    "job_location",
    "job_city",
    "job_state",
    "job_country",
    "job_employment_type",
    "job_apply_link",
    "job_description",
    "job_publisher",
    "job_posted_at",
    "job_is_remote",
    "job_min_salary",
    "job_max_salary",
    "job_salary_currency",
    "job_salary_period",
    "search_role",
    "search_city",
    "search_country",
    "last_seen_at",
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(191) NOT NULL,
    job_title VARCHAR(512) NOT NULL,
    employer_name VARCHAR(512) NULL,
    job_location VARCHAR(512) NULL,
    job_city VARCHAR(191) NULL,
    job_state VARCHAR(191) NULL,
    job_country VARCHAR(191) NULL,
    job_employment_type VARCHAR(64) NULL,
    job_apply_link VARCHAR(2048) NULL,
    job_description MEDIUMTEXT NULL,
    job_publisher VARCHAR(255) NULL,
    job_posted_at VARCHAR(64) NULL,
    job_is_remote TINYINT(1) NOT NULL DEFAULT 0,
    job_min_salary DECIMAL(14, 2) NULL,
    job_max_salary DECIMAL(14, 2) NULL,
    job_salary_currency VARCHAR(16) NULL,
    job_salary_period VARCHAR(32) NULL,
    search_role VARCHAR(128) NULL,
    search_city VARCHAR(128) NULL,
    search_country VARCHAR(128) NULL,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id),
    KEY idx_jobs_country (job_country),
    KEY idx_jobs_city (job_city),
    KEY idx_jobs_title (job_title(191)),
    KEY idx_jobs_role (search_role),
    KEY idx_jobs_publisher (job_publisher)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_SQL = """
INSERT INTO jobs ({columns})
VALUES ({placeholders})
ON DUPLICATE KEY UPDATE
    job_title = VALUES(job_title),
    employer_name = VALUES(employer_name),
    job_location = VALUES(job_location),
    job_city = VALUES(job_city),
    job_state = VALUES(job_state),
    job_country = VALUES(job_country),
    job_employment_type = VALUES(job_employment_type),
    job_apply_link = VALUES(job_apply_link),
    job_description = VALUES(job_description),
    job_publisher = VALUES(job_publisher),
    job_posted_at = VALUES(job_posted_at),
    job_is_remote = VALUES(job_is_remote),
    job_min_salary = VALUES(job_min_salary),
    job_max_salary = VALUES(job_max_salary),
    job_salary_currency = VALUES(job_salary_currency),
    job_salary_period = VALUES(job_salary_period),
    search_role = VALUES(search_role),
    search_city = VALUES(search_city),
    search_country = VALUES(search_country),
    last_seen_at = VALUES(last_seen_at)
""".format(
    columns=", ".join(COLUMNS),
    placeholders=", ".join(["%s"] * len(COLUMNS)),
)

logger = logging.getLogger("job-pipeline")


def clean_text(value: Any, max_length: int | None = None) -> str | None:
    """Strip HTML, decode entities, and collapse whitespace."""
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        value = ", ".join(str(item) for item in value)
    text = html.unescape(str(value))
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = WHITESPACE_RE.sub(" ", text).strip()
    if not text:
        return None
    if max_length and len(text) > max_length:
        text = text[: max_length - 1].rstrip() + "…"
    return text


def normalize_employment_type(value: Any) -> str | None:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    text = clean_text(value)
    if not text:
        return None
    return EMPLOYMENT_TYPES.get(text.upper().replace(" ", "_"), text.replace("_", " ").title())


def normalize_location(
    job: dict[str, Any], query_city: str, query_country: str
) -> tuple[str | None, str | None, str | None, str | None]:
    city = clean_text(job.get("job_city")) or query_city
    state = clean_text(job.get("job_state"))
    raw_country = clean_text(job.get("job_country"))
    country = COUNTRY_NAMES.get(raw_country.upper(), raw_country) if raw_country else query_country

    display_city = city or state
    parts = [part for part in (display_city, country) if part]
    location = ", ".join(parts) if parts else clean_text(job.get("job_location"))

    return (
        clean_text(location, 512),
        clean_text(city, 191),
        clean_text(state, 191),
        clean_text(country, 191),
    )


def to_salary(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def job_identity(job: dict[str, Any]) -> str:
    job_id = clean_text(job.get("job_id"))
    if job_id and len(job_id) <= 191:
        return job_id
    seed = "|".join(
        str(job.get(key) or "")
        for key in (
            "job_title",
            "employer_name",
            "job_city",
            "job_country",
            "job_apply_link",
            "job_google_link",
        )
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def normalize_job(
    job: dict[str, Any], role: str, query_city: str, query_country: str
) -> dict[str, Any] | None:
    title = clean_text(job.get("job_title"), 512)
    if not title:
        return None

    apply_link = clean_text(job.get("job_apply_link") or job.get("job_google_link"), 2048)
    location, city, state, country = normalize_location(job, query_city, query_country)

    return {
        "job_id": job_identity(job),
        "job_title": title,
        "employer_name": clean_text(job.get("employer_name"), 512),
        "job_location": location,
        "job_city": city,
        "job_state": state,
        "job_country": country,
        "job_employment_type": normalize_employment_type(
            job.get("job_employment_type") or job.get("job_employment_types")
        ),
        "job_apply_link": apply_link,
        "job_description": clean_text(job.get("job_description")),
        "job_publisher": clean_text(job.get("job_publisher"), 255),
        "job_posted_at": clean_text(
            job.get("job_posted_at_datetime_utc") or job.get("job_posted_at"), 64
        ),
        "job_is_remote": 1 if job.get("job_is_remote") else 0,
        "job_min_salary": to_salary(job.get("job_min_salary")),
        "job_max_salary": to_salary(job.get("job_max_salary")),
        "job_salary_currency": clean_text(job.get("job_salary_currency"), 16),
        "job_salary_period": clean_text(job.get("job_salary_period"), 32),
        "search_role": role,
        "search_city": query_city,
        "search_country": query_country,
        "last_seen_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def build_session(api_key: str) -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
    return session


def extract_page(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    data = payload.get("data")
    cursor = payload.get("cursor")
    if isinstance(data, dict):
        jobs = data.get("jobs") or data.get("results") or []
        cursor = data.get("cursor") or cursor
    elif isinstance(data, list):
        jobs = data
    else:
        jobs = []
    return [job for job in jobs if isinstance(job, dict)], cursor


def fetch_jobs(session: requests.Session, query: str, max_pages: int, delay: float) -> list[dict]:
    jobs: list[dict[str, Any]] = []
    cursor: str | None = None

    for page in range(max_pages):
        params: dict[str, str] = {"query": query}
        if cursor:
            params["cursor"] = cursor

        response = session.get(API_URL, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")

        payload = response.json()
        if str(payload.get("status", "OK")).upper() == "ERROR":
            raise RuntimeError(payload.get("error") or payload.get("message") or "API error")

        page_jobs, cursor = extract_page(payload)
        jobs.extend(page_jobs)

        if not cursor or not page_jobs or page >= max_pages - 1:
            break
        time.sleep(delay)

    return jobs


def connect_db() -> pymysql.connections.Connection:
    database = os.environ["MYSQL_DATABASE"]
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError("MYSQL_DATABASE may only contain letters, digits, and underscores")

    def connect(db: str | None = None) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.getenv("MYSQL_PORT") or "3306"),
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=db,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=30,
        )

    try:
        return connect(database)
    except pymysql.err.OperationalError as exc:
        if exc.args and exc.args[0] == 1049:
            logger.info("Database %s does not exist, creating it", database)
            conn = connect()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"CREATE DATABASE IF NOT EXISTS `{database}` "
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                conn.commit()
            finally:
                conn.close()
            return connect(database)
        raise


def ensure_schema(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)
    conn.commit()


def upsert_jobs(conn: pymysql.connections.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [tuple(row[column] for column in COLUMNS) for row in rows]
    with conn.cursor() as cursor:
        for start in range(0, len(values), 100):
            cursor.executemany(UPSERT_SQL, values[start : start + 100])
    conn.commit()
    return len(values)



def connect_mongo() -> tuple[MongoClient, Any]:
    """Connect to MongoDB and return the client and jobs collection."""
    mongo_uri = os.getenv("MONGODB_URI", "").strip()
    if not mongo_uri:
        raise ValueError("MONGODB_URI is not set")

    database_name = os.getenv("MONGODB_DATABASE", "healthcare_jobs").strip()
    collection_name = os.getenv("MONGODB_COLLECTION", "jobs").strip()
    if not database_name or not collection_name:
        raise ValueError("MONGODB_DATABASE and MONGODB_COLLECTION must not be empty")

    timeout_ms = int(os.getenv("MONGODB_TIMEOUT_MS", "10000"))
    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=timeout_ms,
    )
    client.admin.command("ping")
    collection = client[database_name][collection_name]
    collection.create_index("job_id", unique=True)
    return client, collection


def upsert_jobs_mongo(collection: Any, rows: list[dict[str, Any]]) -> int:
    """Upsert normalized jobs into MongoDB, keyed by job_id."""
    if not rows:
        return 0

    operations = [
        UpdateOne(
            {"job_id": row["job_id"]},
            {
                "$set": row,
                "$setOnInsert": {"first_seen_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
        for row in rows
    ]
    collection.bulk_write(operations, ordered=False)
    return len(rows)


def main() -> int:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    api_key = os.getenv("JSEARCH_API_KEY", "").strip()
    if not api_key:
        logger.error("JSEARCH_API_KEY is not set")
        return 1

    max_pages = max(1, int(os.getenv("MAX_PAGES_PER_QUERY", "1")))
    delay = max(0.0, float(os.getenv("REQUEST_DELAY_SECONDS", "1")))
    session = build_session(api_key)

    queries = [(role, city, country) for role in ROLES for city, country in LOCATIONS]
    collected: dict[str, dict[str, Any]] = {}
    failures: list[str] = []

    for index, (role, city, country) in enumerate(queries, start=1):
        query = f"{role} jobs in {city}"
        try:
            jobs = fetch_jobs(session, query, max_pages, delay)
            new_jobs = 0
            for job in jobs:
                row = normalize_job(job, role, city, country)
                if row and row["job_id"] not in collected:
                    collected[row["job_id"]] = row
                    new_jobs += 1
            logger.info(
                "[%d/%d] %s -> %d jobs, %d new (total %d)",
                index,
                len(queries),
                query,
                len(jobs),
                new_jobs,
                len(collected),
            )
        except Exception as exc:  # keep other queries running
            failures.append(query)
            logger.warning("[%d/%d] %s failed: %s", index, len(queries), query, exc)

        if index < len(queries):
            time.sleep(delay)

    if not collected:
        logger.error("No jobs collected; %d queries failed", len(failures))
        return 1

    rows = list(collected.values())

    # Preserve the existing MySQL destination and behavior.
    conn = connect_db()
    try:
        ensure_schema(conn)
        mysql_upserted = upsert_jobs(conn, rows)
    finally:
        conn.close()

    # MongoDB is an additional destination. A MongoDB failure is logged and
    # does not prevent the existing MySQL ingestion from completing.
    mongo_upserted = 0
    mongo_failed = False
    mongo_client = None
    try:
        mongo_client, mongo_collection = connect_mongo()
        mongo_upserted = upsert_jobs_mongo(mongo_collection, rows)
        logger.info(
            "MongoDB: %d unique jobs upserted into %s.%s",
            mongo_upserted,
            os.getenv("MONGODB_DATABASE", "healthcare_jobs"),
            os.getenv("MONGODB_COLLECTION", "jobs"),
        )
    except Exception:
        mongo_failed = True
        logger.exception(
            "MongoDB write failed. MySQL ingestion completed; "
            "check MONGODB_URI/database/collection settings and retry."
        )
    finally:
        if mongo_client is not None:
            mongo_client.close()

    logger.info(
        "Pipeline complete: MySQL=%d, MongoDB=%d, %d/%d queries failed",
        mysql_upserted,
        mongo_upserted,
        len(failures),
        len(queries),
    )

    # Return non-zero on total query failure or MongoDB failure for CI monitoring.
    return 1 if len(failures) == len(queries) or mongo_failed else 0


if __name__ == "__main__":
    sys.exit(main())
