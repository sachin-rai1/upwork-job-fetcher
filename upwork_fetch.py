#!/usr/bin/env python3
"""
upwork_fetch_and_mail.py
- fetches mostRecentJobsFeed GraphQL from Upwork
- emails the JSON result (inline + attached)
"""

import os
import sys
import time
import json
import logging
import smtplib
from email.message import EmailMessage
from email.utils import formatdate
from datetime import datetime
from typing import Tuple

import requests

# ---------- CONFIG ----------
API_URL = os.getenv("API_URL", "https://www.upwork.com/api/graphql/v1?alias=mostRecentJobsFeed")
TOKEN = os.getenv("UPWORK_TOKEN")
TENANT_ID = os.getenv("UPWORK_TENANTID")
LIMIT = int(os.getenv("LIMIT", "10"))

RECIPIENT = os.getenv("RECIPIENT_EMAIL")
SENDER = os.getenv("SENDER_EMAIL")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

# retry config
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds multiplier

# logging
LOG_PATH = os.getenv("LOG_PATH", "/var/log/upwork_fetcher.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)]
)

# GraphQL payload
GRAPHQL_PAYLOAD = {
    "query": """query($limit: Int, $toTime: String) {
        mostRecentJobsFeed(limit: $limit, toTime: $toTime) {
          results {
            id,
            uid:id
            title,
            ciphertext,
            description,
            type,
            recno,
            freelancersToHire,
            duration,
            engagement,
            amount {
              amount,
            },
            createdOn:createdDateTime,
            publishedOn:publishedDateTime,
            prefFreelancerLocationMandatory,
            connectPrice,
            client {
              totalHires
              totalSpent
              paymentVerificationStatus,
              location {
                country,
              },
              totalReviews
              totalFeedback,
              hasFinancialPrivacy
            },
            tierText
            tier
            tierLabel
            proposalsTier
            enterpriseJob
            premium,
            jobTs:jobTime,
            attrs:skills {
              id,
              uid:id,
              prettyName:prefLabel
              prefLabel
            }
            hourlyBudget {
              type
              min
              max
            }
            isApplied
          },
          paging {
            total,
            count,
            resultSetTs:minTime,
            maxTime
          }
        }
      }""",
    "variables": {"limit": LIMIT}
}

# ---------- helpers ----------
def validate_env() -> Tuple[bool, str]:
    required = {
        "UPWORK_TOKEN": TOKEN,
        "UPWORK_TENANTID": TENANT_ID,
        "RECIPIENT_EMAIL": RECIPIENT,
        "SENDER_EMAIL": SENDER,
        "SMTP_HOST": SMTP_HOST,
        "SMTP_USER": SMTP_USER,
        "SMTP_PASS": SMTP_PASS,
    }
    missing = [k for k,v in required.items() if not v]
    if missing:
        return False, f"Missing environment variables: {', '.join(missing)}"
    return True, ""

def fetch_upwork() -> requests.Response:
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "x-upwork-api-tenantid": TENANT_ID,
        "Referer": "https://www.upwork.com/nx/find-work/most-recent",
        "Accept": "*/*",
        "User-Agent": "UpworkFetcher/1.0"
    }
    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = requests.post(API_URL, headers=headers, json=GRAPHQL_PAYLOAD, timeout=20)
            if resp.status_code >= 500:
                logging.warning("Server error %s. Attempt %d/%d", resp.status_code, attempt, MAX_RETRIES)
                time.sleep(RETRY_BACKOFF ** attempt)
                continue
            return resp
        except requests.RequestException as e:
            logging.exception("Request failed (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
            time.sleep(RETRY_BACKOFF ** attempt)
    raise RuntimeError("Max retries exceeded fetching Upwork API")

def make_email(subject: str, body_text: str, attachment_bytes: bytes, attachment_name: str) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = SENDER
    msg["To"] = RECIPIENT
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.set_content(body_text)

    # attach JSON file
    msg.add_attachment(attachment_bytes,
                       maintype="application",
                       subtype="json",
                       filename=attachment_name)
    return msg

def send_email(msg: EmailMessage) -> None:
    # For TLS
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        if SMTP_PORT in (587, 25):
            server.starttls()
            server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
    logging.info("Email sent to %s", RECIPIENT)

# ---------- main ----------
def main():
    ok, msg = validate_env()
    if not ok:
        logging.error(msg)
        sys.exit(2)

    logging.info("Fetching Upwork feed (limit=%d)", LIMIT)
    try:
        resp = fetch_upwork()
    except Exception as e:
        logging.exception("Failed to fetch Upwork: %s", e)
        # send error mail optionally
        err_msg = EmailMessage()
        err_msg["From"] = SENDER
        err_msg["To"] = RECIPIENT
        err_msg["Subject"] = f"[Upwork Fetcher] ERROR at {datetime.utcnow().isoformat()}Z"
        err_msg.set_content(f"Failed to fetch Upwork API: {e}")
        try:
            send_email(err_msg)
        except Exception as se:
            logging.exception("Failed sending error mail: %s", se)
        sys.exit(1)

    logging.info("Received status %s", resp.status_code)
    if resp.status_code == 200:
        data = resp.json()
        pretty = json.dumps(data, indent=2)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"upwork_feed_{ts}.json"
        subject = f"[Upwork] mostRecentJobsFeed — {len(data.get('data', {}).get('mostRecentJobsFeed', {}).get('results', []))} results — {ts}"

        body = f"Upwork API call succeeded.\n\nTime (UTC): {ts}\nHTTP Status: {resp.status_code}\n\nAttached: {fname}\n\n(First 1000 chars of payload below)\n\n{pretty[:1000]}"
        email = make_email(subject, body, pretty.encode("utf-8"), fname)
        try:
            send_email(email)
        except Exception as e:
            logging.exception("Failed to send success email: %s", e)
            sys.exit(1)
        logging.info("Done.")
    elif resp.status_code in (401, 403):
        logging.error("Authorization error: %s", resp.status_code)
        # notify via mail
        err_msg = EmailMessage()
        err_msg["From"] = SENDER
        err_msg["To"] = RECIPIENT
        err_msg["Subject"] = f"[Upwork Fetcher] AUTH ERROR {resp.status_code}"
        err_msg.set_content(f"Upwork API returned {resp.status_code}. Response:\n\n{resp.text}")
        try:
            send_email(err_msg)
        except Exception as se:
            logging.exception("Failed sending auth error mail: %s", se)
        sys.exit(3)
    else:
        logging.error("Unexpected HTTP status %s: %s", resp.status_code, resp.text[:500])
        # send failure mail
        err_msg = EmailMessage()
        err_msg["From"] = SENDER
        err_msg["To"] = RECIPIENT
        err_msg["Subject"] = f"[Upwork Fetcher] ERROR {resp.status_code}"
        err_msg.set_content(f"Status: {resp.status_code}\n\n{resp.text}")
        try:
            send_email(err_msg)
        except Exception as se:
            logging.exception("Failed sending failure mail: %s", se)
        sys.exit(4)


if __name__ == "__main__":
    main()
