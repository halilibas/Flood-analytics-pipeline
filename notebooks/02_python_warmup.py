"""
Python warmup for DE patterns
- API calls with requests
- Pagination
- Faker for synthetic data
- Retry + logging boilerplate
"""

import csv
import logging
import requests
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


OPENFEMA_BASE = "https://www.fema.gov/api/open/v2/FimaNfipClaims"


def fetch_fema_page(skip=0, top=1000):
    #Fetch one page of FEMA NFIP claims.
    params = {"$top": top, "$skip": skip, "$format": "json"}
    response = requests.get(OPENFEMA_BASE, params=params, timeout=60)
    response.raise_for_status()
    return response.json().get("FimaNfipClaims", [])


def fetch_with_retry(url, params, max_retries=3):
    #GET with simple retry
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                raise
    return None


# Faker Data
def make_synthetic_customer(seed=None):
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
    return {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "dob": fake.date_of_birth(minimum_age=25, maximum_age=85).isoformat(),
        "address": fake.address().replace("\n", ", "),
        "occupation": fake.job(),
    }


if __name__ == "__main__":
    # Smoke test: fetch 3 records, print synthetic customer
    log.info("Fetching 3 FEMA records...")
    records = fetch_fema_page(skip=0, top=3)
    for r in records:
        log.info(f"id={r['id'][:8]}... loss={r.get('dateOfLoss')} state={r.get('state')}")

    log.info("Generating 1 synthetic customer...")
    log.info(make_synthetic_customer(seed=42))