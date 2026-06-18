"""
Generate one synthetic customer per FEMA claim, keye by fema_claim_id.
Customers state match the FEMA claim's state (they own the property).

"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import uuid4
import random
import pandas as pd
from faker import Faker

SEED = 42
FEMA_CSV = Path("/Users/halil/Desktop/data/raw/FimaNfipClaimsV2.csv").expanduser()
OUTPUT_PATH = Path("/Users/halil/Desktop/data/synthetic/customers.parquet").expanduser()
PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)

def main():
    # Read only what we need from FEMA
    fema = pd.read_csv(
        FEMA_CSV,
        usecols=["id", "dateOfLoss", "state"],
        parse_dates=["dateOfLoss"],
        low_memory=False
    )
    fema = fema.rename(columns={"id": "fema_claim_id"})
    print(f"Loaded {len(fema):,} FEMA claims as seed")
    
    # Generate one customer per claim. Vectorize what we can.
    n = len(fema)
    customer_ids = [str(uuid4()) for _ in range(n)]
    first_names = [fake.first_name() for _ in range(n)]
    last_names = [fake.last_name() for _ in range(n)]
    emails = [fake.email() for _ in range(n)]
    phones = [fake.phone_number() for _ in range(n)]
    occupations = [fake.job() for _ in range(n)]
    address_lines = [fake.street_address() for _ in range(n)]
    
    # DOBs: 25-85 years old at dateofLoss
    dobs = []
    for loss_date in fema["dateOfLoss"]:
        if pd.isna(loss_date):
            dob = fake.date_of_birth(minimum_age=25, maximum_age=85)
        else:
            min_age = 25
            max_age = 85
            earliest = (loss_date - pd.Timedelta(days=365 * max_age)).date()
            latest = (loss_date - pd.Timedelta(days=365 * min_age)).date()
            dob = fake.date_between_dates(date_start=earliest, date_end=latest) 
        dobs.append(dob)
        
    customers = pd.DataFrame({
        "customer_id": customer_ids,
        "fema_claim_id": fema["fema_claim_id"].values,
        "first_name": first_names,
        "last_name": last_names,
        "dob": dobs,
        "email": emails,
        "phone": phones,
        "address_line_1": address_lines,
        "address_state": fema["state"].values,
        "occupation": occupations,
    })
    customers["_ingested_at"] = INGESTED_AT
    customers["_source_file"] = "faker:customers"
    customers["_pipeline_run_id"] = PIPELINE_RUN_ID
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote {len(customers):,} customers to {OUTPUT_PATH}")
    print(f"Customer state distribution (top 5):\n{customers['address_state'].value_counts().head}")
         

if __name__ == "__main__":
    main()