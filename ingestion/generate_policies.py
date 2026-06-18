"""
Generate one synthetic policy per FEMA claim.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import uuid4
import random
import pandas as pd
import numpy as np
from faker import Faker

SEED = 42
FEMA_CSV = Path("/Users/halil/Desktop/data/raw/FimaNfipClaimsV2.csv").expanduser()
AGENTS_PARQUET = Path("/Users/halil/Desktop/data/synthetic/agents.parquet").expanduser()
CUSTOMERS_PARQUET = Path("/Users/halil/Desktop/data/synthetic/customers.parquet").expanduser()
OUTPUT_PATH = Path("/Users/halil/Desktop/data/synthetic/policies.parquet").expanduser()
PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)

"""
Flood zone based premoum rate multipliers
Higher risk zones priced higher
"""
NFIP_BUILDING_CAP = 250_000
NFIP_CONTENTS_CAP = 100_000

ZONE_RATE_PER_100 = { 
    "VE": 1.50, "V": 1.50,
    "AE": 0.80, "A": 0.80, "AO": 0.75, "AH": 0.75,
    "X": 0.35,  "B": 0.35, "C": 0.35,
}

DEFAULT_RATE_PER_100 = 0.60

def premium_for(building_cov, contents_cov, flood_zone):
    """Compute plausible annual premium, capped at NFIP statutory coverage limits."""
    try:
        b = min(float(building_cov), NFIP_BUILDING_CAP) if pd.notna(building_cov) else 0.0
        c = min(float(contents_cov), NFIP_CONTENTS_CAP) if pd.notna(contents_cov) else 0.0
    except (TypeError, ValueError):
        return None
    if b + c == 0:
        return None
    zone_prefix = str(flood_zone)[:2].upper() if pd.notna(flood_zone) else "X"
    rate = ZONE_RATE_PER_100.get(zone_prefix, DEFAULT_RATE_PER_100)
    base = (b + c) / 100.0 * rate
    noise = np.random.uniform(0.92, 1.08)
    return round(base * noise + 50, 2)

def main():
    #Load inputs
    fema = pd.read_csv(
        FEMA_CSV,
        usecols=[
            "id", "dateOfLoss", "state",
            "totalBuildingInsuranceCoverage", "totalContentsInsuranceCoverage",
            "ratedFloodZone",
        ],
        parse_dates=["dateOfLoss"],
        low_memory=False,
    ).rename(columns={"id": "fema_claim_id"})
    print(f"Loaded {len(fema):,} FEMA claims")
    
    customers = pd.read_parquet(CUSTOMERS_PARQUET)[["customer_id", "fema_claim_id"]]
    print(f"Loaded {len(customers):,} customers")
    
    agents = pd.read_parquet(AGENTS_PARQUET)[["agent_id", "agency_state"]]
    print(f"Loaded {len(agents):,} agents")
    
    # Agent pools by state for biased selection
    agents_by_state = agents.groupby("agency_state")["agent_id"].apply(list).to_dict()
    all_agent_ids = agents["agent_id"].tolist()
    
    def pick_agent(claim_state):
        # 70% chance same state agent, 30% any agents
        if claim_state in agents_by_state and random.random() < 0.7:
            return random.choice(agents_by_state[claim_state])
        return random.choice(all_agent_ids)
    
    # Merge customer_id onto claims
    merged = fema.merge(customers, on="fema_claim_id", how="left")
    n = len(merged)
    
    # Effective date: 30-700 days before loss; expiration = effective + 365
    days_offset = np.random.randint(30, 700, size=n)
    effective_dates = merged["dateOfLoss"] - pd.to_timedelta(days_offset, unit="D")
    expiration_dates = effective_dates + pd.Timedelta(days=365)
    
    # Policy numbers
    policy_years = effective_dates.dt.year
    policy_numbers = [
        f"NFIP-{int(y) if pd.notna(y) else 0}-{i:08d}"
        for i, y in enumerate(policy_years)
    ]
    
    # Agent assignment
    agent_ids = [pick_agent(s) for s in merged["state"]]
    
    # Premiums
    premiums = [
        premium_for(b, c, z)
        for b, c, z in zip(
            merged["totalBuildingInsuranceCoverage"],
            merged["totalContentsInsuranceCoverage"],
            merged["ratedFloodZone"],
        )
    ]
    
    # Coverage type
    coverage_type = np.where(
        merged["totalContentsInsuranceCoverage"].fillna(0).astype(float) > 0,
        "Building + Contents",
        "Building Only",
    )
    
    policies = pd.DataFrame({
        "policy_number": policy_numbers,
        "fema_claim_id": merged["fema_claim_id"].values,
        "customer_id": merged["customer_id"].values,
        "agent_id": agent_ids,
        "effective_date": effective_dates.dt.date,
        "expiration_date": expiration_dates.dt.date,
        "building_coverage": merged["totalBuildingInsuranceCoverage"].values,
        "contents_coverage": merged["totalContentsInsuranceCoverage"].values,
        "deductible_amount": 1000,
        "annual_premium": premiums,
        "coverage_type": coverage_type 
    })
    policies["_ingested_at"] = INGESTED_AT
    policies["_source_file"] = "faker:policies"
    policies["_pipeline_run_id"] = PIPELINE_RUN_ID
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    policies.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote {len(policies):,} policies to {OUTPUT_PATH}")
    print(f"\nPremium distribution stats:\n{policies['annual_premium'].describe()}")
    print(f"\nCoverage type distribution:\n{policies['coverage_type'].value_counts()}")
    print(f"\nSample policy:\n{policies.iloc[0].to_dict()}")

if __name__ == "__main__":
    main()
    
