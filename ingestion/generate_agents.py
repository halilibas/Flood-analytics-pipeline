"""
Generate a small syntethic agent pool
"""
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import random

import pandas as pd
from faker import Faker

NUM_AGENTS = 75
SEED = 42
OUTPUT_PATH = Path("/Users/halil/Desktop/data/synthetic/agents.parquet").expanduser()
PIPELINE_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc)

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)

# Agents biased toward high-claim states (LA, FL, TX, NJ, NY) but with some spread
state_pool = (
    ["LA"] * 12 + ["FL"] * 12 + ["TX"] * 10 + ["NJ"] * 7 + ["NY"] * 6
    + ["NC"] * 4 + ["PA"] * 4 + ["MS"] * 3 + ["SC"] * 3 + ["AL"] * 3
    + ["VA"] * 3 + ["MA"] * 2 + ["IL"] * 2 + ["MO"] * 2 + ["CA"] * 2
)

def generate_agent():
    state = random.choice(state_pool)
    return {
        "agent_id": str(uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "agency_name": f"{fake.last_name()} & {fake.last_name()} Insurance",
        "agency_state": state,
        "email": fake.company_email(),
        "phone": fake.phone_number(),
        "hire_date": fake.date_between(start_date="-25y", end_date="-1y"),
        "commision_rate": round(random.uniform(0.05, 0.15), 4)
    }

def main():
    agents = [generate_agent() for _ in range(NUM_AGENTS)]
    df = pd.DataFrame(agents)
    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = "faker:agents"
    df["_pipeline_run_id"] = PIPELINE_RUN_ID
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote {len(df):,} agents to {OUTPUT_PATH}")
    print(f"State distribution:\n{df['agency_state'].value_counts().head(10)}")

if __name__ == "__main__":
    main()