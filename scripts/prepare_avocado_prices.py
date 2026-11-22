import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

csv_path = Path("data/raw/avocado-updated-2020.csv")

if not csv_path.exists():
    print("Generating starter avocado historical price dataset...")
    dates = pd.date_range(start="2018-01-07", end="2020-11-29", freq="W-SUN")
    regions = ["Seattle", "LosAngeles", "SanFrancisco"]
    
    rows = []
    # Seed for reproducibility
    np.random.seed(42)
    
    base_prices = {
        "Seattle": 1.45,
        "LosAngeles": 1.30,
        "SanFrancisco": 1.60
    }
    
    for r in regions:
        base = base_prices[r]
        for d in dates:
            # Add seasonal variation and random fluctuation
            seasonal = 0.25 * np.sin(2 * np.pi * d.dayofyear / 365.25)
            noise = np.random.normal(0, 0.08)
            price = round(max(0.85, base + seasonal + noise), 2)
            rows.append({
                "date": d.strftime("%Y-%m-%d"),
                "average_price": price,
                "total_volume": round(np.random.uniform(500000, 1500000), 2),
                "type": "conventional",
                "geography": r
            })
            
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    print(f"Created {csv_path} with {len(df)} rows.")
else:
    print(f"{csv_path} already exists.")
