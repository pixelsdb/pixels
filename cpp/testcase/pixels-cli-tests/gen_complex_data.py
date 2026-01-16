import os
import random
from datetime import datetime, timedelta

# Create output directory
os.makedirs("test_origin", exist_ok=True)

def random_date(start_year=1970, end_year=2023):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def maybe_null(value, null_probability=0.1):
    """Return empty string (represents null) with given probability, otherwise return value"""
    if random.random() < null_probability:
        return "\\N"  # Common CSV representation for null
    return str(value)

# Configuration
NUM_ROWS = 100
NULL_PROBABILITY = 0.15  # 15% chance of null for each nullable column
OUTPUT_FILE = "test_origin/complex_data_with_nulls.csv"

with open(OUTPUT_FILE, "w") as f:
    for i in range(NUM_ROWS):
        id_val = i  # Keep id non-null as it's often a primary key
        index_val = 1000000 + i
        name_val = f"user_{i}"
        birth_val = random_date().strftime("%Y-%m-%d")
        updated_val = (datetime.now() - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        price_val = round(19.99 + i, 2)
        
        # Apply null probability to each column (except id)
        row = [
            str(id_val),  # id: keep non-null
            maybe_null(index_val, NULL_PROBABILITY),  # index: nullable
            maybe_null(name_val, NULL_PROBABILITY),   # name: nullable
            maybe_null(birth_val, NULL_PROBABILITY),  # birth: nullable
            maybe_null(updated_val, NULL_PROBABILITY),# updated: nullable
            maybe_null(price_val, NULL_PROBABILITY)   # price: nullable
        ]
        f.write(",".join(row) + "\n")

print(f"Complex test data with nulls generated in {OUTPUT_FILE}")
print(f"Total rows: {NUM_ROWS}, Null probability: {NULL_PROBABILITY*100}%")

# Also generate a version without nulls for comparison
OUTPUT_FILE_NO_NULLS = "test_origin/complex_data2.csv"
with open(OUTPUT_FILE_NO_NULLS, "w") as f:
    for i in range(NUM_ROWS):
        id_val = i
        index_val = 1000000 + i
        name_val = f"user_{i}"
        birth_val = random_date().strftime("%Y-%m-%d")
        updated_val = (datetime.now() - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        price_val = round(19.99 + i, 2)
        
        row = [
            str(id_val), str(index_val), 
            name_val, birth_val, updated_val, str(price_val)
        ]
        f.write(",".join(row) + "\n")

print(f"Complex test data (no nulls) generated in {OUTPUT_FILE_NO_NULLS}")
