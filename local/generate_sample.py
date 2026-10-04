"""Generate a small sample NYC taxi CSV with realistic defects.

Runs on plain Python (stdlib only) — no Spark or Azure needed. The defects
mirror what the real TLC data contains, so the local pipeline run exercises
every Silver cleaning rule and DQ check.
"""
import csv
import os
import random

random.seed(42)

OUT = os.path.join(os.path.dirname(__file__), "data", "yellow_sample.csv")
N_ROWS = 300

ZONES = [132, 138, 142, 161, 162, 163, 164, 170, 186, 230, 233, 236, 237, 239]


def good_row(i):
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    pickup = f"2024-01-{(i % 28) + 1:02d} {hour:02d}:{minute:02d}:00"
    dur = random.randint(5, 45)
    dropoff_min = minute + dur
    dh, dm = hour + dropoff_min // 60, dropoff_min % 60
    dropoff = f"2024-01-{(i % 28) + 1:02d} {dh:02d}:{dm:02d}:00"
    distance = round(random.uniform(0.5, 12.0), 2)
    fare = round(2.5 + distance * random.uniform(2.2, 3.0), 2)
    tip = round(fare * random.uniform(0.0, 0.25), 2) if random.random() < 0.7 else 0.0
    return {
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": random.choice([1, 1, 2, 2, 3, 4]),
        "trip_distance": distance,
        "PULocationID": random.choice(ZONES),
        "DOLocationID": random.choice(ZONES),
        "payment_type": random.choice([1, 1, 1, 2]),
        "fare_amount": fare,
        "tip_amount": tip,
    }


def main():
    rows = [good_row(i) for i in range(N_ROWS)]

    # Inject realistic defects (each maps to a Silver rule / DQ check).
    rows[5]["tpep_pickup_datetime"] = None                      # null pickup
    rows[12]["tpep_dropoff_datetime"] = "2024-01-01 08:00:00"    # dropoff...
    rows[12]["tpep_pickup_datetime"] = "2024-01-01 09:30:00"     # ...before pickup
    rows[20]["fare_amount"] = -8.5                               # negative fare
    rows[33]["trip_distance"] = 0.0                              # zero distance
    rows[47]["passenger_count"] = 0                              # zero passengers
    rows[61]["PULocationID"] = None                              # null zone
    rows.append(dict(rows[70]))                                  # exact duplicate
    rows.append(dict(rows[70]))                                  # exact duplicate

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows (with injected defects) to {OUT}")


if __name__ == "__main__":
    main()
