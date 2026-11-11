# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic data
# MAGIC Creates six months (May–October 2026) of raw data for **Parsewell AI**, a fictional startup that sells a
# MAGIC document-parsing API priced per page. All companies and numbers are invented.
# MAGIC
# MAGIC | File | What it represents |
# MAGIC |---|---|
# MAGIC | `customers/customers.csv` | Export from the customer database |
# MAGIC | `api_events/landing_date=*/events.jsonl` | One line per API call, landed daily |
# MAGIC | `model_pricing/model_pricing.csv` | What Parsewell pays its model provider per 1,000 tokens |
# MAGIC | `invoices/invoices.csv` | Export from the billing system |
# MAGIC
# MAGIC The data is deliberately messy, the way real exports are: duplicate events from retries, a few broken
# MAGIC timestamps, inconsistent spellings, internal test traffic, events from deleted accounts and dollar amounts
# MAGIC stored as text. The silver layer has to deal with all of it.
# MAGIC
# MAGIC A fixed random seed makes every run produce the same data.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import csv
import json
import os
import random
import shutil
from datetime import date, datetime, timedelta

rng = random.Random(42)
START, END = date(2026, 5, 1), date(2026, 10, 31)
DAYS = [START + timedelta(d) for d in range((END - START).days + 1)]

# Model provider prices per 1,000 tokens. The large model's price rises 40% on 15 August 2026.
PRICES = {
    "parse-small": [(date(2026, 1, 1), 0.0004, 0.0016)],
    "parse-large": [(date(2026, 1, 1), 0.0030, 0.0150), (date(2026, 8, 15), 0.0042, 0.0210)],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers
# MAGIC Three accounts carry a story for the analysis to find. Everyone else is generated at random.

# COMMAND ----------

STORY = {
    "cus_007": dict(name="Helix Freight", plan="enterprise", base=300_000, fee=12_000, large=0.85, growth=0.36,
                    industry="Logistics"),
    "cus_018": dict(name="Brightpath Health", plan="growth", base=110_000, large=0.08, growth=0.0,
                    industry="Healthcare"),
    "cus_031": dict(name="Northbeam Logistics", plan="free", base=300, large=0.05, growth=0.0,
                    industry="Logistics"),
    "cus_044": dict(name="Quarrystone Analytics", plan="growth", base=40_000, large=0.06, growth=0.03,
                    industry="Financial Services"),
}

FIRST = ["Alder", "Bluefin", "Cinder", "Driftwood", "Ember", "Fernway", "Granite", "Harbor", "Ivory", "Juniper",
         "Kestrel", "Lumen", "Marlow", "Nimbus", "Orchard", "Pebble", "Quill", "Redwood", "Saltmarsh", "Tidewater",
         "Upland", "Vantage", "Willow", "Yarrow", "Zephyr", "Copperleaf", "Meadowlark", "Stonebridge", "Larkspur",
         "Silverpine", "Foxglove", "Brambly", "Clearwater", "Duskfall", "Evergale", "Thistle", "Riverbend",
         "Cobalt", "Highmoor", "Wren", "Sablewood", "Mistral", "Oakhaven", "Palisade", "Ridgeway", "Starling",
         "Tamarack", "Verdant", "Westmere", "Amberly", "Birchline", "Cypress", "Delta Rho", "Eastfield", "Glenrock",
         "Halcyon"]
LAST = ["Labs", "Health", "Legal", "Capital", "Logistics", "Insurance", "Analytics", "Bio", "Systems", "Partners"]
INDUSTRY = {"Labs": "Software", "Health": "Healthcare", "Legal": "Legal", "Capital": "Financial Services",
            "Logistics": "Logistics", "Insurance": "Insurance", "Analytics": "Software", "Bio": "Life Sciences",
            "Systems": "Software", "Partners": "Professional Services"}
REGIONS = ["NA-West", "NA-East", "EMEA", "APAC"]

ids = [f"cus_{i:03d}" for i in range(1, 61)]
others = [c for c in ids if c not in STORY]
rng.shuffle(others)
plan_of = {c: "enterprise" for c in others[:7]}
plan_of.update({c: "growth" for c in others[7:29]})
plan_of.update({c: "free" for c in others[29:]})

names = rng.sample(FIRST, len(others))
customers = {}
for cid in ids:
    if cid in STORY:
        s = STORY[cid]
        customers[cid] = dict(name=s["name"], plan=s["plan"], industry=s["industry"], base=s["base"],
                              large=s["large"], growth=s["growth"], fee=s.get("fee"))
        continue
    plan = plan_of[cid]
    last = rng.choice(LAST)
    c = dict(name=f"{names.pop()} {last}", plan=plan, industry=INDUSTRY[last], fee=None)
    if plan == "enterprise":
        c.update(base=rng.randint(80_000, 450_000), large=rng.uniform(0.25, 0.5),
                 growth=rng.uniform(-0.02, 0.08), fee=rng.choice(range(8_000, 25_001, 500)))
    elif plan == "growth":
        c.update(base=min(150_000, max(5_000, int(rng.lognormvariate(10.0, 0.9)))), large=rng.uniform(0.03, 0.15),
                 growth=rng.uniform(-0.05, 0.10))
    else:
        c.update(base=rng.randint(40, 700), large=0.05, growth=rng.uniform(-0.05, 0.05))
    customers[cid] = c

for cid, c in customers.items():
    c["region"] = rng.choice(REGIONS)
    c["key"] = f"sk_live_{rng.getrandbits(32):08x}"
    c["test_key"] = f"sk_test_{rng.getrandbits(32):08x}"
    c["signup"] = date(2025, 1, 1) + timedelta(days=rng.randint(0, 480))

# COMMAND ----------

# MAGIC %md
# MAGIC ## API events

# COMMAND ----------

def daily_pages(cid, c, day):
    months_in = (day - START).days / 30.4
    expected = c["base"] / 30.4 * (1 + c["growth"]) ** months_in
    expected *= 1.15 if day.weekday() < 5 else 0.6
    if cid == "cus_018" and day >= date(2026, 10, 4):    # Brightpath moves most traffic elsewhere
        expected *= 0.3
    if cid == "cus_031" and day >= date(2026, 8, 1):     # Northbeam outgrows the free plan
        expected *= 13
    if cid == "cus_044" and day >= date(2026, 10, 1):    # Quarrystone rolls out to a new team
        expected *= 2.2
    return expected * rng.uniform(0.85, 1.15)


def error_rate(cid, day):
    return 0.20 if cid == "cus_018" and day >= date(2026, 9, 15) else 0.005


PAGES_PER_CALL = {"enterprise": 400, "growth": 60, "free": 12}

events_by_day = {d: [] for d in DAYS + [END + timedelta(1)]}
billable_truth = {}   # (customer_id, "YYYY-MM") -> pages the billing system charged for
free_used = {}        # (customer_id, "YYYY-MM") -> successful pages, for the free-plan cap


def new_event(cid, day, pages, model, key, status):
    secs = rng.randint(0, 86_399)
    ts = datetime(day.year, day.month, day.day) + timedelta(seconds=secs)
    if status == 200:
        inp = int(pages * 1500 * rng.uniform(0.8, 1.2))
        out = int(pages * 400 * rng.uniform(0.8, 1.2))
    elif status == 500:   # failed partway: the model was called, nothing was returned
        inp, out = int(pages * 1500 * rng.uniform(0.3, 0.7)), 0
    else:                 # 429 quota exceeded: rejected before any model call
        inp, out = 0, 0
    return {
        "event_id": f"evt_{rng.getrandbits(64):016x}",
        "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "customer_id": cid,
        "api_key": key,
        "endpoint": "/v1/parse" if rng.random() < 0.7 else "/v1/extract",
        "model": model,
        "pages": pages,
        "status_code": status,
        "latency_ms": int(rng.lognormvariate(6.5 if model == "parse-large" else 5.8, 0.4)),
        "input_tokens": inp,
        "output_tokens": out,
    }


for day in DAYS:
    month = day.strftime("%Y-%m")
    for cid, c in customers.items():
        expected = daily_pages(cid, c, day)
        n_calls = max(1, min(60, round(expected / PAGES_PER_CALL[c["plan"]])))
        for _ in range(n_calls):
            pages = max(1, round(expected / n_calls * rng.uniform(0.6, 1.4)))
            model = "parse-large" if rng.random() < c["large"] else "parse-small"
            is_test = rng.random() < 0.03
            key = c["test_key"] if is_test else c["key"]
            status = 500 if rng.random() < error_rate(cid, day) else 200
            if status == 200 and c["plan"] == "free" and not is_test:
                used = free_used.get((cid, month), 0)
                if used + pages > FREE_PAGES_PER_MONTH:
                    status = 429
                else:
                    free_used[(cid, month)] = used + pages
            ev = new_event(cid, day, pages, model, key, status)
            if status == 200 and not is_test:
                billable_truth[(cid, month)] = billable_truth.get((cid, month), 0) + pages
            events_by_day[day].append(ev)

    # Parsewell's own QA account hitting production
    for _ in range(rng.randint(30, 50)):
        events_by_day[day].append(new_event("cus_000", day, rng.randint(5, 50), "parse-small", "sk_live_qa000000", 200))

    # Calls from accounts that were deleted from the customer database
    for _ in range(rng.randint(2, 6)):
        events_by_day[day].append(new_event(f"cus_{rng.randint(61, 99):03d}", day, rng.randint(1, 80),
                                            "parse-small", f"sk_live_{rng.getrandbits(32):08x}", 200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make the logs messy

# COMMAND ----------

MODEL_SPELLINGS = {"parse-large": ["Parse-Large", "parse_large", "PARSE-LARGE"],
                   "parse-small": ["Parse-Small", "parse_small", "PARSE-SMALL"]}

for day in DAYS:
    todays = events_by_day[day]
    for ev in todays:
        r = rng.random()
        if r < 0.005:      # an upstream service wrote US-style timestamps (recoverable)
            ts = datetime.strptime(ev["event_ts"], "%Y-%m-%dT%H:%M:%SZ")
            ev["event_ts"] = ts.strftime("%m/%d/%Y %H:%M:%S")
        elif r < 0.007:    # timestamp lost entirely (not recoverable)
            ev["event_ts"] = rng.choice([None, "", "N/A"])
        if rng.random() < 0.03:
            ev["model"] = rng.choice(MODEL_SPELLINGS[ev["model"]])
        if ev["status_code"] == 200 and rng.random() < 0.002:   # corrupted page counts
            ev["pages"] = rng.choice([0, -1, -3])
    retries = [dict(ev) for ev in todays if rng.random() < 0.02]          # client retries: same event_id twice
    late = [ev for ev in todays if rng.random() < 0.01]                   # events that land a day late
    for ev in late:
        todays.remove(ev)
    events_by_day[day + timedelta(1)].extend(late)
    todays.extend(retries)
    rng.shuffle(todays)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the files

# COMMAND ----------

for sub in ["customers", "api_events", "model_pricing", "invoices"]:
    path = f"{RAW_PATH}/{sub}"
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

# Customers, including Parsewell's internal account and a few stale duplicate rows
plan_spellings = lambda p: rng.choice([p, p, p.title(), p.upper() + " ", " " + p])
with open(f"{RAW_PATH}/customers/customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "company_name", "plan", "industry", "region", "signup_date",
                "monthly_contract_usd", "updated_at"])
    w.writerow(["cus_000", "Parsewell Internal QA", "internal", "Software", "NA-West", "2024-11-01", "",
                "2025-01-01 09:00:00"])
    for cid, c in customers.items():
        region = "" if rng.random() < 0.1 else c["region"]
        w.writerow([cid, c["name"], plan_spellings(c["plan"]), c["industry"], region, c["signup"].isoformat(),
                    c["fee"] or "", "2026-04-20 08:00:00"])
    for cid in rng.sample([c for c in ids if customers[c]["plan"] == "growth" and c not in STORY], 3):
        c = customers[cid]   # older row from before the customer upgraded
        w.writerow([cid, c["name"], "free", c["industry"], c["region"], c["signup"].isoformat(), "",
                    "2025-06-01 08:00:00"])

with open(f"{RAW_PATH}/model_pricing/model_pricing.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["model", "effective_from", "input_cost_per_1k_tokens", "output_cost_per_1k_tokens"])
    for model, rows in PRICES.items():
        for eff, i, o in rows:
            w.writerow([model, eff.isoformat(), i, o])

# Invoices: Growth bills per page, Enterprise bills a flat monthly fee, Free is never invoiced
months = sorted({d.strftime("%Y-%m") for d in DAYS})
n = 0
with open(f"{RAW_PATH}/invoices/invoices.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["invoice_id", "customer_id", "invoice_month", "amount", "status"])
    for m in months:
        for cid, c in customers.items():
            if c["plan"] == "growth":
                amount = round(billable_truth.get((cid, m), 0) * GROWTH_PRICE_PER_PAGE, 2)
            elif c["plan"] == "enterprise":
                amount = float(c["fee"])
            else:
                continue
            n += 1
            text = f"${amount:,.2f}" if rng.random() < 0.7 else f"{amount:.2f}"
            w.writerow([f"inv_{n:05d}", cid, m, text, "open" if m == "2026-10" else "paid"])

total = 0
for day, evs in events_by_day.items():
    if not evs:
        continue
    folder = f"{RAW_PATH}/api_events/landing_date={day.isoformat()}"
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/events.jsonl", "w") as f:
        for ev in evs:
            f.write(json.dumps(ev) + "\n")
    total += len(evs)

print(f"Wrote {len(customers) + 1} customers, {total:,} API events and {n} invoices to {RAW_PATH}")
