import argparse
import os

import pandas as pd
import requests
from supabase import create_client

API_ENDPOINT = "https://app.scrapeak.com/v1/scrapers/zillow/listing"
# LISTING_URL_OLD = "https://www.zillow.com/homes/for_rent/?searchQueryState=%7B%22mapBounds%22%3A%7B%22west%22%3A-71.14007799772557%2C%22east%22%3A-71.09412100227442%2C%22south%22%3A42.280931197058244%2C%22north%22%3A42.33774778867289%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A154795%2C%22regionType%22%3A8%7D%5D%2C%22filterState%22%3A%7B%22isForSaleByAgent%22%3A%7B%22value%22%3Afalse%7D%2C%22isComingSoon%22%3A%7B%22value%22%3Afalse%7D%2C%22isAuction%22%3A%7B%22value%22%3Afalse%7D%2C%22isForSaleForeclosure%22%3A%7B%22value%22%3Afalse%7D%2C%22isNewConstruction%22%3A%7B%22value%22%3Afalse%7D%2C%22isForSaleByOwner%22%3A%7B%22value%22%3Afalse%7D%2C%22isForRent%22%3A%7B%22value%22%3Atrue%7D%2C%22isManufactured%22%3A%7B%22value%22%3Afalse%7D%2C%22isLotLand%22%3A%7B%22value%22%3Afalse%7D%2C%22isMultiFamily%22%3A%7B%22value%22%3Afalse%7D%2C%22monthlyPayment%22%3A%7B%22max%22%3A3500%7D%2C%22beds%22%3A%7B%22min%22%3A2%2C%22max%22%3A2%7D%2C%22baths%22%3A%7B%22min%22%3A1.0%7D%2C%22onlyRentalRequestedAvailabilityDate%22%3A%7B%22value%22%3A%222026-09-02%22%7D%2C%22isEntirePlaceForRent%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22savedSearchEnrollmentId%22%3A%22X1-SS50jb7wqdn8id1000000000_5w3uk%22%7D"
LISTING_URL = "https://www.zillow.com/homes/for_rent/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.17220855337914%2C%22east%22%3A-71.0056970177346%2C%22south%22%3A42.259776529885116%2C%22north%22%3A42.39468302306475%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22personalizedsort%22%7D%2C%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A500%2C%22max%22%3A3700%7D%2C%22beds%22%3A%7B%22min%22%3A2%2C%22max%22%3A2%7D%2C%22baths%22%3A%7B%22min%22%3A1%2C%22max%22%3Anull%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22r4re%22%3A%7B%22value%22%3Afalse%7D%2C%22rad%22%3A%7B%22value%22%3A%222026-09-02%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22customRegionId%22%3A%22b298669c57X1-CR7t5ksocawhv7_vztxy%22%2C%22pagination%22%3A%7B%7D%2C%22usersSearchTerm%22%3A%22%22%7D"

def filter_rows(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = df.copy()
    df["availabilityDate"] = pd.to_datetime(df["availabilityDate"], errors="coerce").dt.date
    df = df[df["availabilityDate"] == pd.Timestamp(target_date).date()].copy()

    nested = pd.to_numeric(df.get("hdpData.homeInfo.price"), errors="coerce")
    text = pd.to_numeric(
        df.get("price", pd.Series(index=df.index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.replace(r"[^0-9.]", "", regex=True)
        .replace("", pd.NA),
        errors="coerce",
    )
    df["price"] = nested.fillna(text)

    avg = df["price"].mean()
    df["price_vs_avg"] = ((df["price"] - avg) / avg * 100).round(1) if pd.notna(avg) and avg else pd.NA

    cols = [
        "zpid",
        "imgSrc",
        "detailUrl",
        "price",
        "beds",
        "baths",
        "area",
        "latLong.latitude",
        "latLong.longitude",
        "hdpData.homeInfo.latitude",
        "hdpData.homeInfo.longitude",
        "hdpData.homeInfo.streetAddress",
        "hdpData.homeInfo.city",
        "hdpData.homeInfo.zipcode",
        "price_vs_avg",
        "availabilityDate",
    ]
    return df[[c for c in cols if c in df.columns]].copy()


def save_to_supabase(df: pd.DataFrame) -> None:
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")

    client = create_client(url, key)

    records = df.copy()
    records["availabilityDate"] = records["availabilityDate"].astype(str)
    records["zpid"] = records["zpid"].astype(str)

    rows = []
    for record in records.to_dict(orient="records"):
        cleaned = {}
        for k, v in record.items():
            if isinstance(v, float) and (pd.isna(v) or v == float("inf") or v == float("-inf")):
                cleaned[k] = None
            else:
                cleaned[k] = v
        rows.append(cleaned)

    client.table("listings").upsert(rows).execute()
    print(f"Upserted {len(rows)} rows to Supabase")


def main() -> None:
    parser = argparse.ArgumentParser(description="Call API, filter by date, and save to Supabase.")
    parser.add_argument("--target-date", default="2026-09-01")
    parser.add_argument("--confirm-paid-api", action="store_true")
    args = parser.parse_args()

    if not args.confirm_paid_api:
        raise SystemExit("Blocked paid API call. Re-run with --confirm-paid-api.")

    api_key = os.getenv("SCRAPEAK_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing SCRAPEAK_API_KEY environment variable.")

    res = requests.get(API_ENDPOINT, params={"api_key": api_key, "url": LISTING_URL}, timeout=30)
    res.raise_for_status()
    map_results = res.json()["data"]["cat1"]["searchResults"]["mapResults"]
    df = pd.json_normalize(map_results)

    out = filter_rows(df, args.target_date)
    save_to_supabase(out)
    print(f"Done. {len(out)} listings saved.")


if __name__ == "__main__":
    main()