import argparse
import os

import pandas as pd
import requests
from supabase import create_client

API_ENDPOINT = "https://app.scrapeak.com/v1/scrapers/zillow/listing"
LISTING_URLS = ["https://www.zillow.com/homes/for_rent/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.17228467254621%2C%22east%22%3A-71.04147870330793%2C%22south%22%3A42.281685382718244%2C%22north%22%3A42.35822165547899%7D%2C%22customRegionId%22%3A%22c889969c5aX1-CR17vlkqo16r9fp_1322sh%22%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22days%22%7D%2C%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A500%2C%22max%22%3A3200%7D%2C%22beds%22%3A%7B%22min%22%3A2%2C%22max%22%3A2%7D%2C%22baths%22%3A%7B%22min%22%3A1%2C%22max%22%3Anull%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22r4re%22%3A%7B%22value%22%3Afalse%7D%2C%22rad%22%3A%7B%22value%22%3A%222026-09-02%22%7D%2C%22doz%22%3A%7B%22value%22%3A%2230%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%22%22%7D",
                "https://www.zillow.com/homes/for_rent/2_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A2%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.17228467254621%2C%22east%22%3A-71.04147870330793%2C%22south%22%3A42.281685382718244%2C%22north%22%3A42.35822165547899%7D%2C%22customRegionId%22%3A%22c889969c5aX1-CR17vlkqo16r9fp_1322sh%22%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22days%22%7D%2C%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A500%2C%22max%22%3A3200%7D%2C%22beds%22%3A%7B%22min%22%3A2%2C%22max%22%3A2%7D%2C%22baths%22%3A%7B%22min%22%3A1%2C%22max%22%3Anull%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22r4re%22%3A%7B%22value%22%3Afalse%7D%2C%22rad%22%3A%7B%22value%22%3A%222026-09-02%22%7D%2C%22doz%22%3A%7B%22value%22%3A%2230%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%22%22%7D",
                "https://www.zillow.com/homes/for_rent/3_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A3%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.17228467254621%2C%22east%22%3A-71.04147870330793%2C%22south%22%3A42.281685382718244%2C%22north%22%3A42.35822165547899%7D%2C%22customRegionId%22%3A%22c889969c5aX1-CR17vlkqo16r9fp_1322sh%22%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22days%22%7D%2C%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A500%2C%22max%22%3A3200%7D%2C%22beds%22%3A%7B%22min%22%3A2%2C%22max%22%3A2%7D%2C%22baths%22%3A%7B%22min%22%3A1%2C%22max%22%3Anull%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22r4re%22%3A%7B%22value%22%3Afalse%7D%2C%22rad%22%3A%7B%22value%22%3A%222026-09-02%22%7D%2C%22doz%22%3A%7B%22value%22%3A%2230%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%22%22%7D"]


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
    parser.add_argument("--input-json", default=None, help="Path to cached raw results JSON. Skips API call.")
    args = parser.parse_args()

    if args.input_json:
        df = pd.read_json(args.input_json)
        print(f"Loaded {len(df)} rows from {args.input_json}")
    else:
        if not args.confirm_paid_api:
            raise SystemExit("Blocked paid API call. Re-run with --confirm-paid-api or use --input-json.")

        api_key = os.getenv("SCRAPEAK_API_KEY", "")
        if not api_key:
            raise SystemExit("Missing SCRAPEAK_API_KEY environment variable.")

        all_results = []
        for url in LISTING_URLS:
            res = requests.get(API_ENDPOINT, params={"api_key": api_key, "url": url}, timeout=30)
            res.raise_for_status()
            map_results = res.json()["data"]["cat1"]["searchResults"]["mapResults"]
            all_results.extend(map_results)
            print(f"Retrieved {len(map_results)} rows from {url[:60]}...")

        df = pd.json_normalize(all_results)
        df = df.drop_duplicates(subset=["zpid"])
        print(f"Total retrieved: {len(df)} rows")

        # Save raw results for debugging without API calls
        df.to_json("raw_results.json", orient="records", indent=2)
        print("Saved raw results to raw_results.json")

    # Debug: show date distribution before filtering
    if "availabilityDate" in df.columns:
        print("\nAvailability date distribution:")
        print(df["availabilityDate"].value_counts().head(20))
        print()

    out = filter_rows(df, args.target_date)
    print(f"Rows matching {args.target_date}: {len(out)}")

    save_to_supabase(out)
    print(f"Done. {len(out)} listings saved.")


if __name__ == "__main__":
    main()