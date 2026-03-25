# Rental Listings Viewer

Small pipeline + Streamlit app:

- `sept1_listings.py`: calls API, filters Sept 1 listings, saves JSON/CSV.
- `streamlit_app.py`: displays listings, map, Zillow links, and transit link to Northeastern.

## Run locally

1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run app:
   ```bash
   streamlit run streamlit_app.py
   ```

## Refresh data (paid API)

Set your API key and run:

```bash
SCRAPEAK_API_KEY="your-key" python sept1_listings.py --confirm-paid-api
```

This updates:

- `sept1_filtered_listings.json`
- `sept1_filtered_listings.csv`

