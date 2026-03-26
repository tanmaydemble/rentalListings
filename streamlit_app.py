import os
from urllib.parse import quote_plus

import pandas as pd
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components
from supabase import create_client

BASE = "https://www.zillow.com"
NEU_ADDRESS = "Northeastern University, Boston, MA"
PAGE_SIZE = 15

ZIPCODE_NEIGHBORHOOD = {
    # Somerville
    "02143": "Somerville",
    "02144": "Somerville",
    "02145": "Somerville",
    # Cambridge
    "02138": "Cambridge",
    "02139": "Cambridge",
    "02140": "Cambridge",
    "02141": "Cambridge",
    # Kendall Square
    "02142": "Kendall Square",
    # Charlestown / Bunker Hill
    "02129": "Charlestown / Bunker Hill",
    # East Boston (includes Maverick and Paris Street)
    "02128": "East Boston",
    # Back Bay
    "02116": "Back Bay",
    "02199": "Back Bay",
    # Brookline / Coolidge Corner
    "02445": "Coolidge Corner",
    "02446": "Brookline",
    "02447": "Brookline",
    # Fenway / Kenmore
    "02115": "Fenway / Kenmore",
    "02215": "Fenway / Kenmore",
    # Jamaica Plain
    "02130": "Jamaica Plain",
}

st.set_page_config(page_title="Listings", layout="wide")
st.title("Sept 1 Rental Listings")


@st.cache_data(ttl=86400)
def load_listings() -> pd.DataFrame:
    url = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY", "")
    if not url or not key:
        st.error("Missing SUPABASE_URL or SUPABASE_KEY.")
        st.stop()
    client = create_client(url, key)
    response = client.table("listings").select("*").execute()
    return pd.DataFrame(response.data)


def zillow(url: str) -> str:
    if not url:
        return BASE
    return url if url.startswith("http") else f"{BASE}{url}"


def transit_to_neu_url(origin_address: str) -> str:
    origin = quote_plus(origin_address)
    destination = quote_plus(NEU_ADDRESS)
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={origin}&destination={destination}&travelmode=transit"
    )


def get_map_points(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["latitude"] = pd.to_numeric(out.get("latLong.latitude"), errors="coerce")
    out["longitude"] = pd.to_numeric(out.get("latLong.longitude"), errors="coerce")
    fallback_lat = pd.to_numeric(out.get("hdpData.homeInfo.latitude"), errors="coerce")
    fallback_lon = pd.to_numeric(out.get("hdpData.homeInfo.longitude"), errors="coerce")
    out["latitude"] = out["latitude"].fillna(fallback_lat)
    out["longitude"] = out["longitude"].fillna(fallback_lon)
    return out.dropna(subset=["latitude", "longitude"])


def selected_zpid_from_map_event(event) -> str | None:
    if not event:
        return None
    selection = getattr(event, "selection", None)
    if not selection:
        return None

    def _extract_from_obj(obj):
        if isinstance(obj, dict):
            if obj.get("zpid") is not None:
                return str(obj["zpid"])
            if obj.get("object") and isinstance(obj["object"], dict):
                inner = obj["object"]
                if inner.get("zpid") is not None:
                    return str(inner["zpid"])
        return None

    if isinstance(selection, dict):
        if isinstance(selection.get("objects"), list) and selection["objects"]:
            zpid = _extract_from_obj(selection["objects"][0])
            if zpid:
                return zpid
        for value in selection.values():
            if isinstance(value, dict):
                if isinstance(value.get("objects"), list) and value["objects"]:
                    zpid = _extract_from_obj(value["objects"][0])
                    if zpid:
                        return zpid
                if isinstance(value.get("indices"), list) and value["indices"]:
                    idx = int(value["indices"][0])
                    mapped = st.session_state.get("_mapped_points")
                    if mapped is not None and 0 <= idx < len(mapped):
                        return str(mapped.iloc[idx].get("zpid"))
            if isinstance(value, list) and value:
                first = value[0]
                if isinstance(first, dict):
                    zpid = _extract_from_obj(first)
                    if zpid:
                        return zpid
    return None


def render_listing_card(r: pd.Series, avg_price: float) -> None:
    with st.container(border=True):
        c1, c2 = st.columns([1, 2])
        with c1:
            if r.get("imgSrc"):
                st.image(r["imgSrc"], use_container_width=True)
        with c2:
            st.subheader(r.get("hdpData.homeInfo.streetAddress", "Address unavailable"))
            zipcode = str(r.get("hdpData.homeInfo.zipcode", "")).strip()
            neighborhood = ZIPCODE_NEIGHBORHOOD.get(zipcode, r.get("hdpData.homeInfo.city", ""))
            st.caption(f"{neighborhood} · {zipcode}")
            origin = (
                f"{r.get('hdpData.homeInfo.streetAddress', '')}, "
                f"{r.get('hdpData.homeInfo.city', '')}, MA {zipcode}"
            )
            price_val = r.get("price")
            price_str = "N/A" if pd.isna(price_val) else f"${price_val:,.0f}/mo"
            st.write(f"Price: {price_str}")
            st.write(f"Beds: {r.get('beds', 'N/A')} | Baths: {r.get('baths', 'N/A')} | Area: {r.get('area', 'N/A')}")
            if pd.notna(r.get("price")) and pd.notna(avg_price) and avg_price:
                pct = (r["price"] - avg_price) / avg_price * 100
                st.write(f"Vs average: {pct:+.1f}%")
            if pd.notna(r.get("price")) and pd.notna(r.get("area")) and r.get("area"):
                st.write(f"Price/sqft: ${r['price'] / r['area']:.2f}")
            st.write(f"Available: {str(r.get('availabilityDate', 'N/A'))[:10]}")
            st.link_button("Open Zillow / Book Tour", zillow(r.get("detailUrl", "")))
            st.link_button("Transit to Northeastern (Google Maps)", transit_to_neu_url(origin))


# --- Load data ---
df = load_listings()

if df.empty:
    st.warning("No listings found in database.")
    st.stop()

df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
df["area"] = pd.to_numeric(df.get("area"), errors="coerce")
df["zipcode_str"] = df["hdpData.homeInfo.zipcode"].astype(str).str.strip()
df["neighborhood"] = df["zipcode_str"].map(ZIPCODE_NEIGHBORHOOD).fillna(df.get("hdpData.homeInfo.city", ""))

avg_price = df["price"].mean()

# --- Filters ---
st.subheader("Filters")
col_price, col_neighborhood, col_sort = st.columns([1, 2, 1])

with col_price:
    max_price_input = st.text_input("Max price ($/mo)", placeholder="e.g. 3000")
    try:
        max_price = float(max_price_input) if max_price_input.strip() else None
    except ValueError:
        st.warning("Enter a valid number for max price.")
        max_price = None

with col_neighborhood:
    available_zips = df["zipcode_str"].dropna().unique().tolist()
    neighborhood_options = sorted(set(
        ZIPCODE_NEIGHBORHOOD.get(z, z) for z in available_zips
    ))
    selected_neighborhoods = st.multiselect(
        "Neighborhoods",
        options=neighborhood_options,
        placeholder="All neighborhoods",
    )

with col_sort:
    sort_order = st.selectbox("Sort by price", ["Default", "Low to High", "High to Low"])

# --- Apply filters ---
filtered_df = df.copy()

if max_price is not None:
    filtered_df = filtered_df[filtered_df["price"].isna() | (filtered_df["price"] <= max_price)]

if selected_neighborhoods:
    filtered_df = filtered_df[filtered_df["neighborhood"].isin(selected_neighborhoods)]

if sort_order == "Low to High":
    filtered_df = filtered_df.sort_values("price", ascending=True, na_position="last")
elif sort_order == "High to Low":
    filtered_df = filtered_df.sort_values("price", ascending=False, na_position="last")

filtered_df = filtered_df.reset_index(drop=True)

st.write(f"Showing {len(filtered_df)} of {len(df)} listings · Average price: {'N/A' if pd.isna(avg_price) else f'${avg_price:,.0f}'}")

if filtered_df.empty:
    st.warning("No listings match your filters.")
    st.stop()

# --- Pagination state ---
if "page" not in st.session_state:
    st.session_state["page"] = 0

# Reset to page 0 when filters change
filter_key = (max_price_input, tuple(selected_neighborhoods), sort_order)
if st.session_state.get("_last_filter_key") != filter_key:
    st.session_state["page"] = 0
    st.session_state["_last_filter_key"] = filter_key

total_pages = max(1, -(-len(filtered_df) // PAGE_SIZE))
st.session_state["page"] = min(st.session_state["page"], total_pages - 1)
start = st.session_state["page"] * PAGE_SIZE
page_df = filtered_df.iloc[start: start + PAGE_SIZE]

# --- Map (current page only) ---
mapped = get_map_points(page_df)
if mapped.empty:
    st.warning("No coordinates available for map view.")
else:
    st.subheader("Map")
    mapped = mapped.copy()
    mapped["map_id"] = range(start + 1, start + len(mapped) + 1)
    mapped["zpid"] = mapped["zpid"].astype(str)
    mapped["address"] = (
        mapped.get("hdpData.homeInfo.streetAddress", pd.Series(dtype=str)).fillna("")
        + ", "
        + mapped.get("hdpData.homeInfo.city", pd.Series(dtype=str)).fillna("")
        + " "
        + mapped.get("hdpData.homeInfo.zipcode", pd.Series(dtype=str)).fillna("")
    )
    mapped["price_text"] = mapped["price"].apply(
        lambda x: "N/A" if pd.isna(x) else f"${x:,.0f}/mo"
    )

    center_lat = float(mapped["latitude"].mean())
    center_lon = float(mapped["longitude"].mean())

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=mapped,
        get_position="[longitude, latitude]",
        get_radius=12,
        get_fill_color=[220, 38, 38, 170],
        pickable=True,
        stroked=True,
        get_line_color=[120, 20, 20, 220],
        line_width_min_pixels=1,
    )

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=13,
        pitch=0,
    )

    st.session_state["_mapped_points"] = mapped.reset_index(drop=True)
    map_event = st.pydeck_chart(
        pdk.Deck(
            map_provider="carto",
            map_style=pdk.map_styles.CARTO_LIGHT,
            initial_view_state=view_state,
            layers=[layer],
            tooltip={
                "html": "<b>#{map_id}</b><br/>{address}<br/>Price: {price_text}",
                "style": {"backgroundColor": "#111827", "color": "white"},
            },
        ),
        key="listing_map",
        on_select="rerun",
        selection_mode="single-object",
    )

    st.caption("Hover a dot to see listing details.")

    jump_options = {
        f"#{int(row.map_id)} - {row.address}": str(row.zpid)
        for row in mapped.itertuples(index=False)
    }
    jump_labels = list(jump_options.keys())

    selected_index = 0
    current_selected = st.session_state.get("selected_zpid")
    if current_selected:
        for i, label in enumerate(jump_labels):
            if jump_options[label] == str(current_selected):
                selected_index = i
                break

    chosen_label = st.selectbox("Jump to listing", jump_labels, index=selected_index)
    if st.button("Jump to listing"):
        chosen_zpid = jump_options[chosen_label]
        st.session_state["selected_zpid"] = chosen_zpid
        st.session_state["jump_to_zpid"] = chosen_zpid

    selected_zpid = selected_zpid_from_map_event(map_event)
    if selected_zpid:
        st.session_state["selected_zpid"] = selected_zpid
        st.session_state["jump_to_zpid"] = selected_zpid


def pagination_controls(key_suffix: str) -> None:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("← Previous", key=f"prev_{key_suffix}", disabled=st.session_state["page"] == 0):
            st.session_state["page"] -= 1
            st.rerun()
    with col2:
        st.markdown(
            f"<div style='text-align:center; padding-top: 8px;'>Page {st.session_state['page'] + 1} of {total_pages}</div>",
            unsafe_allow_html=True,
        )
    with col3:
        if st.button("Next →", key=f"next_{key_suffix}", disabled=st.session_state["page"] >= total_pages - 1):
            st.session_state["page"] += 1
            st.rerun()


pagination_controls("top")

for _, r in page_df.iterrows():
    row_zpid = str(r.get("zpid", ""))
    st.markdown(f"<div id='listing-{row_zpid}'></div>", unsafe_allow_html=True)
    if st.session_state.get("selected_zpid") == row_zpid:
        st.info("Selected from map")
    render_listing_card(r, avg_price)

pagination_controls("bottom")

jump_to = st.session_state.get("jump_to_zpid")
if jump_to:
    components.html(
        f"""
        <script>
        const target = window.parent.document.getElementById('listing-{jump_to}');
        if (target) target.scrollIntoView({{behavior: 'smooth', block: 'start'}});
        </script>
        """,
        height=0,
    )
    st.session_state["jump_to_zpid"] = None