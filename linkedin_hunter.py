import requests
import pandas as pd
import re
from datetime import datetime
import os
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

# API keys
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")

# Spreadsheet
INPUT_SHEET_ID = os.getenv("INPUT_SHEET_ID", "")

# Google service account creds (will come from env var)
GOOGLE_CREDS = os.getenv("GOOGLE_CREDS")


# ----------------------------
# Google Sheets Setup
# ----------------------------

def connect_sheets():
    """Connect to Google Sheets using creds from env var."""
    if not GOOGLE_CREDS:
        raise RuntimeError("Missing GOOGLE_CREDS environment variable")

    creds_dict = json.loads(GOOGLE_CREDS)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def append_results(client, sheet_id, worksheet_name, new_rows):
    """Append results to Google Sheet."""
    sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
    rows = [
        [
            r.get("Source", ""),
            r.get("Company", ""),
            r.get("URL", ""),
            r.get("Name", ""),
            r.get("Email", ""),
            r.get("Date Email was added", ""),
            r.get("Position", ""),
            r.get("Linkedin", "")
        ]
        for r in new_rows
    ]
    sheet.append_rows(rows, value_input_option="USER_ENTERED")


# ----------------------------
# Helpers
# ----------------------------

def load_job_titles(filename="job_titles.txt"):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return ["SEO Manager", "Editor in Chief", "Marketing Manager"]

JOB_TITLES = load_job_titles()


def parse_title(title: str):
    """Cleanly split LinkedIn title into name + position."""
    title = title.replace("–", "-").replace("—", "-")
    parts = title.split("-", 1)
    name = parts[0].strip()
    position = ""
    if len(parts) > 1:
        position = re.sub(r"(\||-)?\s*LinkedIn.*", "", parts[1]).strip()
    return name, position


# ----------------------------
# Serper Search
# ----------------------------

def search_profiles(company_name, company_url):
    job_query = " OR ".join([f'"{t}"' for t in JOB_TITLES])
    query = f'site:linkedin.com/in ({job_query}) "{company_name}"'

    def run_search(page=1):
        try:
            res = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 10, "page": page},
                timeout=10
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            print(f"⚠️ Serper error ({company_name}, page {page}): {e}")
            return {}

    results = []
    for page in [1, 2]:
        data = run_search(page)
        for r in data.get("organic", []):
            link = r.get("link", "")
            snippet = r.get("snippet", "").lower()

            if (
                "linkedin.com/in/" in link
                and all(x not in link for x in ["/jobs/", "/posts/", "/company/"])
                and (company_name.lower() in snippet or company_url.lower() in snippet)
            ):
                name, position = parse_title(r.get("title", ""))
                results.append({
                    "Source": "Serper",
                    "Company": company_name,
                    "URL": company_url,
                    "Name": name,
                    "Email": "",
                    "Date Email was added": "",
                    "Position": position,
                    "Linkedin": link,
                })
        if results:
            break

    return results[:3]


# ----------------------------
# Hunter API
# ----------------------------

def enrich_with_hunter(name: str, company_url: str):
    try:
        parts = name.split()
        first, last = "", ""
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            if len(last) <= 1:
                last = ""
        elif parts:
            first = parts[0]

        params = {
            "domain": company_url,
            "first_name": first,
            "api_key": HUNTER_API_KEY,
        }
        if last:
            params["last_name"] = last

        res = requests.get(
            "https://api.hunter.io/v2/email-finder",
            params=params,
            timeout=10
        )
        if res.status_code == 200:
            return res.json().get("data", {}).get("email", "")
        else:
            print(f"⚠️ Hunter Finder error {res.status_code}: {res.text}")
            return ""
    except Exception as e:
        print(f"⚠️ Hunter Finder exception: {e}")
        return ""

def load_input_sheet(client, sheet_id, worksheet_name="Partners"):
    """Load company data from Google Sheet into a DataFrame, even with duplicate headers."""
    sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
    values = sheet.get_all_values()

    if not values:
        return pd.DataFrame(), sheet

    headers = values[0]
    rows = values[1:]

    # Deduplicate headers: URL, URL → URL, URL_1, URL_2
    seen = {}
    unique_headers = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)

    df = pd.DataFrame(rows, columns=unique_headers)
    return df, sheet


def hunter_domain_search(company_url, limit=5):
    try:
        res = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={"domain": company_url, "api_key": HUNTER_API_KEY, "limit": limit},
            timeout=10
        )
        if res.status_code == 200:
            data = res.json().get("data", {})
            emails = data.get("emails", [])
            results = []
            for e in emails:
                results.append({
                    "Source": "Hunter Domain",
                    "Company": data.get("domain", ""),
                    "URL": company_url,
                    "Name": f"{e.get('first_name', '')} {e.get('last_name', '')}".strip(),
                    "Email": e.get("value", ""),
                    "Date Email was added": datetime.now().strftime("%Y-%m-%d") if e.get("value") else "",
                    "Position": e.get("position", ""),
                    "Linkedin": e.get("linkedin", ""),
                })
            return results
        else:
            print(f"⚠️ Hunter Domain error {res.status_code}: {res.text}")
            return []
    except Exception as e:
        print(f"⚠️ Hunter Domain exception: {e}")
        return []


# ----------------------------
# Main pipeline
# ----------------------------

def main():
    client = connect_sheets()

    # Replace with your actual sheet IDs
    INPUT_SHEET_ID = os.getenv("INPUT_SHEET_ID", "")
    OUTPUT_SHEET_ID = INPUT_SHEET_ID


    df, input_sheet = load_input_sheet(client, INPUT_SHEET_ID, "Partners")

    for idx, row in df.iterrows():
        # Check Processed flag
        processed_flag = str(row.get("Processed", "")).strip().lower()
        if processed_flag in ["true", "yes", "1"]:  # robust check
            continue  # ✅ skip already processed rows

        company_name = str(row.get("companyName", "")).strip()
        company_url = str(row.get("URL", "")).strip()

        if not company_name or not company_url:
            continue

        print(f"🔎 Processing {company_name} ({company_url})")
        company_results = []

        # Step 1
        profiles = search_profiles(company_name, company_url)

        # Step 2
        for p in profiles:
            email = enrich_with_hunter(p["Name"], company_url)
            if email:
                p["Email"] = email
                p["Date Email was added"] = datetime.now().strftime("%Y-%m-%d")
            company_results.append(p)

        # Step 3
        if not any(p["Email"] for p in company_results):
            hunter_contacts = hunter_domain_search(company_url, limit=3)
            company_results.extend(hunter_contacts)

        # ✅ Save to Google Sheets immediately
        if company_results:
            append_results(client, OUTPUT_SHEET_ID, "Output", company_results)
            print(f"💾 Saved {len(company_results)} results for {company_name}")

        # ✅ Mark as processed back in Partners sheet
        try:
            col_index = input_sheet.find("Processed").col
            input_sheet.update_cell(idx + 2, col_index, "True")  # +2 because df idx=0 → row 2 in sheet
            print(f"✔️ Marked {company_name} as processed")
        except Exception as e:
            print(f"⚠️ Could not update Processed column for row {idx + 2}: {e}")

    print("✅ Done! Results saved incrementally to Google Sheets")


if __name__ == "__main__":
    main()

