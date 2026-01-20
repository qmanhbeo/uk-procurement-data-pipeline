import os
import requests
from bs4 import BeautifulSoup
from time import sleep

# ===========================
# CONFIG
# ===========================
START_YEAR = 2021
END_YEAR = 2025  # inclusive

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataEDA-Scraper/0.1; +https://example.com)"
}

# Month number -> English month name used in search/title
MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


# ===========================
# CORE HELPERS
# ===========================

def build_target_text(year: int, month: int) -> str:
    """
    Dataset title on data.gov.uk looks like:
    'UK Public Procurement Notices - January 2021'
    """
    month_name = MONTH_NAMES[month]
    return f"UK Public Procurement Notices - {month_name} {year}"


def build_search_url(year: int, month: int) -> str:
    """
    Search query looks like:
    https://www.data.gov.uk/search?q=Find+a+Tender+January+2021&...
    """
    month_name = MONTH_NAMES[month]
    query = f"Find+a+Tender+{month_name}+{year}"
    return (
        "https://www.data.gov.uk/search"
        f"?q={query}"
        "&filters%5Bpublisher%5D=Crown+Commercial+Service"
        "&filters%5Btopic%5D="
        "&filters%5Bformat%5D="
        "&sort=best"
    )


def build_download_dir(year: int, month: int) -> str:
    mm = f"{month:02d}"
    return os.path.join(SCRIPT_DIR, "raw_data", "find_a_tender", str(year), mm)


def fetch_page(url: str, max_retries: int = 3) -> str:
    """Fetch a page with basic retry + longer read timeout."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=(5, 60))
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.ReadTimeout:
            print(f"[Attempt {attempt}] Read timeout for {url}, retrying...")
            sleep(2)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            break
    raise RuntimeError(f"Failed to fetch page after retries: {url}")


def find_dataset_links(html: str, target_text: str):
    """Return dataset links whose title exactly matches the month/year we want."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for a in soup.select("a.govuk-link"):
        text = a.get_text(strip=True)
        href = (a.get("href") or "").strip()

        # Exact match: "UK Public Procurement Notices - January 2021"
        if text == target_text:
            if href.startswith("/"):
                href = "https://www.data.gov.uk" + href

            results.append(
                {
                    "title": text,
                    "url": href,
                }
            )

    return results


def sanitize_filename(name: str) -> str:
    """Make a safe filename for Windows/macOS/Linux."""
    bad_chars = '<>:"/\\|?*'
    for ch in bad_chars:
        name = name.replace(ch, "_")
    return name.strip()


def parse_and_download_files(dataset_url: str, download_dir: str):
    """
    Visit the dataset page, find all 'UK Public Procurement Notices ...' ZIP links,
    and download them into download_dir.
    """
    print(f"\n  Fetching dataset page: {dataset_url}")
    html = fetch_page(dataset_url)
    soup = BeautifulSoup(html, "html.parser")

    os.makedirs(download_dir, exist_ok=True)

    # Each file is in a <tr> with <td>s, first <td> has <a>, second <td> has format
    rows = soup.select("tbody.govuk-table__body tr.govuk-table__row")

    downloaded = 0

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        link_td = tds[0]
        format_td = tds[1]

        format_text = format_td.get_text(strip=True)
        link = link_td.find("a", class_="govuk-link")

        if not link:
            continue

        link_text = link.get_text(strip=True)
        href = (link.get("href") or "").strip()

        # We only want ZIP files for UK Public Procurement Notices
        if "UK Public Procurement Notices" not in link_text:
            continue
        if "ZIP" not in format_text.upper():
            continue

        # Some hrefs are absolute (S3), some might be relative
        if href.startswith("/"):
            file_url = "https://www.data.gov.uk" + href
        else:
            file_url = href

        # Build a local filename from the link text:
        # take only the part before the first comma
        clean_name = link_text.split(",")[0].strip()
        # remove any leading "Download" prefix
        if clean_name.lower().startswith("download"):
            clean_name = clean_name[8:].strip()

        filename = sanitize_filename(clean_name) + ".zip"
        filepath = os.path.join(download_dir, filename)

        print(f"    Downloading: {clean_name}")
        print(f"      URL:  {file_url}")
        print(f"      ->   {filepath}")

        try:
            resp = requests.get(file_url, headers=HEADERS, timeout=(5, 300))
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(resp.content)
            downloaded += 1
        except requests.exceptions.RequestException as e:
            print(f"      !!! Failed to download {file_url}: {e}")

    print(f"  Downloaded {downloaded} file(s) from dataset: {dataset_url}")


def print_progress(current: int, total: int, year: int, month: int):
    """Simple text progress bar."""
    width = 30
    frac = current / total
    filled = int(width * frac)
    bar = "#" * filled + "-" * (width - filled)
    print(
        f"\r[{bar}] {current}/{total} "
        f"({frac*100:5.1f}%)  Year {year}, Month {month:02d}",
        end="",
        flush=True,
    )


# ===========================
# MAIN LOOP
# ===========================

if __name__ == "__main__":
    years = list(range(START_YEAR, END_YEAR + 1))
    months = list(range(1, 13))
    total_jobs = len(years) * len(months)
    job_idx = 0

    print(f"Total (year, month) combinations to try: {total_jobs}")

    for year in years:
        for month in months:
            job_idx += 1
            print_progress(job_idx, total_jobs, year, month)

            target_text = build_target_text(year, month)
            search_url = build_search_url(year, month)
            download_dir = build_download_dir(year, month)

            try:
                search_html = fetch_page(search_url)
            except RuntimeError as e:
                print(f"\n  Skipping {year}-{month:02d}: search fetch failed ({e})")
                continue

            datasets = find_dataset_links(search_html, target_text)

            if not datasets:
                # No dataset published for that month/year
                continue

            for ds in datasets:
                print(f"\n\nProcessing {ds['title']} -> {ds['url']}")
                parse_and_download_files(ds["url"], download_dir)

    print("\n\nDone.")
