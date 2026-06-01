"""Download AOP-Wiki quarterly XML snapshots.

The list of versions lives in ``versions.txt`` (one date per line) so a new
quarter can be added by editing a data file — or automatically by the
``quarterly-update`` workflow — rather than editing code. A built-in list is
kept as a fallback when the file is absent.

Also exposes small helpers (``quarter_dates``, ``next_missing_quarter``,
``remote_version_exists``) used by ``add_version.py`` to detect and fetch a
newly-published quarter.
"""
import os
from datetime import date
from time import sleep

import requests

# Built-in fallback list (used only if versions.txt is missing). versions.txt is
# the source of truth.
_FALLBACK_VERSIONS = [
    "2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01",
    "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01", "2020-07-01",
    "2020-10-01", "2021-01-03", "2021-04-01", "2021-07-01", "2021-10-01",
    "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01", "2023-01-01",
    "2023-04-01", "2023-07-01", "2023-10-01", "2024-01-01", "2024-04-01",
    "2024-07-01", "2024-10-01", "2025-01-01", "2025-04-01", "2025-07-01",
    "2025-10-01", "2026-01-01", "2026-04-01",
]

BASE_URL = "https://aopwiki.org/downloads"
DEST_DIR = "versions"
VERSIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "versions.txt")

# SSL verification on by default (CODE_REVIEW flagged the old verify=False).
# Override with AOPWIKI_VERIFY_SSL=false only if aopwiki.org's chain breaks.
VERIFY_SSL = os.getenv("AOPWIKI_VERIFY_SSL", "true").lower() != "false"
if not VERIFY_SSL:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_versions(path: str = VERSIONS_FILE) -> list:
    """Return the list of version dates from versions.txt (or the fallback)."""
    if not os.path.exists(path):
        return list(_FALLBACK_VERSIONS)
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line)
    return out or list(_FALLBACK_VERSIONS)


# Module-level list kept for backwards compatibility with importers.
versions = load_versions()


def quarter_dates(start: str = "2018-04-01", end: date | None = None) -> list:
    """Enumerate AOP-Wiki quarterly release dates from `start` up to `end`.

    Quarters fall on YYYY-{01,04,07,10}-01. The 2021-01-03 historical exception
    is normalised to 2021-01-01 here; callers that need the exact filename should
    consult versions.txt.
    """
    end = end or date.today()
    start_year = int(start[:4])
    out = []
    for year in range(start_year, end.year + 1):
        for month in (1, 4, 7, 10):
            d = date(year, month, 1)
            if d <= end:
                out.append(d.isoformat())
    return out


def remote_version_exists(version_date: str, timeout: int = 30) -> bool:
    """True if aop-wiki-xml-<date>.gz is published on aopwiki.org."""
    url = f"{BASE_URL}/aop-wiki-xml-{version_date}.gz"
    try:
        resp = requests.head(url, timeout=timeout, verify=VERIFY_SSL, allow_redirects=True)
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def next_missing_quarter(known: list | None = None, today: date | None = None) -> str | None:
    """Return the newest published quarter not yet in `known`, or None.

    Walks candidate quarter dates newest-first and returns the first one that is
    both absent from `known` and actually downloadable from aopwiki.org.
    """
    known = set(known if known is not None else load_versions())
    candidates = quarter_dates(end=today or date.today())
    for cand in reversed(candidates):
        if cand in known:
            continue
        if remote_version_exists(cand):
            return cand
    return None


def download_version(version_date: str) -> str | None:
    """Download one quarterly snapshot into versions/<date>/. Returns its path."""
    folder = os.path.join(DEST_DIR, version_date)
    os.makedirs(folder, exist_ok=True)

    file_name = f"aop-wiki-xml-{version_date}.gz"
    url = f"{BASE_URL}/{file_name}"
    dest_path = os.path.join(folder, file_name)

    if os.path.exists(dest_path):
        print(f"[✓] Already downloaded: {dest_path}")
        return dest_path

    print(f"[↓] Downloading {url}")
    try:
        response = requests.get(url, timeout=30, verify=VERIFY_SSL)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(response.content)
        print(f"[✔] Saved to {dest_path}")
        sleep(1)  # Be polite to the server
        return dest_path
    except requests.exceptions.RequestException as e:
        print(f"[!] Failed to download {url}: {e}")
        return None


def main():
    os.makedirs(DEST_DIR, exist_ok=True)
    for version in load_versions():
        download_version(version)


if __name__ == "__main__":
    main()
