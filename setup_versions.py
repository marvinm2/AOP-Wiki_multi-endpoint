import os
import requests
from time import sleep
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# List of available AOP-Wiki versions
versions = [
    "2026-04-01", "2026-01-01", "2025-10-01",
    "2025-07-01", "2025-04-01", "2025-01-01", "2024-10-01", "2024-07-01", "2024-04-01",
    "2024-01-01", "2023-10-01", "2023-07-01", "2023-04-01", "2023-01-01", "2022-10-01",
    "2022-07-01", "2022-04-01", "2022-01-01", "2021-10-01", "2021-07-01", "2021-04-01",
    "2021-01-03", "2020-10-01", "2020-07-01", "2020-04-01", "2020-01-01", "2019-10-01",
    "2019-07-01", "2019-04-01", "2019-01-01", "2018-10-01", "2018-07-01", "2018-04-01"
]

BASE_URL = "https://aopwiki.org/downloads"
DEST_DIR = "versions"

def download_version(version_date):
    folder = os.path.join(DEST_DIR, version_date)
    os.makedirs(folder, exist_ok=True)
    
    file_name = f"aop-wiki-xml-{version_date}.gz"
    url = f"{BASE_URL}/{file_name}"
    dest_path = os.path.join(folder, file_name)

    if os.path.exists(dest_path):
        print(f"[✓] Already downloaded: {dest_path}")
        return

    print(f"[↓] Downloading {url}")
    try:
        response = requests.get(url, timeout=30, verify=False)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            f.write(response.content)
        print(f"[✔] Saved to {dest_path}")
        sleep(1)  # Be polite to the server
    except requests.exceptions.RequestException as e:
        print(f"[!] Failed to download {url}: {e}")

def main():
    os.makedirs(DEST_DIR, exist_ok=True)
    for version in versions:
        download_version(version)

if __name__ == "__main__":
    main()
