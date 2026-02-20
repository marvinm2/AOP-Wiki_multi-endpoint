import os
import subprocess

VERSIONS_DIR = "versions"
CONVERSION_SCRIPT = "AOP-Wiki_XML_to_RDF_conversion.py"

def find_all_gz_files(base_dir):
    gz_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".gz") and file.startswith("aop-wiki-xml-"):
                gz_files.append(os.path.join(root, file))
    return sorted(gz_files)

def main():
    gz_files = find_all_gz_files(VERSIONS_DIR)
    print(f"Found {len(gz_files)} versioned .gz files to process.")

    for gz_file in gz_files:
        version_dir = os.path.dirname(gz_file)

        # Extract version from filename, e.g., "aop-wiki-xml-2024-01-01.gz"
        filename = os.path.basename(gz_file)
        version = filename.replace("aop-wiki-xml-", "").replace(".gz", "")

        print(f"\n[→] Processing: {gz_file} (version {version})")

                # Check if .ttl files already exist
        expected_files = [
            os.path.join(version_dir, f"AOPWikiRDF-{version}.ttl"),
            os.path.join(version_dir, f"AOPWikiRDF-Genes-{version}.ttl"),
            os.path.join(version_dir, f"AOPWikiRDF-Void-{version}.ttl")
        ]
        if all(os.path.exists(f) for f in expected_files):
            print(f"[✓] Skipping {version}: TTL files already exist.")
            continue

        try:
            subprocess.run([
                "python", CONVERSION_SCRIPT,
                "--xml", gz_file,
                "--out", version_dir,
                "--version", version
            ], check=True)
        except subprocess.CalledProcessError:
            print(f"[!] RDF conversion failed for {gz_file}")


if __name__ == "__main__":
    main()