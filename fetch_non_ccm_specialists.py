#!/usr/bin/env python3
"""
Fetch specialists (Neurology, Nephrology, Rheumatology, Urology) who are
NOT billing CCM codes from CMS Medicare Provider Utilization and Payment Data.

Data sources:
  - "by Provider" dataset (one row per NPI):
    https://data.cms.gov/data-api/v1/dataset/8889d81e-2ee7-448f-8713-f071038289b5/data
  - "by Provider and Service" dataset (one row per NPI × HCPCS):
    https://data.cms.gov/data-api/v1/dataset/92396110-2aed-4d63-a6a2-5d6207d46a29/data
Year: CY 2023

Output: non_ccm_specialists.csv — providers in these specialties who do NOT bill any CCM code.
"""

import requests
import csv
import json
import time
import os
import sys

# ── API Endpoints ───────────────────────────────────────────────────────
BY_PROVIDER_URL = (
    "https://data.cms.gov/data-api/v1/dataset/"
    "8889d81e-2ee7-448f-8713-f071038289b5/data"
)
BY_PROVIDER_AND_SERVICE_URL = (
    "https://data.cms.gov/data-api/v1/dataset/"
    "92396110-2aed-4d63-a6a2-5d6207d46a29/data"
)

PAGE_SIZE = 5000

TARGET_SPECIALTIES = ["Neurology", "Nephrology", "Rheumatology", "Urology"]

CCM_CODES = ["99490", "99491", "99487", "99489", "99437", "99439"]

# NP/PA credential patterns
NP_PA_PATTERNS = [
    "PA", "NP", "PA-C", "PAC", "FNP", "CFNP", "ARNP", "DNP", "CNP",
    "ACNP", "ANP", "CRNP", "FNP-C", "FNP-BC", "AGNP", "GNP",
    "NP-C", "PMHNP", "WHNP", "PNP", "CNS", "APRN",
    "RPA", "RPA-C",
]

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


def classify_provider(credentials):
    """Classify provider as 'Doctor' or 'NP/PA' based on credentials string."""
    if not credentials or not credentials.strip():
        # In specialty-specific datasets (Neurology, Nephrology, Rheumatology,
        # Urology), empty credentials are almost always physicians whose
        # credential field wasn't populated in CMS data.
        return "Doctor"
    # Normalize: strip, uppercase, remove dots/commas/spaces for matching
    raw = credentials.strip().upper()
    stripped = raw.replace(".", "").replace(",", "").replace(" ", "")

    # Direct doctor matches (handles MD, DO, and all spacing/punctuation variants)
    if stripped in ("MD", "DO", "MDPHD", "MDMPH", "DOPHD", "DOMPH"):
        return "Doctor"

    # International medical degrees → Doctor
    intl_patterns = ("MBBS", "MBBCH", "MBCHB", "MBBCHBAO", "MBCH", "MB")
    for p in intl_patterns:
        if stripped.startswith(p):
            return "Doctor"

    # Contains M.D or D.O (with any spacing/punctuation)
    if "MD" in stripped or "DO" in stripped:
        return "Doctor"

    # Check NP/PA patterns on the stripped version
    for pattern in NP_PA_PATTERNS:
        if pattern in stripped:
            return "NP/PA"

    # DR as credential
    if stripped == "DR":
        return "Doctor"

    return "Other"


def fetch_paginated(base_url, params_base, label=""):
    """Fetch all pages from a CMS API endpoint."""
    records = []
    offset = 0

    while True:
        params = {**params_base, "size": PAGE_SIZE, "offset": offset}
        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error at offset {offset}: {e} — retrying in 5s...")
            time.sleep(5)
            try:
                resp = requests.get(base_url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e2:
                print(f"  Retry failed: {e2}. Stopping pagination for {label}.")
                break

        if not data:
            break

        records.extend(data)
        fetched = len(data)
        print(f"  {label}: offset={offset}, fetched={fetched}, total={len(records)}")

        if fetched < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    return records


def main():
    print("=" * 70)
    print("Non-CCM Specialist Provider Database Builder")
    print("Specialties: Neurology, Nephrology, Rheumatology, Urology")
    print("Data: CMS Medicare Physician & Other Practitioners, CY 2023")
    print("=" * 70)

    # ── Step 1: Fetch all providers in target specialties ────────────────
    print("\n[Step 1] Fetching all providers in target specialties...")
    all_providers = {}  # NPI → provider record

    for specialty in TARGET_SPECIALTIES:
        print(f"\n  Fetching {specialty}...")
        records = fetch_paginated(
            BY_PROVIDER_URL,
            {"filter[Rndrng_Prvdr_Type]": specialty},
            label=specialty,
        )
        for rec in records:
            npi = rec.get("Rndrng_NPI", "")
            if npi:
                all_providers[npi] = rec
        print(f"  {specialty}: {len(records)} records fetched")

    print(f"\n  Total unique providers across all specialties: {len(all_providers):,}")

    # ── Step 2: Find CCM-billing NPIs among these specialties ───────────
    print("\n[Step 2] Identifying CCM-billing providers for exclusion...")
    ccm_npis = set()

    for specialty in TARGET_SPECIALTIES:
        for code in CCM_CODES:
            label = f"{specialty}/{code}"
            records = fetch_paginated(
                BY_PROVIDER_AND_SERVICE_URL,
                {
                    "filter[Rndrng_Prvdr_Type]": specialty,
                    "filter[HCPCS_Cd]": code,
                },
                label=label,
            )
            for rec in records:
                npi = rec.get("Rndrng_NPI", "")
                if npi:
                    ccm_npis.add(npi)

    print(f"\n  CCM-billing providers found in target specialties: {len(ccm_npis):,}")

    # ── Step 3: Subtract CCM billers ────────────────────────────────────
    print("\n[Step 3] Building non-CCM provider list...")
    non_ccm = {
        npi: rec for npi, rec in all_providers.items() if npi not in ccm_npis
    }
    print(f"  Providers after CCM exclusion: {len(non_ccm):,}")
    print(f"  Excluded (CCM billers): {len(all_providers) - len(non_ccm):,}")

    # ── Step 4: Classify and build output rows ──────────────────────────
    print("\n[Step 4] Classifying providers and counting per address...")

    rows = []
    for npi, rec in non_ccm.items():
        creds = rec.get("Rndrng_Prvdr_Crdntls", "")
        rows.append({
            "NPI": npi,
            "First_Name": rec.get("Rndrng_Prvdr_First_Name", ""),
            "Last_Name": rec.get("Rndrng_Prvdr_Last_Org_Name", ""),
            "Credentials": creds,
            "Provider_Type": classify_provider(creds),
            "Specialty": rec.get("Rndrng_Prvdr_Type", ""),
            "Street_1": rec.get("Rndrng_Prvdr_St1", ""),
            "Street_2": rec.get("Rndrng_Prvdr_St2", ""),
            "City": rec.get("Rndrng_Prvdr_City", ""),
            "State": rec.get("Rndrng_Prvdr_State_Abrvtn", ""),
            "Zip": rec.get("Rndrng_Prvdr_Zip5", ""),
        })

    # Count providers per billing address (within dataset)
    addr_counts = {}
    for row in rows:
        key = (row["Street_1"].lower().strip(),
               row["City"].lower().strip(),
               row["State"].strip(),
               row["Zip"].strip())
        addr_counts[key] = addr_counts.get(key, 0) + 1

    for row in rows:
        key = (row["Street_1"].lower().strip(),
               row["City"].lower().strip(),
               row["State"].strip(),
               row["Zip"].strip())
        row["Providers_At_Address"] = addr_counts[key]

    # Sort by last name
    rows.sort(key=lambda r: (r["State"], r["Last_Name"], r["First_Name"]))

    # ── Step 5: Write CSV ───────────────────────────────────────────────
    print("\n[Step 5] Writing output files...")

    fieldnames = [
        "NPI", "First_Name", "Last_Name", "Credentials", "Provider_Type",
        "Specialty", "Street_1", "Street_2", "City", "State", "Zip",
        "Providers_At_Address",
    ]

    csv_path = os.path.join(OUTPUT_DIR, "non_ccm_specialists.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved CSV: {csv_path}")

    # Also copy to public/data for the frontend
    public_data_dir = os.path.join(OUTPUT_DIR, "..", "public", "data")
    os.makedirs(public_data_dir, exist_ok=True)
    public_csv = os.path.join(public_data_dir, "non_ccm_specialists.csv")
    with open(public_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved public CSV: {public_csv}")

    # ── Step 6: Generate zip code coordinates ───────────────────────────
    print("\n[Step 6] Generating zip code coordinate lookup...")
    generate_zip_coords(rows, public_data_dir)

    # ── Summary ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total providers in target specialties: {len(all_providers):,}")
    print(f"CCM-billing providers excluded:        {len(ccm_npis):,}")
    print(f"Non-CCM providers in output:           {len(rows):,}")

    print(f"\nBy specialty:")
    spec_counts = {}
    for r in rows:
        spec_counts[r["Specialty"]] = spec_counts.get(r["Specialty"], 0) + 1
    for spec, count in sorted(spec_counts.items()):
        print(f"  {spec}: {count:,}")

    print(f"\nBy provider type:")
    type_counts = {}
    for r in rows:
        type_counts[r["Provider_Type"]] = type_counts.get(r["Provider_Type"], 0) + 1
    for ptype, count in sorted(type_counts.items()):
        print(f"  {ptype}: {count:,}")

    print(f"\nBy state (top 15):")
    state_counts = {}
    for r in rows:
        state_counts[r["State"]] = state_counts.get(r["State"], 0) + 1
    for state, count in sorted(state_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {state}: {count:,}")


def generate_zip_coords(rows, output_dir):
    """Generate a zip → {lat, lng} JSON file using pgeocode or fallback."""
    try:
        import pgeocode
        nomi = pgeocode.Nominatim("us")

        # Get all unique zips from the dataset
        zips = sorted(set(r["Zip"] for r in rows if r["Zip"]))
        print(f"  Looking up coordinates for {len(zips)} unique zip codes...")

        coords = {}
        batch_size = 100
        for i in range(0, len(zips), batch_size):
            batch = zips[i : i + batch_size]
            for z in batch:
                result = nomi.query_postal_code(z)
                if hasattr(result, "latitude") and not (
                    result.latitude != result.latitude  # NaN check
                ):
                    coords[z] = {
                        "lat": round(float(result.latitude), 4),
                        "lng": round(float(result.longitude), 4),
                    }
            if (i // batch_size) % 10 == 0:
                print(f"    Processed {min(i + batch_size, len(zips))}/{len(zips)} zips...")

        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            json.dump(coords, f, separators=(",", ":"))
        print(f"  Saved zip coordinates: {out_path} ({len(coords)} zips)")

    except ImportError:
        print("  WARNING: pgeocode not installed. Install with: pip install pgeocode")
        print("  Generating zip_coords.json with fallback method...")
        generate_zip_coords_fallback(rows, output_dir)


def generate_zip_coords_fallback(rows, output_dir):
    """Fallback: download a free zip code database from the web."""
    url = "https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv"
    print(f"  Downloading zip code data from GitHub...")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        coords = {}
        for line in lines[1:]:  # skip header
            parts = line.split(",")
            if len(parts) >= 4:
                zipcode = parts[0].strip().zfill(5)
                try:
                    lat = round(float(parts[1]), 4)
                    lng = round(float(parts[2]), 4)
                    coords[zipcode] = {"lat": lat, "lng": lng}
                except ValueError:
                    pass

        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            json.dump(coords, f, separators=(",", ":"))
        print(f"  Saved zip coordinates: {out_path} ({len(coords)} zips)")
    except Exception as e:
        print(f"  ERROR generating zip coords: {e}")
        # Write an empty file so frontend doesn't break
        out_path = os.path.join(output_dir, "zip_coords.json")
        with open(out_path, "w") as f:
            f.write("{}")
        print(f"  Wrote empty zip_coords.json — distance feature will be unavailable")


if __name__ == "__main__":
    main()
