import os
import re
import requests
import pandas as pd
from time import sleep
from datetime import date

# Important clarification: Here I left a lot of duplicated/unimportant fields because at the time of writing this
# I wasn't sure what could be used for analysis, so I tried to make it as comprehensive as possible

# ===========================
# CONFIG
# ===========================
START_YEAR = 2014
START_MONTH = 1
END_YEAR = 2025
END_MONTH = 11

# Folder where this script lives
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Input folder: raw_data/contracts_finder/{YEAR}/{MM}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ContractFinder-EDA/0.1; +https://example.com)"
}


# ===========================
# HELPERS
# ===========================

def fetch_json(url: str, max_retries: int = 3):
    """Fetch JSON from a URL with basic retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=(5, 30))
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ReadTimeout:
            print(f"      [Attempt {attempt}] Timeout for {url}, retrying...")
            sleep(2)
        except requests.exceptions.RequestException as e:
            print(f"      Request failed for {url}: {e}")
            break
        except ValueError as e:
            print(f"      JSON decode failed for {url}: {e}")
            break
    return None


def get_csv_files(input_dir: str):
    """List all CSV files in the target folder."""
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.lower().endswith(".csv")
    ]
    return sorted(files)


def extract_date_from_filename(filename: str):
    """
    Try to find YYYY-MM-DD in the filename.
    e.g. 'Contracts Finder OCDS 2016-11-18.csv'
    """
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", filename)
    if not m:
        return None
    year, month, day = m.groups()
    return year, month, day


def first_or_none(seq):
    """Return first element of sequence or None."""
    if not seq:
        return None
    return seq[0]


def find_buyer_party(release: dict):
    """Match buyer.id to party.id and return that party dict (or None)."""
    buyer = release.get("buyer") or {}
    buyer_id = buyer.get("id")
    if not buyer_id:
        return None
    for p in release.get("parties", []) or []:
        if p.get("id") == buyer_id:
            return p
    return None


def find_supplier_parties(release: dict):
    """Return list of parties whose roles include 'supplier'."""
    suppliers = []
    for p in release.get("parties", []) or []:
        roles = p.get("roles") or []
        if "supplier" in roles:
            suppliers.append(p)
    return suppliers


def find_tender_notice_doc(tender: dict):
    """Return first document of type 'tenderNotice' (or None)."""
    for doc in tender.get("documents", []) or []:
        if doc.get("documentType") == "tenderNotice":
            return doc
    return None


def find_award_notice_doc(award: dict):
    """Return first document of type 'awardNotice' (or None)."""
    for doc in award.get("documents", []) or []:
        if doc.get("documentType") == "awardNotice":
            return doc
    return None


def extract_delivery_location(tender: dict):
    """
    Extract basic delivery location fields from items[0].deliveryAddresses.
    We try to get postalCode, region, countryName from any of the addresses.
    """
    delivery_postal = None
    delivery_region = None
    delivery_country = None

    items = tender.get("items") or []
    if not items:
        return delivery_postal, delivery_region, delivery_country

    addresses = items[0].get("deliveryAddresses") or []
    for addr in addresses:
        if not isinstance(addr, dict):
            continue
        if delivery_postal is None and addr.get("postalCode"):
            delivery_postal = addr.get("postalCode")
        if delivery_region is None and addr.get("region"):
            delivery_region = addr.get("region")
        if delivery_country is None and addr.get("countryName"):
            delivery_country = addr.get("countryName")

    return delivery_postal, delivery_region, delivery_country


def pipe_join(values):
    """Join a list of values with '|' or return None if list is empty."""
    cleaned = [str(v) for v in values if v is not None and str(v) != ""]
    return "|".join(cleaned) if cleaned else None


def month_sequence(start_year: int, start_month: int, end_year: int, end_month: int):
    """Yield (year, month) tuples inclusive from start to end."""
    current = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)
    while current <= end_date:
        yield current.year, current.month
        year = current.year + (current.month // 12)
        month = current.month % 12 + 1
        current = date(year, month, 1)


def process_month(year: int, month: int):
    print("=" * 40)
    print(f"Processing Year: {year}, Month: {month:02d}")

    input_dir = os.path.join(
        SCRIPT_DIR, "raw_data", "contracts_finder", str(year), f"{month:02d}"
    )
    print(f"Looking for CSVs in: {input_dir}")

    try:
        csv_files = get_csv_files(input_dir)
    except FileNotFoundError:
        print("Input directory missing. Skipping this month.\n")
        return

    if not csv_files:
        print("No CSV files found for this month. Skipping.\n")
        return

    print(f"Found {len(csv_files)} CSV file(s).\n")

    for csv_path in csv_files:
        base_name = os.path.basename(csv_path)
        print(f"Processing CSV: {base_name}")

        date_info = extract_date_from_filename(base_name)
        if not date_info:
            print("  Could not extract date from filename, skipping this file.")
            print("  (Expecting pattern like YYYY-MM-DD in the name.)\n")
            continue

        yyyy, mm_str, dd = date_info
        # Per-day tag
        day_tag = f"{yyyy}_{mm_str}_{dd}"

        # Output folder: extracted_data/
        output_dir = os.path.join(SCRIPT_DIR, "extracted_data", "contracts_finder")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(
            output_dir, f"contracts_finder_{day_tag}.xlsx"
        )

        print(f"  Date detected: {yyyy}-{mm_str}-{dd}")
        print(f"  Output folder: {output_dir}")
        print(f"  Output file:   {output_path}")

        # Read CSV
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"  Failed to read {csv_path}: {e}\n")
            continue

        if df.shape[1] == 0:
            print("  CSV has no columns, skipping.\n")
            continue

        first_col = df.iloc[:, 0].dropna()
        print(f"  Found {len(first_col)} URIs in first column (including potential duplicates).")

        records = []
        seen_uris = set()  # optional per-day de-duplication

        for idx, uri in first_col.items():
            uri = str(uri).strip()
            if not uri:
                continue

            if uri in seen_uris:
                records.append({
                    "csv_file": base_name,
                    "row_index": idx,
                    "uri": uri,
                    "publishedDate": None,
                    "status": "duplicate_uri_skipped_fetch",
                })
                continue

            seen_uris.add(uri)

            print(f"    Fetching JSON for row {idx}: {uri}")
            data = fetch_json(uri)
            if data is None:
                records.append({
                    "csv_file": base_name,
                    "row_index": idx,
                    "uri": uri,
                    "publishedDate": None,
                    "status": "fetch_failed_or_invalid_json",
                })
                continue

            # -------- Top-level --------
            top_uri = data.get("uri")
            published_date = data.get("publishedDate")

            publisher = data.get("publisher") or {}
            publisher_name = publisher.get("name")
            publisher_scheme = publisher.get("scheme")
            publisher_uid = publisher.get("uid")
            publisher_uri = publisher.get("uri")

            version = data.get("version")
            extensions = data.get("extensions") or []
            extensions_joined = pipe_join(extensions)

            # NEW: license and publicationPolicy
            top_license = data.get("license")
            publication_policy = data.get("publicationPolicy")

            releases = data.get("releases") or []
            release = first_or_none(releases) or {}

            # -------- Release-level --------
            ocid = release.get("ocid")
            release_title = release.get("title")
            release_id = release.get("id")
            release_date = release.get("date")
            release_language = release.get("language")

            tags = release.get("tag") or []
            release_tag = first_or_none(tags)
            release_tags_all = pipe_join(tags)
            initiation_type = release.get("initiationType")

            planning = release.get("planning") or {}
            planning_milestones = planning.get("milestones") or []
            planning_milestone_ids = pipe_join([m.get("id") for m in planning_milestones])
            planning_milestone_titles = pipe_join([m.get("title") for m in planning_milestones])
            planning_milestone_types = pipe_join([m.get("type") for m in planning_milestones])
            planning_milestone_due_dates = pipe_join([m.get("dueDate") for m in planning_milestones])

            planning_documents = planning.get("documents") or []
            planning_document_ids = pipe_join([d.get("id") for d in planning_documents])
            planning_document_types = pipe_join([d.get("documentType") for d in planning_documents])
            planning_document_descriptions = pipe_join([d.get("description") for d in planning_documents])
            planning_document_urls = pipe_join([d.get("url") for d in planning_documents])
            planning_document_dates_published = pipe_join([d.get("datePublished") for d in planning_documents])
            planning_document_formats = pipe_join([d.get("format") for d in planning_documents])
            planning_document_languages = pipe_join([d.get("language") for d in planning_documents])

            # -------- Tender --------
            tender = release.get("tender") or {}
            tender_id = tender.get("id")
            tender_title = tender.get("title")
            tender_description = tender.get("description")
            tender_status = tender.get("status")
            main_procurement_category = tender.get("mainProcurementCategory")

            tender_documents = tender.get("documents") or []
            tender_document_ids = pipe_join([d.get("id") for d in tender_documents])
            tender_document_types = pipe_join([d.get("documentType") for d in tender_documents])
            tender_document_descriptions = pipe_join([d.get("description") for d in tender_documents])
            tender_document_urls = pipe_join([d.get("url") for d in tender_documents])
            tender_document_dates_published = pipe_join([d.get("datePublished") for d in tender_documents])
            tender_document_dates_modified = pipe_join([d.get("dateModified") for d in tender_documents])
            tender_document_formats = pipe_join([d.get("format") for d in tender_documents])
            tender_document_languages = pipe_join([d.get("language") for d in tender_documents])

            classification = tender.get("classification") or {}
            cpv_scheme = classification.get("scheme")  # NEW field
            cpv_id = classification.get("id")
            cpv_description = classification.get("description")

            # additionalClassifications → pipe-joined
            add_class = tender.get("additionalClassifications") or []
            additional_cpv_ids = pipe_join(
                [c.get("id") for c in add_class]
            )
            additional_cpv_descriptions = pipe_join(
                [c.get("description") for c in add_class]
            )

            # -------- Value --------
            value = tender.get("value") or {}
            value_amount = value.get("amount")
            value_currency = value.get("currency")

            min_value = tender.get("minValue") or {}
            min_value_amount = min_value.get("amount")
            min_value_currency = min_value.get("currency")

            # -------- Items + geography --------
            items = tender.get("items") or []
            tender_item_ids = pipe_join([item.get("id") for item in items])

            def append_unique(target_list, value):
                if value is None or value == "":
                    return
                if value not in target_list:
                    target_list.append(value)

            tender_delivery_postal_codes = []
            tender_delivery_regions = []
            tender_delivery_countries = []
            for item in items:
                addresses = item.get("deliveryAddresses") or []
                for addr in addresses:
                    if not isinstance(addr, dict):
                        continue
                    append_unique(tender_delivery_postal_codes, addr.get("postalCode"))
                    append_unique(tender_delivery_regions, addr.get("region"))
                    append_unique(tender_delivery_countries, addr.get("countryName"))

            tender_delivery_postal_codes_joined = pipe_join(tender_delivery_postal_codes)
            tender_delivery_regions_joined = pipe_join(tender_delivery_regions)
            tender_delivery_countries_joined = pipe_join(tender_delivery_countries)

            delivery_postal, delivery_region, delivery_country = extract_delivery_location(tender)

            # -------- Time structure --------
            tender_date_published = tender.get("datePublished")

            tender_period = tender.get("tenderPeriod") or {}
            tender_end_date = tender_period.get("endDate")

            contract_period = tender.get("contractPeriod") or {}
            contract_start_date = contract_period.get("startDate")
            contract_end_date = contract_period.get("endDate")

            # -------- Method / SME flags --------
            procurement_method = tender.get("procurementMethod")
            procurement_method_details = tender.get("procurementMethodDetails")
            suitability = tender.get("suitability") or {}
            suitability_sme = suitability.get("sme")
            suitability_vcse = suitability.get("vcse")

            # -------- Buyer + party details --------
            buyer = release.get("buyer") or {}
            buyer_id = buyer.get("id")
            buyer_name = buyer.get("name")

            buyer_party = find_buyer_party(release) or {}
            buyer_identifier = buyer_party.get("identifier") or {}
            buyer_address = buyer_party.get("address") or {}
            buyer_contact = buyer_party.get("contactPoint") or {}
            buyer_details = buyer_party.get("details") or {}
            buyer_roles = buyer_party.get("roles") or []

            buyer_legal_name = buyer_identifier.get("legalName")
            buyer_identifier_scheme = buyer_identifier.get("scheme")
            buyer_identifier_id = buyer_identifier.get("id")

            buyer_street_address = buyer_address.get("streetAddress")
            buyer_locality = buyer_address.get("locality")
            buyer_postal_code = buyer_address.get("postalCode")
            buyer_country_name = buyer_address.get("countryName")

            buyer_contact_name = buyer_contact.get("name")
            buyer_contact_email = buyer_contact.get("email")
            buyer_contact_telephone = buyer_contact.get("telephone")
            buyer_details_url = buyer_details.get("url")

            buyer_roles_joined = pipe_join(buyer_roles)

            # -------- Supplier party details (from parties.roles == 'supplier') --------
            supplier_parties = find_supplier_parties(release)

            supplier_party_ids = pipe_join([p.get("id") for p in supplier_parties])
            supplier_party_names = pipe_join([p.get("name") for p in supplier_parties])

            supplier_identifiers = [p.get("identifier") or {} for p in supplier_parties]
            supplier_legal_names = pipe_join([i.get("legalName") for i in supplier_identifiers])
            supplier_identifier_schemes = pipe_join([i.get("scheme") for i in supplier_identifiers])
            supplier_identifier_ids = pipe_join([i.get("id") for i in supplier_identifiers])

            supplier_addresses = [p.get("address") or {} for p in supplier_parties]
            supplier_street_addresses = pipe_join([a.get("streetAddress") for a in supplier_addresses])
            supplier_localities = pipe_join([a.get("locality") for a in supplier_addresses])
            supplier_postal_codes = pipe_join([a.get("postalCode") for a in supplier_addresses])
            supplier_country_names = pipe_join([a.get("countryName") for a in supplier_addresses])

            supplier_details = [p.get("details") or {} for p in supplier_parties]
            supplier_scales = pipe_join([d.get("scale") for d in supplier_details])
            supplier_vcse_flags = pipe_join([d.get("vcse") for d in supplier_details])
            supplier_details_urls = pipe_join([d.get("url") for d in supplier_details])

            supplier_roles_lists = [p.get("roles") or [] for p in supplier_parties]
            # Flatten roles then de-duplicate
            supplier_roles_flat = []
            for roles in supplier_roles_lists:
                for r in roles or []:
                    if r not in supplier_roles_flat:
                        supplier_roles_flat.append(r)
            supplier_roles_joined = pipe_join(supplier_roles_flat)

            # -------- Tender notice document --------
            tender_notice_doc = find_tender_notice_doc(tender) or {}
            tender_notice_url = tender_notice_doc.get("url")
            tender_notice_description = tender_notice_doc.get("description")

            # -------- Awards (take first award if present) --------
            awards = release.get("awards") or []
            award = first_or_none(awards) or {}

            award_id = award.get("id")
            award_status = award.get("status")
            award_date = award.get("date")
            award_date_published = award.get("datePublished")

            award_value = award.get("value") or {}
            award_value_amount = award_value.get("amount")
            award_value_currency = award_value.get("currency")

            award_contract_period = award.get("contractPeriod") or {}
            award_contract_start_date = award_contract_period.get("startDate")
            award_contract_end_date = award_contract_period.get("endDate")

            award_suppliers = award.get("suppliers") or []
            award_suppliers_ids = pipe_join([s.get("id") for s in award_suppliers])
            award_suppliers_names = pipe_join([s.get("name") for s in award_suppliers])

            award_notice_doc = find_award_notice_doc(award) or {}
            award_notice_url = award_notice_doc.get("url")
            award_notice_description = award_notice_doc.get("description")
            award_notice_date_published = award_notice_doc.get("datePublished")
            award_notice_format = award_notice_doc.get("format")
            award_notice_language = award_notice_doc.get("language")

            award_documents = award.get("documents") or []
            award_document_ids = pipe_join([d.get("id") for d in award_documents])
            award_document_types = pipe_join([d.get("documentType") for d in award_documents])
            award_document_descriptions = pipe_join([d.get("description") for d in award_documents])
            award_document_urls = pipe_join([d.get("url") for d in award_documents])
            award_document_dates_published = pipe_join([d.get("datePublished") for d in award_documents])
            award_document_formats = pipe_join([d.get("format") for d in award_documents])
            award_document_languages = pipe_join([d.get("language") for d in award_documents])
            award_document_dates_modified = pipe_join([d.get("dateModified") for d in award_documents])

            record = {
                # bookkeeping
                "csv_file": base_name,
                "row_index": idx,
                "status": "ok",

                # identification
                "uri": top_uri or uri,
                "publishedDate": published_date,
                "ocid": ocid,
                "release_id": release_id,
                "release_title": release_title,
                "release_date": release_date,
                "release_language": release_language,
                "release_tag": release_tag,
                "release_tags_all": release_tags_all,
                "initiationType": initiation_type,

                # planning
                "planning_milestone_ids": planning_milestone_ids,
                "planning_milestone_titles": planning_milestone_titles,
                "planning_milestone_types": planning_milestone_types,
                "planning_milestone_dueDates": planning_milestone_due_dates,
                "planning_document_ids": planning_document_ids,
                "planning_document_types": planning_document_types,
                "planning_document_descriptions": planning_document_descriptions,
                "planning_document_urls": planning_document_urls,
                "planning_document_datePublished": planning_document_dates_published,
                "planning_document_formats": planning_document_formats,
                "planning_document_languages": planning_document_languages,

                # publisher / meta
                "publisher_name": publisher_name,
                "publisher_scheme": publisher_scheme,
                "publisher_uid": publisher_uid,
                "publisher_uri": publisher_uri,
                "version": version,
                "extensions": extensions_joined,
                "license": top_license,
                "publicationPolicy": publication_policy,

                # tender basics
                "tender_id": tender_id,
                "tender_title": tender_title,
                "tender_description": tender_description,
                "tender_status": tender_status,
                "mainProcurementCategory": main_procurement_category,

                # value
                "value_amount": value_amount,
                "value_currency": value_currency,
                "minValue_amount": min_value_amount,
                "minValue_currency": min_value_currency,

                # CPV
                "cpv_scheme": cpv_scheme,
                "cpv_id": cpv_id,
                "cpv_description": cpv_description,
                "additional_cpv_ids": additional_cpv_ids,
                "additional_cpv_descriptions": additional_cpv_descriptions,
                "tender_document_ids": tender_document_ids,
                "tender_document_types": tender_document_types,
                "tender_document_descriptions": tender_document_descriptions,
                "tender_document_urls": tender_document_urls,
                "tender_document_datePublished": tender_document_dates_published,
                "tender_document_dateModified": tender_document_dates_modified,
                "tender_document_formats": tender_document_formats,
                "tender_document_languages": tender_document_languages,

                # geography
                "tender_item_ids": tender_item_ids,
                "tender_delivery_postalCodes_all": tender_delivery_postal_codes_joined,
                "tender_delivery_regions_all": tender_delivery_regions_joined,
                "tender_delivery_countryNames_all": tender_delivery_countries_joined,
                "delivery_postalCode": delivery_postal,
                "delivery_region": delivery_region,
                "delivery_country": delivery_country,

                # timing
                "tender_datePublished": tender_date_published,
                "tender_endDate": tender_end_date,
                "contract_startDate": contract_start_date,
                "contract_endDate": contract_end_date,

                # method / SME flags
                "procurementMethod": procurement_method,
                "procurementMethodDetails": procurement_method_details,
                "suitability_sme": suitability_sme,
                "suitability_vcse": suitability_vcse,

                # buyer
                "buyer_id": buyer_id,
                "buyer_name": buyer_name,
                "buyer_legalName": buyer_legal_name,
                "buyer_identifier_scheme": buyer_identifier_scheme,
                "buyer_identifier_id": buyer_identifier_id,
                "buyer_streetAddress": buyer_street_address,
                "buyer_locality": buyer_locality,
                "buyer_postalCode": buyer_postal_code,
                "buyer_countryName": buyer_country_name,
                "buyer_contact_name": buyer_contact_name,
                "buyer_contact_email": buyer_contact_email,
                "buyer_contact_telephone": buyer_contact_telephone,
                "buyer_details_url": buyer_details_url,
                "buyer_roles": buyer_roles_joined,

                # supplier parties (from parties.roles == 'supplier')
                "supplier_party_ids": supplier_party_ids,
                "supplier_party_names": supplier_party_names,
                "supplier_legalNames": supplier_legal_names,
                "supplier_identifier_schemes": supplier_identifier_schemes,
                "supplier_identifier_ids": supplier_identifier_ids,
                "supplier_streetAddresses": supplier_street_addresses,
                "supplier_localities": supplier_localities,
                "supplier_postalCodes": supplier_postal_codes,
                "supplier_countryNames": supplier_country_names,
                "supplier_scales": supplier_scales,
                "supplier_vcse_flags": supplier_vcse_flags,
                "supplier_details_urls": supplier_details_urls,
                "supplier_roles": supplier_roles_joined,

                # links
                "tender_notice_url": tender_notice_url,
                "tender_notice_description": tender_notice_description,

                # award-level fields (first award only)
                "award_id": award_id,
                "award_status": award_status,
                "award_date": award_date,
                "award_datePublished": award_date_published,
                "award_value_amount": award_value_amount,
                "award_value_currency": award_value_currency,
                "award_contract_startDate": award_contract_start_date,
                "award_contract_endDate": award_contract_end_date,
                "award_suppliers_ids": award_suppliers_ids,
                "award_suppliers_names": award_suppliers_names,
                "award_notice_url": award_notice_url,
                "award_notice_description": award_notice_description,
                "award_notice_datePublished": award_notice_date_published,
                "award_notice_format": award_notice_format,
                "award_notice_language": award_notice_language,
                "award_document_ids": award_document_ids,
                "award_document_types": award_document_types,
                "award_document_descriptions": award_document_descriptions,
                "award_document_urls": award_document_urls,
                "award_document_datePublished": award_document_dates_published,
                "award_document_dateModified": award_document_dates_modified,
                "award_document_formats": award_document_formats,
                "award_document_languages": award_document_languages,
            }

            records.append(record)

        if not records:
            print("  No records extracted for this day. Nothing to write.\n")
            continue

        out_df = pd.DataFrame(records)

        try:
            out_df.to_excel(output_path, index=False)
            print(f"  Wrote {len(out_df)} rows to {output_path}\n")
        except Exception as e:
            print(f"  Failed to write Excel for {base_name}: {e}\n")

    print("Done with this month.\n")


if __name__ == "__main__":
    for yr, mo in month_sequence(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        process_month(yr, mo)
    print("All requested months processed.\n")
