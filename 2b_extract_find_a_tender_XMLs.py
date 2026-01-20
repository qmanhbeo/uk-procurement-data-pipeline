import zipfile
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
import calendar
from datetime import date, timedelta


def _text(el):
    return el.text.strip() if el is not None and el.text is not None else None


def _join_unique(values):
    cleaned = [v.strip() for v in values if v and v.strip()]
    return ";".join(sorted(set(cleaned))) if cleaned else None


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _map_notice_type_group(td_code, form=None):
    if td_code is None:
        return "OTHER"
    code = td_code.strip().upper()
    if code == "0":
        return "PIN"
    if code in {"3", "O", "V"}:
        return "CONTRACT_NOTICE"
    if code == "7":
        return "CONTRACT_AWARD"
    if code == "K":
        return "MODIFICATION"
    return "OTHER"


# -------- TED / UK R2.0.9 PARSER (classic F01–F21) -------- #

def parse_ted_style_xml(root: ET.Element) -> dict:
    # dynamic namespace
    if "}" in root.tag:
        main_ns = root.tag[root.tag.find("{") + 1: root.tag.find("}")]
    else:
        main_ns = None

    ns = {}
    if main_ns:
        ns["ted"] = main_ns
    ns["n2016"] = "http://enotice.service.gov.uk/resource/schema/ted/2016/nuts"
    ns["n2021"] = "http://enotice.service.gov.uk/resource/schema/ted/2021/nuts"

    # IDs / basic meta
    doc_id = root.attrib.get("DOC_ID")
    edition = root.attrib.get("EDITION")

    date_pub = _text(root.find(".//ted:REF_OJS/ted:DATE_PUB", ns))
    ds_date_dispatch = _text(root.find(".//ted:CODIF_DATA/ted:DS_DATE_DISPATCH", ns))

    iso_country_el = root.find(".//ted:NOTICE_DATA/ted:ISO_COUNTRY", ns)
    iso_country = iso_country_el.attrib.get("VALUE") if iso_country_el is not None else None

    notice_url = _text(root.find(".//ted:NOTICE_DATA/ted:URI_LIST/ted:URI_DOC[@LG='EN']", ns))
    no_doc_ojs = _text(root.find(".//ted:NOTICE_DATA/ted:NO_DOC_OJS", ns))

    # CPV
    original_cpv_el = root.find(".//ted:NOTICE_DATA/ted:ORIGINAL_CPV", ns)
    original_cpv_code = original_cpv_el.attrib.get("CODE") if original_cpv_el is not None else None

    cpv_main_el = root.find(".//ted:OBJECT_CONTRACT/ted:CPV_MAIN/ted:CPV_CODE", ns)
    cpv_main_code = cpv_main_el.attrib.get("CODE") if cpv_main_el is not None else None

    add_cpvs = []
    for cpv_add in root.findall(".//ted:OBJECT_DESCR/ted:CPV_ADDITIONAL/ted:CPV_CODE", ns):
        code = cpv_add.attrib.get("CODE")
        if code:
            add_cpvs.append(code)
    additional_cpv_codes = _join_unique(add_cpvs)

    # NUTS
    perf_codes = []
    for el in root.findall(".//ted:NOTICE_DATA/n2021:PERFORMANCE_NUTS", ns) + \
               root.findall(".//ted:NOTICE_DATA/n2016:PERFORMANCE_NUTS", ns):
        code = el.attrib.get("CODE")
        if code:
            perf_codes.append(code)
    perf_nuts_code = _join_unique(perf_codes)

    ca_ce_nuts_el = (root.find(".//ted:NOTICE_DATA/n2021:CA_CE_NUTS", ns)
                     or root.find(".//ted:NOTICE_DATA/n2016:CA_CE_NUTS", ns))
    ca_ce_nuts_code = ca_ce_nuts_el.attrib.get("CODE") if ca_ce_nuts_el is not None else None

    # Translation / title
    ti_doc = root.find(".//ted:TRANSLATION_SECTION/ted:ML_TITLES/ted:ML_TI_DOC[@LG='EN']", ns)
    ti_country = _text(ti_doc.find("ted:TI_CY", ns)) if ti_doc is not None else None
    ti_town = _text(ti_doc.find("ted:TI_TOWN", ns)) if ti_doc is not None else None
    ti_text_p = ti_doc.find("ted:TI_TEXT/ted:P", ns) if ti_doc is not None else None
    ti_text = _text(ti_text_p)

    # Contracting authority
    ca_addr = root.find(".//ted:CONTRACTING_BODY/ted:ADDRESS_CONTRACTING_BODY", ns)
    ca_name = _text(ca_addr.find("ted:OFFICIALNAME", ns)) if ca_addr is not None else None
    ca_town = _text(ca_addr.find("ted:TOWN", ns)) if ca_addr is not None else None
    ca_postcode = _text(ca_addr.find("ted:POSTAL_CODE", ns)) if ca_addr is not None else None
    ca_email = _text(ca_addr.find("ted:E_MAIL", ns)) if ca_addr is not None else None
    ca_url = _text(ca_addr.find("ted:URL_GENERAL", ns)) if ca_addr is not None else None
    ca_country_el = ca_addr.find("ted:COUNTRY", ns) if ca_addr is not None else None
    ca_country_code = ca_country_el.attrib.get("VALUE") if ca_country_el is not None else None

    ca_nuts_el = (ca_addr.find("n2021:NUTS", ns) if ca_addr is not None else None) \
                 or (ca_addr.find("n2016:NUTS", ns) if ca_addr is not None else None)
    ca_nuts_code = ca_nuts_el.attrib.get("CODE") if ca_nuts_el is not None else None

    # Object / description
    obj_title_p = root.find(".//ted:OBJECT_CONTRACT/ted:TITLE/ted:P", ns)
    obj_title = _text(obj_title_p)

    short_descr_el = root.find(".//ted:OBJECT_CONTRACT/ted:SHORT_DESCR/ted:P", ns)
    if short_descr_el is None:
        short_descr_el = root.find(".//ted:OBJECT_DESCR/ted:SHORT_DESCR/ted:P", ns)
    short_descr = _text(short_descr_el)

    type_contract_el = root.find(".//ted:OBJECT_CONTRACT/ted:TYPE_CONTRACT", ns)
    type_contract_ctype = type_contract_el.attrib.get("CTYPE") if type_contract_el is not None else None

    # Values (notice-level)
    val_total_el = root.find(".//ted:OBJECT_CONTRACT/ted:VAL_TOTAL", ns)
    val_total = _text(val_total_el)
    val_total_currency = val_total_el.attrib.get("CURRENCY") if val_total_el is not None else None

    est_total_el = root.find(".//ted:NOTICE_DATA/ted:VALUES/ted:VALUE[@TYPE='ESTIMATED_TOTAL']", ns)
    est_total_val = _text(est_total_el)
    est_total_val_currency = est_total_el.attrib.get("CURRENCY") if est_total_el is not None else None

    proc_total_el = root.find(".//ted:NOTICE_DATA/ted:VALUES/ted:VALUE[@TYPE='PROCUREMENT_TOTAL']", ns)
    proc_total_val = _text(proc_total_el)
    proc_total_val_currency = proc_total_el.attrib.get("CURRENCY") if proc_total_el is not None else None

    # Award section (if present)
    aw_conclusion_el = root.find(".//ted:AWARD_CONTRACT/ted:AWARDED_CONTRACT/ted:DATE_CONCLUSION_CONTRACT", ns)
    award_date = _text(aw_conclusion_el)

    aw_val_total_el = root.find(".//ted:AWARD_CONTRACT/ted:AWARDED_CONTRACT/ted:VALUES/ted:VAL_TOTAL", ns)
    aw_val_total = _text(aw_val_total_el)
    aw_val_currency = aw_val_total_el.attrib.get("CURRENCY") if aw_val_total_el is not None else None

    nb_tenders_el = root.find(".//ted:AWARD_CONTRACT/ted:AWARDED_CONTRACT/ted:TENDERS/ted:NB_TENDERS_RECEIVED", ns)
    nb_tenders = _text(nb_tenders_el)

    # Contractors (winning suppliers) – flattened
    contractor_names = []
    for contr in root.findall(".//ted:AWARD_CONTRACT/ted:AWARDED_CONTRACT/ted:CONTRACTORS/ted:CONTRACTOR", ns):
        addr = contr.find("ted:ADDRESS_CONTRACTOR", ns)
        name = _text(addr.find("ted:OFFICIALNAME", ns)) if addr is not None else None
        if name:
            contractor_names.append(name)
    contractor_names_str = _join_unique(contractor_names)

    # CODIF_DATA
    td_doc_type_el = root.find(".//ted:CODIF_DATA/ted:TD_DOCUMENT_TYPE", ns)
    td_document_type_code = td_doc_type_el.attrib.get("CODE") if td_doc_type_el is not None else None

    nc_contract_nature_el = root.find(".//ted:CODIF_DATA/ted:NC_CONTRACT_NATURE", ns)
    nc_contract_nature_code = nc_contract_nature_el.attrib.get("CODE") if nc_contract_nature_el is not None else None

    pr_proc_el = root.find(".//ted:CODIF_DATA/ted:PR_PROC", ns)
    pr_proc_code = pr_proc_el.attrib.get("CODE") if pr_proc_el is not None else None

    ac_award_crit_el = root.find(".//ted:CODIF_DATA/ted:AC_AWARD_CRIT", ns)
    ac_award_crit_code = ac_award_crit_el.attrib.get("CODE") if ac_award_crit_el is not None else None

    ma_main_activities_el = root.find(".//ted:CODIF_DATA/ted:MA_MAIN_ACTIVITIES", ns)
    ma_main_activities_code = ma_main_activities_el.attrib.get("CODE") if ma_main_activities_el is not None else None

    rp_regulation_el = root.find(".//ted:CODIF_DATA/ted:RP_REGULATION", ns)
    rp_regulation_code = rp_regulation_el.attrib.get("CODE") if rp_regulation_el is not None else None

    # form type
    form_type = None
    if main_ns:
        form_section = root.find(".//ted:FORM_SECTION", ns)
        if form_section is not None:
            for child in list(form_section):
                if "FORM" in child.attrib:
                    form_type = child.attrib.get("FORM")
                    break

    notice_type_group = _map_notice_type_group(td_document_type_code, form_type)

    return {
        "schema_type": "TED_R2.0.9",
        "form_type": form_type,

        "td_document_type_code": td_document_type_code,
        "notice_type_group": notice_type_group,

        "doc_id": doc_id,
        "edition": edition,
        "no_doc_ojs": no_doc_ojs,
        "notice_url": notice_url,

        "date_pub": date_pub,
        "ds_date_dispatch": ds_date_dispatch,
        "award_date": award_date,

        "iso_country": iso_country,
        "ti_country": ti_country,
        "ti_town": ti_town,
        "ca_country_code": ca_country_code,
        "ca_town": ca_town,
        "ca_postcode": ca_postcode,
        "ca_nuts_code": ca_nuts_code,
        "perf_nuts_code": perf_nuts_code,
        "ca_ce_nuts_code": ca_ce_nuts_code,

        "ca_name": ca_name,
        "ca_email": ca_email,
        "ca_url": ca_url,

        "original_cpv_code": original_cpv_code,
        "cpv_main_code": cpv_main_code,
        "additional_cpv_codes": additional_cpv_codes,

        "ti_text": ti_text,
        "obj_title": obj_title,
        "short_descr": short_descr,
        "type_contract_ctype": type_contract_ctype,

        "val_total": val_total,
        "val_total_currency": val_total_currency,
        "est_total_val": est_total_val,
        "est_total_val_currency": est_total_val_currency,
        "proc_total_val": proc_total_val,
        "proc_total_val_currency": proc_total_val_currency,
        "aw_val_total": aw_val_total,
        "aw_val_currency": aw_val_currency,
        "nb_tenders": nb_tenders,

        "nc_contract_nature_code": nc_contract_nature_code,
        "pr_proc_code": pr_proc_code,
        "ac_award_crit_code": ac_award_crit_code,
        "ma_main_activities_code": ma_main_activities_code,
        "rp_regulation_code": rp_regulation_code,

        "contractor_names": contractor_names_str,
    }


# -------- UK2 / UK6 / UK7 (OCDS-style) PARSER -------- #

def parse_ukx_xml(root: ET.Element, form_tag: str) -> dict:
    notice_data = root.find("NOTICE_DATA")
    no_doc_ext = _text(notice_data.find("NO_DOC_EXT")) if notice_data is not None else None
    doc_id = _text(notice_data.find("DOC_ID")) if notice_data is not None else None
    notice_url = _text(notice_data.find("URI_DOC")) if notice_data is not None else None
    date_pub = _text(notice_data.find("PUBLISHED")) if notice_data is not None else None

    ukx = root.find(f".//{form_tag}")
    if ukx is None:
        return {
            "schema_type": form_tag,
            "form_type": form_tag.replace("_2023", ""),
            "td_document_type_code": form_tag.replace("_2023", ""),
            "notice_type_group": "OTHER",
            "doc_id": doc_id,
            "edition": None,
            "no_doc_ojs": no_doc_ext,
            "notice_url": notice_url,
            "date_pub": date_pub,
        }

    uk_id = _text(ukx.find("id"))
    uk_date = _text(ukx.find("date"))

    # Parties: buyer + suppliers
    buyer_name = None
    buyer_country = None
    buyer_town = None
    buyer_postcode = None
    buyer_region = None
    buyer_url = None

    supplier_names = []
    supplier_regions = []

    for p in ukx.findall("parties"):
        roles = [r.text for r in p.findall("roles") if r.text]
        name = _text(p.find("name"))
        addr = p.find("address")
        region = _text(addr.find("region")) if addr is not None else None
        country = _text(addr.find("country")) if addr is not None else None
        town = _text(addr.find("locality")) if addr is not None else None
        postcode = _text(addr.find("postalCode")) if addr is not None else None
        details = p.find("details")
        url_el = details.find("url") if details is not None else None
        url_val = _text(url_el)

        if "buyer" in roles and buyer_name is None:
            buyer_name = name
            buyer_country = country
            buyer_town = town
            buyer_postcode = postcode
            buyer_region = region
            buyer_url = url_val

        if "supplier" in roles:
            if name:
                supplier_names.append(name)
            if region:
                supplier_regions.append(region)

    # fallback buyer element
    buyer_el = ukx.find("buyer")
    if buyer_el is not None and not buyer_name:
        buyer_name = _text(buyer_el.find("name"))

    # CPV + perf regions
    cpv_codes = []
    perf_codes = []
    for aw in ukx.findall("awards"):
        for item in aw.findall("items"):
            for ac in item.findall("additionalClassifications"):
                scheme = _text(ac.find("scheme"))
                cid = _text(ac.find("id"))
                if scheme == "CPV" and cid:
                    cpv_codes.append(cid)
            for da in item.findall("deliveryAddresses"):
                region = _text(da.find("region"))
                if region:
                    perf_codes.append(region)

    cpv_main_code = cpv_codes[0] if cpv_codes else None
    additional_cpv_codes = _join_unique(cpv_codes[1:]) if len(cpv_codes) > 1 else None
    perf_nuts_code = _join_unique(perf_codes)

    # Tender text
    tender = ukx.find("tender")
    obj_title = _text(tender.find("title")) if tender is not None else None
    short_descr = _text(tender.find("description")) if tender is not None else None

    # mainProcurementCategory
    main_proc_cat = None
    for aw in ukx.findall("awards"):
        mpc = _text(aw.find("mainProcurementCategory"))
        if mpc:
            main_proc_cat = mpc
            break

    type_contract_ctype = None
    if main_proc_cat:
        l = main_proc_cat.lower()
        if "work" in l:
            type_contract_ctype = "WORKS"
        elif "service" in l:
            type_contract_ctype = "SERVICES"
        elif "supply" in l or "good" in l:
            type_contract_ctype = "SUPPLIES"

    # tags → notice_type_group
    tags = [t.text for t in ukx.findall("tag") if t.text]
    if form_tag in {"UK6_2023", "UK7_2023"} and "award" in tags:
        notice_type_group = "UK7_AWARD"
    elif "planning" in tags:
        notice_type_group = "PLANNING"
    else:
        notice_type_group = "OTHER"

    supplier_names_str = _join_unique(supplier_names)
    supplier_regions_str = _join_unique(supplier_regions)

    return {
        "schema_type": form_tag,
        "form_type": form_tag.replace("_2023", ""),
        "td_document_type_code": form_tag.replace("_2023", ""),
        "notice_type_group": notice_type_group,

        "doc_id": doc_id or uk_id,
        "edition": None,
        "no_doc_ojs": no_doc_ext,
        "notice_url": notice_url,

        "date_pub": date_pub or uk_date,
        "ds_date_dispatch": None,
        "award_date": None,

        "iso_country": buyer_country,
        "ti_country": None,
        "ti_town": buyer_town,
        "ca_country_code": buyer_country,
        "ca_town": buyer_town,
        "ca_postcode": buyer_postcode,
        "ca_nuts_code": buyer_region,
        "perf_nuts_code": perf_nuts_code,
        "ca_ce_nuts_code": None,

        "ca_name": buyer_name,
        "ca_email": None,
        "ca_url": buyer_url,

        "original_cpv_code": cpv_main_code,
        "cpv_main_code": cpv_main_code,
        "additional_cpv_codes": additional_cpv_codes,

        "ti_text": obj_title,
        "obj_title": obj_title,
        "short_descr": short_descr,
        "type_contract_ctype": type_contract_ctype,

        "val_total": None,
        "val_total_currency": None,
        "est_total_val": None,
        "est_total_val_currency": None,
        "proc_total_val": None,
        "proc_total_val_currency": None,
        "aw_val_total": None,
        "aw_val_currency": None,
        "nb_tenders": None,

        "nc_contract_nature_code": None,
        "pr_proc_code": None,
        "ac_award_crit_code": None,
        "ma_main_activities_code": None,
        "rp_regulation_code": None,

        "contractor_names": supplier_names_str,  # for UKx these are the suppliers
    }


# -------- DISPATCH -------- #

def parse_find_a_tender_xml(xml_content: str) -> dict:
    root = ET.fromstring(xml_content)

    # UK2 / UK4 / UK6 / UK7
    for tag in ["UK16_2023", "UK15_2023", "UK14_2023", "UK13_2023", "UK12_2023",
                "UK11_2023", "UK10_2023",
                "UK9_2023", "UK8_2023", "UK7_2023", "UK6_2023", "UK5_2023",
                "UK4_2023", "UK3_2023", "UK2_2023", "UK1_2023", "UK1_2022"]:
        if root.find(f".//{tag}") is not None:
            return parse_ukx_xml(root, tag)

    # otherwise TED
    return parse_ted_style_xml(root)


# -------- DAY PROCESSOR -------- #

def process_find_a_tender_day(year, month, day):
    script_dir = Path(__file__).resolve().parent
    day_int = int(day)
    month_int = int(month)

    ord_day = _ordinal(day_int)
    month_name = calendar.month_name[month_int]
    zip_name = f"UK Public Procurement Notices - {ord_day} {month_name} {year}.zip"

    zip_path = script_dir / "raw_data" / "find_a_tender" / str(year) / f"{month_int:02d}" / zip_name

    output_dir = script_dir / "extracted_data" / "find_a_tender"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / f"find_a_tender_{year}_{month_int:02d}_{day_int:02d}.xlsx"

    if not zip_path.exists():
        print(f"ZIP not found: {zip_path}")
        return

    rows = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if not name.lower().endswith(".xml"):
                continue
            with z.open(name) as f:
                xml_bytes = f.read()

            try:
                xml_str = xml_bytes.decode("utf-8")
            except UnicodeDecodeError:
                xml_str = xml_bytes.decode("latin-1", errors="replace")

            try:
                record = parse_find_a_tender_xml(xml_str)
                record["parse_error"] = None
            except Exception as e:
                record = {"doc_id": None, "parse_error": str(e)}
            record["source_xml_file"] = name
            record["source_zip"] = zip_path.name
            rows.append(record)

    if not rows:
        print(f"No XML files found in {zip_path}")
        return

    df = pd.DataFrame(rows)
    df.to_excel(out_file, index=False)
    print(f"Saved {len(df)} notices to {out_file}")


if __name__ == "__main__":
    start_date = date(2021, 1, 1)
    end_date = date(2025, 10, 31)
    current = start_date

    while current <= end_date:
        process_find_a_tender_day(current.year, current.month, current.day)
        current += timedelta(days=1)
