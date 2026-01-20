# UK Procurement Data Pipeline

## Overview

This repository provides a modular pipeline for collecting, extracting, and merging UK public procurement data from two official sources:

1. **Contracts Finder** (2014–2025)
2. **Find a Tender Service (FATS)** (2021–2025)

Each stage is independent. Scraping tasks for the two sources can run in parallel, and extraction tasks can also run in parallel.

---

## 1. Scraping Raw Data

### **1a. `1a_gov_uk_scrape_contracts_finder.py`**

Scrapes monthly **Contracts Finder Notices** from data.gov.uk and downloads all CSV files into:

```
raw_data/contracts_finder/YYYY/MM/
```

Runs a month-by-month search, follows dataset links, and stores cleaned filenames.

### **1b. `1b_gov_uk_scrape_find_a_tender.py`**

Scrapes **Find a Tender (UK Public Procurement Notices)** daily ZIP archives and downloads them into:

```
raw_data/find_a_tender/YYYY/MM/
```

Handles date-based search, ZIP discovery, and robust retries.

### **Parallelism**

`1a` and `1b` can run **simultaneously** because they:

* Target different datasets
* Write to different directories
* Share no resources

---

## 2. Extracting Structured Data

### **2a. `2a_extract_contracts_finder.py`**

Processes CSV rows from Contracts Finder, fetches JSON detail records, normalises structured fields, and outputs daily Excel files to:

```
extracted_data/contracts_finder/
```

### **2b. `2b_extract_find_a_tender_XMLs.py`**

Processes ZIP files from Find a Tender, extracts XML notices (TED and UK2023 formats), parses metadata fields, and outputs daily Excel files to:

```
extracted_data/find_a_tender/
```

### **Parallelism**

`2a` and `2b` can run **simultaneously** because they:

* Process different raw data formats (JSON vs XML)
* Operate in separate directories
* Produce independent outputs

---

## 3. Merging Extracted Daily Files

### **`3_merge_to_two.py`**

Stream-merges all extracted Excel files per dataset to avoid memory overload. Produces two final unified CSVs:

```
merged_data/contracts_finder_merged.csv
merged_data/find_a_tender_merged.csv
```

---

## Pipeline Summary

```
(1a) Scrape Contracts Finder  ┐
                              ├── run independently & in parallel
(1b) Scrape Find a Tender     ┘

(2a) Extract CF JSON -> XLSX  ┐
                              ├── run independently & in parallel
(2b) Extract FATS XML -> XLSX ┘

(3) Merge -> Final Unified CSVs
```

---

## Notes

* Each stage assumes the prior stage for its specific dataset has finished (e.g., `2a` requires `1a` but not `1b`).
* The pipeline is designed to be modular, scalable, and memory-efficient.
