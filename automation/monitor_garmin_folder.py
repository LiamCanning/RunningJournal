#!/usr/bin/env python3
"""
Monitor the garmin_imports/ folder for new Garmin CSV exports.

Checks for unprocessed CSVs, validates they have expected Garmin columns,
merges them into the master Garmin CSV, deduplicates, and archives.
"""

import csv
import hashlib
import os
import shutil
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, "garmin_monitor.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Expected Garmin CSV columns (Spanish locale, as used in the existing export)
REQUIRED_COLUMNS = {"Fecha", "Distancia", "Tiempo", "Tipo de actividad"}


def md5_of_file(filepath):
    """Compute MD5 hash of a file's contents."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_processed_hashes():
    """Load the set of already-processed file hashes."""
    if not os.path.exists(config.PROCESSED_FILES_LOG):
        return set()
    with open(config.PROCESSED_FILES_LOG, "r") as f:
        return set(line.strip() for line in f if line.strip())


def record_processed_hash(file_hash):
    """Append a hash to the processed-files log."""
    with open(config.PROCESSED_FILES_LOG, "a") as f:
        f.write(file_hash + "\n")


def find_master_csv():
    """
    Find the most recent master Garmin CSV in Data/Garmin/.
    Returns the path, or None if no CSV exists.
    """
    garmin_dir = config.GARMIN_DATA_DIR
    if not os.path.isdir(garmin_dir):
        return None
    csvs = [
        os.path.join(garmin_dir, f)
        for f in os.listdir(garmin_dir)
        if f.endswith(".csv") and not f.startswith(".")
    ]
    if not csvs:
        return None
    # Return the most recently modified
    return max(csvs, key=os.path.getmtime)


def validate_garmin_csv(filepath):
    """
    Check that a CSV file has the expected Garmin columns.
    Returns (True, header_list) or (False, reason_string).
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
        if header is None:
            return False, "Empty file (no header row)"
        header_set = set(h.strip() for h in header)
        missing = REQUIRED_COLUMNS - header_set
        if missing:
            return False, f"Missing required columns: {missing}"
        return True, header
    except Exception as e:
        return False, f"Could not read CSV: {e}"


def dedup_key(row, header):
    """
    Generate a dedup key from date + distance.
    Uses the Fecha (date) and Distancia (distance) columns.
    """
    fecha_idx = None
    dist_idx = None
    for i, col in enumerate(header):
        col_clean = col.strip()
        if col_clean == "Fecha":
            fecha_idx = i
        elif col_clean == "Distancia":
            dist_idx = i
    if fecha_idx is None or dist_idx is None:
        return None
    date_val = row[fecha_idx].strip() if fecha_idx < len(row) else ""
    dist_val = row[dist_idx].strip() if dist_idx < len(row) else ""
    return f"{date_val}|{dist_val}"


def merge_csv(new_filepath, new_header):
    """
    Merge a new Garmin CSV into the master CSV.
    Deduplicates by date + distance.
    """
    master_path = find_master_csv()

    # Load existing rows
    existing_rows = []
    existing_header = None
    existing_keys = set()

    if master_path:
        log.info("Master CSV: %s", master_path)
        with open(master_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_header = next(reader, None)
            for row in reader:
                existing_rows.append(row)
                key = dedup_key(row, existing_header)
                if key:
                    existing_keys.add(key)
        log.info("Existing master has %d rows.", len(existing_rows))
    else:
        log.info("No existing master CSV found. Will create new one.")

    # Read new rows
    new_rows = []
    with open(new_filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        file_header = next(reader, None)
        for row in reader:
            new_rows.append(row)

    # Determine the header to use
    if existing_header is None:
        existing_header = file_header

    # Check column compatibility
    if file_header != existing_header:
        log.warning(
            "New CSV header differs from master. Columns: %d vs %d. "
            "Attempting positional merge.",
            len(file_header), len(existing_header),
        )

    # Deduplicate and merge
    added = 0
    for row in new_rows:
        key = dedup_key(row, file_header)
        if key and key in existing_keys:
            continue
        existing_rows.append(row)
        if key:
            existing_keys.add(key)
        added += 1

    log.info("Adding %d new rows (skipped %d duplicates).", added, len(new_rows) - added)

    # Sort by date descending (Fecha is typically the second column)
    fecha_idx = None
    for i, col in enumerate(existing_header):
        if col.strip() == "Fecha":
            fecha_idx = i
            break

    if fecha_idx is not None:
        def sort_key(row):
            try:
                return row[fecha_idx].strip()
            except IndexError:
                return ""
        existing_rows.sort(key=sort_key, reverse=True)

    # Write the merged master
    if master_path is None:
        master_path = os.path.join(
            config.GARMIN_DATA_DIR,
            f"Garmin Connect Activities ({datetime.now().strftime('%d:%m:%y')}).csv",
        )
    os.makedirs(os.path.dirname(master_path), exist_ok=True)

    with open(master_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(existing_header)
        writer.writerows(existing_rows)

    log.info("Master CSV updated: %s (%d total rows).", master_path, len(existing_rows))
    return added


def process_new_files():
    """
    Scan garmin_imports/ for new CSV files and process them.
    Returns the number of new files processed.
    """
    processed_hashes = load_processed_hashes()
    imports_dir = config.GARMIN_IMPORTS_DIR
    processed_count = 0

    if not os.path.isdir(imports_dir):
        log.info("Imports directory does not exist: %s", imports_dir)
        return 0

    csv_files = [
        f for f in os.listdir(imports_dir)
        if f.endswith(".csv") and not f.startswith(".")
    ]

    if not csv_files:
        log.info("No CSV files found in garmin_imports/.")
        return 0

    log.info("Found %d CSV file(s) in garmin_imports/.", len(csv_files))

    for filename in csv_files:
        filepath = os.path.join(imports_dir, filename)
        file_hash = md5_of_file(filepath)

        if file_hash in processed_hashes:
            log.info("Skipping already-processed file: %s (hash: %s)", filename, file_hash[:8])
            continue

        log.info("Processing new file: %s", filename)

        valid, result = validate_garmin_csv(filepath)
        if not valid:
            log.warning("Invalid Garmin CSV '%s': %s. Skipping.", filename, result)
            continue

        header = result
        try:
            added = merge_csv(filepath, header)
        except Exception as e:
            log.error("Failed to merge '%s': %s", filename, e)
            continue

        # Archive the file
        archive_dest = os.path.join(config.GARMIN_ARCHIVE_DIR, filename)
        if os.path.exists(archive_dest):
            base, ext = os.path.splitext(filename)
            archive_dest = os.path.join(
                config.GARMIN_ARCHIVE_DIR,
                f"{base}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}",
            )
        shutil.move(filepath, archive_dest)
        log.info("Archived to: %s", archive_dest)

        record_processed_hash(file_hash)
        processed_count += 1

    return processed_count


if __name__ == "__main__":
    try:
        count = process_new_files()
        log.info("Done. Processed %d new file(s).", count)
    except Exception as e:
        log.exception("Unhandled error in monitor_garmin_folder: %s", e)
