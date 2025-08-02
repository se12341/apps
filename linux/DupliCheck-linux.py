import os
import hashlib
import json
import time
import csv
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime

# Log/cache path - use home directory on Linux
BASE_DIR = os.path.expanduser("~/Duplifiles")
os.makedirs(BASE_DIR, exist_ok=True)
CACHE_FILE = os.path.join(BASE_DIR, ".file_hash_cache.json")
LOG_FILE = os.path.join(BASE_DIR, "duplicate_cleanup_log.csv")

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def hash_file(path, chunk_size=65536):
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_file_signature(path):
    try:
        stat = os.stat(path)
        return (stat.st_mtime, stat.st_size)
    except FileNotFoundError:
        return None

def find_duplicates(directory):
    start = time.time()
    cache = load_cache()
    updated_cache = {}
    hashes = defaultdict(list)
    all_files = []

    for root, _, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            all_files.append(path)

    print(f"\nScanning {len(all_files)} files using SHA256 + cache...\n")

    for file_path in tqdm(all_files, desc="Hashing files"):
        sig = get_file_signature(file_path)
        if sig is None:
            continue

        key = f"{file_path}"
        cached_entry = cache.get(key)

        if cached_entry and cached_entry['sig'] == sig:
            file_hash = cached_entry['hash']
        else:
            try:
                file_hash = hash_file(file_path)
            except Exception as e:
                print(f"Error hashing {file_path}: {e}")
                continue

        updated_cache[key] = {'sig': sig, 'hash': file_hash}
        hashes[file_hash].append(file_path)

    save_cache(updated_cache)

    duplicates = {h: paths for h, paths in hashes.items() if len(paths) > 1}
    print(f"\nFinished hashing in {time.time() - start:.1f} seconds.")

    if not duplicates:
        print("No duplicates found.")
        return

    print("\nDuplicate files detected. Deleting all but one in each group...\n")
    total_deleted = 0

    # Create/open CSV log
    log_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not log_exists:
            writer.writerow(["Timestamp", "Action", "File Path", "Hash"])

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for file_list in duplicates.values():
            kept_file = file_list[0]
            file_hash = hash_file(kept_file)
            writer.writerow([timestamp, "Kept", kept_file, file_hash])
            for duplicate in file_list[1:]:
                try:
                    os.remove(duplicate)
                    print(f"Deleted: {duplicate}")
                    writer.writerow([timestamp, "Deleted", duplicate, file_hash])
                    total_deleted += 1
                except Exception as e:
                    print(f"Failed to delete {duplicate}: {e}")
                    writer.writerow([timestamp, "Failed", duplicate, f"{file_hash} ({e})"])

    print(f"\nDeleted {total_deleted} duplicate files.")
    print(f"CSV log saved to: {LOG_FILE}")

if __name__ == "__main__":
    folder = input("Enter folder path to scan: ").strip()
    find_duplicates(folder)
