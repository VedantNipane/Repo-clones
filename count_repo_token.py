import os
import csv
import mimetypes
import argparse
import logging
from tqdm import tqdm
from transformers import AutoTokenizer

# ================== CONFIG ==================
BASE_DIR = "cloned_repos"
OUTPUT_CSV = "repo_token_count.csv"
MODEL_ID = "google/gemma-3-27b-it"
HF_TOKEN = os.getenv("HF_TOKEN")
MAX_FILE_SIZE_MB = 100
MAX_BATCH_SIZE_MB = 100  # batch roughly 100MB of text or ~250 files
BATCH_FILE_LIMIT = 250
# ============================================

# --- Logging ---
logging.basicConfig(
    filename="repo_token_count.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# --- Exclusions ---
EXCLUDE_EXTENSIONS = {
    ".exe", ".dll", ".so", ".dylib", ".bin", ".obj", ".o", ".a", ".lib",
    ".zip", ".tar", ".gz", ".rar", ".7z", ".xz", ".bz2",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".pyc", ".pyo", ".class", ".jar",
    ".tmp", ".log", ".bak"
}

# --- Inclusions ---
INCLUDE_EXTENSIONS = {
    ".py", ".ipynb", ".c", ".cpp", ".h", ".hpp", ".java", ".js", ".ts", ".tsx", ".jsx",
    ".html", ".htm", ".css", ".php", ".go", ".rs", ".swift", ".kt", ".m", ".r", ".sh",
    ".sql", ".pl", ".rb", ".lua", ".scala", ".dart", ".json", ".yaml", ".yml", ".toml",
    ".md", ".rst", ".txt", ".ini", ".cfg", ".conf", ".csv"
}


def is_valid_file(filepath):
    """Return True if the file should be considered for tokenization."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext in EXCLUDE_EXTENSIONS:
        return False
    if any(part.startswith('.') for part in filepath.split(os.sep)):
        return False
    if os.path.getsize(filepath) > MAX_FILE_SIZE_MB * 1024 * 1024:
        return False
    if ext in INCLUDE_EXTENSIONS:
        return True

    mime_type, _ = mimetypes.guess_type(filepath)
    if mime_type and (
        mime_type.startswith("text") or "json" in mime_type or "xml" in mime_type or "yaml" in mime_type
    ):
        return True
    return False


def collect_valid_files(repo_path):
    valid_files = []
    for root, _, files in os.walk(repo_path):
        for f in files:
            full_path = os.path.join(root, f)
            try:
                if is_valid_file(full_path):
                    valid_files.append(full_path)
            except Exception as e:
                logging.warning(f"Skipping {full_path}: {e}")
    return valid_files


def count_tokens_chunked(valid_files, tokenizer):
    """Tokenize files in manageable batches and show progress."""
    total_tokens = 0
    current_batch = []
    current_batch_size = 0

    for path in tqdm(valid_files, desc="Files", unit="file"):
        try:
            size_mb = os.path.getsize(path) / (1024 * 1024)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            current_batch.append(content)
            current_batch_size += size_mb

            if len(current_batch) >= BATCH_FILE_LIMIT or current_batch_size >= MAX_BATCH_SIZE_MB:
                combined_text = "\n\n".join(current_batch)
                total_tokens += len(tokenizer.encode(combined_text))
                current_batch = []
                current_batch_size = 0

        except Exception as e:
            logging.warning(f"Error reading {path}: {e}")

    # process any leftover files
    if current_batch:
        combined_text = "\n\n".join(current_batch)
        total_tokens += len(tokenizer.encode(combined_text))

    return total_tokens


def load_processed_repos():
    if not os.path.exists(OUTPUT_CSV):
        return set()
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return {row["repo_path"] for row in reader}


def append_to_csv(repo_path, token_count, file_count):
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["repo_path", "number_of_tokens", "file_count"])
        writer.writerow([repo_path, token_count, file_count])


def process_repo(repo_path, tokenizer):
    logging.info(f"Processing repo: {repo_path}")
    valid_files = collect_valid_files(repo_path)
    logging.info(f"Found {len(valid_files)} valid files.")

    if not valid_files:
        append_to_csv(repo_path, 0, 0)
        return

    num_tokens = count_tokens_chunked(valid_files, tokenizer)
    append_to_csv(repo_path, num_tokens, len(valid_files))
    logging.info(f"Repo processed: {repo_path} | Tokens: {num_tokens:,}")


def main():
    parser = argparse.ArgumentParser(description="Count tokens for multiple repos.")
    parser.add_argument("count", help="Number of repos to process (1,2,3 or 'all')")
    args = parser.parse_args()

    logging.info(f"Starting token counting for repos in: {BASE_DIR}")

    all_repos = [
        os.path.join(BASE_DIR, d)
        for d in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, d))
    ]

    processed = load_processed_repos()
    pending_repos = [r for r in all_repos if r not in processed]

    if args.count.lower() != "all":
        try:
            n = int(args.count)
            pending_repos = pending_repos[:n]
        except ValueError:
            logging.error("Invalid count argument.")
            return

    if not pending_repos:
        logging.info("No new repos to process.")
        return

    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, token=HF_TOKEN)
    logging.info(f"Tokenizer {MODEL_ID} loaded. Processing {len(pending_repos)} repos...")

    for repo_path in tqdm(pending_repos, desc="Repos", unit="repo"):
        try:
            process_repo(repo_path, tokenizer)
        except Exception as e:
            logging.error(f"Failed processing {repo_path}: {e}")

    logging.info("All done.")


if __name__ == "__main__":
    main()
