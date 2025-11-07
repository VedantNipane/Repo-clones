import urllib.request
import csv
import os
from datetime import datetime
from pathlib import Path

# ===== CONFIGURABLE VARIABLES =====
INPUT_CSV = "combined_doc_rows.csv"  # Your input CSV file
OUTPUT_DIR = "downloaded_pdfs"  # Directory to save PDFs
LOG_CSV = "download_log.csv"  # Log file to track downloads
# ==================================

def setup_directories():
    """Create output directory if it doesn't exist"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def load_download_log():
    """Load existing download log to avoid re-downloading"""
    downloaded = {}
    if os.path.exists(LOG_CSV):
        with open(LOG_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Key: url, Value: status
                downloaded[row['url']] = row['status']
    return downloaded

def save_to_log(csv_source, url, source, type_val, status, issue=""):
    """Append download result to log CSV"""
    file_exists = os.path.exists(LOG_CSV)
    
    with open(LOG_CSV, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['timestamp', 'csv_source', 'url', 'source', 'type', 
                      'status', 'issue', 'saved_filename']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        # Generate filename from source or url
        if source:
            filename = f"{source.replace(' ', '_').replace('/', '_')}.pdf"
        else:
            filename = url.split('/')[-1]
            if not filename.endswith('.pdf'):
                filename += '.pdf'
        
        writer.writerow({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'csv_source': csv_source,
            'url': url,
            'source': source,
            'type': type_val,
            'status': status,
            'issue': issue,
            'saved_filename': filename if status == 'success' else ''
        })

def download_pdf(url, filename):
    """Download PDF from URL"""
    try:
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Add headers to avoid bot detection
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        
        urllib.request.urlretrieve(url, filepath)
        return True, "Downloaded successfully"
    
    except urllib.error.HTTPError as e:
        return False, f"HTTP Error {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return False, f"URL Error: {e.reason}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def process_csv():
    """Main function to process CSV and download PDFs"""
    
    # Setup
    setup_directories()
    download_log = load_download_log()
    
    # Check if input CSV exists
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Error: {INPUT_CSV} not found!")
        return
    
    # Read input CSV
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Found {len(rows)} entries to process\n")
    
    # Process each row
    success_count = 0
    skipped_count = 0
    failed_count = 0
    
    for idx, row in enumerate(rows, 1):
        csv_source = row.get('csv_source', '')
        url = row.get('url', '').strip()
        source = row.get('source', '')
        type_val = row.get('type', '')
        
        if not url:
            print(f"[{idx}/{len(rows)}] ⚠️  Skipping: No URL provided")
            continue
        
        # Generate filename
        if source:
            filename = f"{source.replace(' ', '_').replace('/', '_')}.pdf"
        else:
            filename = url.split('/')[-1]
            if not filename.endswith('.pdf'):
                filename += '.pdf'
        
        # Check if already downloaded successfully
        if url in download_log and download_log[url] == 'success':
            print(f"[{idx}/{len(rows)}] ✓ Skipped: {filename} (already downloaded)")
            skipped_count += 1
            continue
        
        # Download PDF
        print(f"[{idx}/{len(rows)}] ⬇️  Downloading: {filename}...")
        success, message = download_pdf(url, filename)
        
        if success:
            print(f"[{idx}/{len(rows)}] ✅ Success: {filename}")
            save_to_log(csv_source, url, source, type_val, 'success')
            success_count += 1
        else:
            print(f"[{idx}/{len(rows)}] ❌ Failed: {filename}")
            print(f"    Issue: {message}")
            save_to_log(csv_source, url, source, type_val, 'failed', message)
            failed_count += 1
    
    # Summary
    print("\n" + "="*50)
    print("DOWNLOAD SUMMARY")
    print("="*50)
    print(f"✅ Successfully downloaded: {success_count}")
    print(f"⏭️  Skipped (already exists): {skipped_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📁 PDFs saved to: {OUTPUT_DIR}")
    print(f"📝 Log saved to: {LOG_CSV}")

if __name__ == "__main__":
    process_csv()