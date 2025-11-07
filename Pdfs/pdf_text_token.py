import fitz  # PyMuPDF
import csv
import os
import logging
from pathlib import Path
from datetime import datetime
from transformers import AutoTokenizer

# ===== CONFIGURABLE VARIABLES =====
INPUT_CSV = "combined_doc_rows.csv"  # CSV with PDF info
PDF_DIR = "downloaded_pdfs"  # Directory with downloaded PDFs
TEXT_DIR = "pdf_text_files"  # Directory to save text files
OUTPUT_CSV = "pdf_token_count.csv"  # Output CSV with statistics
LOG_FILE = "pdf_processing.log"  # Log file for tracking
MODEL_ID = "google/gemma-3-27b-it"  # Tokenizer model
HF_TOKEN = "***"  # Your Hugging Face token
# ==================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Create text output directory if it doesn't exist"""
    Path(TEXT_DIR).mkdir(parents=True, exist_ok=True)
    logger.info(f"Setup: Text directory '{TEXT_DIR}' ready")

def get_pdf_files():
    """Get list of all PDF files in the PDF directory"""
    pdf_files = {}
    if os.path.exists(PDF_DIR):
        for filename in os.listdir(PDF_DIR):
            if filename.endswith('.pdf'):
                pdf_files[filename] = os.path.join(PDF_DIR, filename)
    return pdf_files

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF and return it"""
    try:
        pdf = fitz.open(pdf_path)
        text = ""
        for page in pdf:
            text += page.get_text()
        pdf.close()
        return text, None
    except Exception as e:
        return None, str(e)

def save_text_to_file(text, output_path):
    """Save text to file"""
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        return True, None
    except Exception as e:
        return False, str(e)

def count_tokens(text, tokenizer):
    """Count tokens using the tokenizer"""
    try:
        tokens = tokenizer.encode(text)
        return len(tokens), None
    except Exception as e:
        return 0, str(e)

def get_pdf_size(pdf_path):
    """Get PDF file size in bytes"""
    try:
        return os.path.getsize(pdf_path)
    except:
        return 0

def process_pdfs():
    """Main function to process all PDFs and generate statistics CSV"""
    
    print("Starting PDF processing...")
    print("="*60)
    
    # Setup
    setup_directories()
    
    # Load tokenizer
    print(f"Loading tokenizer: {MODEL_ID}...")
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, token=HF_TOKEN)
        print("✓ Tokenizer loaded successfully\n")
    except Exception as e:
        print(f"✗ Error loading tokenizer: {e}")
        return
    
    # Get all PDF files
    pdf_files = get_pdf_files()
    if not pdf_files:
        print(f"✗ No PDF files found in {PDF_DIR}")
        return
    
    print(f"Found {len(pdf_files)} PDF files to process\n")
    
    # Prepare output CSV
    results = []
    
    # Process each PDF
    for idx, (pdf_filename, pdf_path) in enumerate(pdf_files.items(), 1):
        print(f"[{idx}/{len(pdf_files)}] Processing: {pdf_filename}")
        
        # Extract text from PDF
        text, error = extract_text_from_pdf(pdf_path)
        if error:
            print(f"  ✗ Failed to extract text: {error}")
            results.append({
                'doc_name': pdf_filename,
                'doc_size': get_pdf_size(pdf_path),
                'text_file_name': '',
                'char_count': 0,
                'word_count': 0,
                'token_count': 0,
                'status': 'failed',
                'error': error
            })
            continue
        
        # Generate text filename
        text_filename = pdf_filename.replace('.pdf', '.txt')
        text_path = os.path.join(TEXT_DIR, text_filename)
        
        # Save text to file
        success, error = save_text_to_file(text, text_path)
        if not success:
            print(f"  ✗ Failed to save text file: {error}")
            continue
        
        # Calculate statistics
        char_count = len(text)
        word_count = len(text.split())
        
        # Count tokens
        print(f"  ⏳ Counting tokens...")
        token_count, token_error = count_tokens(text, tokenizer)
        if token_error:
            print(f"  ⚠️  Token counting failed: {token_error}")
        
        # Get PDF size
        doc_size = get_pdf_size(pdf_path)
        
        print(f"  ✓ Characters: {char_count:,}")
        print(f"  ✓ Words: {word_count:,}")
        print(f"  ✓ Tokens: {token_count:,}")
        print(f"  ✓ Text saved: {text_filename}\n")
        
        # Add to results
        results.append({
            'doc_name': pdf_filename,
            'doc_size': doc_size,
            'text_file_name': text_filename,
            'char_count': char_count,
            'word_count': word_count,
            'token_count': token_count,
            'status': 'success',
            'error': ''
        })
    
    # Save results to CSV
    print("="*60)
    print(f"Saving results to {OUTPUT_CSV}...")
    
    try:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['doc_name', 'doc_size', 'text_file_name', 
                         'char_count', 'word_count', 'token_count', 'status', 'error']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"✓ Results saved successfully!\n")
        
        # Summary
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful
        total_tokens = sum(r['token_count'] for r in results)
        
        print("="*60)
        print("PROCESSING SUMMARY")
        print("="*60)
        print(f"✅ Successfully processed: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"📊 Total tokens across all documents: {total_tokens:,}")
        print(f"📁 Text files saved to: {TEXT_DIR}")
        print(f"📝 Statistics saved to: {OUTPUT_CSV}")
        
    except Exception as e:
        print(f"✗ Error saving CSV: {e}")

if __name__ == "__main__":
    process_pdfs()