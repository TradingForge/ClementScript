#!/usr/bin/env python3
"""
Download historical data files from Betfair Historical Data service.
This script downloads football data files from Betfair's free plan.
"""

import os
import sys
from datetime import datetime, timedelta
import betfairlightweight


# Configuration
BETFAIR_USERNAME = "m.tradingforge@gmail.com"  # Fill in your Betfair username
BETFAIR_PASSWORD = "KLUHd7d%jhg!!"  # Fill in your Betfair password
BETFAIR_APP_KEY = "vPNcomt9ZMAxsVqw"   # Fill in your Betfair app key

OUTPUT_DIR = "football_data_zip"  # Directory to save downloaded files
SPORT = "Soccer"  # Sport name in Betfair system

# Default certificates directory inside the project root
# Will be resolved relative to this file, i.e. "<project>/Certificates"
DEFAULT_CERTS_DIR = os.path.join(os.path.dirname(__file__), "Certificates")

def _find_cert_files(certs_dir):
    """Try to find client cert and key files in the given directory."""
    if not certs_dir or not os.path.isdir(certs_dir):
        return None
    # Ensure certs_dir is a string, not a tuple
    if isinstance(certs_dir, tuple):
        certs_dir = certs_dir[0] if certs_dir else None
    if not certs_dir:
        return None
    entries = os.listdir(certs_dir)
    # Prefer common Betfair names
    candidates_cert = [
        "client-2048.crt",
        "client.crt",
        "client.pem",
        "certificate.pem",
    ]
    candidates_key = [
        "client-2048.key",
        "client.key",
        "key.pem",
        "private.key",
    ]
    cert_path = None
    key_path = None
    for name in candidates_cert:
        p = os.path.join(certs_dir, name)
        if os.path.isfile(p):
            cert_path = p
            break
    for name in candidates_key:
        p = os.path.join(certs_dir, name)
        if os.path.isfile(p):
            key_path = p
            break
    if cert_path and key_path:
        return (cert_path, key_path)
    # As a fallback, try to pick any .crt/.pem and any .key in the directory
    if not cert_path:
        for e in entries:
            if e.lower().endswith((".crt", ".pem")):
                cert_path = os.path.join(certs_dir, e)
                break
    if not key_path:
        for e in entries:
            if e.lower().endswith(".key"):
                key_path = os.path.join(certs_dir, e)
                break
    if cert_path and key_path:
        return (cert_path, key_path)
    return None

def ensure_output_directory():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")


def connect_to_betfair():
    """Connect and login to Betfair API."""
    print("Connecting to Betfair API...")
    
    if not BETFAIR_USERNAME or not BETFAIR_PASSWORD or not BETFAIR_APP_KEY:
        print("ERROR: Please configure BETFAIR_USERNAME, BETFAIR_PASSWORD, and BETFAIR_APP_KEY")
        sys.exit(1)
    
    try:
        # Prefer certificate login if ./Certificates exists and contains cert+key
        # Check if certificates exist in the Certificates directory
        cert_tuple = _find_cert_files(DEFAULT_CERTS_DIR)
        if cert_tuple:
            # betfairlightweight expects a directory path, not a tuple
            trading = betfairlightweight.APIClient(
                username=BETFAIR_USERNAME,
                password=BETFAIR_PASSWORD,
                app_key=BETFAIR_APP_KEY,
                certs=DEFAULT_CERTS_DIR  # Pass directory path
            )
            print(f"Using certificates from: {DEFAULT_CERTS_DIR}")
            trading.login()
        else:
            # Fallback to interactive login (may require 2FA)
            trading = betfairlightweight.APIClient(
                username=BETFAIR_USERNAME,
                password=BETFAIR_PASSWORD,
                app_key=BETFAIR_APP_KEY,
                certs=None
            )
            trading.login_interactive()
        print("Successfully logged in to Betfair")
        return trading
    except Exception as e:
        print(f"ERROR: Failed to login to Betfair: {e}")
        sys.exit(1)


def get_available_files(trading):
    """Get list of available historical data files."""
    print("\nFetching available historical data files...")
    
    try:
        # Get the list of purchased/available historical data
        my_data = trading.historic.get_my_data()
        
        if not my_data:
            return []
        
        print(f"\nFound {len(my_data)} data items:")
        for idx, item in enumerate(my_data, 1):
            print(f"  {idx}. Sport: {item.get('sport', 'N/A')}, "
                  f"Date: {item.get('forDate', 'N/A')}, "
                  f"Plan: {item.get('plan', 'N/A')}, "
                  f"ID: {item.get('purchaseItemId', 'N/A')}")
        
        return my_data
    
    except Exception as e:
        print(f"ERROR: Failed to get available files: {e}")
        return []


def filter_football_files(data_items, years_back=20):
    """Filter for football/soccer files from the last N years."""
    football_items = []
    cutoff_date = datetime.now() - timedelta(days=years_back * 365)
    
    for item in data_items:
        sport = item.get('sport', '')
        date_str = item.get('forDate', '')
        
        # Filter by sport (Soccer/Football)
        if sport.lower() not in ['soccer', 'football']:
            continue
        
        # Filter by date if available
        if date_str:
            try:
                # Parse date string (format: '2017-06-01T00:00:00')
                item_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
                if item_date < cutoff_date:
                    continue
            except:
                pass  # Include if we can't parse the date
        
        football_items.append(item)
    
    return football_items


def get_file_list(trading, purchase_item_id):
    """Get detailed file list for a specific purchase item."""
    try:
        file_list = trading.historic.get_file_list(
            purchase_item_id=purchase_item_id
        )
        return file_list
    except Exception as e:
        print(f"  ERROR: Failed to get file list for item {purchase_item_id}: {e}")
        return None


def download_file(trading, file_path, purchase_item_id):
    """Download a specific historical data file."""
    try:
        # Extract filename from path
        filename = os.path.basename(file_path)
        output_path = os.path.join(OUTPUT_DIR, filename)
        
        # Skip if already downloaded
        if os.path.exists(output_path):
            print(f"  SKIP: {filename} (already exists)")
            return True
        
        print(f"  Downloading: {filename}...", end='', flush=True)
        
        # Download the file
        response = trading.historic.download_file(
            file_path=file_path,
            purchase_item_id=purchase_item_id
        )
        
        # Save to local file
        with open(output_path, 'wb') as f:
            f.write(response)
        
        file_size = os.path.getsize(output_path)
        print(f" OK ({file_size:,} bytes)")
        return True
        
    except Exception as e:
        print(f" FAILED: {e}")
        return False


def main():
    """Main function to download historical data."""
    print("=" * 70)
    print("Betfair Historical Data Download Script")
    print("=" * 70)
    
    # Setup
    ensure_output_directory()
    
    # Connect to Betfair
    trading = connect_to_betfair()
    
    # Get available data
    my_data = get_available_files(trading)
    
    if not my_data:
        print("\nNo data available to download.")
        return
    
    # Filter for football data from last 10 years
    football_data = filter_football_files(my_data)
    
    if not football_data:
        print(f"\nNo football/soccer data found in the last 10 years.")
        print(f"Total items available: {len(my_data)}")
        return
    
    print(f"\nFiltered to {len(football_data)} football/soccer items")
    
    # Download files
    print("\nStarting downloads...")
    total_downloaded = 0
    total_failed = 0
    
    for item in football_data:
        purchase_item_id = item.get('purchaseItemId')
        sport = item.get('sport', 'N/A')
        date = item.get('forDate', 'N/A')
        
        print(f"\nProcessing: {sport} - {date} (ID: {purchase_item_id})")
        
        # Get file list for this item
        file_list = get_file_list(trading, purchase_item_id)
        
        if not file_list:
            print("  No files found for this item")
            continue
        
        # Download each file
        for file_info in file_list:
            file_path = file_info.get('file_path') or file_info.get('filePath')
            if file_path:
                success = download_file(trading, file_path, purchase_item_id)
                if success:
                    total_downloaded += 1
                else:
                    total_failed += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Total files downloaded: {total_downloaded}")
    print(f"Total files failed: {total_failed}")
    print(f"Files saved to: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


