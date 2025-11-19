#!/usr/bin/env python3
"""
Download historical data files from Betfair Historical Data service.
This script downloads football data files from Betfair's free plan.
"""

import os
import sys
import time
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


def get_available_files(trading, max_retries=3, retry_delay=5):
    """Get list of available historical data files with retry logic."""
    print("\nFetching available historical data files...")
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Attempt {attempt}/{max_retries}...", end='', flush=True)
            # Get the list of purchased/available historical data
            my_data = trading.historic.get_my_data()
            
            if not my_data:
                print(" No data found")
                return []
            
            print(f" Success! Found {len(my_data)} data items")
            
            for idx, item in enumerate(my_data, 1):
                print(f"  {idx}. Sport: {item.get('sport', 'N/A')}, "
                      f"Date: {item.get('forDate', 'N/A')}, "
                      f"Plan: {item.get('plan', 'N/A')}, "
                      f"ID: {item.get('purchaseItemId', 'N/A')}")
            
            return my_data
        
        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                if attempt < max_retries:
                    print(f" Timeout - retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    print(f" Failed after {max_retries} attempts")
                    print(f"ERROR: Connection timeout. The Betfair API may be slow or unavailable.")
                    print(f"       Try running the script again later.")
            else:
                print(f" Failed: {e}")
                return []
    
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


def _map_plan_name(plan_value: str) -> str:
    """Normalise plan name for historic.get_file_list positional arg."""
    if not plan_value:
        return "Basic"
    pv = plan_value.lower()
    if "basic" in pv:
        return "Basic"
    if "advanced" in pv:
        return "Advanced"
    if "pro" in pv:
        return "Pro"
    return plan_value


def _build_local_path(plan_value: str, file_path: str):
    """Build local directory structure mirroring Betfair hierarchy."""
    plan_token = _map_plan_name(plan_value).upper()
    norm = os.path.normpath(file_path)
    parts = norm.replace("\\", "/").split("/")
    parts = [p for p in parts if p and p not in (".", "..")]

    rel_parts = parts
    if plan_token in parts:
        idx = parts.index(plan_token)
        rel_parts = parts[idx:]

    if not rel_parts:
        return "", os.path.basename(file_path.strip("/"))

    local_dir = os.path.join(*rel_parts[:-1]) if len(rel_parts) > 1 else ""
    filename = rel_parts[-1]
    return local_dir, filename


def _month_range_from_iso(date_str: str):
    """Return first and last day components for the month of date_str (YYYY-MM-DD...)."""
    d = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")
    first_day = datetime(d.year, d.month, 1)
    # compute last day by jumping to next month minus one day
    if d.month == 12:
        next_month = datetime(d.year + 1, 1, 1)
    else:
        next_month = datetime(d.year, d.month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return (first_day.day, first_day.month, first_day.year, last_day.day, last_day.month, last_day.year)


def _extract_file_paths(api_response):
    """Extract list of file paths from API response structure."""
    if not api_response:
        return []
    if isinstance(api_response, list):
        if all(isinstance(item, str) for item in api_response):
            return api_response
    if isinstance(api_response, dict):
        for key in ("filePaths", "files", "data", "result"):
            value = api_response.get(key)
            if isinstance(value, list):
                return value
    if hasattr(api_response, "_data"):
        return _extract_file_paths(api_response._data)
    return api_response if isinstance(api_response, list) else []


def get_file_list(trading, data_item: dict):
    """Call Betfair DownloadListOfFiles API for a purchased month."""
    purchase_item_id = data_item.get("purchaseItemId")
    plan_display = data_item.get("plan") or "Basic Plan"
    plan_name = _map_plan_name(plan_display)
    sport = data_item.get("sport") or SPORT
    date_str = data_item.get("forDate")

    if not purchase_item_id or not date_str:
        return None

    try:
        (
            from_day,
            from_month,
            from_year,
            to_day,
            to_month,
            to_year,
        ) = _month_range_from_iso(date_str)
    except Exception:
        print(f"  WARNING: Unable to parse date {date_str}")
        return None

    response = trading.historic.get_file_list(
        sport=sport,
        plan=plan_display,
        from_day=str(from_day),
        from_month=str(from_month),
        from_year=str(from_year),
        to_day=str(to_day),
        to_month=str(to_month),
        to_year=str(to_year),
        event_id=None,
        event_name=None,
        market_types_collection=None,
        countries_collection=None,
        file_type_collection=None,
    )

    file_paths = _extract_file_paths(response)
    if not file_paths:
        print(f"  WARNING: No file paths returned for {sport} {date_str}")
        return None

    files = []
    for path in file_paths:
        if not isinstance(path, str):
            continue
        local_dir, local_name = _build_local_path(plan_name, path)
        files.append(
            {
                "file_path": path,
                "filePath": path,
                "local_dir": local_dir,
                "local_filename": local_name,
                "purchaseItemId": purchase_item_id,
            }
        )

    return files or None


def download_file(trading, file_path, purchase_item_id, local_dir=None, local_filename=None):
    """Download a specific historical data file."""
    try:
        filename = local_filename or os.path.basename(file_path.strip("/"))
        base_dir = os.path.abspath(OUTPUT_DIR)
        dest_dir = (
            os.path.abspath(os.path.join(base_dir, local_dir))
            if local_dir
            else base_dir
        )
        os.makedirs(dest_dir, exist_ok=True)
        output_path = os.path.join(dest_dir, filename)

        if os.path.exists(output_path):
            print(f"  SKIP: {output_path} (already exists)")
            return True

        print(
            f"  Downloading to {output_path} (API path: {file_path})...",
            end="",
            flush=True,
        )

        # Let betfairlightweight handle streaming directly into dest_dir
        trading.historic.download_file(file_path=file_path, store_directory=dest_dir)

        if not os.path.exists(output_path):
            raise Exception("Download reported success but file not found")

        file_size = os.path.getsize(output_path)
        if file_size < 50:
            with open(output_path, "rb") as f:
                preview = f.read(200).decode("utf-8", errors="ignore")
            raise Exception(
                f"Downloaded file looks invalid (size={file_size} bytes): {preview}"
            )

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
        
        # Get file list for this item/month (constructs path directly)
        try:
            file_list = get_file_list(trading, item)
            
            if not file_list:
                print("  No files found for this item")
                continue
            
            # Try each file path returned by API until one succeeds
            downloaded = False
            for file_info in file_list:
                file_path = file_info.get('file_path') or file_info.get('filePath')
                local_filename = file_info.get('local_filename')
                local_dir = file_info.get('local_dir')
                if file_path:
                    '''
                    filename = local_filename or os.path.basename(file_path.strip("/"))
                    base_dir = os.path.abspath(OUTPUT_DIR)
                    dest_dir = (
                        os.path.abspath(os.path.join(base_dir, local_dir))
                        if local_dir
                        else base_dir
                    )
                    output_path = os.path.join(dest_dir, filename)
                    if os.path.exists(output_path):
                        print(f"  SKIP: {output_path} (already exists)")
                        downloaded = True
                        continue
                    '''
                    success = download_file(trading, file_path, purchase_item_id, local_dir, local_filename)
                    if success:
                        total_downloaded += 1
                        downloaded = True
                       #break

            if not downloaded:
                total_failed += 1
                print("  All available file paths failed for this item")
        except Exception as e:
            print(f"  ERROR processing item: {e}")
            total_failed += 1
            continue
    
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


