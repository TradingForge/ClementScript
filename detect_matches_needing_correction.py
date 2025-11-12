#!/usr/bin/env python3
"""
Detect football matches that may need kick-off time correction.

This script analyzes match files and identifies those where:
- Match duration (last_price - marketTime) >= 2 hours

These matches may have incorrect marketTime and require kick-off correction.
"""

import json
import csv
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def analyze_match_file(file_path: Path) -> Optional[Dict]:
    """
    Analyze a single match file and return timing information.
    
    Returns dict with match details if duration >= 2 hours, None otherwise.
    """
    try:
        first_market_time_str = None
        last_price_timestamp_ms = None
        market_id = None
        event_name = None
        event_id = None
        country_code = None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                if msg.get('op') != 'mcm':
                    continue
                
                mc_list = msg.get('mc', [])
                if not mc_list:
                    continue
                
                for market_change in mc_list:
                    current_market_id = market_change.get('id')
                    
                    # Extract first market definition for football Match Odds
                    if 'marketDefinition' in market_change:
                        md = market_change['marketDefinition']
                        event_type = str(md.get('eventTypeId', ''))
                        
                        if md.get('marketType') == 'MATCH_ODDS' and event_type == '1':
                            if first_market_time_str is None:
                                first_market_time_str = md.get('marketTime')
                                market_id = current_market_id
                                event_name = md.get('eventName', 'Unknown')
                                event_id = md.get('eventId', '')
                                country_code = md.get('countryCode', '')
                    
                    # Track last PRICE update (not marketDefinition updates)
                    if 'rc' in market_change and current_market_id == market_id:
                        timestamp_ms = msg.get('pt')
                        if timestamp_ms:
                            # Check if this is a real price update (has ltp)
                            has_price_update = False
                            for runner in market_change['rc']:
                                if 'ltp' in runner:
                                    has_price_update = True
                                    break
                            
                            # Check if market is suspended/closed
                            is_suspended_or_closed = False
                            if 'marketDefinition' in market_change:
                                status = market_change['marketDefinition'].get('status', '')
                                if status in ['SUSPENDED', 'CLOSED']:
                                    is_suspended_or_closed = True
                            
                            # Only update if real price update and market not suspended/closed
                            if has_price_update and not is_suspended_or_closed:
                                if isinstance(timestamp_ms, (int, float)):
                                    ts_ms = int(timestamp_ms)
                                    if last_price_timestamp_ms is None or ts_ms > last_price_timestamp_ms:
                                        last_price_timestamp_ms = ts_ms
        
        # Skip if no football Match Odds market found
        if not first_market_time_str or not last_price_timestamp_ms:
            return None
        
        # Parse first market time
        first_market_time = None
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                first_market_time = datetime.strptime(first_market_time_str, fmt)
                break
            except ValueError:
                continue
        
        if not first_market_time:
            return None
        
        # Convert last price timestamp to UTC
        last_price_time = datetime.fromtimestamp(last_price_timestamp_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
        
        # Calculate match duration (marketTime to last_price)
        match_duration_hours = (last_price_time - first_market_time).total_seconds() / 3600
        
        # Only return matches needing correction (duration >= 2 hours)
        if match_duration_hours < 2.0:
            return None
        
        # Extract minute pattern
        minute_pattern = first_market_time.minute
        
        # Parse team names from event name
        team1 = ''
        team2 = ''
        if ' v ' in event_name:
            teams = event_name.split(' v ')
            team1 = teams[0].strip()
            team2 = teams[1].strip() if len(teams) > 1 else ''
        else:
            team1 = event_name
        
        return {
            'file_path': str(file_path),
            'market_id': market_id,
            'event_id': event_id,
            'event_name': event_name,
            'team1': team1,
            'team2': team2,
            'country_code': country_code,
            'original_market_time': first_market_time.strftime('%Y-%m-%d %H:%M:%S'),
            'last_price_time': last_price_time.strftime('%Y-%m-%d %H:%M:%S'),
            'match_duration_hours': match_duration_hours,
            'minute_pattern': minute_pattern,
        }
        
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}")
        return None


def scan_football_data(input_dir: str, output_file: str = 'matches_needing_correction.csv'):
    """
    Scan all football data files and identify matches needing kick-off correction.
    
    Args:
        input_dir: Root directory of football data
        output_file: Output CSV filename
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        logger.error(f"Input directory not found: {input_dir}")
        return
    
    # Find all match files
    match_files = []
    for file_path in input_path.rglob('*'):
        if file_path.is_file() and not file_path.name.startswith('.'):
            # Skip results/output directories
            if 'results' in str(file_path) or 'output' in str(file_path):
                continue
            match_files.append(file_path)
    
    logger.info(f"Found {len(match_files)} potential match files")
    logger.info(f"Analyzing matches with duration >= 2 hours...")
    print()
    
    # Analyze all files
    matches_needing_correction = []
    analyzed_count = 0
    
    for idx, file_path in enumerate(match_files, 1):
        if idx % 500 == 0:
            logger.info(f"Progress: {idx}/{len(match_files)} ({idx*100//len(match_files)}%)")
        
        result = analyze_match_file(file_path)
        if result:
            matches_needing_correction.append(result)
            analyzed_count += 1
    
    logger.info(f"Analysis complete: {analyzed_count} matches need correction out of {len(match_files)} total")
    print()
    
    # Write results to CSV
    if matches_needing_correction:
        output_path = Path(output_file)
        
        headers = [
            'market_id',
            'event_id',
            'team1',
            'team2',
            'event_name',
            'country_code',
            'original_market_time',
            'last_price_time',
            'match_duration_hours',
            'minute_pattern',
            'file_path',
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            # Sort by duration (descending)
            matches_needing_correction.sort(key=lambda x: x['match_duration_hours'], reverse=True)
            
            for match in matches_needing_correction:
                row = [
                    match['market_id'],
                    match['event_id'],
                    match['team1'],
                    match['team2'],
                    match['event_name'],
                    match['country_code'],
                    match['original_market_time'],
                    match['last_price_time'],
                    f"{match['match_duration_hours']:.2f}",
                    match['minute_pattern'],
                    match['file_path'],
                ]
                writer.writerow(row)
        
        print("=" * 80)
        print(f"Results written to: {output_path}")
        print(f"Total matches needing correction: {len(matches_needing_correction)}")
        print()
        print("Distribution by match duration:")
        
        # Show distribution
        ranges = [
            (2.0, 3.0, "2-3 hours"),
            (3.0, 5.0, "3-5 hours"),
            (5.0, 10.0, "5-10 hours"),
            (10.0, 24.0, "10-24 hours"),
            (24.0, float('inf'), ">24 hours"),
        ]
        
        for min_h, max_h, label in ranges:
            count = sum(1 for m in matches_needing_correction if min_h <= m['match_duration_hours'] < max_h)
            if count > 0:
                pct = count / len(matches_needing_correction) * 100
                print(f"  {label:15s}: {count:5d} matches ({pct:5.1f}%)")
        
        print()
        print("Top 10 longest duration matches:")
        for idx, match in enumerate(matches_needing_correction[:10], 1):
            duration_hours = int(match['match_duration_hours'])
            print(f"{idx:2d}. {match['event_name']:40s} - {duration_hours}h ({match['country_code']})")
        
        print("=" * 80)
    else:
        logger.info("No matches found that need correction (all have duration < 2 hours)")


if __name__ == '__main__':
    import sys
    
    input_dir = sys.argv[1] if len(sys.argv) > 1 else 'football_data'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'matches_needing_correction.csv'
    
    print(f"Scanning football data in: {input_dir}")
    print(f"Output file: {output_file}")
    print()
    
    scan_football_data(input_dir, output_file)

