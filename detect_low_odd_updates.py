#!/usr/bin/env python3
"""
Detect football matches with low odd updates.

This script analyzes match files and identifies those where the total number
of LTP (Last Traded Price) updates across all selections is below a minimum threshold.
"""

import json
import csv
import logging
import configparser
from pathlib import Path
from typing import Dict, Optional, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def load_settings(config_path: str = 'settings.ini') -> Dict[str, str]:
    """Load configuration from settings.ini file."""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Settings file '{config_path}' not found. Using defaults.")
        return {}
    
    settings = {}
    with path.open('r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue
            
            if '=' not in line:
                continue
            
            key, value = line.split('=', 1)
            key = key.strip().lower()
            value = value.strip()
            settings[key] = value
    
    logger.info(f"Loaded settings from '{config_path}'")
    return settings


def count_odd_updates(file_path: Path) -> Optional[Dict]:
    """
    Count the total number of LTP updates across all selections in a match.
    
    Returns dict with match details if it's a football Match Odds market, None otherwise.
    """
    try:
        market_id = None
        event_name = None
        event_id = None
        country_code = None
        total_ltp_updates = 0
        selection_updates = {}
        
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
                            if market_id is None:
                                market_id = current_market_id
                                event_name = md.get('eventName', 'Unknown')
                                event_id = md.get('eventId', '')
                                country_code = md.get('countryCode', '')
                    
                    # Count LTP updates for this market
                    if 'rc' in market_change and current_market_id == market_id:
                        for runner in market_change['rc']:
                            if 'ltp' in runner and 'id' in runner:
                                runner_id = runner['id']
                                total_ltp_updates += 1
                                selection_updates[runner_id] = selection_updates.get(runner_id, 0) + 1
        
        # Skip if no football Match Odds market found
        if market_id is None:
            return None
        
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
            'total_ltp_updates': total_ltp_updates,
            'num_selections': len(selection_updates),
            'selection_updates': dict(selection_updates),
        }
        
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}")
        return None


def scan_football_data(input_dir: str, min_odd_updates: int, output_file: str = 'low_odd_updates.csv'):
    """
    Scan all football data files and identify matches with low odd updates.
    
    Args:
        input_dir: Root directory of football data
        min_odd_updates: Minimum threshold for odd updates
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
    logger.info(f"Analyzing matches with odd updates < {min_odd_updates}...")
    print()
    
    # Analyze all files
    low_update_matches = []
    all_matches = []
    analyzed_count = 0
    
    for idx, file_path in enumerate(match_files, 1):
        if idx % 500 == 0:
            logger.info(f"Progress: {idx}/{len(match_files)} ({idx*100//len(match_files)}%)")
        
        result = count_odd_updates(file_path)
        if result:
            all_matches.append(result)
            analyzed_count += 1
            
            if result['total_ltp_updates'] < min_odd_updates:
                low_update_matches.append(result)
    
    logger.info(f"Analysis complete: {analyzed_count} football matches analyzed")
    logger.info(f"Matches with low odd updates (< {min_odd_updates}): {len(low_update_matches)} out of {analyzed_count} total")
    print()
    
    # Write results to CSV
    if low_update_matches:
        output_path = Path(output_file)
        
        headers = [
            'market_id',
            'event_id',
            'team1',
            'team2',
            'event_name',
            'country_code',
            'total_ltp_updates',
            'num_selections',
            'file_path',
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            # Sort by total_ltp_updates (ascending)
            low_update_matches.sort(key=lambda x: x['total_ltp_updates'])
            
            for match in low_update_matches:
                row = [
                    match['market_id'],
                    match['event_id'],
                    match['team1'],
                    match['team2'],
                    match['event_name'],
                    match['country_code'],
                    match['total_ltp_updates'],
                    match['num_selections'],
                    match['file_path'],
                ]
                writer.writerow(row)
        
        print("=" * 80)
        print(f"Results written to: {output_path}")
        print(f"Total matches with low odd updates: {len(low_update_matches)}")
        print(f"Percentage: {len(low_update_matches) / analyzed_count * 100:.1f}%")
        print()
        print("Distribution by update count:")
        
        # Show distribution
        ranges = [
            (0, 20, "0-20 updates"),
            (20, 40, "20-40 updates"),
            (40, 60, "40-60 updates"),
            (60, 80, "60-80 updates"),
            (80, 100, "80-100 updates"),
            (100, min_odd_updates, f"100-{min_odd_updates} updates"),
        ]
        
        for min_u, max_u, label in ranges:
            count = sum(1 for m in low_update_matches if min_u <= m['total_ltp_updates'] < max_u)
            if count > 0:
                pct = count / len(low_update_matches) * 100
                print(f"  {label:25s}: {count:5d} matches ({pct:5.1f}%)")
        
        print()
        print("Top 10 matches with fewest updates:")
        for idx, match in enumerate(low_update_matches[:10], 1):
            updates = match['total_ltp_updates']
            print(f"{idx:2d}. {match['event_name']:40s} - {updates:3d} updates ({match['country_code']})")
        
        print("=" * 80)
    else:
        logger.info(f"No matches found with odd updates < {min_odd_updates}")
    
    # Show statistics for all matches
    if all_matches:
        print()
        print("=" * 80)
        print("Overall Statistics:")
        print(f"Total matches analyzed: {len(all_matches)}")
        
        total_updates = [m['total_ltp_updates'] for m in all_matches]
        avg_updates = sum(total_updates) / len(total_updates)
        min_updates = min(total_updates)
        max_updates = max(total_updates)
        
        print(f"Average odd updates per match: {avg_updates:.1f}")
        print(f"Minimum odd updates: {min_updates}")
        print(f"Maximum odd updates: {max_updates}")
        print("=" * 80)


if __name__ == '__main__':
    import sys
    
    # Load settings
    settings = load_settings('settings.ini')
    
    input_dir = settings.get('input', 'football_data')
    min_odd_updates = int(settings.get('min_odd_updates', '120'))
    output_file = sys.argv[1] if len(sys.argv) > 1 else 'low_odd_updates.csv'
    
    print(f"Scanning football data in: {input_dir}")
    print(f"Minimum odd updates threshold: {min_odd_updates}")
    print(f"Output file: {output_file}")
    print()
    
    scan_football_data(input_dir, min_odd_updates, output_file)

