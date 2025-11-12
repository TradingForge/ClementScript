#!/usr/bin/env python3
"""
Simple match duration analysis: categorize by match duration (marketTime to last_price).
"""

import json
from pathlib import Path
from datetime import datetime, timezone


def analyze_match_file(file_path: Path) -> dict:
    """Analyze a single match file and return timing information."""
    try:
        first_market_time_str = None
        last_price_timestamp_ms = None
        market_id = None
        event_name = None
        
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
                    
                    # Extract first market definition
                    if 'marketDefinition' in market_change:
                        md = market_change['marketDefinition']
                        event_type = str(md.get('eventTypeId', ''))
                        
                        # Only process football Match Odds
                        if md.get('marketType') == 'MATCH_ODDS' and event_type == '1':
                            if first_market_time_str is None:
                                first_market_time_str = md.get('marketTime')
                                market_id = current_market_id
                                event_name = md.get('eventName', 'Unknown')
                    
                    # Track last PRICE update
                    if 'rc' in market_change and current_market_id == market_id:
                        timestamp_ms = msg.get('pt')
                        if timestamp_ms:
                            has_price_update = False
                            for runner in market_change['rc']:
                                if 'ltp' in runner:
                                    has_price_update = True
                                    break
                            
                            # Check if suspended/closed
                            is_suspended_or_closed = False
                            if 'marketDefinition' in market_change:
                                status = market_change['marketDefinition'].get('status', '')
                                if status in ['SUSPENDED', 'CLOSED']:
                                    is_suspended_or_closed = True
                            
                            if has_price_update and not is_suspended_or_closed:
                                if isinstance(timestamp_ms, (int, float)):
                                    ts_ms = int(timestamp_ms)
                                    if last_price_timestamp_ms is None or ts_ms > last_price_timestamp_ms:
                                        last_price_timestamp_ms = ts_ms
        
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
        
        return {
            'file_path': str(file_path),
            'market_id': market_id,
            'event_name': event_name,
            'first_market_time': first_market_time,
            'last_price_time': last_price_time,
            'match_duration_hours': match_duration_hours,
        }
        
    except Exception as e:
        return None


def scan_football_data(input_dir: str):
    """Scan all football data and categorize by match duration."""
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"Error: Input directory not found: {input_dir}")
        return
    
    # Find all match files
    match_files = []
    for file_path in input_path.rglob('*'):
        if file_path.is_file() and not file_path.name.startswith('.'):
            if 'results' not in str(file_path) and 'output' not in str(file_path):
                match_files.append(file_path)
    
    print(f"Scanning {len(match_files)} files...")
    print()
    
    # Analyze all files
    results = []
    for idx, file_path in enumerate(match_files, 1):
        if idx % 500 == 0:
            print(f"Progress: {idx}/{len(match_files)}")
        
        result = analyze_match_file(file_path)
        if result:
            results.append(result)
    
    print(f"Analyzed {len(results)} football Match Odds markets")
    print()
    
    # Categorize by duration
    target_range = []  # 1.5h to 2.15h (90 min to 129 min)
    outside_range = []
    
    for r in results:
        duration = r['match_duration_hours']
        if 1.5 <= duration <= 2.15:
            target_range.append(r)
        else:
            outside_range.append(r)
    
    # Output results
    print("=" * 100)
    print("MATCH DURATION ANALYSIS")
    print("=" * 100)
    print()
    print(f"Matches with duration 1.5h - 2.15h (90-129 min): {len(target_range)}")
    print(f"  Percentage: {len(target_range)/len(results)*100:.1f}%")
    print()
    print(f"Matches outside this range: {len(outside_range)}")
    print(f"  Percentage: {len(outside_range)/len(results)*100:.1f}%")
    print()
    
    # Show details for matches outside the target range
    if outside_range:
        print("=" * 100)
        print("MATCHES OUTSIDE 1.5h-2.15h RANGE:")
        print("=" * 100)
        print()
        
        # Sort by duration
        outside_range.sort(key=lambda x: x['match_duration_hours'])
        
        # Categorize
        very_short = [r for r in outside_range if r['match_duration_hours'] < 0.5]  # < 30 min
        short = [r for r in outside_range if 0.5 <= r['match_duration_hours'] < 1.5]  # 30-90 min
        long = [r for r in outside_range if 2.15 < r['match_duration_hours'] < 3.0]  # 129-180 min
        very_long = [r for r in outside_range if r['match_duration_hours'] >= 3.0]  # > 180 min
        negative = [r for r in outside_range if r['match_duration_hours'] < 0]  # Negative duration
        
        print(f"Distribution:")
        print(f"  Negative duration (data error): {len(negative)}")
        print(f"  < 30 minutes: {len(very_short)}")
        print(f"  30-90 minutes (short): {len(short)}")
        print(f"  129-180 minutes (long): {len(long)}")
        print(f"  > 180 minutes (very long): {len(very_long)}")
        print()
        
        # Show examples from each category
        categories = [
            ("NEGATIVE DURATION (Data Error)", negative, 10),
            ("VERY SHORT (< 30 min)", very_short, 10),
            ("SHORT (30-90 min)", short, 20),
            ("LONG (129-180 min)", long, 20),
            ("VERY LONG (> 180 min)", very_long, 20),
        ]
        
        for title, matches, show_count in categories:
            if matches:
                print("-" * 100)
                print(f"{title}: {len(matches)} matches")
                print("-" * 100)
                
                for r in matches[:show_count]:
                    duration_min = int(r['match_duration_hours'] * 60)
                    print(f"\nMarket: {r['market_id']}")
                    print(f"  Event: {r['event_name']}")
                    print(f"  Market Time: {r['first_market_time'].strftime('%Y-%m-%d %H:%M')} UTC")
                    print(f"  Last Price:  {r['last_price_time'].strftime('%Y-%m-%d %H:%M')} UTC")
                    print(f"  Duration: {r['match_duration_hours']:.2f}h ({duration_min} minutes)")
                    print(f"  File: {r['file_path']}")
                
                if len(matches) > show_count:
                    print(f"\n... and {len(matches) - show_count} more")
                print()
    
    print("=" * 100)


if __name__ == '__main__':
    import sys
    
    input_dir = sys.argv[1] if len(sys.argv) > 1 else 'football_data'
    
    print(f"Analyzing football data in: {input_dir}")
    print()
    
    scan_football_data(input_dir)

