#!/usr/bin/env python3
"""
Football Half-Time Odds Triad Extractor

This script processes Betfair historical JSON data files to extract synchronized
half-time odds for football matches. It finds "triads" where all three outcomes
(Home, Draw, Away) have Last Traded Prices (LTP) within 60 seconds of each other
in the +55 to +60 minute window from kick-off.

Output:
- result.csv: Main output file with one row per match
- Excel files: Detailed analysis files in football_data_results directory

Author: Generated for Clement
Date: 2025-11-06
"""

import os
import sys
import json
import csv
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

# Configure logging (console only)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class FootballTriadExtractor:
    """Main class for extracting synchronized triads from football match data"""
    
    def __init__(self, input_dir: str, output_dir: str, time_from: int = 55, time_to: int = 60):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.time_from = time_from  # Minutes from match start
        self.time_to = time_to      # Minutes from match start
        self.results_dir = Path('football_data_results')
        self.results_dir.mkdir(exist_ok=True)
        
        # Statistics
        self.total_files = 0
        self.processed_files = 0
        self.matches_with_triads = 0
        self.matches_without_triads = 0
        self.errors = 0
        
    
    @staticmethod
    def _normalize_timestamp(ts_value):
        """Normalize various timestamp formats to (milliseconds, datetime UTC)."""
        if isinstance(ts_value, (int, float)):
            ts_ms = int(ts_value)
        elif isinstance(ts_value, str):
            dt_obj = None
            for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
                try:
                    dt_obj = datetime.strptime(ts_value, fmt)
                    break
                except ValueError:
                    continue
            if dt_obj is None:
                raise ValueError(f"Unrecognized timestamp format: {ts_value}")
            ts_ms = int(dt_obj.replace(tzinfo=timezone.utc).timestamp() * 1000)
        else:
            raise TypeError(f"Unsupported timestamp type: {type(ts_value)}")

        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return ts_ms, dt
    
    def process_match_file(self, file_path: Path) -> Optional[Dict]:
        """Process a single match file and extract triad data"""
        try:
            market_definition = None
            runner_ltps = defaultdict(list)  # runner_id -> [(timestamp_ms, ltp), ...]
            match_odds_market_id = None
            
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
                    
                    mc = msg.get('mc', [])
                    if not mc:
                        continue
                    
                    for market_change in mc:
                        current_market_id = market_change.get('id', '')
                        
                        # Extract market definition (metadata)
                        if 'marketDefinition' in market_change:
                            md = market_change['marketDefinition']
                            # Only process football Match Odds markets
                            # eventTypeId can be string or integer, '1' = football/soccer
                            event_type = str(md.get('eventTypeId', ''))
                            if md.get('marketType') == 'MATCH_ODDS' and event_type == '1':
                                market_definition = md
                                match_odds_market_id = current_market_id
                        
                        # Extract runner changes (LTP updates) - only for the Match Odds market
                        if 'rc' in market_change and match_odds_market_id and current_market_id == match_odds_market_id:
                            timestamp_ms = msg.get('pt')
                            if timestamp_ms:
                                for runner in market_change['rc']:
                                    if 'ltp' in runner and 'id' in runner:
                                        runner_id = runner['id']
                                        ltp = runner['ltp']
                                        runner_ltps[runner_id].append((timestamp_ms, ltp))
            
            # Skip if not a football Match Odds market
            if not market_definition or str(market_definition.get('eventTypeId', '')) != '1':
                return None
            
            # Skip if not exactly 3 runners (1X2)
            runners = market_definition.get('runners', [])
            if len(runners) != 3:
                return None
            
            # Find triad
            match_data = self._find_best_triad(market_definition, runner_ltps, match_odds_market_id)
            
            return match_data
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            self.errors += 1
            return None
    
    def _find_best_triad(self, market_definition: Dict, runner_ltps: Dict, market_id: str) -> Dict:
        """Find the best synchronized triad in the +55 to +60 minute window"""
        
        # Extract match metadata
        runners = market_definition.get('runners', [])
        market_time_str = market_definition.get('marketTime', '')
        event_name = market_definition.get('eventName', 'Unknown')
        country_code = market_definition.get('countryCode', '')
        event_id = market_definition.get('eventId', '')
        market_type = market_definition.get('marketType', 'MATCH_ODDS')
        
        # Parse scheduled start time
        try:
            market_time = datetime.strptime(market_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                market_time = datetime.strptime(market_time_str, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                logger.warning(f"Cannot parse market time: {market_time_str}")
                market_time = None
        
        # Determine winner/loser based on runner status
        home_result = ''
        away_result = ''
        draw_result = ''
        
        # Map runner IDs to names and determine home/draw/away
        runner_info = {}
        for idx, runner in enumerate(runners):
            runner_id = runner['id']
            runner_name = runner.get('name', '')
            runner_status = runner.get('status', 'ACTIVE')
            runner_info[runner_id] = {
                'name': runner_name,
                'status': runner_status,
                'sort_priority': runner.get('sortPriority', idx + 1)
            }
        
        # Sort runners by sort priority (typically: 1=Home, 2=Draw, 3=Away)
        sorted_runners = sorted(runner_info.items(), key=lambda x: x[1]['sort_priority'])
        
        if len(sorted_runners) == 3:
            home_runner_id = sorted_runners[0][0]
            draw_runner_id = sorted_runners[1][0]
            away_runner_id = sorted_runners[2][0]
            
            home_name = sorted_runners[0][1]['name']
            draw_name = sorted_runners[1][1]['name']
            away_name = sorted_runners[2][1]['name']
            
            # Determine results
            for runner_id, info in runner_info.items():
                if info['status'] == 'WINNER':
                    if runner_id == home_runner_id:
                        home_result = 'WINNER'
                        away_result = 'LOSER'
                        draw_result = 'LOSER'
                    elif runner_id == draw_runner_id:
                        draw_result = 'WINNER'
                        home_result = 'DRAW'
                        away_result = 'DRAW'
                    elif runner_id == away_runner_id:
                        away_result = 'WINNER'
                        home_result = 'LOSER'
                        draw_result = 'LOSER'
        else:
            logger.warning(f"Expected 3 runners but found {len(sorted_runners)}")
            home_runner_id = list(runner_info.keys())[0] if len(runner_info) > 0 else None
            draw_runner_id = list(runner_info.keys())[1] if len(runner_info) > 1 else None
            away_runner_id = list(runner_info.keys())[2] if len(runner_info) > 2 else None
            home_name = event_name
            draw_name = 'Draw'
            away_name = ''
        
        # Parse team names from event name (typically "Team A v Team B")
        if ' v ' in event_name:
            teams = event_name.split(' v ')
            home_team = teams[0].strip()
            away_team = teams[1].strip() if len(teams) > 1 else ''
        else:
            home_team = event_name
            away_team = ''
        
        # Find triads in +55 to +60 minute window
        triad = None
        triad_timestamp = None
        triad_candidates = []
        all_ticks = []
        filtered_ticks = []
        
        if market_time:
            market_time_utc = market_time.replace(tzinfo=timezone.utc)
            window_start_utc = market_time_utc + timedelta(minutes=self.time_from)
            window_end_utc = market_time_utc + timedelta(minutes=self.time_to)
            window_start_ms = int(window_start_utc.timestamp() * 1000)
            window_end_ms = int(window_end_utc.timestamp() * 1000)
            
            # Collect all ticks for each runner
            for runner_id, ltps in runner_ltps.items():
                for timestamp_ms, ltp in ltps:
                    tick_time = datetime.fromtimestamp(timestamp_ms / 1000)
                    all_ticks.append({
                        'timestamp_ms': timestamp_ms,
                        'time': tick_time,
                        'market_id': market_id,
                        'runner_id': runner_id,
                        'ltp': ltp
                    })
            
            # Filter ticks within window
            for tick in all_ticks:
                if window_start_ms <= tick['timestamp_ms'] <= window_end_ms:
                    filtered_ticks.append(tick)
            
            # Find synchronized triads
            # Group ticks by timestamp to find potential triads
            filtered_ticks.sort(key=lambda x: x['timestamp_ms'])
            
            # For each timestamp, try to find a triad where all 3 runners have LTPs within 60s
            timestamps = sorted(set(tick['timestamp_ms'] for tick in filtered_ticks))
            
            for candidate_ts in timestamps:
                # Find closest LTP for each runner relative to candidate_ts
                runner_ltps_at_ts = {}
                
                for runner_id in [home_runner_id, draw_runner_id, away_runner_id]:
                    if runner_id not in runner_ltps:
                        continue
                    
                    # Get the most recent LTP for this runner at or before candidate_ts
                    # that is within the window and within 60s of candidate_ts
                    closest_ltp = None
                    closest_ts = None
                    
                    for ts, ltp in runner_ltps[runner_id]:
                        if ts <= candidate_ts and window_start_ms <= ts <= window_end_ms:
                            time_diff = abs(candidate_ts - ts) / 1000  # in seconds
                            if time_diff <= 60:
                                if closest_ts is None or ts > closest_ts:
                                    closest_ts = ts
                                    closest_ltp = ltp
                    
                    if closest_ltp is not None:
                        runner_ltps_at_ts[runner_id] = (closest_ts, closest_ltp)
                
                # Check if we have all 3 runners
                if len(runner_ltps_at_ts) == 3:
                    # Check if all timestamps are within 60s of each other
                    timestamps_list = [ts for ts, ltp in runner_ltps_at_ts.values()]
                    max_diff = max(timestamps_list) - min(timestamps_list)
                    
                    if max_diff <= 60000:  # 60 seconds in milliseconds
                        entries = []
                        for role, runner_id in [
                            ('home', home_runner_id),
                            ('draw', draw_runner_id),
                            ('away', away_runner_id),
                        ]:
                            runner_ts, runner_ltp = runner_ltps_at_ts.get(runner_id, (None, None))
                            if runner_id is None or runner_ts is None:
                                continue
                            runner_details = runner_info.get(runner_id, {})
                            entries.append({
                                'role': role,
                                'runner_id': runner_id,
                                'runner_name': runner_details.get('name', ''),
                                'timestamp_ms': runner_ts,
                                'ltp': runner_ltp,
                            })
                        
                        if len(entries) == 3:
                            triad_candidates.append({
                                'snapshot_timestamp_ms': candidate_ts,
                                'max_time_diff_ms': max_diff,
                                'entries': entries,
                            })
            
            # Select the latest triad (closest to +60 minutes)
            if triad_candidates:
                best = max(triad_candidates, key=lambda x: x['snapshot_timestamp_ms'])
                triad_timestamp = datetime.fromtimestamp(best['snapshot_timestamp_ms'] / 1000)
                
                def extract_entry(role):
                    for entry in best['entries']:
                        if entry['role'] == role:
                            return entry
                    return {}
                
                home_entry = extract_entry('home')
                draw_entry = extract_entry('draw')
                away_entry = extract_entry('away')
                
                triad = {
                    'timestamp': best['snapshot_timestamp_ms'],
                    'home_ts': home_entry.get('timestamp_ms'),
                    'home_ltp': home_entry.get('ltp'),
                    'draw_ts': draw_entry.get('timestamp_ms'),
                    'draw_ltp': draw_entry.get('ltp'),
                    'away_ts': away_entry.get('timestamp_ms'),
                    'away_ltp': away_entry.get('ltp'),
                }
        
        # Prepare result
        match_data = {
            'div': country_code,
            'date': market_time.strftime('%Y-%m-%d') if market_time else '',
            'time': market_time.strftime('%H:%M') if market_time else '',
            'home_team': home_team,
            'away_team': away_team,
            'home_result': home_result,
            'away_result': away_result,
            'draw_result': draw_result,
            'home_odd_ht': triad['home_ltp'] if triad else '',
            'away_odd_ht': triad['away_ltp'] if triad else '',
            'draw_odd_ht': triad['draw_ltp'] if triad else '',
            'market_id': market_id,
            'event_id': event_id,
            'event_name': event_name,
            'market_type': market_type,
            'all_ticks': all_ticks,
            'filtered_ticks': filtered_ticks,
            'triad': triad,
            'triad_timestamp': triad_timestamp,
            'market_time': market_time,
            'runner_info': runner_info,
            'home_runner_id': home_runner_id if len(sorted_runners) == 3 else None,
            'draw_runner_id': draw_runner_id if len(sorted_runners) == 3 else None,
            'away_runner_id': away_runner_id if len(sorted_runners) == 3 else None,
            'runner_ltps': {runner_id: list(ltps) for runner_id, ltps in runner_ltps.items()},
            'triad_candidates': triad_candidates,
        }
        
        if triad:
            self.matches_with_triads += 1
        else:
            self.matches_without_triads += 1
        
        return match_data
    
    def create_timestamp_text_file(self, match_data: Dict, file_path: Path):
        """Create JSON file with converted readable timestamps"""
        try:
            # Create output directory structure (mirror input structure)
            relative_path = file_path.relative_to(self.input_dir)
            output_file_path = self.results_dir / relative_path
            output_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            def convert_timestamps_recursive(obj):
                """Recursively convert Unix timestamps to readable format"""
                if isinstance(obj, dict):
                    result = {}
                    for key, value in obj.items():
                        # Convert known timestamp fields
                        if key in ['pt', 'settledTime', 'suspendTime', 'bspReconciled'] and isinstance(value, (int, float)):
                            # Check if it looks like a Unix timestamp in milliseconds (13 digits)
                            if value > 1000000000000 and value < 9999999999999:
                                result[key] = datetime.fromtimestamp(value / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            else:
                                result[key] = value
                        else:
                            result[key] = convert_timestamps_recursive(value)
                    return result
                elif isinstance(obj, list):
                    return [convert_timestamps_recursive(item) for item in obj]
                else:
                    return obj
            
            # Read original file and convert timestamps
            with open(file_path, 'r', encoding='utf-8') as infile, \
                 open(output_file_path, 'w', encoding='utf-8') as outfile:
                
                for line in infile:
                    line = line.strip()
                    if not line:
                        outfile.write('\n')
                        continue
                    
                    try:
                        msg = json.loads(line)
                        
                        # Convert all timestamps recursively
                        converted_msg = convert_timestamps_recursive(msg)
                        
                        # Write converted JSON
                        outfile.write(json.dumps(converted_msg, ensure_ascii=False) + '\n')
                        
                    except json.JSONDecodeError:
                        # If line is not valid JSON, write as-is
                        outfile.write(line + '\n')
            
            logger.debug(f"Created timestamp-converted file: {output_file_path}")
            
        except Exception as e:
            logger.error(f"Error creating timestamp file: {e}")
    
    def create_selection_csv(self, match_data: Dict, file_path: Path):
        """Create CSV with per-selection tick data (pt, pt_utc, marketId, selectionId, ltp)"""
        try:
            runner_ltps = match_data.get('runner_ltps')
            market_id = match_data.get('market_id')
            if not runner_ltps or not market_id:
                return

            # Order selections by selectionId (matches Excel reference)
            selection_ids = sorted(runner_ltps.keys())
            if not selection_ids:
                return

            header = []
            for _ in selection_ids:
                header.extend(['pt', 'pt_utc', 'marketId', 'selectionId', 'ltp'])

            max_len = max(len(runner_ltps.get(selection_id, [])) for selection_id in selection_ids)
            if max_len == 0:
                return

            rows = []
            for idx in range(max_len):
                row = []
                for selection_id in selection_ids:
                    ticks = runner_ltps.get(selection_id, [])
                    if idx < len(ticks):
                        ts_value, ltp = ticks[idx]
                        try:
                            ts_ms, dt = self._normalize_timestamp(ts_value)
                            pt_utc = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        except Exception:
                            ts_ms = ts_value
                            pt_utc = ''
                        row.extend([ts_ms, pt_utc, market_id, selection_id, ltp if ltp is not None else ''])
                    else:
                        row.extend(['', '', '', '', ''])
                rows.append(row)

            # Prepare output path
            relative_path = file_path.relative_to(self.input_dir)
            output_dir = self.results_dir / relative_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{file_path.name}_selections.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                writer.writerows(rows)

            logger.debug(f"Created selections CSV: {output_file}")

        except Exception as e:
            logger.error(f"Error creating selections CSV: {e}")

    def create_selection_filtered_csv(
        self,
        match_data: Dict,
        file_path: Path,
        window_start_min: int = 55,
        window_end_min: int = 60,
    ):
        """Create CSV with per-selection tick data limited to a minute window from kick-off."""
        try:
            runner_ltps = match_data.get('runner_ltps')
            market_id = match_data.get('market_id')
            market_time = match_data.get('market_time')
            if not runner_ltps or not market_id or not market_time:
                return

            selection_ids = sorted(runner_ltps.keys())
            if not selection_ids:
                return

            window_start = market_time + timedelta(minutes=window_start_min)
            window_end = market_time + timedelta(minutes=window_end_min)
            window_start_ms = int(window_start.replace(tzinfo=timezone.utc).timestamp() * 1000)
            window_end_ms = int(window_end.replace(tzinfo=timezone.utc).timestamp() * 1000)

            filtered_runner_ltps = {}
            for selection_id in selection_ids:
                filtered_ticks = []
                for ts_value, ltp in runner_ltps.get(selection_id, []):
                    try:
                        ts_ms, dt = self._normalize_timestamp(ts_value)
                    except Exception:
                        continue
                    if window_start_ms <= ts_ms <= window_end_ms:
                        filtered_ticks.append((ts_ms, dt, ltp))
                filtered_runner_ltps[selection_id] = filtered_ticks

            if not any(filtered_runner_ltps.values()):
                return

            header = []
            for _ in selection_ids:
                header.extend(['pt', 'pt_utc', 'marketId', 'selectionId', 'ltp'])

            max_len = max(len(filtered_runner_ltps.get(selection_id, [])) for selection_id in selection_ids)

            rows = []
            for idx in range(max_len):
                row = []
                for selection_id in selection_ids:
                    ticks = filtered_runner_ltps.get(selection_id, [])
                    if idx < len(ticks):
                        ts_ms, dt, ltp = ticks[idx]
                        pt_utc = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        row.extend([ts_ms, pt_utc, market_id, selection_id, ltp if ltp is not None else ''])
                    else:
                        row.extend(['', '', '', '', ''])
                rows.append(row)

            relative_path = file_path.relative_to(self.input_dir)
            output_dir = self.results_dir / relative_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{file_path.name}_selections_filtered.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                writer.writerows(rows)

            logger.debug(f"Created filtered selections CSV: {output_file}")

        except Exception as e:
            logger.error(f"Error creating filtered selections CSV: {e}")

    def create_triad_csv(self, match_data: Dict, file_path: Path):
        """Create CSV containing all synchronized triad snapshots."""
        try:
            triad_candidates = match_data.get('triad_candidates') or []
            if not triad_candidates:
                return

            market_id = match_data.get('market_id')
            market_time = match_data.get('market_time')
            runner_info = match_data.get('runner_info', {})
            if not market_id or not market_time:
                return

            market_time_ms = int(market_time.replace(tzinfo=timezone.utc).timestamp() * 1000)

            header = [
                'triad_index',
                'snapshot_pt',
                'snapshot_pt_utc',
                'snapshot_offset_min',
                'marketId',
                'max_time_diff_sec',
                'role',
                'selectionId',
                'selectionName',
                'runner_pt',
                'runner_pt_utc',
                'ltp',
            ]

            rows = []
            role_order = {'home': 0, 'draw': 1, 'away': 2}

            for idx, triad in enumerate(sorted(triad_candidates, key=lambda x: x['snapshot_timestamp_ms']), start=1):
                snapshot_ms = triad['snapshot_timestamp_ms']
                snapshot_dt = datetime.fromtimestamp(snapshot_ms / 1000, tz=timezone.utc)
                snapshot_pt_utc = snapshot_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                offset_min = (snapshot_ms - market_time_ms) / 60000 if market_time_ms else ''
                max_diff_sec = (triad.get('max_time_diff_ms') or 0) / 1000

                entries = sorted(triad['entries'], key=lambda e: role_order.get(e.get('role', ''), 99))

                for entry in entries:
                    runner_ts_ms = entry.get('timestamp_ms')
                    runner_dt = datetime.fromtimestamp(runner_ts_ms / 1000, tz=timezone.utc) if runner_ts_ms is not None else None
                    runner_pt_utc = runner_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if runner_dt else ''

                    rows.append([
                        idx,
                        snapshot_ms,
                        snapshot_pt_utc,
                        offset_min,
                        market_id,
                        max_diff_sec,
                        entry.get('role', ''),
                        entry.get('runner_id', ''),
                        runner_info.get(entry.get('runner_id'), {}).get('name', entry.get('runner_name', '')),
                        runner_ts_ms,
                        runner_pt_utc,
                        entry.get('ltp', ''),
                    ])

            if not rows:
                return

            relative_path = file_path.relative_to(self.input_dir)
            output_dir = self.results_dir / relative_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{file_path.name}_triad.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                writer.writerows(rows)

            logger.debug(f"Created triad CSV: {output_file}")

        except Exception as e:
            logger.error(f"Error creating triad CSV: {e}")

    def create_market_info_file(self, match_data: Dict, file_path: Path):
        """Create market info file with match metadata and runner information."""
        try:
            market_id = match_data.get('market_id')
            if not market_id:
                return

            # Prepare market info data
            market_time = match_data.get('market_time')
            market_time_utc = market_time.strftime('%Y-%m-%d %H:%M:%S') if market_time else ''
            
            runner_info = match_data.get('runner_info', {})
            sorted_runners = sorted(runner_info.items(), key=lambda x: x[1].get('sort_priority', 0))

            # Create output directory
            relative_path = file_path.relative_to(self.input_dir)
            output_dir = self.results_dir / relative_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{file_path.name}_info"

            # Write market info file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("=== MARKET INFORMATION ===\n\n")
                f.write(f"Market ID: {market_id}\n")
                f.write(f"Event ID: {match_data.get('event_id', '')}\n")
                f.write(f"Event Name: {match_data.get('event_name', '')}\n")
                f.write(f"Market Type: {match_data.get('market_type', 'MATCH_ODDS')}\n")
                f.write(f"Country Code: {match_data.get('div', '')}\n")
                f.write(f"Match Start Time (UTC): {market_time_utc}\n")
                f.write(f"Home Team: {match_data.get('home_team', '')}\n")
                f.write(f"Away Team: {match_data.get('away_team', '')}\n")
                f.write(f"\n")
                f.write(f"=== MATCH RESULTS ===\n\n")
                f.write(f"Home Result: {match_data.get('home_result', '')}\n")
                f.write(f"Away Result: {match_data.get('away_result', '')}\n")
                f.write(f"Draw Result: {match_data.get('draw_result', '')}\n")
                f.write(f"\n")
                f.write(f"=== RUNNERS ===\n\n")
                
                for runner_id, info in sorted_runners:
                    f.write(f"Selection ID: {runner_id}\n")
                    f.write(f"  Name: {info.get('name', '')}\n")
                    f.write(f"  Status: {info.get('status', '')}\n")
                    f.write(f"  Sort Priority: {info.get('sort_priority', '')}\n")
                    f.write(f"\n")
                
                # Add triad information if available
                triad = match_data.get('triad')
                if triad:
                    f.write(f"=== HALF-TIME TRIAD (55-60 min) ===\n\n")
                    f.write(f"Triad Found: Yes\n")
                    f.write(f"Home Odd HT: {match_data.get('home_odd_ht', '')}\n")
                    f.write(f"Away Odd HT: {match_data.get('away_odd_ht', '')}\n")
                    f.write(f"Draw Odd HT: {match_data.get('draw_odd_ht', '')}\n")
                    
                    triad_time = datetime.fromtimestamp(triad['timestamp'] / 1000, tz=timezone.utc)
                    f.write(f"Triad Timestamp (UTC): {triad_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                else:
                    f.write(f"=== HALF-TIME TRIAD (55-60 min) ===\n\n")
                    f.write(f"Triad Found: No\n")

            logger.debug(f"Created market info file: {output_file}")

        except Exception as e:
            logger.error(f"Error creating market info file: {e}", exc_info=True)
    
    def process_all_files(self) -> List[Dict]:
        """Process all match files in the input directory"""
        logger.info(f"Scanning directory: {self.input_dir}")
        
        # Find all files (not .bz2, those should be unpacked already)
        all_files = []
        for root, dirs, files in os.walk(self.input_dir):
            for file in files:
                if not file.endswith('.bz2') and not file.endswith('.xlsx') and not file.endswith('.log') and not file.endswith('.txt'):
                    file_path = Path(root) / file
                    all_files.append(file_path)
        
        self.total_files = len(all_files)
        logger.info(f"Found {self.total_files} files to process")
        logger.info(f"Time window: +{self.time_from} to +{self.time_to} minutes from kick-off")
        
        results = []
        
        for idx, file_path in enumerate(all_files, 1):
            if idx % 100 == 0:
                logger.info(f"Progress: {idx}/{self.total_files} files processed ({idx*100//self.total_files}%)")
            
            match_data = self.process_match_file(file_path)
            
            if match_data:
                results.append(match_data)
                self.processed_files += 1
                
                # Create text file with timestamps
                self.create_timestamp_text_file(match_data, file_path)

                # Create selections CSV
                self.create_selection_csv(match_data, file_path)
                self.create_selection_filtered_csv(match_data, file_path)
                self.create_triad_csv(match_data, file_path)
                
                # Create market info file
                self.create_market_info_file(match_data, file_path)
        
        logger.info(f"Processing complete: {self.processed_files} matches processed")
        logger.info(f"Matches with triads: {self.matches_with_triads}")
        logger.info(f"Matches without triads: {self.matches_without_triads}")
        logger.info(f"Errors: {self.errors}")
        
        return results
    
    def _write_rows_to_csv(self, file_path: Path, headers: List[str], rows: List[Dict]):
        """Helper to write a list of match dictionaries to CSV."""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing results to {file_path}")
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                
                for match in rows:
                    writer.writerow([
                        match.get('div', ''),
                        match.get('date', ''),
                        match.get('time', ''),
                        match.get('home_team', ''),
                        match.get('away_team', ''),
                        match.get('home_result', ''),
                        match.get('away_result', ''),
                        match.get('draw_result', ''),
                        match.get('home_odd_ht', ''),
                        match.get('away_odd_ht', ''),
                        match.get('draw_odd_ht', ''),
                    ])
            
            logger.info(f"Successfully wrote {len(rows)} matches to {file_path}")
        
        except Exception as e:
            logger.error(f"Error writing CSV file '{file_path}': {e}")
            sys.exit(1)

    def write_csv_output(self, results: List[Dict], output_file: str = 'result.csv'):
        """Write full results CSV and a triad-only CSV."""
        headers = [
            'Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam',
            'Home result', 'Away result', 'Draw result',
            'Home odd HT', 'Away odd HT', 'Draw odd HT'
        ]
        
        output_path = Path(output_file)
        self._write_rows_to_csv(output_path, headers, results)
        
        valid_results = [
            match for match in results
            if match.get('triad')
            and all(match.get(field) not in (None, '') for field in ['home_odd_ht', 'away_odd_ht', 'draw_odd_ht'])
        ]
        valid_output_path = output_path.with_name('results_only_valid_triad.csv')
        self._write_rows_to_csv(valid_output_path, headers, valid_results)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract synchronized half-time odds triads from Betfair football data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all files with default 55-60 minute window
  python football_60_triad.py
  
  # Use custom time window (e.g., 50-65 minutes from kick-off)
  python football_60_triad.py --time-from 50 --time-to 65
  
  # Process with custom input directory and time window
  python football_60_triad.py --input my_data --time-from 45 --time-to 60
        """
    )
    
    parser.add_argument(
        '--input',
        default='football_data_output',
        help='Input directory containing unpacked Betfair data files (default: football_data_output)'
    )
    
    parser.add_argument(
        '--output',
        default='.',
        help='Output directory for result.csv (default: current directory)'
    )
    
    parser.add_argument(
        '--time-from',
        type=int,
        default=55,
        help='Start of time window in minutes from kick-off (default: 55)'
    )
    
    parser.add_argument(
        '--time-to',
        type=int,
        default=60,
        help='End of time window in minutes from kick-off (default: 60)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Create extractor
    extractor = FootballTriadExtractor(
        input_dir=args.input,
        output_dir=args.output,
        time_from=args.time_from,
        time_to=args.time_to
    )
    
    # Process all files
    logger.info("=" * 70)
    logger.info("Football Half-Time Odds Triad Extractor")
    logger.info("=" * 70)
    
    results = extractor.process_all_files()
    
    # Write CSV output
    output_csv = os.path.join(args.output, 'result.csv')
    extractor.write_csv_output(results, output_csv)
    
    logger.info("=" * 70)
    logger.info("Processing complete!")
    logger.info(f"Results written to: {output_csv}")
    logger.info(f"CSV files written to: {extractor.results_dir}")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()

