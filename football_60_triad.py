#!/usr/bin/env python3
"""
Football Half-Time Odds Triad Extractor

This script processes Betfair historical JSON data files to extract synchronized
half-time odds for football matches. It finds "triads" where all three outcomes
(Home, Draw, Away) have Last Traded Prices (LTP) synchronized within a time window.

Two-Phase Triad Selection:
1. Exact triad (primary): Window +52 to +60 minutes, max 60s between 1/X/2, picks latest
2. Relaxed triad (fallback): Window +54 to +60 minutes, max 180s between 1/X/2,
   picks smallest gap then closest to +60:00

Output:
- result.csv: Main output file with one row per match
- Excel files: Detailed analysis files in football_data_results directory
- ht_selection_method column: 'exact', 'relaxed', or 'none'

Author: Generated for Clement
Date: 2025-11-06
Updated: 2025-11-13 (Added relaxed triad fallback)
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


def extract_minute_pattern(market_time_str: str) -> Optional[int]:
    """
    Extract the minute pattern from marketTime string.
    
    Returns the minute value (0, 15, 30, 45, or other) that represents
    the scheduled pattern for this match.
    
    Examples:
        "2019-05-12T14:00:00.000Z" -> 0  (pattern is XX:00)
        "2019-05-12T14:15:00.000Z" -> 15 (pattern is XX:15)
        "2019-05-12T14:35:00.000Z" -> 35 (pattern is XX:35)
    """
    try:
        # Parse the ISO format timestamp
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                dt = datetime.strptime(market_time_str, fmt)
                return dt.minute
            except ValueError:
                continue
        logger.warning(f"Could not parse market time: {market_time_str}")
        return None
    except Exception as e:
        logger.error(f"Error extracting minute pattern: {e}")
        return None


def snap_down_to_pattern(dt: datetime, minute_pattern: int) -> datetime:
    """
    Snap a datetime down to preserve the exact minute pattern from marketTime.
    Only the hour is adjusted; minutes remain the same as the pattern.
    
    Args:
        dt: The datetime to snap
        minute_pattern: The target minute from marketTime (e.g., 0, 15, 30, 45, etc.)
    
    Returns:
        datetime with the exact minute pattern, snapped to current or previous hour
    
    Example:
        dt = 2019-05-12 17:23:00, minute_pattern = 45
        returns 2019-05-12 16:45:00 (previous hour, since 23 < 45)
        
        dt = 2019-05-12 17:50:00, minute_pattern = 45
        returns 2019-05-12 17:45:00 (current hour, since 50 >= 45)
    """
    # Preserve the exact minute pattern from marketTime
    # Snap DOWN to the pattern minute in current or previous hour
    # We want the most recent occurrence of the pattern minute that is <= dt
    
    # Try current hour first
    candidate = dt.replace(minute=minute_pattern, second=0, microsecond=0)
    
    if candidate <= dt:
        # Pattern minute in current hour is before or at dt
        return candidate
    else:
        # Pattern minute hasn't occurred yet this hour, go to previous hour
        return (dt - timedelta(hours=1)).replace(minute=minute_pattern, second=0, microsecond=0)


def calculate_correct_kickoff(
    first_market_time_str: str,
    last_price_timestamp_ms: int
) -> Tuple[Optional[datetime], Optional[datetime], Optional[int]]:
    """
    Calculate the correct kick-off time based on the logic:
    1. Extract minute pattern from first market time
    2. Go back 1h30 from last price update
    3. Snap down to minute pattern
    
    Args:
        first_market_time_str: The marketTime from first marketDefinition
        last_price_timestamp_ms: The timestamp of the last price update in milliseconds
    
    Returns:
        Tuple of (corrected_kickoff_time, original_market_time, minute_pattern)
    """
    try:
        # Extract minute pattern
        minute_pattern = extract_minute_pattern(first_market_time_str)
        if minute_pattern is None:
            return None, None, None
        
        # Parse original market time (Betfair provides UTC time with 'Z' suffix)
        # Parse as naive datetime for comparison purposes (we'll compare times in the same reference frame)
        original_time_naive = None
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                original_time_naive = datetime.strptime(first_market_time_str, fmt)
                break
            except ValueError:
                continue
        
        if original_time_naive is None:
            logger.warning(f"Could not parse original market time: {first_market_time_str}")
            return None, None, None
        
        # Convert last price timestamp to datetime in UTC
        last_price_time = datetime.fromtimestamp(last_price_timestamp_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
        
        # Go back 1h30
        estimated_end_time = last_price_time - timedelta(hours=1, minutes=30)
        
        # Snap down to minute pattern (result will be in UTC, as naive datetime)
        corrected_kickoff_utc = snap_down_to_pattern(estimated_end_time, minute_pattern)
        
        # Return: corrected time in UTC, original time as naive datetime (both in same reference frame)
        return corrected_kickoff_utc, original_time_naive, minute_pattern
        
    except Exception as e:
        logger.error(f"Error calculating correct kickoff: {e}")
        return None, None, None


class FootballTriadExtractor:
    """Main class for extracting synchronized triads from football match data"""
    
    def __init__(self, input_dir: str, output_dir: str, time_from: int = 55, time_to: int = 60, 
                 window_secs: int = 60, relaxed_time_from: int = 54, relaxed_time_to: int = 60, 
                 relaxed_window_secs: int = 180, debug: bool = False):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.time_from = time_from  # Minutes from match start (exact triad)
        self.time_to = time_to      # Minutes from match start (exact triad)
        self.window_secs = window_secs  # Max seconds between 1/X/2 for exact triad
        self.relaxed_time_from = relaxed_time_from  # Minutes from match start (relaxed triad)
        self.relaxed_time_to = relaxed_time_to      # Minutes from match start (relaxed triad)
        self.relaxed_window_secs = relaxed_window_secs  # Max seconds between 1/X/2 for relaxed triad
        self.debug = debug          # Controls generation of debug artifacts
        self.results_dir = self.output_dir / f'football_data_results_{self.time_from}_{self.time_to}'
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistics
        self.total_files = 0
        self.processed_files = 0
        self.matches_with_triads = 0
        self.matches_without_triads = 0
        self.matches_with_exact_triads = 0
        self.matches_with_relaxed_triads = 0
        self.errors = 0
        self.aligned_count = 0  # Track how many times we aligned
        self.kickoff_corrected_count = 0  # Track how many times kickoff was corrected
        self.corrected_matches = []  # Track matches where correction was applied
        
    
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
    
    @staticmethod
    def _align_market_time_to_5min(market_time_str: Optional[str]) -> Tuple[Optional[str], bool]:
        """Align market time to nearest 5-minute interval.
        
        Rounds down to the nearest 5-minute mark (e.g., 18:01 -> 18:00, 18:07 -> 18:05).
        
        Returns:
            Tuple of (aligned_time_str, was_aligned)
            was_aligned is True if the time needed alignment
        """
        if not market_time_str:
            return None, False
        
        try:
            dt = datetime.strptime(market_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                dt = datetime.strptime(market_time_str, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                return market_time_str, False  # Return as-is if can't parse
        
        # Check if already aligned
        already_aligned = (dt.minute % 5 == 0) and (dt.second == 0) and (dt.microsecond == 0)
        
        # Round down to nearest 5 minutes
        aligned_minute = (dt.minute // 5) * 5
        aligned_dt = dt.replace(minute=aligned_minute, second=0, microsecond=0)
        
        # Return in standard Betfair format
        return aligned_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z'), not already_aligned
    
    def process_match_file(self, file_path: Path) -> Optional[Dict]:
        """Process a single match file and extract triad data"""
        try:
            market_definition = None
            first_market_time_str = None
            first_open_date_str = None
            runner_ltps = defaultdict(list)  # runner_id -> [(timestamp_ms, ltp), ...]
            match_odds_market_id = None
            last_price_timestamp_ms = None  # Track the last price update timestamp for kickoff correction
            last_tick_timestamp_ms = None  # Track the absolute last timestamp in the file
            
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
                    
                    timestamp_ms = msg.get('pt')
                    
                    for market_change in mc:
                        current_market_id = market_change.get('id', '')
                        
                        # Track the absolute last timestamp for this market
                        if timestamp_ms and match_odds_market_id and current_market_id == match_odds_market_id:
                            try:
                                ts_ms, _ = self._normalize_timestamp(timestamp_ms)
                                if last_tick_timestamp_ms is None or ts_ms > last_tick_timestamp_ms:
                                    last_tick_timestamp_ms = ts_ms
                            except Exception:
                                pass
                        
                        # Extract market definition (metadata)
                        if 'marketDefinition' in market_change:
                            md = market_change['marketDefinition']
                            # Only process football Match Odds markets
                            # eventTypeId can be string or integer, '1' = football/soccer
                            event_type = str(md.get('eventTypeId', ''))
                            if md.get('marketType') == 'MATCH_ODDS' and event_type == '1':
                                if first_market_time_str is None:
                                    first_market_time_str = md.get('marketTime')
                                if first_open_date_str is None:
                                    first_open_date_str = md.get('openDate')
                                market_definition = md
                                match_odds_market_id = current_market_id
                        
                        # Extract runner changes (LTP updates) - only for the Match Odds market
                        if 'rc' in market_change and match_odds_market_id and current_market_id == match_odds_market_id:
                            if timestamp_ms:
                                # Track last PRICE update timestamp for kickoff correction
                                has_price_update = False
                                for runner in market_change['rc']:
                                    if 'ltp' in runner and 'id' in runner:
                                        runner_id = runner['id']
                                        ltp = runner['ltp']
                                        runner_ltps[runner_id].append((timestamp_ms, ltp))
                                        has_price_update = True
                                
                                # Update last price timestamp only when there's a real price update
                                # Ignore price updates that occur during market suspension/closure
                                if has_price_update:
                                    # Check if this update includes a market suspension/closure
                                    is_suspended_or_closed = False
                                    if 'marketDefinition' in market_change:
                                        status = market_change['marketDefinition'].get('status', '')
                                        if status in ['SUSPENDED', 'CLOSED']:
                                            is_suspended_or_closed = True
                                    
                                    # Only update timestamp if market is not being suspended/closed
                                    if not is_suspended_or_closed:
                                        try:
                                            ts_ms, _ = self._normalize_timestamp(timestamp_ms)
                                            if last_price_timestamp_ms is None or ts_ms > last_price_timestamp_ms:
                                                last_price_timestamp_ms = ts_ms
                                        except Exception:
                                            pass
            
            # Skip if not a football Match Odds market
            if not market_definition or str(market_definition.get('eventTypeId', '')) != '1':
                return None
            
            # Skip if not exactly 3 runners (1X2)
            runners = market_definition.get('runners', [])
            if len(runners) != 3:
                return None
            
            # Determine if kick-off correction is needed based on match duration
            corrected_kickoff_time = None
            original_market_time = None
            minute_pattern = None
            kickoff_was_corrected = False
            match_duration_hours = None
            
            # Always parse and save the original market time from first marketDefinition
            if first_market_time_str:
                for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
                    try:
                        original_market_time = datetime.strptime(first_market_time_str, fmt)
                        break
                    except ValueError:
                        continue
            
            if first_market_time_str and last_price_timestamp_ms:
                # Parse market time
                original_market_time_parsed = None
                for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
                    try:
                        original_market_time_parsed = datetime.strptime(first_market_time_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if original_market_time_parsed:
                    # Calculate match duration (last_price - marketTime)
                    last_price_time = datetime.fromtimestamp(last_price_timestamp_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                    match_duration_hours = (last_price_time - original_market_time_parsed).total_seconds() / 3600
                    
                    logger.debug(f"Processing {match_odds_market_id}: marketTime={first_market_time_str}, "
                               f"last_price={last_price_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                               f"duration={match_duration_hours:.2f}h")
                    
                    # Step 1: Apply kick-off correction using lastODDDateTime
                    corrected_kickoff_time, original_market_time, minute_pattern = calculate_correct_kickoff(
                        first_market_time_str,
                        last_price_timestamp_ms
                    )
                    
                    # Step 2: Check if correction matches marketTime
                    correction_matches = False
                    if corrected_kickoff_time and original_market_time:
                        time_diff = (corrected_kickoff_time - original_market_time).total_seconds() / 3600
                        if abs(time_diff) <= 0.01:  # Within ~30 seconds
                            correction_matches = True
                    
                    # Step 3: If doesn't match, try with lastTickDateTime
                    if not correction_matches and last_tick_timestamp_ms:
                        logger.debug(f"Correction with lastODD didn't match, trying with lastTick for {match_odds_market_id}")
                        corrected_kickoff_time, original_market_time, minute_pattern = calculate_correct_kickoff(
                            first_market_time_str,
                            last_tick_timestamp_ms
                        )
                        
                        if corrected_kickoff_time and original_market_time:
                            time_diff = (corrected_kickoff_time - original_market_time).total_seconds() / 3600
                            if abs(time_diff) <= 0.01:  # Within ~30 seconds
                                correction_matches = True
                    
                    # Step 4: Log if still doesn't match
                    if corrected_kickoff_time and original_market_time:
                        time_diff = (corrected_kickoff_time - original_market_time).total_seconds() / 3600
                        logger.debug(f"Correction calculated: {original_market_time.strftime('%H:%M')} -> "
                                   f"{corrected_kickoff_time.strftime('%H:%M')} "
                                   f"(diff: {time_diff:.2f}h)")
                        if abs(time_diff) > 0.01:  # More than ~30 seconds difference
                            kickoff_was_corrected = True
                            self.kickoff_corrected_count += 1
                            logger.info(f"Kickoff corrected for {match_odds_market_id}: "
                                       f"{original_market_time.strftime('%Y-%m-%d %H:%M')} -> "
                                       f"{corrected_kickoff_time.strftime('%Y-%m-%d %H:%M')} "
                                       f"(pattern: XX:{minute_pattern:02d}, diff: {time_diff:.2f}h)")
                            
                            # Track this correction for the CSV report
                            self.corrected_matches.append({
                                'market_id': match_odds_market_id,
                                'original_time': original_market_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'corrected_time': corrected_kickoff_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'time_diff_hours': time_diff,
                                'match_duration_hours': match_duration_hours,
                                'minute_pattern': minute_pattern,
                            })
            
            # Use corrected kickoff if available, otherwise use original market time
            if corrected_kickoff_time:
                # corrected_kickoff_time is already in UTC as naive datetime, format it
                scheduled_market_time_str = corrected_kickoff_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            else:
                # Use original market time (no correction needed or correction failed)
                scheduled_market_time_str = first_market_time_str or ''
            
            # Find triad
            match_data = self._find_best_triad(
                market_definition,
                runner_ltps,
                match_odds_market_id,
                scheduled_market_time_str=scheduled_market_time_str,
                scheduled_open_date_str=first_open_date_str,
            )
            
            # Add kickoff correction metadata to match_data
            if match_data:
                match_data['kickoff_corrected'] = kickoff_was_corrected
                match_data['original_market_time'] = original_market_time.strftime('%Y-%m-%d %H:%M:%S') if original_market_time else ''
                match_data['minute_pattern'] = minute_pattern
                match_data['match_duration_hours'] = match_duration_hours
                
                # Add last price update time
                if last_price_timestamp_ms:
                    last_price_dt = datetime.fromtimestamp(last_price_timestamp_ms / 1000, tz=timezone.utc)
                    match_data['last_price_update_time'] = last_price_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    match_data['last_price_update_time'] = ''
                
                # Add last tick update time (absolute last timestamp in file)
                if last_tick_timestamp_ms:
                    last_tick_dt = datetime.fromtimestamp(last_tick_timestamp_ms / 1000, tz=timezone.utc)
                    match_data['last_tick_update_time'] = last_tick_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    match_data['last_tick_update_time'] = ''
            
            return match_data
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            self.errors += 1
            return None
    
    def _find_triads_in_window(
        self,
        runner_ltps: Dict,
        home_runner_id: int,
        draw_runner_id: int,
        away_runner_id: int,
        runner_info: Dict,
        window_start_ms: int,
        window_end_ms: int,
        max_gap_ms: int,
    ) -> List[Dict]:
        """
        Find all triads in a given window with a maximum gap between odds.
        
        Returns a list of triad candidates with their metadata.
        """
        triad_candidates = []
        
        # Collect all ticks within the window
        window_ticks = []
        for runner_id, ltps in runner_ltps.items():
            for timestamp_ms, ltp in ltps:
                if window_start_ms <= timestamp_ms <= window_end_ms:
                    window_ticks.append({
                        'timestamp_ms': timestamp_ms,
                        'runner_id': runner_id,
                        'ltp': ltp
                    })
        
        window_ticks.sort(key=lambda x: x['timestamp_ms'])
        timestamps = sorted(set(tick['timestamp_ms'] for tick in window_ticks))
        
        # For each timestamp, try to find a triad where all 3 runners have LTPs within max_gap_ms
        for candidate_ts in timestamps:
            # Find closest LTP for each runner relative to candidate_ts
            runner_ltps_at_ts = {}
            
            for runner_id in [home_runner_id, draw_runner_id, away_runner_id]:
                if runner_id not in runner_ltps:
                    continue
                
                # Get the most recent LTP for this runner at or before candidate_ts
                # that is within the window and within max_gap_ms of candidate_ts
                closest_ltp = None
                closest_ts = None
                
                for ts, ltp in runner_ltps[runner_id]:
                    if ts <= candidate_ts and window_start_ms <= ts <= window_end_ms:
                        time_diff = abs(candidate_ts - ts) / 1000  # in seconds
                        if time_diff <= max_gap_ms / 1000:
                            if closest_ts is None or ts > closest_ts:
                                closest_ts = ts
                                closest_ltp = ltp
                
                if closest_ltp is not None:
                    runner_ltps_at_ts[runner_id] = (closest_ts, closest_ltp)
            
            # Check if we have all 3 runners
            if len(runner_ltps_at_ts) == 3:
                # Check if all timestamps are within max_gap_ms of each other
                timestamps_list = [ts for ts, ltp in runner_ltps_at_ts.values()]
                max_diff = max(timestamps_list) - min(timestamps_list)
                
                if max_diff <= max_gap_ms:
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
        
        return triad_candidates
    
    def _find_best_triad(
        self,
        market_definition: Dict,
        runner_ltps: Dict,
        market_id: str,
        *,
        scheduled_market_time_str: Optional[str] = None,
        scheduled_open_date_str: Optional[str] = None,
    ) -> Dict:
        """
        Find the best synchronized triad using a two-phase approach:
        1. Exact triad: window [time_from, time_to], max gap window_secs
        2. Relaxed triad (fallback): window [relaxed_time_from, relaxed_time_to], max gap relaxed_window_secs
        """
        
        # Extract match metadata
        runners = market_definition.get('runners', [])
        market_time_str = scheduled_market_time_str or market_definition.get('marketTime', '')
        if not market_time_str:
            market_time_str = scheduled_open_date_str or ''
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
            home_result = 'LOSER'
            draw_result = 'LOSER'
            away_result = 'LOSER'
            for runner_id, info in runner_info.items():
                if info['status'] == 'WINNER':
                    if runner_id == home_runner_id:
                        home_result = 'WINNER'
                    elif runner_id == draw_runner_id:
                        draw_result = 'WINNER'
                    elif runner_id == away_runner_id:
                        away_result = 'WINNER'
        else:
            logger.warning(f"Expected 3 runners but found {len(sorted_runners)}")
            home_runner_id = list(runner_info.keys())[0] if len(runner_info) > 0 else None
            draw_runner_id = list(runner_info.keys())[1] if len(runner_info) > 1 else None
            away_runner_id = list(runner_info.keys())[2] if len(runner_info) > 2 else None
            home_name = event_name
            draw_name = 'Draw'
            away_name = ''
            home_result = ''
            draw_result = ''
            away_result = ''
        
        # Parse team names from event name (typically "Team A v Team B")
        if ' v ' in event_name:
            teams = event_name.split(' v ')
            home_team = teams[0].strip()
            away_team = teams[1].strip() if len(teams) > 1 else ''
        else:
            home_team = event_name
            away_team = ''
        
        # Initialize triad search variables
        triad = None
        triad_timestamp = None
        triad_candidates = []
        all_ticks = []
        filtered_ticks = []
        ht_selection_method = 'none'
        
        if market_time:
            # market_time_str comes from corrected kickoff which is already in UTC format
            market_time_utc = market_time.replace(tzinfo=timezone.utc)
            
            # Collect all ticks for each runner
            for runner_id, ltps in runner_ltps.items():
                for timestamp_ms, ltp in ltps:
                    tick_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    all_ticks.append({
                        'timestamp_ms': timestamp_ms,
                        'time': tick_time,
                        'market_id': market_id,
                        'runner_id': runner_id,
                        'ltp': ltp
                    })
            
            # PHASE 1: Try to find exact triad
            exact_window_start_utc = market_time_utc + timedelta(minutes=self.time_from)
            exact_window_end_utc = market_time_utc + timedelta(minutes=self.time_to)
            exact_window_start_ms = int(exact_window_start_utc.timestamp() * 1000)
            exact_window_end_ms = int(exact_window_end_utc.timestamp() * 1000)
            
            exact_candidates = self._find_triads_in_window(
                runner_ltps,
                home_runner_id,
                draw_runner_id,
                away_runner_id,
                runner_info,
                exact_window_start_ms,
                exact_window_end_ms,
                self.window_secs * 1000,  # Convert to milliseconds
            )
            
            # If exact triad found, pick the latest one (closest to +60)
            if exact_candidates:
                best = max(exact_candidates, key=lambda x: x['snapshot_timestamp_ms'])
                triad_candidates = exact_candidates
                ht_selection_method = 'exact'
                self.matches_with_exact_triads += 1
                
                triad_timestamp = datetime.fromtimestamp(best['snapshot_timestamp_ms'] / 1000, tz=timezone.utc)
                
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
            
            # PHASE 2: If no exact triad, try relaxed triad
            if not exact_candidates:
                relaxed_window_start_utc = market_time_utc + timedelta(minutes=self.relaxed_time_from)
                relaxed_window_end_utc = market_time_utc + timedelta(minutes=self.relaxed_time_to)
                relaxed_window_start_ms = int(relaxed_window_start_utc.timestamp() * 1000)
                relaxed_window_end_ms = int(relaxed_window_end_utc.timestamp() * 1000)
                
                relaxed_candidates = self._find_triads_in_window(
                    runner_ltps,
                    home_runner_id,
                    draw_runner_id,
                    away_runner_id,
                    runner_info,
                    relaxed_window_start_ms,
                    relaxed_window_end_ms,
                    self.relaxed_window_secs * 1000,  # Convert to milliseconds
                )
                
                # If relaxed triad found, pick by: smallest max gap, then closest to +60
                if relaxed_candidates:
                    # Sort by: 1) smallest max_time_diff_ms, 2) latest timestamp
                    best = min(relaxed_candidates, 
                              key=lambda x: (x['max_time_diff_ms'], -x['snapshot_timestamp_ms']))
                    
                    triad_candidates = relaxed_candidates
                    ht_selection_method = 'relaxed'
                    self.matches_with_relaxed_triads += 1
                    
                    triad_timestamp = datetime.fromtimestamp(best['snapshot_timestamp_ms'] / 1000, tz=timezone.utc)
                    
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
            
            # Filter ticks within exact window for backwards compatibility
            for tick in all_ticks:
                if exact_window_start_ms <= tick['timestamp_ms'] <= exact_window_end_ms:
                    filtered_ticks.append(tick)
        
        # Prepare result
        # Use UTC time for display (to match results file format)
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
            'ht_selection_method': ht_selection_method,
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
            'total_ltp_updates': len(all_ticks),
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
                        # Convert known timestamp fields to UTC
                        if key in ['pt', 'settledTime', 'suspendTime', 'bspReconciled'] and isinstance(value, (int, float)):
                            # Check if it looks like a Unix timestamp in milliseconds (13 digits)
                            if value > 1000000000000 and value < 9999999999999:
                                result[key] = datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
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
                f.write(f"\n")
                f.write(f"=== TIMING INFORMATION ===\n\n")
                
                # Original market time
                original_market_time = match_data.get('original_market_time', '')
                if original_market_time:
                    f.write(f"Original Market Time (UTC): {original_market_time}\n")
                
                # Match start time (used for triad search)
                f.write(f"Match Start Time Used (UTC): {market_time_utc}\n")
                
                # Last price update time
                last_update_time = match_data.get('last_price_update_time', '')
                if last_update_time:
                    f.write(f"Last Price Update (UTC): {last_update_time}\n")
                
                # Match duration
                match_duration = match_data.get('match_duration_hours')
                if match_duration is not None:
                    duration_min = int(match_duration * 60)
                    f.write(f"Match Duration: {match_duration:.2f} hours ({duration_min} minutes)\n")
                
                # Kick-off correction status
                kickoff_corrected = match_data.get('kickoff_corrected', False)
                f.write(f"Kick-off Correction Applied: {'Yes' if kickoff_corrected else 'No'}\n")
                if kickoff_corrected:
                    minute_pattern = match_data.get('minute_pattern')
                    if minute_pattern is not None:
                        f.write(f"Minute Pattern: XX:{minute_pattern:02d}\n")
                
                f.write(f"\n")
                f.write(f"=== TEAM INFORMATION ===\n\n")
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
                    f.write(f"=== HALF-TIME TRIAD ===\n\n")
                    f.write(f"Triad Found: Yes\n")
                    f.write(f"Home Odd HT: {match_data.get('home_odd_ht', '')}\n")
                    f.write(f"Away Odd HT: {match_data.get('away_odd_ht', '')}\n")
                    f.write(f"Draw Odd HT: {match_data.get('draw_odd_ht', '')}\n")
                    
                    triad_time = datetime.fromtimestamp(triad['timestamp'] / 1000, tz=timezone.utc)
                    f.write(f"Triad Timestamp (UTC): {triad_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                else:
                    f.write(f"=== HALF-TIME TRIAD ===\n\n")
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
        logger.info(f"Debug artifacts enabled: {self.debug}")
        
        results = []
        
        for idx, file_path in enumerate(all_files, 1):
            if idx % 100 == 0:
                logger.info(f"Progress: {idx}/{self.total_files} files processed ({idx*100//self.total_files}%)")
            
            match_data = self.process_match_file(file_path)
            
            if match_data:
                results.append(match_data)
                self.processed_files += 1
                
                if self.debug:
                    # Create text file with timestamps
                    self.create_timestamp_text_file(match_data, file_path)

                    # Create selections CSVs and triad diagnostics
                    self.create_selection_csv(match_data, file_path)
                    self.create_selection_filtered_csv(match_data, file_path, self.time_from, self.time_to)
                    self.create_triad_csv(match_data, file_path)
                    
                    # Create market info file
                    self.create_market_info_file(match_data, file_path)
        
        logger.info(f"Processing complete: {self.processed_files} matches processed")
        logger.info(f"Matches with triads: {self.matches_with_triads}")
        logger.info(f"  - Exact triads: {self.matches_with_exact_triads}")
        logger.info(f"  - Relaxed triads: {self.matches_with_relaxed_triads}")
        logger.info(f"Matches without triads: {self.matches_without_triads}")
        if self.processed_files > 0:
            coverage = (self.matches_with_triads / self.processed_files) * 100
            logger.info(f"Coverage: {coverage:.1f}%")
        logger.info(f"Games with timestamp mismatch (kick-off corrected): {self.kickoff_corrected_count} out of {self.processed_files}")
        logger.info(f"Market times aligned to 5 minutes (fallback): {self.aligned_count}")
        logger.info(f"Errors: {self.errors}")
        
        return results
    
    def _write_simple_csv(self, file_path: Path, rows: List[Dict]):
        """Write simple CSV with basic match information."""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing results to {file_path}")
        
        try:
            headers = [
                'MarketId', 'Div', 'DateTime', 'HomeTeam', 'AwayTeam',
                'Home result', 'Away result', 'Draw result',
                'Home odd HT', 'Away odd HT', 'Draw odd HT', 'ht_selection_method',
                'KickOff_2_30_lastodd', 'KickOff_2_30_lasttick',
                'total_ltp_updates'
            ]
            
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                
                for match in rows:
                    # Combine date and time into DateTime format: "2019-06-01 14:00"
                    date_str = match.get('date', '')
                    time_str = match.get('time', '')
                    datetime_str = f"{date_str} {time_str}" if date_str and time_str else ''
                    
                    # KickOff_2_30_lastodd: Y if lastODD - marketTime > 2h 30min
                    match_duration = match.get('match_duration_hours')
                    kickoff_2_30_lastodd = 'Y' if match_duration and match_duration > 2.5 else 'N'
                    
                    # KickOff_2_30_lasttick: Y if lastTick - marketTime > 2h 30min
                    # Need to calculate lastTick duration
                    kickoff_2_30_lasttick = 'N'
                    original_market_time_str = match.get('original_market_time', '')
                    last_tick_time_str = match.get('last_tick_update_time', '')
                    if original_market_time_str and last_tick_time_str:
                        try:
                            original_dt = datetime.strptime(original_market_time_str, '%Y-%m-%d %H:%M:%S')
                            last_tick_dt = datetime.strptime(last_tick_time_str, '%Y-%m-%d %H:%M:%S')
                            tick_duration_hours = (last_tick_dt - original_dt).total_seconds() / 3600
                            kickoff_2_30_lasttick = 'Y' if tick_duration_hours > 2.5 else 'N'
                        except Exception:
                            pass
                    
                    total_ltp_updates = match.get('total_ltp_updates')
                    if total_ltp_updates is None:
                        total_ltp_updates = len(match.get('all_ticks', []))
                    
                    writer.writerow([
                        match.get('market_id', ''),
                        match.get('div', ''),
                        datetime_str,
                        match.get('home_team', ''),
                        match.get('away_team', ''),
                        match.get('home_result', ''),
                        match.get('away_result', ''),
                        match.get('draw_result', ''),
                        match.get('home_odd_ht', ''),
                        match.get('away_odd_ht', ''),
                        match.get('draw_odd_ht', ''),
                        match.get('ht_selection_method', 'none'),
                        kickoff_2_30_lastodd,
                        kickoff_2_30_lasttick,
                        total_ltp_updates,
                    ])
            
            logger.info(f"Successfully wrote {len(rows)} matches to {file_path}")
        
        except Exception as e:
            logger.error(f"Error writing CSV file '{file_path}': {e}")
            sys.exit(1)
    
    def _write_extended_csv(self, file_path: Path, rows: List[Dict]):
        """Write extended CSV with detailed timing information."""
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing extended results to {file_path}")
        
        try:
            headers = [
                'MarketId', 'Div', 'correctedDateTime', 'definitionDateTime', 
                'lastODDDateTime', 'lastTickDateTime', 'lastTriadDateTime', 
                'KickOff_2_30_lastodd', 'KickOff_2_30_lasttick', 'total_ltp_updates',
                'HomeTeam', 'AwayTeam', 'Home result', 'Away result', 'Draw result',
                'Home odd HT', 'Away odd HT', 'Draw odd HT', 'ht_selection_method'
            ]
            
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                
                for match in rows:
                    # correctedDateTime: The kick-off time used (format: "2019-06-01 14:00")
                    date_str = match.get('date', '')
                    time_str = match.get('time', '')
                    corrected_datetime = f"{date_str} {time_str}" if date_str and time_str else ''
                    
                    # definitionDateTime: Raw marketTime from first marketDefinition
                    definition_datetime = match.get('original_market_time', '')
                    if definition_datetime:
                        # Format to "2019-06-01 14:00" (remove seconds if present)
                        try:
                            dt = datetime.strptime(definition_datetime, '%Y-%m-%d %H:%M:%S')
                            definition_datetime = dt.strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            pass
                    
                    # lastODDDateTime: Time from last odd update
                    last_odd_datetime = match.get('last_price_update_time', '')
                    if last_odd_datetime:
                        # Format to "2019-06-01 14:00" (remove seconds if present)
                        try:
                            dt = datetime.strptime(last_odd_datetime, '%Y-%m-%d %H:%M:%S')
                            last_odd_datetime = dt.strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            pass
                    
                    # lastTickDateTime: Absolute last timestamp in file
                    last_tick_datetime = match.get('last_tick_update_time', '')
                    if last_tick_datetime:
                        # Format to "2019-06-01 14:00" (remove seconds if present)
                        try:
                            dt = datetime.strptime(last_tick_datetime, '%Y-%m-%d %H:%M:%S')
                            last_tick_datetime = dt.strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            pass
                    
                    # lastTriadDateTime: Last triad time
                    triad_timestamp = match.get('triad_timestamp')
                    if triad_timestamp:
                        try:
                            last_triad_datetime = triad_timestamp.strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            last_triad_datetime = '0000-00-00 00:00'
                    else:
                        last_triad_datetime = '0000-00-00 00:00'
                    
                    # KickOff_2_30_lastodd: Y if lastODD - marketTime > 2h 30min
                    match_duration = match.get('match_duration_hours')
                    kickoff_2_30_lastodd = 'Y' if match_duration and match_duration > 2.5 else 'N'
                    
                    # KickOff_2_30_lasttick: Y if lastTick - marketTime > 2h 30min
                    kickoff_2_30_lasttick = 'N'
                    original_market_time_str = match.get('original_market_time', '')
                    last_tick_time_str = match.get('last_tick_update_time', '')
                    if original_market_time_str and last_tick_time_str:
                        try:
                            original_dt = datetime.strptime(original_market_time_str, '%Y-%m-%d %H:%M:%S')
                            last_tick_dt = datetime.strptime(last_tick_time_str, '%Y-%m-%d %H:%M:%S')
                            tick_duration_hours = (last_tick_dt - original_dt).total_seconds() / 3600
                            kickoff_2_30_lasttick = 'Y' if tick_duration_hours > 2.5 else 'N'
                        except Exception:
                            pass
                    
                    total_ltp_updates = match.get('total_ltp_updates')
                    if total_ltp_updates is None:
                        total_ltp_updates = len(match.get('all_ticks', []))
                    
                    writer.writerow([
                        match.get('market_id', ''),
                        match.get('div', ''),
                        corrected_datetime,
                        definition_datetime,
                        last_odd_datetime,
                        last_tick_datetime,
                        last_triad_datetime,
                        kickoff_2_30_lastodd,
                        kickoff_2_30_lasttick,
                        total_ltp_updates,
                        match.get('home_team', ''),
                        match.get('away_team', ''),
                        match.get('home_result', ''),
                        match.get('away_result', ''),
                        match.get('draw_result', ''),
                        match.get('home_odd_ht', ''),
                        match.get('away_odd_ht', ''),
                        match.get('draw_odd_ht', ''),
                        match.get('ht_selection_method', 'none'),
                    ])
            
            logger.info(f"Successfully wrote {len(rows)} matches to {file_path}")
        
        except Exception as e:
            logger.error(f"Error writing CSV file '{file_path}': {e}")
            sys.exit(1)

    def write_csv_output(self, results: List[Dict], output_dir: Path):
        """Write simple and extended CSV files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        suffix = f"{self.time_from}_{self.time_to}"
        
        # Write simple CSV
        result_path = output_dir / f"result_{suffix}.csv"
        self._write_simple_csv(result_path, results)
        
        # Write extended CSV
        extended_path = output_dir / f"result_{suffix}_extended.csv"
        self._write_extended_csv(extended_path, results)
    


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {'y', 'yes', 'true', '1', 'on'}


def load_settings(config_path: str) -> Dict[str, str]:
    """Load configuration key/value pairs from a simple settings file."""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Settings file '{config_path}' not found. Using defaults only.")
        return {}

    settings: Dict[str, str] = {}

    with path.open('r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue

            if '=' not in line:
                logger.warning(f"Ignoring malformed settings line: '{line}'")
                continue

            key, value = line.split('=', 1)
            key = key.strip().lower()
            value = value.strip()
            settings[key] = value

    logger.info(f"Loaded settings from '{config_path}': {settings}")
    return settings


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract synchronized half-time odds triads from Betfair football data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process using default settings.ini
  python football_60_triad.py
  
  # Specify a custom settings file
  python football_60_triad.py --config my_settings.ini
        """
    )
    
    parser.add_argument(
        '--config',
        default='settings.ini',
        help='Path to settings file (default: settings.ini)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    raw_settings = load_settings(args.config)

    # Defaults
    input_dir = raw_settings.get('input', raw_settings.get('input_dir', 'football_data_output')) or 'football_data_output'
    output_dir = raw_settings.get('output', raw_settings.get('output_dir', '.')) or '.'

    def _get_int(key: str, fallback: int) -> int:
        value = raw_settings.get(key)
        if value is None:
            return fallback
        try:
            return int(value)
        except ValueError:
            logger.warning(f"Invalid integer for {key}: '{value}'. Using fallback {fallback}.")
            return fallback

    time_from = _get_int('time_from', 55)
    time_to = _get_int('time_to', 60)
    window_secs = _get_int('window_secs', 60)
    relaxed_time_from = _get_int('relaxed_time_from', 54)
    relaxed_time_to = _get_int('relaxed_time_to', 60)
    relaxed_window_secs = _get_int('relaxed_window_secs', 180)

    debug_value = raw_settings.get('debug', raw_settings.get('enable_debug', 'N'))
    debug = _parse_bool(debug_value) if debug_value is not None else False

    if time_from > time_to:
        logger.warning(f"time_from ({time_from}) greater than time_to ({time_to}). Swapping values.")
        time_from, time_to = time_to, time_from
    
    if relaxed_time_from > relaxed_time_to:
        logger.warning(f"relaxed_time_from ({relaxed_time_from}) greater than relaxed_time_to ({relaxed_time_to}). Swapping values.")
        relaxed_time_from, relaxed_time_to = relaxed_time_to, relaxed_time_from

    # Create extractor
    extractor = FootballTriadExtractor(
        input_dir=input_dir,
        output_dir=output_dir,
        time_from=time_from,
        time_to=time_to,
        window_secs=window_secs,
        relaxed_time_from=relaxed_time_from,
        relaxed_time_to=relaxed_time_to,
        relaxed_window_secs=relaxed_window_secs,
        debug=debug
    )
    
    # Process all files
    logger.info("=" * 70)
    logger.info("Football Half-Time Odds Triad Extractor")
    logger.info("=" * 70)
    
    results = extractor.process_all_files()
    
    # Write CSV output
    extractor.write_csv_output(results, Path(output_dir))
    
    suffix = f"{time_from}_{time_to}"
    result_path = Path(output_dir) / f"result_{suffix}.csv"
    extended_path = Path(output_dir) / f"result_{suffix}_extended.csv"
    
    logger.info("=" * 70)
    logger.info("Processing complete!")
    logger.info(f"Simple results written to: {result_path}")
    logger.info(f"Extended results written to: {extended_path}")
    logger.info(f"Debug/output directory: {extractor.results_dir}")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()

