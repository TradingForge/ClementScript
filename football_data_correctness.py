#!/usr/bin/env python3
"""
Football data correctness analyser.

This auxiliary script scans the football Match Odds input data and reports how
many markets have a low number of odds updates in the 0..time_to minute range.

It reuses the same settings file and extraction logic as `football_60_triad.py`
but does not generate any output files; it only prints summary statistics to
the console.
"""

import argparse
import logging
import os
from datetime import timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

from football_60_triad import FootballTriadExtractor, load_settings

DEFAULT_THRESHOLD = 5
logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def _collect_input_files(input_dir: Path) -> Iterable[Path]:
    excluded_suffixes = {".bz2", ".xlsx", ".log", ".txt"}
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if any(filename.endswith(suffix) for suffix in excluded_suffixes):
                continue
            yield Path(root) / filename


def _create_extractor_from_settings(settings: dict) -> Tuple[FootballTriadExtractor, Path]:
    input_dir = settings.get("input", settings.get("input_dir", "football_data_output")) or "football_data_output"
    output_dir = settings.get("output", settings.get("output_dir", ".")) or "."

    def _get_int(key: str, fallback: int) -> int:
        raw_value = settings.get(key)
        if raw_value is None:
            return fallback
        try:
            return int(raw_value)
        except ValueError:
            logger.warning("Invalid integer for %s: '%s'. Using fallback %s.", key, raw_value, fallback)
            return fallback

    time_from = _get_int("time_from", 55)
    time_to = _get_int("time_to", 60)

    extractor = FootballTriadExtractor(
        input_dir=input_dir,
        output_dir=output_dir,
        time_from=time_from,
        time_to=time_to,
        debug=False,
    )

    return extractor, Path(input_dir)


def _count_updates_within_range(match_data: Dict, time_to: int) -> Dict[int, int]:
    runner_ltps = match_data.get("runner_ltps") or {}
    market_time = match_data.get("market_time")
    if not runner_ltps or not market_time:
        return {}

    match_start_ms = int(market_time.replace(tzinfo=timezone.utc).timestamp() * 1000)
    window_end_ms = match_start_ms + int(time_to) * 60000

    counts: Dict[int, int] = {}
    for selection_id, ticks in runner_ltps.items():
        count = 0
        for ts_value, _ in ticks:
            try:
                ts_int = int(ts_value)
            except (TypeError, ValueError):
                continue
            if match_start_ms <= ts_int <= window_end_ms:
                count += 1
        counts[selection_id] = count
    return counts


def analyse_correctness(extractor: FootballTriadExtractor, input_dir: Path, threshold: int) -> None:
    logger.info("Analysing Match Odds data in %s", input_dir)
    all_files = list(_collect_input_files(input_dir))
    total_files = len(all_files)
    logger.info("Found %s files for analysis", total_files)
    logger.info("Low update threshold: %s updates (0..%s minutes)", threshold, extractor.time_to)

    analysed_matches = 0
    matches_with_counts = 0
    low_update_matches = 0

    for file_path in all_files:
        match_data = extractor.process_match_file(file_path)
        if not match_data:
            continue

        analysed_matches += 1
        counts = _count_updates_within_range(match_data, extractor.time_to)
        if not counts:
            continue

        matches_with_counts += 1
        if any(count < threshold for count in counts.values()):
            low_update_matches += 1

    logger.info("Matches analysed: %s", analysed_matches)
    logger.info("Matches with update counts: %s", matches_with_counts)
    logger.info(
        "Games with odds updates less than %s in range 0..%s = %s",
        threshold,
        extractor.time_to,
        low_update_matches,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyse odds update counts in Betfair football Match Odds data.",
    )
    parser.add_argument(
        "--config",
        default="settings.ini",
        help="Path to settings file (default: settings.ini)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=None,
        help=f"Override the minimum expected number of odds updates (default: {DEFAULT_THRESHOLD}).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging output.",
    )

    args = parser.parse_args()
    _configure_logging(args.verbose)

    settings = load_settings(args.config)
    extractor, input_dir = _create_extractor_from_settings(settings)
    threshold = args.threshold if args.threshold is not None else DEFAULT_THRESHOLD

    analyse_correctness(extractor, input_dir, threshold)


if __name__ == "__main__":
    main()

