# Football Half-Time Odds Triad Extractor

This script processes Betfair historical JSON data files to extract synchronized half-time odds for football matches.

## Overview

The script finds "triads" where all three match outcomes (Home, Draw, Away) have Last Traded Prices (LTP) within 60 seconds of each other in a configurable time window relative to kick-off (default: +55 to +60 minutes).

## Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

## Input Data

The script expects unpacked Betfair JSON data files in the `football_data_output` directory. Use `unpack_files.py` to extract .bz2 files first if needed.

## Usage

### Process all files with default 55-60 minute window

```bash
python football_60_triad.py
```

### Use a custom time window (minutes from kick-off)

```bash
# Scan from +50 to +65 minutes
python football_60_triad.py --time-from 50 --time-to 65
```

### Custom input directory

```bash
python football_60_triad.py --input my_data_folder
```

### Verbose logging

```bash
python football_60_triad.py --verbose
```

## Output

### 1. result.csv

Main output file (TSV format) containing one row per match with columns:
- Div: Country/competition code
- Date: Match date (YYYY-MM-DD)
- Time: Kick-off time (HH:MM)
- HomeTeam: Home team name
- AwayTeam: Away team name
- Home result: WINNER/LOSER/DRAW
- Away result: WINNER/LOSER/DRAW
- Draw result: WINNER/LOSER
- Home odd HT: Home odds in the selected window (empty if no triad)
- Away odd HT: Away odds in the selected window (empty if no triad)
- Draw odd HT: Draw odds in the selected window (empty if no triad)

### 2. Excel Analysis Files

Detailed Excel files for each match in `football_data_results` with tabs:
- **Market Info**: Match metadata and results
- **All Ticks**: All LTP updates throughout the match
- **Filtered Ticks**: LTPs within the configured time window
- **Grouped Ticks**: Filtered ticks grouped by timestamp
- **Selected Triad**: The selected synchronized triad (if found)

### 3. Timestamp-Converted JSON Files

For every processed JSON input, a mirrored copy is written to `football_data_results` with the same directory structure and filename. All Unix millisecond timestamps (`pt`, `settledTime`, etc.) are replaced by human-readable UTC datetimes.

## Algorithm

1. Scans each match file for Match Odds (1X2) markets
2. Identifies the configured time window relative to scheduled kick-off
3. Finds all timestamps where all three outcomes (Home, Draw, Away) have LTPs within 60 seconds of each other
4. Selects the latest valid triad (closest to the end of the window)
5. If no synchronized triad exists, the match is still included but with empty half-time odds

## Logging

All log messages are written to the console. Redirect output to a file if you need a persistent log.

## Notes

- Only football (eventTypeId = '1') Match Odds markets are processed
- Markets must have exactly 3 runners (1X2)
- The script preserves the directory structure from input to output for both Excel files and timestamp-converted JSON copies
- Use the `--time-from` / `--time-to` flags to hone in on alternative match periods if needed

