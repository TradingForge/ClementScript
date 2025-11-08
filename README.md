# Football Half-Time Odds Triad Extractor

This script processes Betfair historical JSON data files to extract synchronized half-time odds for football matches.

## Overview

The script finds "triads" where all three match outcomes (Home, Draw, Away) have Last Traded Prices (LTP) within 60 seconds of each other in the +55 to +60 minute window from kick-off.

## Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

## Input Data

The script expects unpacked Betfair JSON data files in the `football_data_output` directory. Use `unpack_files.py` to extract .bz2 files first if needed.

## Usage

### Process all files

```bash
python football_60_triad_sonnet45.py
```

### Process specific date range

```bash
# Process May 2019 only
python football_60_triad_sonnet45.py --date-from 2019-05-01 --date-to 2019-05-31

# Process entire 2019
python football_60_triad_sonnet45.py --date-from 2019-01-01 --date-to 2019-12-31
```

### Custom input directory

```bash
python football_60_triad_sonnet45.py --input my_data_folder
```

### Verbose logging

```bash
python football_60_triad_sonnet45.py --verbose
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
- Home odd HT: Home odds at half-time (or empty if no triad)
- Away odd HT: Away odds at half-time (or empty if no triad)
- Draw odd HT: Draw odds at half-time (or empty if no triad)

### 2. Excel Analysis Files

Detailed Excel files for each match in `football_data_results` directory with tabs:
- **Market Info**: Match metadata and results
- **All Ticks**: All LTP updates throughout the match
- **Filtered Ticks [55-60]**: LTPs within the +55 to +60 minute window
- **Grouped Ticks**: Filtered ticks grouped by timestamp
- **Selected Triad**: The selected synchronized triad (if found)

## Algorithm

1. Scans each match file for Match Odds (1X2) markets
2. Identifies the +55 to +60 minute window from scheduled kick-off
3. Finds all timestamps where all three outcomes (Home, Draw, Away) have LTPs within 60 seconds of each other
4. Selects the latest valid triad (closest to +60 minutes)
5. If no synchronized triad exists, the match is still included but with empty half-time odds

## Logging

Processing logs are saved to `football_60_triad.log`.

## Notes

- Only football (eventTypeId = '1') Match Odds markets are processed
- Markets must have exactly 3 runners (1X2)
- The script preserves the directory structure from input to output for Excel files
- Date filtering is based on the file path structure (Year/Month/Day)

