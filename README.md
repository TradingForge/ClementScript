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

### Configure settings

Edit `settings.ini` (or another file passed via `--config`) with key=value pairs:

```
input=football_data_output
output=.
time_from=55
time_to=60
debug=N
```

Supported keys:
- `input`: directory containing unpacked Betfair data
- `output`: directory where result CSVs and debug artifacts (when enabled) will be written (defaults to current directory). Output filenames and directories include the time window, e.g. `result_55_60.csv`, `results_only_valid_triad_55_60.csv`, `football_data_results_55_60/`.
- `time_from` / `time_to`: minutes from kick-off defining the processing window
- `debug`: `Y/N`, `true/false`, etc. Controls whether auxiliary diagnostic files are generated

### Run the extractor

```bash
# Use default settings.ini
python football_60_triad.py

# Use a custom settings file and verbose logging
python football_60_triad.py --config my_settings.ini --verbose
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

### 2. results_only_valid_triad.csv

Subset of matches where a synchronized triad was found and all three odds are populated.

### 3. Debug artifacts (when `debug` setting is enabled)

Written to `football_data_results` using the input directory structure:
- Selection tick CSVs (`*_selections.csv`, `*_selections_filtered.csv`)
- Triad diagnostics (`*_triad.csv`)
- Market metadata (`*_info`)
- Timestamp-converted JSON copies (mirroring the original filenames)

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
- The script preserves the directory structure from input to output for all generated artifacts
- Use the `--time-from` / `--time-to`