import argparse
import csv
import sys
from pathlib import Path
from typing import Optional, Sequence

DEFAULT_INPUT = Path("results/result_52_60_extended.csv")
DEFAULT_OUTPUT = Path("results/result_52_60_extended_notmatch.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter rows from a CSV where 'correctedDateTime' "
            "and 'definitionDateTime' differ."
        )
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=str(DEFAULT_INPUT),
        help=(
            "Path to the CSV file to inspect "
            f"(default: {DEFAULT_INPUT.as_posix()})."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=(
            "Path to write the filtered rows. "
            f"Use '-' to stream to stdout (default: {DEFAULT_OUTPUT.as_posix()})."
        ),
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="File encoding to use when reading/writing CSV files (default: utf-8).",
    )
    return parser.parse_args()


def validate_columns(fieldnames: Optional[Sequence[str]]) -> None:
    required_columns = {"correctedDateTime", "definitionDateTime"}
    missing = required_columns - set(fieldnames or [])
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(
            f"Input CSV is missing required column(s): {missing_list}"
        )


def main() -> int:
    args = parse_args()
    try:
        with Path(args.input_csv).open(encoding=args.encoding, newline="") as infile:
            reader = csv.DictReader(infile)
            validate_columns(reader.fieldnames)
            filtered_rows = [
                row
                for row in reader
                if row.get("correctedDateTime") != row.get("definitionDateTime")
            ]
    except FileNotFoundError:
        sys.stderr.write(f"Input file not found: {args.input_csv}\n")
        return 1
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    except Exception as exc:  # pylint: disable=broad-except
        sys.stderr.write(f"Failed to read '{args.input_csv}': {exc}\n")
        return 1

    if not filtered_rows:
        sys.stderr.write("No rows with differing corrected and definition datetimes found.\n")
        return 0

    if args.output == "-":
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=list(filtered_rows[0].keys()),
        )
        writer.writeheader()
        writer.writerows(filtered_rows)
        return 0

    output_path = Path(args.output)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding=args.encoding, newline="") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=list(filtered_rows[0].keys()),
            )
            writer.writeheader()
            writer.writerows(filtered_rows)
    except Exception as exc:  # pylint: disable=broad-except
        sys.stderr.write(f"Failed to write '{output_path}': {exc}\n")
        return 1
    sys.stdout.write(f"Wrote {len(filtered_rows)} rows to '{output_path}'.\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

