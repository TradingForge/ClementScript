from datetime import datetime, timezone

ts3 = 1557676398604 / 1000
dt3 = datetime.fromtimestamp(ts3, tz=timezone.utc)
print(f"\nTimestamp 3: {ts3}")
print(f"UTC datetime 3: {dt3}")
print(f"Formatted 3: {dt3.strftime('%Y-%m-%d %H:%M:%S')}")

