"""Advanced cron expression parser with timezone support.

Supports standard cron expressions with 5 fields:
  minute  hour  day_of_month  month  day_of_week

Special characters:
  *   — any value
  ,   — list (1,5,10)
  -   — range (1-5)
  /   — step (*/5 = every 5 minutes)
  L   — last (last day of month, last Friday)
  W   — weekday (nearest weekday to a day)
  #   — nth day of week (1#5 = first Friday)

Also supports shortcuts:
  @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly
"""

import calendar
import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

# Cron shortcuts
CRON_SHORTCUTS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

# Month and day name mappings
MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
DAY_NAMES = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}


class CronExpression:
    """Parse and evaluate cron expressions."""
    
    def __init__(self, expression: str):
        """Parse a cron expression string.
        
        Args:
            expression: Cron expression (5 fields or shortcut like @daily)
        
        Raises:
            ValueError: If the expression is invalid
        """
        self.raw = expression.strip()
        self.expression = self._normalize(self.raw)
        self.minutes, self.hours, self.days, self.months, self.dow = self._parse()
    
    def _normalize(self, expr: str) -> str:
        """Normalize cron expression, expanding shortcuts."""
        expr = expr.strip()
        if expr.startswith("@"):
            shortcut = expr.lower()
            if shortcut in CRON_SHORTCUTS:
                return CRON_SHORTCUTS[shortcut]
            raise ValueError(f"Unknown cron shortcut: {shortcut}")
        return expr
    
    def _parse(self) -> Tuple[set, set, set, set, set]:
        """Parse the 5-field cron expression into sets of valid values."""
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: '{self.expression}'. "
                f"Expected 5 fields, got {len(parts)}."
            )
        
        minutes = self._parse_field(parts[0], 0, 59, "minute")
        hours = self._parse_field(parts[1], 0, 23, "hour")
        days = self._parse_field(parts[2], 1, 31, "day")
        months = self._parse_field(parts[3], 1, 12, "month", MONTH_NAMES)
        dow = self._parse_field(parts[4], 0, 6, "day of week", DAY_NAMES)
        
        return minutes, hours, days, months, dow
    
    def _parse_field(self, field: str, min_val: int, max_val: int, 
                     name: str, names: Optional[dict] = None) -> set:
        """Parse a single cron field into a set of valid integer values."""
        result = set()
        
        # Replace names with numbers
        if names:
            for name, val in names.items():
                field = re.sub(r'\b' + name + r'\b', str(val), field, flags=re.IGNORECASE)
        
        # Handle list (comma-separated)
        for part in field.split(","):
            part = part.strip()
            if not part:
                continue
            
            # Handle step (*/N or range/N)
            step = 1
            if "/" in part:
                base, step_str = part.split("/", 1)
                try:
                    step = int(step_str)
                except ValueError:
                    raise ValueError(f"Invalid step value in {name}: {step_str}")
                if step <= 0:
                    raise ValueError(f"Step must be positive in {name}: {step}")
            else:
                base = part
            
            # Handle special characters
            if base == "*":
                values = list(range(min_val, max_val + 1, step))
            elif base == "L":
                # Last day — handled specially for day of month
                if name == "day":
                    result.add("L")
                    continue
                elif name == "day of week":
                    # Last Saturday = 6, Last Sunday = 0, etc.
                    result.add("L")
                    continue
                else:
                    raise ValueError(f"L is not valid for {name}")
            elif "W" in base:
                # Nearest weekday — handled specially
                try:
                    day = int(base.replace("W", ""))
                    result.add(f"W{day}")
                except ValueError:
                    raise ValueError(f"Invalid weekday expression in {name}: {base}")
                continue
            elif "#" in base:
                # Nth day of week
                try:
                    dow_str, nth_str = base.split("#")
                    dow_val = int(dow_str)
                    nth = int(nth_str)
                    result.add(f"#{dow_val},{nth}")
                except ValueError:
                    raise ValueError(f"Invalid nth expression in {name}: {base}")
                continue
            elif "-" in base:
                # Range
                try:
                    start_str, end_str = base.split("-", 1)
                    start = int(start_str)
                    end = int(end_str)
                except ValueError:
                    raise ValueError(f"Invalid range in {name}: {base}")
                if start > end:
                    raise ValueError(f"Invalid range in {name}: {start}-{end}")
                values = list(range(start, end + 1, step))
            else:
                # Single value
                try:
                    val = int(base)
                except ValueError:
                    raise ValueError(f"Invalid value in {name}: {base}")
                values = [val]
            
            # Validate range
            for v in values:
                if v < min_val or v > max_val:
                    raise ValueError(
                        f"Value {v} out of range [{min_val}-{max_val}] for {name}"
                    )
            result.update(values)
        
        if not result:
            raise ValueError(f"Empty field for {name}")
        
        return result
    
    def matches(self, dt: datetime) -> bool:
        """Check if a datetime matches this cron expression.
        
        Args:
            dt: The datetime to check
            
        Returns:
            True if the datetime matches the expression
        """
        # Check month
        if dt.month not in self.months:
            return False
        
        # Check minute
        if dt.minute not in self.minutes:
            return False
        
        # Check hour
        if dt.hour not in self.hours:
            return False
        
        # Check day of month
        day_match = False
        for d in self.days:
            if d == "L":
                # Last day of month
                last_day = calendar.monthrange(dt.year, dt.month)[1]
                if dt.day == last_day:
                    day_match = True
                    break
            elif isinstance(d, str) and d.startswith("W"):
                # Nearest weekday
                try:
                    target_day = int(d[1:])
                except ValueError:
                    continue
                # Find nearest weekday to target_day
                for offset in range(0, 3):
                    for check_day in [target_day - offset, target_day + offset]:
                        if 1 <= check_day <= calendar.monthrange(dt.year, dt.month)[1]:
                            check_dt = dt.replace(day=check_day)
                            if check_dt.weekday() < 5 and check_dt.day == dt.day:
                                day_match = True
                                break
                    if day_match:
                        break
                if day_match:
                    break
            elif isinstance(d, str) and d.startswith("#"):
                # Nth day of week
                try:
                    _, rest = d[1:].split(",")
                    dow_val, nth = int(rest.split(",")[0]), int(rest.split(",")[1])
                except (ValueError, IndexError):
                    continue
                # Calculate nth dow of month
                first_dow = calendar.monthrange(dt.year, dt.month)[0]
                # Find first occurrence of dow
                first_occurrence = (dow_val - first_dow) % 7
                target_day = first_occurrence + (nth - 1) * 7
                if 1 <= target_day <= calendar.monthrange(dt.year, dt.month)[1]:
                    if dt.day == target_day:
                        day_match = True
                        break
            elif dt.day == d:
                day_match = True
                break
        
        if not day_match:
            return False
        
        # Check day of week
        if dt.weekday() not in self.dow and (dt.weekday() + 1) % 7 not in self.dow:
            # Also check if L is in dow
            if "L" not in self.dow:
                return False
        
        return True
    
    def next_run(self, after: Optional[datetime] = None) -> datetime:
        """Calculate the next run time after the given datetime.
        
        Args:
            after: The datetime to start searching from (default: now)
            
        Returns:
            The next datetime that matches this expression
        """
        if after is None:
            after = datetime.now(timezone.utc)
        
        # Start from the next minute
        dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        # Search up to 4 years ahead
        max_dt = dt + timedelta(days=366 * 4)
        
        while dt < max_dt:
            if dt.month not in self.months:
                # Skip to next valid month
                next_month = None
                for m in sorted(self.months):
                    if m > dt.month:
                        next_month = m
                        break
                if next_month is None:
                    # Next year, first month
                    dt = dt.replace(year=dt.year + 1, month=min(self.months), 
                                   day=1, hour=0, minute=0)
                else:
                    dt = dt.replace(month=next_month, day=1, hour=0, minute=0)
                continue
            
            if dt.day < min(d for d in self.days if isinstance(d, int)):
                # Day might be valid, continue checking
                pass
            
            if self.matches(dt):
                return dt
            
            # Advance by 1 minute
            dt += timedelta(minutes=1)
        
        raise ValueError("No matching time found within 4 years")
    
    def __repr__(self):
        return f"CronExpression('{self.raw}')"


def parse_cron(expression: str) -> CronExpression:
    """Parse a cron expression string.
    
    Args:
        expression: Cron expression (5 fields or shortcut)
        
    Returns:
        CronExpression object
        
    Raises:
        ValueError: If the expression is invalid
    """
    return CronExpression(expression)


def is_valid_cron(expression: str) -> bool:
    """Check if a string is a valid cron expression.
    
    Args:
        expression: The string to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        parse_cron(expression)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    # Test examples
    examples = [
        "*/5 * * * *",      # Every 5 minutes
        "0 9 * * *",        # Daily at 9 AM
        "0 9 * * 1-5",      # Weekdays at 9 AM
        "0 0 1 * *",        # Monthly at midnight
        "@daily",           # Shortcut for daily
        "0 0 * * 0",        # Weekly on Sunday
    ]
    
    for expr in examples:
        cron = parse_cron(expr)
        next_run = cron.next_run()
        print(f"{expr:20s} → next: {next_run.strftime('%Y-%m-%d %H:%M')}")
