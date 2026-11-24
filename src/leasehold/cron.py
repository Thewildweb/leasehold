"""The longest time between two firings of a cron schedule (5 fields, or a macro)."""

from __future__ import annotations

from datetime import date, timedelta

MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
}
# Four years and a bit, so that 29 February is in it.
_START, _DAYS = date(2023, 1, 1), 366 * 5


def _values(field: str, low: int, high: int) -> set[int]:
    out: set[int] = set()
    for part in field.split(","):
        rng, _, step = part.partition("/")
        if rng == "*":
            a, b = low, high
        elif "-" in rng:
            a, b = (int(x) for x in rng.split("-"))
        else:
            a = int(rng)
            b = high if step else a
        out.update(range(a, b + 1, int(step or 1)))
    return {v for v in out if low <= v <= high}


def max_gap(schedule: str) -> timedelta | None:
    """None when it fires less than twice in the years looked at."""
    minute, hour, dom, month, dow = MACROS.get(schedule, schedule).split()
    minutes = sorted(
        h * 60 + m for h in _values(hour, 0, 23) for m in _values(minute, 0, 59)
    )  # fmt: skip
    doms, months = _values(dom, 1, 31), _values(month, 1, 12)
    dows = {d % 7 for d in _values(dow, 0, 7)}  # 0 and 7 are Sunday
    # As cron (and Nomad) do it: with both day fields restricted, either one is enough.
    either = not dom.startswith("*") and not dow.startswith("*")
    if not minutes:
        return None
    gap = max((b - a for a, b in zip(minutes, minutes[1:], strict=False)), default=0)
    previous: date | None = None
    seen = 0
    for i in range(_DAYS):
        d = _START + timedelta(days=i)
        in_dom, in_dow = d.day in doms, d.isoweekday() % 7 in dows
        if d.month not in months or not ((in_dom or in_dow) if either else (in_dom and in_dow)):
            continue
        seen += 1
        if previous is not None:
            gap = max(gap, (d - previous).days * 1440 - minutes[-1] + minutes[0])
        previous = d
    return timedelta(minutes=gap) if seen * len(minutes) >= 2 else None
