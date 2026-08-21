"""
callback_scheduler.py – AWS Lambda: Callback Time-Slot Recommender

This function is invoked from an Amazon Connect contact flow. It looks at the
caller's queue, works out which day(s) the queue is open, and hands back a short
list of available callback time slots that still have room.

Contact Attributes (every one is optional and overrides its env default)
------------------------------------------------------------------------
maxSuggestions  – How many slots to hand back (default 4, clamped to 1..10)
offsetDays      – 0 = today, 1 = next open day (default), 2 = the open day after that, ...
timeRanges      – Comma-separated windows, e.g. "09:00-11:00,13:00-15:30".
                  When absent, we build one window from bizStartHour/bizEndHour.
slotMinutes     – Length of each slot in minutes (default: SLOT_MINUTES env, else 15)
slotCapacity    – How many bookings one slot can hold (default: SLOT_CAPACITY env, else 10)
windowDays      – How many days ahead we're allowed to look (default 7, capped at 7)
bizStartHour    – Start hour for the fallback single window (int; ignored when timeRanges is set)
bizEndHour      – End hour for the fallback single window   (int; ignored when timeRanges is set)
phoneNumber     – Caller's number (falls back to CustomerEndpoint.Address)

Environment Variables
---------------------
AWS_REGION, BUSINESS_TZ, SLOT_MINUTES, SLOT_CAPACITY, LEAD_MINUTES,
FALLBACK_START_HOUR, FALLBACK_END_HOUR, END_BUFFER_MINUTES,
SLOTS_TABLE, CALLBACKS_TABLE, CONNECT_QUEUE_ID
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration pulled from the environment (with sane fallbacks)
# ---------------------------------------------------------------------------
REGION            = os.getenv("AWS_REGION", "us-west-2")
TIMEZONE_NAME     = os.getenv("BUSINESS_TZ", "America/New_York")
LOCAL_TZ          = ZoneInfo(TIMEZONE_NAME)
UTC_TZ            = ZoneInfo("UTC")

DEFAULT_SLOT_LEN  = int(os.getenv("SLOT_MINUTES", "3"))
DEFAULT_SLOT_CAP  = int(os.getenv("SLOT_CAPACITY", "10"))
LEAD_TIME_MIN     = int(os.getenv("LEAD_MINUTES", "5"))
DEFAULT_OPEN_HR   = int(os.getenv("FALLBACK_START_HOUR", "9"))
DEFAULT_CLOSE_HR  = int(os.getenv("FALLBACK_END_HOUR", "20"))
TAIL_BUFFER_MIN   = int(os.getenv("END_BUFFER_MINUTES", "15"))

MAX_WINDOW_DAYS   = 7
DEFAULT_MAX_SLOTS = 4

# DynamoDB + shared state
_dynamo       = boto3.resource("dynamodb", region_name=REGION)
_slots_table  = _dynamo.Table(os.getenv("SLOTS_TABLE", "callback_slots"))
WEEKDAY_NAMES = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
_queue_name_cache: dict[str, str] = {}

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# A wall-clock time as (hour, minute)
Clock = tuple[int, int]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def round_up_to_slot(moment: datetime, step_min: int) -> datetime:
    """Round `moment` up to the next slot boundary of `step_min` minutes."""
    floored = moment.replace(minute=(moment.minute // step_min) * step_min,
                             second=0, microsecond=0)
    return floored if floored == moment else floored + timedelta(minutes=step_min)


def subtract_minutes(clock: Clock, minutes: int) -> Clock:
    """Shift a (hour, minute) tuple earlier by `minutes`, never crossing before midnight."""
    anchor = datetime(2000, 1, 1, *clock, tzinfo=LOCAL_TZ) - timedelta(minutes=max(0, minutes))
    return (0, 0) if anchor.day < 1 else (anchor.hour, anchor.minute)


def clamp_clock(hour: int, minute: int) -> Clock:
    """Force hour into 0..23 and minute into 0..59."""
    return (max(0, min(hour, 23)), max(0, min(minute, 59)))


def make_slot_key(queue_id: str, moment_utc: datetime) -> str:
    """Build the DynamoDB partition key for a queue + UTC slot start."""
    return f"{queue_id}#{moment_utc.astimezone(UTC_TZ).strftime('%Y%m%d%H%M')}"


def speak_datetime(moment: datetime) -> str:
    """Render a datetime the way we want Connect to read it aloud."""
    try:
        return moment.strftime("%A, %d %B at %-I:%M %p")
    except Exception:
        return moment.strftime("%A, %d %B at %I:%M %p").replace(" 0", " ")


def read_attr(event: dict, key: str, default=None):
    """Read a contact attribute; fall back to a top-level key for direct test events."""
    try:
        return event["Details"]["ContactData"]["Attributes"].get(key, default)
    except Exception:
        return event.get(key, default)


def read_int_attr(event: dict, key: str, default: int, lo: int = 1, hi: int = 10_000) -> int:
    """Read a contact attribute as a bounded int, falling back to `default` on any problem."""
    try:
        value = int(read_attr(event, key, default))
        return value if lo <= value <= hi else default
    except Exception:
        return default


def assemble_response(queue_id, phone, slots, *, result="FULL",
                      message=None, reason=None) -> dict:
    """Flatten the chosen slots into the response shape the contact flow expects."""
    payload = {"result": result, "queueId": str(queue_id), "phoneNumber": str(phone)}
    if message:
        payload["message"] = message
    if reason:
        payload["reason"] = reason
    for idx, slot in enumerate(slots, 1):
        payload[f"alt{idx}Iso"]   = slot["iso"]
        payload[f"alt{idx}Speak"] = slot["speak"]
    return payload


# ---------------------------------------------------------------------------
# Parsing the time windows
# ---------------------------------------------------------------------------

def parse_clock(text: str) -> Clock:
    """Turn 'HH:MM' or 'HH' into a (hour, minute) tuple."""
    pieces = text.strip().split(":")
    return clamp_clock(int(pieces[0]), int(pieces[1]) if len(pieces) > 1 else 0)


def parse_time_ranges(raw: str) -> list[tuple[Clock, Clock]]:
    """
    Turn "09:00-11:00,13:00-15:30" into a sorted list of
    ((start_h, start_m), (end_h, end_m)) tuples.

    Anything malformed is dropped. Overlapping windows are left as-is (not merged).
    """
    windows: list[tuple[Clock, Clock]] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if "-" not in chunk:
            continue
        try:
            left, right = chunk.split("-", 1)
            start, end = parse_clock(left), parse_clock(right)
            if start < end:
                windows.append((start, end))
        except Exception as err:
            logger.warning("Ignoring bad time range %r: %s", chunk, err)
    return sorted(windows)


def fallback_time_ranges(event: dict) -> list[tuple[Clock, Clock]]:
    """Construct a single window from bizStartHour / bizEndHour (or env defaults)."""
    open_hr  = read_int_attr(event, "bizStartHour", DEFAULT_OPEN_HR, 0, 23)
    close_hr = read_int_attr(event, "bizEndHour",   DEFAULT_CLOSE_HR, 0, 24)
    start = clamp_clock(open_hr, 0)
    end   = (23, 59) if close_hr >= 24 else clamp_clock(close_hr, 0)
    if start >= end:
        logger.warning("Fallback window invalid (start >= end): %s >= %s — using 09:00-20:00",
                       start, end)
        return [((9, 0), (20, 0))]
    return [(start, end)]


def resolve_time_ranges(event: dict) -> list[tuple[Clock, Clock]]:
    """Prefer the timeRanges attribute; otherwise build the single fallback window."""
    raw = read_attr(event, "timeRanges")
    if raw:
        parsed = parse_time_ranges(str(raw))
        if parsed:
            return parsed
        logger.warning("timeRanges %r yielded nothing usable — falling back to defaults", raw)
    return fallback_time_ranges(event)


def window_containing(clock: Clock,
                      windows: list[tuple[Clock, Clock]]) -> Optional[tuple[Clock, Clock]]:
    """Return the first window that contains `clock`, else None."""
    for start, end in windows:
        if start <= clock < end:
            return start, end
    return None


def window_after(clock: Clock,
                 windows: list[tuple[Clock, Clock]]) -> Optional[tuple[Clock, Clock]]:
    """Return the first window that begins strictly after `clock`, else None."""
    for start, end in windows:
        if start > clock:
            return start, end
    return None


# ---------------------------------------------------------------------------
# Amazon Connect lookups
# ---------------------------------------------------------------------------

def lookup_queue_name(client, instance_id: Optional[str], queue_id: str) -> str:
    """Fetch (and cache) the human-readable queue name."""
    if queue_id in _queue_name_cache:
        return _queue_name_cache[queue_id]
    name = f"Unknown-{queue_id}"
    if instance_id:
        try:
            name = client.describe_queue(
                InstanceId=instance_id, QueueId=queue_id
            )["Queue"]["Name"]
        except Exception as err:
            logger.warning("Could not resolve queue name for %s: %s", queue_id, err)
    _queue_name_cache[queue_id] = name
    return name


def resolve_queue_id(event: dict) -> Optional[str]:
    """Work out the queue id from the attribute, the contact's queue ARN, or the env."""
    raw = read_attr(event, "queueId")
    if not raw:
        try:
            raw = event["Details"]["ContactData"]["Queue"]["ARN"]
        except Exception:
            raw = None
    raw = raw or os.getenv("CONNECT_QUEUE_ID")
    if raw and raw.startswith("arn:aws:connect:"):
        raw = raw.split("/queue/")[-1]
    return raw or None


def resolve_instance_id(event: dict) -> Optional[str]:
    """Pull the Connect instance id out of the contact's InstanceARN."""
    try:
        return event["Details"]["ContactData"]["InstanceARN"].split("instance/")[-1]
    except Exception:
        return None


def fetch_open_weekdays(client, instance_id: Optional[str],
                        queue_id: str) -> Optional[set[int]]:
    """
    Return the set of weekday indices (0=Mon ... 6=Sun) the queue is open on.

    Returns None when Hours-of-Operation data can't be read — callers treat
    None as "open every day".
    """
    if not instance_id:
        return None
    try:
        hop_id = client.describe_queue(
            InstanceId=instance_id, QueueId=queue_id
        )["Queue"].get("HoursOfOperationId")
        if not hop_id:
            return None
        config = client.describe_hours_of_operation(
            InstanceId=instance_id, HoursOfOperationId=hop_id
        )["HoursOfOperation"]["Config"]
        return {WEEKDAY_NAMES.index(entry["Day"])
                for entry in config if entry.get("Day") in WEEKDAY_NAMES}
    except Exception as err:
        logger.error("Failed to load hours of operation: %s", err)
        return None


# ---------------------------------------------------------------------------
# Figuring out which calendar day to target
# ---------------------------------------------------------------------------

def is_open_on(day: date, open_weekdays: Optional[set[int]]) -> bool:
    """True when the queue is open on `day` (or when HOP data is missing)."""
    return open_weekdays is None or day.weekday() in open_weekdays


def pick_target_date(now: datetime, offset: int,
                     open_weekdays: Optional[set[int]],
                     last_day: date) -> Optional[date]:
    """
    Choose the calendar date to schedule against:
        offset=0 → today (only if open)
        offset=1 → the next open day
        offset=2 → the open day after that
        ...

    Closed days are skipped. Returns None if no qualifying open day exists
    on or before `last_day`.
    """
    if offset == 0:
        # Same day only works when the queue is actually open today.
        if is_open_on(now.date(), open_weekdays):
            return now.date()
        logger.warning("offset=0 but today (%s) is closed; no date to return", now.date())
        return None

    open_days_seen = 0
    cursor = now.date() + timedelta(days=1)
    while cursor <= last_day:
        if is_open_on(cursor, open_weekdays):
            open_days_seen += 1
            if open_days_seen == offset:
                return cursor
        cursor += timedelta(days=1)

    logger.warning("Ran out of window before finding %d open day(s) by %s", offset, last_day)
    return None


# ---------------------------------------------------------------------------
# DynamoDB slot bookkeeping
# ---------------------------------------------------------------------------

def get_slot_item(key: str) -> Optional[dict]:
    """Strongly-consistent read of a single slot record."""
    return _slots_table.get_item(Key={"slotKey": key}, ConsistentRead=True).get("Item")


def get_or_create_slot(queue_id: str, moment_utc: datetime,
                       capacity: int, queue_name: str) -> tuple[int, int]:
    """
    Fetch a slot record, creating it if it doesn't exist yet.
    Returns (current_count, capacity); returns (cap, cap) if anything goes wrong,
    which effectively marks the slot as full so we skip it.
    """
    key  = make_slot_key(queue_id, moment_utc)
    item = get_slot_item(key)
    if item:
        return int(item.get("count", 0)), int(item.get("capacity", capacity))
    try:
        _slots_table.put_item(
            Item={
                "slotKey": key,
                "count": 0,
                "capacity": capacity,
                "windowStartIso":
                    moment_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "windowStartLocal":
                    moment_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "tz": TIMEZONE_NAME,
                "queueName": queue_name,
                "slotTtl": int(datetime.now().timestamp()) + 2 * 86_400,
            },
            ConditionExpression="attribute_not_exists(slotKey)",
        )
        return 0, capacity
    except ClientError as err:
        # Someone else created the slot between our read and write — re-read it.
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            item = get_slot_item(key)
            if item:
                return int(item.get("count", 0)), int(item.get("capacity", capacity))
        logger.error("put_item failed for slot %s: %s", key, err)
    return capacity, capacity


# ---------------------------------------------------------------------------
# Walking a day and collecting free slots
# ---------------------------------------------------------------------------

def collect_slots_on_day(queue_id: str, queue_name: str, target_date: date,
                         windows: list[tuple[Clock, Clock]], slot_len: int,
                         slot_cap: int, want: int,
                         floor_clock: Optional[Clock] = None) -> list[dict]:
    """
    Gather up to `want` open slots on a single date, scanning every window in order.

    floor_clock: (h, m) lower bound so same-day calls skip slots already in the past.
                 Leave None on future dates so scanning begins at each window's open.
    """
    found: list[dict] = []

    for win_start, win_end in windows:
        # Reserve a tail buffer so we don't offer a slot right at closing.
        effective_end = subtract_minutes(win_end, TAIL_BUFFER_MIN)
        if win_start >= effective_end:
            continue

        # Decide where to start scanning inside this window.
        if floor_clock and floor_clock > win_start:
            begin_clock = floor_clock if floor_clock < effective_end else None
        else:
            begin_clock = win_start

        if begin_clock is None:
            continue

        pointer = round_up_to_slot(
            datetime(target_date.year, target_date.month, target_date.day,
                     begin_clock[0], begin_clock[1], tzinfo=LOCAL_TZ),
            slot_len,
        )

        while len(found) < want:
            if (pointer.hour, pointer.minute) >= effective_end:
                break

            pointer_utc = pointer.astimezone(UTC_TZ)
            get_or_create_slot(queue_id, pointer_utc, slot_cap, queue_name)
            item = get_slot_item(make_slot_key(queue_id, pointer_utc))

            if item:
                count = int(item.get("count", 0))
                cap   = int(item.get("capacity", slot_cap))
                if count < cap:
                    found.append({
                        "iso":
                            pointer_utc.replace(microsecond=0)
                                       .isoformat().replace("+00:00", "Z"),
                        "speak": speak_datetime(pointer),
                    })

            pointer += timedelta(minutes=slot_len)

        if len(found) >= want:
            break

    return found


def search_for_slots(queue_id: str, queue_name: str, first_date: date, last_day: date,
                     open_weekdays: Optional[set[int]],
                     windows: list[tuple[Clock, Clock]], slot_len: int, slot_cap: int,
                     want: int, floor_clock: Optional[Clock] = None) -> list[dict]:
    """
    Look for up to `want` open slots starting at `first_date`. If a day comes back
    fully booked, roll forward to the next open day and keep going until we either
    find slots or run past `last_day`.
    """
    cursor = first_date
    while cursor <= last_day:
        if not is_open_on(cursor, open_weekdays):
            cursor += timedelta(days=1)
            continue

        # The floor only matters on the day the caller originally asked for.
        day_floor = floor_clock if cursor == first_date else None
        slots = collect_slots_on_day(queue_id, queue_name, cursor, windows,
                                     slot_len, slot_cap, want, floor_clock=day_floor)
        if slots:
            return slots

        cursor += timedelta(days=1)

    return []


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, _context) -> dict:
    region  = event.get("Details", {}).get("ContactData", {}).get("AwsRegion") or REGION
    connect = boto3.client("connect", region_name=region)

    queue_id = resolve_queue_id(event)
    if not queue_id:
        return {"result": "ERROR", "message": "Missing queueId", "reason": "MISSING_QUEUE"}

    instance_id = resolve_instance_id(event)
    queue_name  = lookup_queue_name(connect, instance_id, queue_id)

    # Scheduling parameters (attribute overrides, otherwise env/hardcoded defaults)
    slot_len   = read_int_attr(event, "slotMinutes",    DEFAULT_SLOT_LEN, 1, 60)
    slot_cap   = read_int_attr(event, "slotCapacity",   DEFAULT_SLOT_CAP, 1, 1000)
    want_slots = read_int_attr(event, "maxSuggestions", DEFAULT_MAX_SLOTS, 1, 10)
    offset     = read_int_attr(event, "offsetDays",     1, 0, 7)
    win_days   = read_int_attr(event, "windowDays",     MAX_WINDOW_DAYS, 1, 7)

    # Windows from the timeRanges attribute, or the bizStartHour/bizEndHour fallback.
    windows = resolve_time_ranges(event)

    # Hours of operation — only used to know which weekdays the queue is open.
    open_weekdays = fetch_open_weekdays(connect, instance_id, queue_id)

    now      = datetime.now(LOCAL_TZ)
    last_day = (now + timedelta(days=win_days - 1)).date()

    # Caller phone number.
    phone = read_attr(event, "phoneNumber") or ""
    if not phone:
        try:
            phone = event["Details"]["ContactData"]["CustomerEndpoint"]["Address"]
        except Exception:
            pass

    # Which calendar day are we aiming at?
    target_date = pick_target_date(now, offset, open_weekdays, last_day)
    if target_date is None:
        return assemble_response(
            queue_id, phone, [],
            result="NO_AVAILABILITY",
            message="No open business day found within the scheduling window",
            reason="NO_OPEN_DAY",
        )

    # For a same-day request, start no earlier than now + lead time; otherwise
    # let scanning begin at each window's open.
    floor_clock: Optional[Clock] = None
    if target_date == now.date():
        earliest   = round_up_to_slot(now + timedelta(minutes=LEAD_TIME_MIN), slot_len)
        floor_clock = (earliest.hour, earliest.minute)

    slots = search_for_slots(
        queue_id, queue_name,
        target_date, last_day, open_weekdays,
        windows, slot_len, slot_cap, want_slots,
        floor_clock=floor_clock,
    )

    if not slots:
        return assemble_response(
            queue_id, phone, [],
            result="NO_AVAILABILITY",
            message="No available callback slots found within the scheduling window",
            reason="WINDOW_CAPACITY_FULL",
        )

    return assemble_response(
        queue_id, phone, slots,
        result="FULL",
        message=f"Suggesting {len(slots)} available slot(s)",
        reason="AUTO_SUGGEST",
    )
