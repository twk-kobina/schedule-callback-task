"""
callback_booking.py – AWS Lambda: Callback Slot Booking

Confirms a chosen callback time, atomically claims the DynamoDB slot, writes a
SCHEDULED callback record, and creates a scheduled Amazon Connect task that
fires at the requested callback time.

Contact Attributes
------------------
chosenIso / scheduleIso  – UTC ISO timestamp of the picked slot
slotCapacity             – How many bookings a slot can hold (default: SLOT_CAPACITY env)
slotMinutes              – Slot length in minutes (default: SLOT_MINUTES env)
queueId                  – Queue id or ARN (falls back to ContactData.Queue.ARN,
                           then the CONNECT_QUEUE_ID env var)
contactFlowId            – Unused; the flow id comes from the CONTACT_FLOW_ID env var

Environment Variables
---------------------
AWS_REGION, BUSINESS_TZ, SLOT_CAPACITY, SLOT_MINUTES,
SLOTS_TABLE, CALLBACKS_TABLE, CONNECT_QUEUE_ID, CONTACT_FLOW_ID
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration pulled from the environment (with sane fallbacks)
# ---------------------------------------------------------------------------
REGION           = os.getenv("AWS_REGION", "us-west-2")
TIMEZONE_NAME    = os.getenv("BUSINESS_TZ", "America/New_York")
LOCAL_TZ         = ZoneInfo(TIMEZONE_NAME)
UTC_TZ           = ZoneInfo("UTC")

DEFAULT_SLOT_CAP = int(os.getenv("SLOT_CAPACITY", "10"))
DEFAULT_SLOT_LEN = int(os.getenv("SLOT_MINUTES", "15"))
FLOW_ID          = os.getenv("CONTACT_FLOW_ID", "1a5d0c2d-d850-4d9b-b733-4cd51f964073")
SLOTS_TABLE_NAME = os.getenv("SLOTS_TABLE", "callback_slots")
CALLBACKS_TABLE_NAME = os.getenv("CALLBACKS_TABLE", "callbacks_records")

# DynamoDB handles
_dynamo          = boto3.resource("dynamodb", region_name=REGION)
_slots_table     = _dynamo.Table(SLOTS_TABLE_NAME)
_callbacks_table = _dynamo.Table(CALLBACKS_TABLE_NAME)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

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


def normalize_iso(raw: str) -> str:
    """Coerce any ISO string into a clean UTC 'YYYY-MM-DDTHH:MM:SSZ' form."""
    return (datetime.fromisoformat(raw.replace("Z", "+00:00"))
            .replace(microsecond=0).astimezone(UTC_TZ)
            .isoformat().replace("+00:00", "Z"))


def make_slot_key(queue_id: str, iso_utc: str) -> str:
    """Build the DynamoDB partition key for a queue + UTC slot start."""
    moment = datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(UTC_TZ)
    return f"{queue_id}#{moment.strftime('%Y%m%d%H%M')}"


def speak_iso(iso_utc: str) -> str:
    """Render a UTC ISO time in local time, the way we want Connect to read it aloud."""
    local = datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
    try:
        return local.strftime("%A, %d %B at %-I:%M %p")
    except Exception:
        return local.strftime("%A, %d %B at %I:%M %p").replace(" 0", " ")


def build_callback_id(phone: str, queue_id: str, iso_utc: str) -> str:
    """Deterministic id from phone + queue + time, so retries stay idempotent."""
    digest = hashlib.sha1(f"{phone}|{queue_id}|{iso_utc}".encode()).hexdigest()[:12]
    return f"cb-{digest}"


def string_map(**kwargs) -> dict:
    """Stringify every non-None keyword into a flat dict for the Connect response."""
    return {k: str(v) for k, v in kwargs.items() if v is not None}


def error_response(message: str, **extra) -> dict:
    """Standard error payload; also logs it."""
    out = {"result": "ERROR", "message": str(message)}
    out.update({k: str(v) for k, v in extra.items() if v is not None})
    logger.error(out)
    return out


# ---------------------------------------------------------------------------
# Resolving the queue and instance
# ---------------------------------------------------------------------------

def resolve_queue_id(event: dict) -> Optional[str]:
    """
    Work out the queue id, trying each source in priority order:
    1. Attributes.queueId
    2. Details.Parameters.queueId
    3. ContactData.Queue.ARN
    4. ContactData.SystemEndpoint.Address (only when it looks ARN-like)
    5. CONNECT_QUEUE_ID env var
    """
    raw = None
    try:
        raw = event["Details"]["ContactData"]["Attributes"].get("queueId")
    except Exception:
        pass

    if not raw:
        try:
            raw = event["Details"]["Parameters"].get("queueId")
        except Exception:
            pass

    if not raw:
        try:
            raw = event["Details"]["ContactData"]["Queue"]["ARN"]
        except Exception:
            pass

    if not raw:
        try:
            address = event["Details"]["ContactData"]["SystemEndpoint"]["Address"]
            if address and "queue" in address.lower():
                raw = address
        except Exception:
            pass

    raw = raw or os.getenv("CONNECT_QUEUE_ID")
    if not raw:
        return None

    if raw.startswith("arn:aws:connect:") and "/queue/" in raw:
        return raw.split("/queue/")[-1]
    return raw


def resolve_instance_id(event: dict) -> Optional[str]:
    """Pull the Connect instance id out of the contact's InstanceARN."""
    try:
        return event["Details"]["ContactData"]["InstanceARN"].split("instance/")[-1]
    except Exception:
        return None


# ---------------------------------------------------------------------------
# DynamoDB slot bookkeeping
# ---------------------------------------------------------------------------

def precreate_following_slot(queue_id: str, iso_utc: str,
                             slot_len: int, capacity: int) -> None:
    """
    Once a slot fills up, seed the very next slot ahead of time so concurrent
    callers don't race to create it. Best-effort — failures are only logged.
    """
    try:
        next_utc = (datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
                    .astimezone(UTC_TZ) + timedelta(minutes=slot_len))
        next_iso = next_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        next_key = make_slot_key(queue_id, next_iso)
        _slots_table.put_item(
            Item={
                "slotKey":          next_key,
                "count":            0,
                "capacity":         capacity,
                "windowStartIso":   next_iso,
                "windowStartLocal": next_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "tz":               TIMEZONE_NAME,
                "slotTtl":          int(time.time()) + 2 * 86_400,
            },
            ConditionExpression="attribute_not_exists(slotKey)",
        )
    except ClientError as err:
        # A ConditionalCheckFailed just means it already exists — that's fine.
        if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
            logger.warning("Could not pre-create the following slot: %s", err)
    except Exception as err:
        logger.warning("Unexpected error pre-creating the following slot: %s", err)


def claim_slot(key: str, iso_utc: str, slot_cap: int) -> tuple[str, int, int]:
    """
    Atomically bump a slot's count, but only while it still has room.
    Returns ("OK", new_count, cap) | ("FULL", 0, 0) | ("ERROR", 0, 0).
    """
    local_str = (datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
                 .astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"))
    now_ts = int(time.time())
    try:
        resp = _slots_table.update_item(
            Key={"slotKey": key},
            UpdateExpression=(
                "SET #c = if_not_exists(#c, :zero) + :one,"
                "    #cap = if_not_exists(#cap, :cap),"
                "    windowStartIso   = if_not_exists(windowStartIso, :iso),"
                "    windowStartLocal = if_not_exists(windowStartLocal, :local),"
                "    tz               = if_not_exists(tz, :tz),"
                "    slotTtl = :ttl, updatedAt = :now"
            ),
            ConditionExpression=(
                "attribute_not_exists(slotKey) OR "
                "(attribute_exists(#c) AND attribute_exists(#cap) AND #c < #cap)"
            ),
            ExpressionAttributeNames={"#c": "count", "#cap": "capacity"},
            ExpressionAttributeValues={
                ":zero": 0, ":one": 1, ":cap": slot_cap,
                ":iso": iso_utc, ":local": local_str, ":tz": TIMEZONE_NAME,
                ":ttl": now_ts + 2 * 86_400, ":now": now_ts,
            },
            ReturnValues="ALL_NEW",
        )
        count = int(resp["Attributes"].get("count", 1))
        cap   = int(resp["Attributes"].get("capacity", slot_cap))
        return "OK", count, cap
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "FULL", 0, 0
        logger.error("Slot update_item error: %s", err)
        return "ERROR", 0, 0


# ---------------------------------------------------------------------------
# Amazon Connect task creation
# ---------------------------------------------------------------------------

def create_scheduled_task(connect_client, instance_id: str, queue_id: str,
                          flow_id: str, phone: str, callback_number: str,
                          iso_utc: str, callback_id: str) -> str:
    """
    Create a scheduled Connect task that fires at the callback time, carrying the
    customer's callback number and queue as attributes. Returns the new task's
    ContactId. Raises on failure so the caller can decide what to do.
    """
    scheduled_dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(UTC_TZ)
    task_name    = f"Scheduled callback for {callback_number} at {speak_iso(iso_utc)}"

    params = dict(
        InstanceId    = instance_id,
        ContactFlowId = flow_id,
        Name          = task_name,
        ScheduledTime = scheduled_dt,
        ClientToken   = callback_id,   # SHA1-based, so retries stay idempotent
        Attributes    = {
            "customerCallbackNumber": callback_number,
            "queueId":      queue_id,
            "callbackId":   callback_id,
            "scheduledIso": iso_utc,
        },
    )

    resp = connect_client.start_task_contact(**params)
    return resp["ContactId"]


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, _context) -> dict:
    # The caller's phone must come from the Connect endpoint, never from input.
    try:
        phone = event["Details"]["ContactData"]["CustomerEndpoint"]["Address"]
    except (KeyError, TypeError):
        return error_response("Missing caller phone from CustomerEndpoint")

    # The callback number can differ from the caller's phone — e.g. calling from
    # the office but wanting the callback on a mobile.
    callback_number = read_attr(event, "customerCallbackNumber") or phone

    queue_id    = resolve_queue_id(event)
    instance_id = resolve_instance_id(event)

    if not queue_id:
        return error_response(
            "Missing queueId - not found in Attributes, Parameters, ContactData, or env")
    if not instance_id:
        return error_response("Missing InstanceId - cannot create Connect task")

    flow_id = FLOW_ID
    if not flow_id:
        return error_response("Missing contactFlowId - required to create a Connect task")

    # The slot the caller chose.
    raw_iso = read_attr(event, "chosenIso") or read_attr(event, "scheduleIso")
    if not raw_iso:
        return error_response("Missing chosenIso/scheduleIso - no time selected to book")

    iso      = normalize_iso(raw_iso)
    key      = make_slot_key(queue_id, iso)
    slot_cap = read_int_attr(event, "slotCapacity", DEFAULT_SLOT_CAP, 1, 1000)
    slot_len = read_int_attr(event, "slotMinutes",  DEFAULT_SLOT_LEN, 1, 60)

    # Cheap read-first check so an already-full slot fails fast, before the
    # atomic update below.
    try:
        item = _slots_table.get_item(Key={"slotKey": key}).get("Item")
        if item and int(item.get("count", 0)) >= int(item.get("capacity", slot_cap)):
            return string_map(result="SLOT_TAKEN", queueId=queue_id, phoneNumber=phone,
                              chosenIso=iso, message="This time slot is already full")
    except Exception as err:
        logger.warning("Pre-check read failed for %s: %s - proceeding", key, err)

    # Atomically reserve the slot.
    status, new_count, cap = claim_slot(key, iso, slot_cap)
    if status == "FULL":
        return string_map(result="SLOT_TAKEN", queueId=queue_id, phoneNumber=phone,
                          chosenIso=iso,
                          message="This time slot was just filled by another caller")
    if status == "ERROR":
        return error_response("Failed to reserve slot - DynamoDB error")

    # If this booking just filled the slot, seed the next one.
    if new_count >= cap:
        precreate_following_slot(queue_id, iso, slot_len, slot_cap)

    callback_id = build_callback_id(phone, queue_id, iso)
    now = int(time.time())

    # Create the scheduled Connect task.
    connect = boto3.client("connect", region_name=REGION)
    try:
        task_contact_id = create_scheduled_task(
            connect, instance_id, queue_id, flow_id,
            phone, callback_number, iso, callback_id,
        )
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code", "Unknown")
        logger.error("start_task_contact failed for %s: %s", callback_id, err)
        return error_response(f"Slot booked but task creation failed ({code})",
                              callbackId=callback_id)
    except Exception as err:
        logger.error("Unexpected error creating task for %s: %s", callback_id, err)
        return error_response("Slot booked but task creation failed", callbackId=callback_id)

    # Persist the callback record, including the taskContactId from the step above.
    try:
        _callbacks_table.update_item(
            Key={"callbackId": callback_id},
            UpdateExpression=(
                "SET phoneNumber=:p, callbackNumber=:cb, queueId=:q, scheduleIso=:t, #s=:s,"
                "    taskContactId=:tid,"
                "    createdAt = if_not_exists(createdAt, :ca), updatedAt=:u, #ttl=:ttl"
            ),
            ExpressionAttributeNames={"#s": "status", "#ttl": "ttl"},
            ExpressionAttributeValues={
                ":p": phone, ":cb": callback_number, ":q": queue_id, ":t": iso, ":s": "SCHEDULED",
                ":tid": task_contact_id,
                ":ca": now, ":u": now, ":ttl": now + 14 * 86_400,
            },
        )
    except Exception as err:
        logger.error("Callback record write failed for %s: %s", callback_id, err)
        return error_response("Slot booked and task created but callback record failed",
                              callbackId=callback_id, taskContactId=task_contact_id)

    return string_map(
        result         = "SCHEDULED",
        callbackId     = callback_id,
        taskContactId  = task_contact_id,
        scheduledIso   = iso,
        scheduledSpeak = speak_iso(iso),
        queueId        = queue_id,
        phoneNumber    = phone,
    )
