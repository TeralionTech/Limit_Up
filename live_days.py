"""Bounded, read-only exports for the central strategy-health collector.

No imports from runner/session/config: these routes only inspect output files.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import secrets
from datetime import date as Date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import JSONResponse

OUTPUT_DIR = Path(__file__).parent / "output"
TZ = timezone(timedelta(hours=8))
MAX_ROWS = 20_000
MAX_BYTES = 8 * 1024 * 1024
MAX_LINE_BYTES = 64 * 1024
MAX_DIRECTORY_ENTRIES = 10_000
MAX_DATES = 366
CSV_COLUMNS = (
    "recv_time", "symbol", "action", "price", "lots", "quantity",
    "order_no", "filled_no", "broker_filled_time", "strategy",
)
# 2026-10-28: TradingSession._append_fill_csv appends account_match ("1"/"0"/"")
# and account_masked. A same-day restart after the deploy appends 12-cell rows
# under a v1 header, so both headers accept both row widths.
CSV_COLUMNS_V2 = CSV_COLUMNS + ("account_match", "account_masked")
MAX_ACCOUNT_MASKED = 16
ACCOUNT_MATCH = {"1": True, "0": False, "": None}


def require_token(x_limitup_token: str = Header(default="")) -> None:
    # Same header/env and byte compare as server._token_ok. Do not import server
    # (startup dependencies), or inherit its mutating-route loopback exemption.
    expected = os.environ.get("LIMITUP_API_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="LIMITUP_API_TOKEN not configured")
    if not secrets.compare_digest(x_limitup_token.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="unauthorized")


router = APIRouter(prefix="/central/live-days", dependencies=[Depends(require_token)])


def valid_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("expected YYYY-MM-DD")
    Date.fromisoformat(value)
    return value


def output_file(name: str) -> Path:
    root = OUTPUT_DIR.resolve()
    path = root / name
    # Reject file symlinks/junction escapes, including orders presence checks.
    if path.is_symlink() or path.resolve().parent != root:
        raise HTTPException(status_code=503, detail="unsafe output file")
    return path


def filled_at(day: str, broker_time: str, recv_time: str) -> str:
    value = broker_time or recv_time
    if re.fullmatch(r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?", value):
        stamp = datetime.combine(Date.fromisoformat(day), time.fromisoformat(value), TZ)
    else:
        if not re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}", value):
            raise ValueError("invalid timestamp")
        stamp = datetime.fromisoformat(value)
        stamp = stamp.replace(tzinfo=TZ) if stamp.tzinfo is None else stamp.astimezone(TZ)
    if stamp.date().isoformat() != day:
        raise ValueError("fill date differs from filename")
    return stamp.isoformat()


def parse_fill(day: str, cells: list[str]) -> dict:
    if len(cells) == len(CSV_COLUMNS):
        row = dict(zip(CSV_COLUMNS, cells)) | {"account_match": "", "account_masked": ""}
    elif len(cells) == len(CSV_COLUMNS_V2):
        row = dict(zip(CSV_COLUMNS_V2, cells))
    else:
        raise ValueError("wrong column count")
    if row["account_match"] not in ACCOUNT_MATCH:
        raise ValueError("invalid account_match")
    if len(row["account_masked"]) > MAX_ACCOUNT_MASKED:
        raise ValueError("invalid account_masked")
    if not row["symbol"] or not row["order_no"] or row["action"] not in ("buy", "sell"):
        raise ValueError("missing identity or invalid side")
    if row["strategy"] not in ("0", "1"):
        raise ValueError("invalid attribution")
    quantity = int(row["quantity"])
    csv_lots = Decimal(row["lots"])
    if quantity <= 0 or not csv_lots.is_finite():
        raise ValueError("invalid quantity")
    lots_decimal = Decimal(quantity) / 1000
    # broker._handle_filled writes lots = qty // 1000. Recover odd lots from
    # quantity; also accept a fractional lots field when it agrees exactly.
    if csv_lots not in (Decimal(quantity // 1000), lots_decimal):
        raise ValueError("inconsistent quantity/lots")
    lots = int(lots_decimal) if quantity % 1000 == 0 else float(lots_decimal)
    price = float(row["price"])
    if not math.isfinite(price) or price <= 0 or not math.isfinite(lots):
        raise ValueError("invalid price/lots")
    stamp = filled_at(day, row["broker_filled_time"], row["recv_time"])
    key_text = "|".join((day, row["order_no"], row["filled_no"],
                         row["broker_filled_time"], str(lots)))
    return {
        "fill_key": hashlib.sha1(key_text.encode("utf-8")).hexdigest(),
        "identity": "strong" if row["filled_no"] else "weak",
        "order_no": row["order_no"], "filled_no": row["filled_no"],
        "symbol": row["symbol"], "side": row["action"], "lots": lots,
        "price": price, "filled_at": stamp, "strategy": int(row["strategy"]),
        "account_match": ACCOUNT_MATCH[row["account_match"]],
        "account_masked": row["account_masked"],
        "source": "fills_csv",
    }


class Budget:
    """Shared across all CSVs in a dates request, not multiplied per file."""

    def __init__(self):
        self.rows = MAX_ROWS
        self.bytes = MAX_BYTES


def read_fills(path: Path, day: str, budget: Budget, *, count_only: bool = False) -> dict:
    result = {"fills": [], "row_count": 0, "skipped_rows": 0,
              "truncated": False, "header_valid": False}
    with path.open("rb") as stream:
        # Freeze the length so a concurrent append cannot extend this request.
        remaining = os.fstat(stream.fileno()).st_size

        def line():
            nonlocal remaining
            size = min(remaining, budget.bytes, MAX_LINE_BYTES)
            if size <= 0:
                result["truncated"] = remaining > 0
                return None
            raw = stream.readline(size)
            budget.bytes -= len(raw)
            remaining -= len(raw)
            if not raw:
                remaining = 0
                return None
            if not raw.endswith(b"\n"):
                # Includes a concurrently-written partial row. Never emit it.
                result["truncated"] = True
                return None
            return raw

        raw = line()
        if raw is None:
            return result
        try:
            header = next(csv.reader([raw.decode("utf-8-sig")], strict=True))
            result["header_valid"] = header in (list(CSV_COLUMNS), list(CSV_COLUMNS_V2))
        except (UnicodeError, csv.Error):
            pass
        if not result["header_valid"]:
            return result
        while remaining > 0:
            if budget.rows <= 0:
                result["truncated"] = True
                break
            raw = line()
            if raw is None:
                break
            budget.rows -= 1
            try:
                cells = next(csv.reader([raw.decode("utf-8")], strict=True))
                fill = parse_fill(day, cells)
            except (ValueError, InvalidOperation, OverflowError, UnicodeError, csv.Error):
                result["skipped_rows"] += 1
                continue
            result["row_count"] += 1
            if not count_only:
                result["fills"].append(fill)
    return result


def read_positions(day: str) -> dict:
    result = {"holdings": None, "truncated": False, "skipped_rows": 0}
    path = output_file("positions_history.jsonl")
    try:
        with path.open("rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            start = max(0, size - MAX_BYTES)
            stream.seek(start)
            data = stream.read(min(size, MAX_BYTES))
    except FileNotFoundError:
        return result
    if start:
        # Discard the potentially partial first line of the bounded tail.
        data = data.partition(b"\n")[2]
        result["truncated"] = True
    lines = data.rstrip(b"\n").rsplit(b"\n", MAX_ROWS) if data else []
    if len(lines) > MAX_ROWS:
        result["truncated"] = True
        lines = lines[-MAX_ROWS:]
    for raw in reversed(lines):
        try:
            if len(raw) > MAX_LINE_BYTES:
                raise ValueError("oversized snapshot")
            record = json.loads(raw, parse_constant=_invalid_json_constant, parse_float=_finite_float)
            if not isinstance(record, dict) or "date" not in record:
                raise ValueError("invalid snapshot")
            if record["date"] != day:
                continue
            holdings = record.get("holdings")
            if not isinstance(holdings, list):
                result["skipped_rows"] += 1
                return result  # Do not substitute an older snapshot for this date.
            result["holdings"] = holdings
            return result
        except (ValueError, UnicodeError, RecursionError):
            result["skipped_rows"] += 1
    return result


def _invalid_json_constant(value: str):
    raise ValueError("non-finite JSON number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


@router.get("/dates")
def dates():
    days = set()
    truncated = False
    try:
        with os.scandir(OUTPUT_DIR) as entries:
            for index, entry in enumerate(entries):
                if index >= MAX_DIRECTORY_ENTRIES:
                    truncated = True
                    break
                if not entry.name.endswith("_fills.csv") or not entry.is_file(follow_symlinks=False):
                    continue
                try:
                    days.add(valid_date(entry.name.removesuffix("_fills.csv")))
                except ValueError:
                    continue
    except FileNotFoundError:
        return {"dates": [], "truncated": False}
    except OSError:
        raise HTTPException(status_code=503, detail="output unavailable") from None
    truncated = truncated or len(days) > MAX_DATES
    budget = Budget()
    items = []
    for day in sorted(days)[:MAX_DATES]:
        try:
            data = read_fills(output_file(f"{day}_fills.csv"), day, budget, count_only=True)
            item = {"date": day, "row_count": data["row_count"] if data["header_valid"] else None,
                    "skipped_rows": data["skipped_rows"], "truncated": data["truncated"],
                    "header_valid": data["header_valid"]}
        except OSError:
            item = {"date": day, "row_count": None, "skipped_rows": 0,
                    "truncated": False, "header_valid": False, "error": "output unavailable"}
        items.append(item)
    return {"dates": items, "truncated": truncated or any(i["truncated"] for i in items)}


@router.get("")
def live_day(date: str):
    try:
        valid_date(date)
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD and a valid date") from None
    try:
        try:
            data = read_fills(output_file(f"{date}_fills.csv"), date, Budget())
        except FileNotFoundError:
            return JSONResponse(status_code=404, content={"error": "no_data"})
        positions = read_positions(date)
        orders_file = output_file(f"{date}_orders.csv").is_file()
    except OSError:
        raise HTTPException(status_code=503, detail="output unavailable") from None
    return {
        "date": date, "fills": data["fills"], "orders_file": orders_file,
        "positions_after": positions["holdings"],
        "complete": {
            "fills_file": data["header_valid"] and not data["truncated"] and not data["skipped_rows"],
            "positions_snapshot": positions["holdings"] is not None and not positions["skipped_rows"],
            "all_fills": False,
            "reason": "callback CSV only; reconcile_orders fills are not written to this CSV",
        },
        "skipped_rows": data["skipped_rows"],
        "truncated": data["truncated"] or positions["truncated"],
        "fills_truncated": data["truncated"],
        "positions_truncated": positions["truncated"],
        "positions_skipped_rows": positions["skipped_rows"],
        "generated_at": datetime.now(TZ).isoformat(),
    }
