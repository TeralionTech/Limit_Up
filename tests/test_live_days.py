"""Read-only node contract; temporary files, in-process ASGI, no lifespan/broker."""
import builtins
import csv
import hashlib
import io
import json
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient

import live_days as ld

DAY = "2026-10-22"
BASE = "/api/central/live-days"
TOKEN = "test-only-token"
HEADERS = {"X-Limitup-Token": TOKEN}
# Independently transcribed from TradingSession._append_fill_csv.
COLUMNS = ["recv_time", "symbol", "action", "price", "lots", "quantity",
           "order_no", "filled_no", "broker_filled_time", "strategy"]


def fill(**changes):
    row = dict(zip(COLUMNS, [f"{DAY}T09:00:01", "2330", "buy", "900.5", "2",
                            "2000", "A001", "F001", "09:00:00.123", "1"]))
    return row | changes


def write_fills(root, rows, day=DAY):
    path = root / f"{day}_fills.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


# v2 (2026-10-28): _append_fill_csv appends account_match / account_masked.
COLUMNS_V2 = COLUMNS + ["account_match", "account_masked"]


def write_raw(root, header, rows, day=DAY):
    """Header and rows as plain cell lists, so v1/v2 headers and 10/12-cell rows can mix."""
    path = root / f"{day}_fills.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def v1_cells(**changes):
    return [fill(**changes)[c] for c in COLUMNS]


def v2_cells(match, masked="abc***", **changes):
    return v1_cells(**changes) + [match, masked]


def write_positions(root, records):
    (root / "positions_history.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(ld, "OUTPUT_DIR", tmp_path)
    monkeypatch.setenv("LIMITUP_API_TOKEN", TOKEN)
    app = FastAPI()
    app.include_router(ld.router, prefix="/api")
    test_client = TestClient(app)
    test_client._trust_env = False  # httpx must not inspect an operator's .netrc.
    return test_client


def get_day(client, day=DAY):
    return client.get(BASE, params={"date": day}, headers=HEADERS)


@pytest.mark.parametrize("path", [BASE + "?date=" + DAY, BASE + "/dates"])
def test_token_missing_wrong_unset_empty(client, monkeypatch, path):
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"X-Limitup-Token": "wrong"}).status_code == 401
    monkeypatch.delenv("LIMITUP_API_TOKEN")
    assert client.get(path, headers=HEADERS).status_code == 503
    monkeypatch.setenv("LIMITUP_API_TOKEN", "")
    assert client.get(path, headers={"X-Limitup-Token": ""}).status_code == 503


def test_token_constant_time_and_rotation(client, monkeypatch):
    compared = []
    original = ld.secrets.compare_digest

    def compare(left, right):
        compared.append((left, right))
        return original(left, right)

    monkeypatch.setattr(ld.secrets, "compare_digest", compare)
    assert client.get(BASE + "/dates", headers={"x-limitup-token": TOKEN}).status_code == 200
    assert compared == [(TOKEN.encode(), TOKEN.encode())]
    monkeypatch.setenv("LIMITUP_API_TOKEN", "rotated")
    assert client.get(BASE + "/dates", headers=HEADERS).status_code == 401


@pytest.mark.parametrize("host", ["127.0.0.1", "::1"])
def test_loopback_still_requires_token(client, host):
    async def local(scope, receive, send):
        scope = dict(scope, client=(host, 54321))
        await client.app(scope, receive, send)

    local_client = TestClient(local)
    local_client._trust_env = False
    assert local_client.get(BASE + "/dates").status_code == 401
    assert local_client.get(BASE, params={"date": DAY}).status_code == 401


@pytest.mark.parametrize("day", ["", "20261022", "2026-1-22", "2026-02-30", "0000-01-01",
    "2026-10-22T09:00:00", "2026-10-22\n", " 2026-10-22", "２０２６-１０-２２",
    "../2026-10-22", "..\\2026-10-22", "../../.env", "%2e%2e%2f.env",
    "C:\\secret", "/etc/passwd", "2026-10-22\x00"])
def test_bad_dates_and_traversal_never_open_files(client, monkeypatch, day):
    monkeypatch.setattr(ld, "output_file", lambda name: pytest.fail("invalid date reached filesystem"))
    assert get_day(client, day).status_code == 422


def test_missing_date_and_missing_data(client, tmp_path):
    assert client.get(BASE, headers=HEADERS).status_code == 422
    (tmp_path / f"{DAY}_orders.csv").write_text("irrelevant", encoding="utf-8")
    write_positions(tmp_path, [{"date": DAY, "holdings": []}])
    response = get_day(client)
    assert response.status_code == 404
    assert response.json() == {"error": "no_data"}


def test_fills_strategy_odd_lots_identity_and_time(client, tmp_path):
    write_fills(tmp_path, [fill(), fill(action="sell", strategy="0", filled_no="",
                                       lots="0", quantity="125", broker_filled_time="")])
    data = get_day(client).json()
    assert data["fills"] == [
        {"fill_key": hashlib.sha1(f"{DAY}|A001|F001|09:00:00.123|2".encode()).hexdigest(),
         "identity": "strong", "order_no": "A001", "filled_no": "F001", "symbol": "2330",
         "side": "buy", "lots": 2, "price": 900.5, "filled_at": f"{DAY}T09:00:00.123000+08:00",
         "strategy": 1, "account_match": None, "account_masked": "", "source": "fills_csv"},
        {"fill_key": hashlib.sha1(f"{DAY}|A001|||0.125".encode()).hexdigest(),
         "identity": "weak", "order_no": "A001", "filled_no": "", "symbol": "2330",
         "side": "sell", "lots": 0.125, "price": 900.5, "filled_at": f"{DAY}T09:00:01+08:00",
         "strategy": 0, "account_match": None, "account_masked": "", "source": "fills_csv"},
    ]
    assert get_day(client).json()["fills"] == data["fills"]
    assert data["complete"]["fills_file"] is True
    assert data["complete"]["all_fills"] is False
    assert "reconcile_orders" in data["complete"]["reason"]
    assert data["positions_after"] is None
    assert data["complete"]["positions_snapshot"] is False
    assert data["generated_at"].endswith("+08:00")
    assert data["orders_file"] is False


@pytest.mark.parametrize("broker_time", ["09:00:00", f"{DAY}T09:00:00", f"{DAY}T01:00:00+00:00"])
def test_timestamp_formats(client, tmp_path, broker_time):
    write_fills(tmp_path, [fill(broker_filled_time=broker_time, lots="1.125", quantity="1125")])
    row = get_day(client).json()["fills"][0]
    assert row["filled_at"] == f"{DAY}T09:00:00+08:00"
    assert row["lots"] == 1.125


@pytest.mark.parametrize("changes", [
    {"price": "NaN"}, {"price": "inf"}, {"price": "0"}, {"price": "bad"},
    {"quantity": "-1"}, {"quantity": "1.2"}, {"lots": "NaN"}, {"lots": "99"},
    {"strategy": "2"}, {"strategy": ""}, {"action": "unknown"}, {"symbol": ""},
    {"order_no": ""}, {"broker_filled_time": "garbage"},
    {"broker_filled_time": "2026-10-21T09:00:00"},
])
def test_corrupt_rows_skipped_and_counted(client, tmp_path, changes):
    path = write_fills(tmp_path, [fill(), fill(**changes), fill(filled_no="F002")])
    with path.open("ab") as stream:
        stream.write(b'broken,row\n\xff\n"unclosed\n')
    data = get_day(client).json()
    assert len(data["fills"]) == 2
    assert data["skipped_rows"] == 4
    assert data["complete"]["fills_file"] is False


def test_v2_header_twelve_cells_account_match(client, tmp_path):
    write_raw(tmp_path, COLUMNS_V2, [
        v2_cells("1", filled_no="F1"),
        v2_cells("0", "xyz***", filled_no="F2", strategy="0"),
        v2_cells("", "", filled_no="F3"),
    ])
    data = get_day(client).json()
    assert [(f["filled_no"], f["account_match"], f["account_masked"]) for f in data["fills"]] == [
        ("F1", True, "abc***"), ("F2", False, "xyz***"), ("F3", None, "")]
    assert [f["strategy"] for f in data["fills"]] == [1, 0, 1]
    assert data["skipped_rows"] == 0
    assert data["complete"]["fills_file"] is True


def test_v1_header_then_twelve_cell_rows_same_day_restart(client, tmp_path):
    # File created before the deploy (10-column header + 10-cell row); the restarted
    # program keeps appending 12-cell rows to the same file.
    write_raw(tmp_path, COLUMNS, [v1_cells(filled_no="F1"), v2_cells("0", filled_no="F2")])
    data = get_day(client).json()
    assert [(f["filled_no"], f["account_match"], f["account_masked"]) for f in data["fills"]] == [
        ("F1", None, ""), ("F2", False, "abc***")]
    assert data["skipped_rows"] == 0
    assert data["complete"]["fills_file"] is True


def test_v1_rows_under_v2_header_accepted(client, tmp_path):
    write_raw(tmp_path, COLUMNS_V2, [v1_cells(), v2_cells("1", filled_no="F2")])
    data = get_day(client).json()
    assert [(f["account_match"], f["account_masked"]) for f in data["fills"]] == [
        (None, ""), (True, "abc***")]
    assert data["skipped_rows"] == 0


@pytest.mark.parametrize("cells", [
    v2_cells("2"), v2_cells("true"), v2_cells("True"), v2_cells(" 1"), v2_cells("None"),
    v2_cells("1", "x" * 17),                     # account_masked longer than 16 characters
    v1_cells() + ["1"],                          # 11 cells
    v2_cells("1") + [""],                        # 13 cells
])
def test_invalid_account_cells_skipped_and_counted(client, tmp_path, cells):
    write_raw(tmp_path, COLUMNS_V2, [v2_cells("1"), cells, v2_cells("0", filled_no="F002")])
    data = get_day(client).json()
    assert len(data["fills"]) == 2
    assert data["skipped_rows"] == 1
    assert data["complete"]["fills_file"] is False


def test_account_masked_sixteen_chars_accepted(client, tmp_path):
    write_raw(tmp_path, COLUMNS_V2, [v2_cells("1", "x" * 16)])
    assert get_day(client).json()["fills"][0]["account_masked"] == "x" * 16


def test_fill_key_and_identity_unchanged_by_v2(client, tmp_path):
    # The platform deduplicates on fill_key / identity: a v2 row must key exactly like
    # the v1 row with the same identity cells, whatever its account cells say.
    write_raw(tmp_path, COLUMNS, [v1_cells(), v1_cells(filled_no="")])
    legacy = get_day(client).json()["fills"]
    write_raw(tmp_path, COLUMNS_V2, [v2_cells("0", "xyz***"), v2_cells("1", filled_no="")])
    current = get_day(client).json()["fills"]
    assert [(f["fill_key"], f["identity"]) for f in current] == \
        [(f["fill_key"], f["identity"]) for f in legacy]
    assert [f["identity"] for f in current] == ["strong", "weak"]
    assert current[0]["fill_key"] == hashlib.sha1(
        f"{DAY}|A001|F001|09:00:00.123|2".encode()).hexdigest()


def test_dates_accepts_v2_header(client, tmp_path):
    write_raw(tmp_path, COLUMNS_V2, [v2_cells("1"), v2_cells("bad")])
    result = client.get(BASE + "/dates", headers=HEADERS).json()
    assert result == {"dates": [{"date": DAY, "row_count": 1, "skipped_rows": 1,
                                 "truncated": False, "header_valid": True}], "truncated": False}


def test_positions_last_record_for_date_and_orders_presence(client, tmp_path):
    write_fills(tmp_path, [])
    (tmp_path / f"{DAY}_orders.csv").write_bytes(b"not parsed")
    last = [{"symbol": "2330", "lots": 2, "buy_price": 900.5}]
    write_positions(tmp_path, [{"date": DAY, "holdings": []}, {"date": DAY, "holdings": last},
                               {"date": "2026-10-23", "holdings": []}])
    data = get_day(client).json()
    assert data["positions_after"] == last
    assert data["orders_file"] is True
    assert data["complete"]["positions_snapshot"] is True


@pytest.mark.parametrize("holdings", [[], None, "bad"])
def test_positions_empty_or_invalid_latest_does_not_use_older(client, tmp_path, holdings):
    write_fills(tmp_path, [])
    write_positions(tmp_path, [{"date": DAY, "holdings": [{"symbol": "2330"}]},
                               {"date": DAY, "holdings": holdings}])
    data = get_day(client).json()
    assert data["positions_after"] == ([] if holdings == [] else None)
    assert data["complete"]["positions_snapshot"] is (holdings == [])


def test_positions_other_date_and_corruption(client, tmp_path):
    write_fills(tmp_path, [])
    write_positions(tmp_path, [{"date": "2026-10-21", "holdings": []}])
    with (tmp_path / "positions_history.jsonl").open("ab") as stream:
        stream.write(b'{bad json\n[]\n')
    data = get_day(client).json()
    assert data["positions_after"] is None
    assert data["positions_skipped_rows"] == 2


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e9999"])
def test_positions_nonfinite_json_skipped(client, tmp_path, number):
    write_fills(tmp_path, [])
    (tmp_path / "positions_history.jsonl").write_text(
        '{"date":"' + DAY + '","holdings":[{"lots":' + number + '}]}\n', encoding="utf-8")
    data = get_day(client).json()
    assert data["positions_after"] is None
    assert data["positions_skipped_rows"] == 1
    assert data["complete"]["positions_snapshot"] is False


def test_empty_positions_file(client, tmp_path):
    write_fills(tmp_path, [])
    (tmp_path / "positions_history.jsonl").write_bytes(b"")
    data = get_day(client).json()
    assert data["positions_after"] is None
    assert data["positions_skipped_rows"] == 0


def test_resolved_path_escape_rejected_before_open(client, tmp_path, monkeypatch):
    original = Path.resolve

    def resolve(path, *args, **kwargs):
        if path.name == f"{DAY}_fills.csv":
            return tmp_path.parent / path.name
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert get_day(client).status_code == 503


def test_dates_sorted_valid_files_counts(client, tmp_path):
    write_fills(tmp_path, [fill(), fill(strategy="bad")])
    write_fills(tmp_path, [], day="2026-10-21")
    write_fills(tmp_path, [], day="2026-02-30")
    write_fills(tmp_path, [], day="2026-1-01")
    (tmp_path / "2026-10-23_fills.csv").mkdir()
    (tmp_path / "2026-10-24_orders.csv").write_bytes(b"ignored")
    result = client.get(BASE + "/dates", headers=HEADERS).json()
    assert result == {"dates": [
        {"date": "2026-10-21", "row_count": 0, "skipped_rows": 0,
         "truncated": False, "header_valid": True},
        {"date": DAY, "row_count": 1, "skipped_rows": 1,
         "truncated": False, "header_valid": True}], "truncated": False}


def test_row_limit_including_bad_rows_and_shared_dates_budget(client, tmp_path, monkeypatch):
    monkeypatch.setattr(ld, "MAX_ROWS", 2)
    write_fills(tmp_path, [fill(strategy="bad"), fill(), fill()])
    write_fills(tmp_path, [fill(recv_time="2026-10-23T09:00:00")], day="2026-10-23")
    data = get_day(client).json()
    assert len(data["fills"]) == 1 and data["skipped_rows"] == 1
    assert data["fills_truncated"] is True and data["complete"]["fills_file"] is False
    listed = client.get(BASE + "/dates", headers=HEADERS).json()
    assert listed["truncated"] is True
    assert [r["row_count"] for r in listed["dates"]] == [1, 0]
    assert all(r["truncated"] for r in listed["dates"])


def test_byte_limit_and_partial_append(client, tmp_path, monkeypatch):
    path = write_fills(tmp_path, [fill(), fill()])
    monkeypatch.setattr(ld, "MAX_BYTES", len(path.read_bytes().splitlines(keepends=True)[0]) + 10)
    assert get_day(client).json()["fills_truncated"] is True
    monkeypatch.setattr(ld, "MAX_BYTES", 8 * 1024 * 1024)
    path.write_bytes(path.read_bytes().rstrip(b"\r\n"))
    data = get_day(client).json()
    assert len(data["fills"]) == 1 and data["fills_truncated"] is True


def test_oversized_line_and_invalid_header(client, tmp_path, monkeypatch):
    path = write_fills(tmp_path, [fill()])
    monkeypatch.setattr(ld, "MAX_LINE_BYTES", 10)
    assert get_day(client).json()["fills_truncated"] is True
    monkeypatch.setattr(ld, "MAX_LINE_BYTES", 65536)
    path.write_bytes(b"symbol,price\n2330,900\n")
    data = get_day(client).json()
    assert data["fills"] == [] and data["complete"]["fills_file"] is False


def test_positions_bounded_tail_finds_latest(client, tmp_path, monkeypatch):
    write_fills(tmp_path, [])
    write_positions(tmp_path, [{"date": DAY, "holdings": [1]}] * 5 + [{"date": DAY, "holdings": []}])
    monkeypatch.setattr(ld, "MAX_BYTES", 150)
    monkeypatch.setattr(ld, "MAX_ROWS", 1)
    data = get_day(client).json()
    assert data["positions_after"] == []
    assert data["positions_truncated"] is True
    assert data["complete"]["positions_snapshot"] is True


def test_dates_directory_and_date_limits(client, tmp_path, monkeypatch):
    write_fills(tmp_path, [])
    write_fills(tmp_path, [], day="2026-10-21")
    monkeypatch.setattr(ld, "MAX_DATES", 1)
    result = client.get(BASE + "/dates", headers=HEADERS).json()
    assert result["truncated"] is True and len(result["dates"]) == 1
    monkeypatch.setattr(ld, "MAX_DIRECTORY_ENTRIES", 1)
    assert client.get(BASE + "/dates", headers=HEADERS).json()["truncated"] is True


def test_no_disk_writes_or_unrelated_file_reads(client, tmp_path, monkeypatch):
    write_fills(tmp_path, [fill()])
    write_positions(tmp_path, [{"date": DAY, "holdings": []}])
    orders = tmp_path / f"{DAY}_orders.csv"
    orders.write_bytes(b"presence only")
    before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir()}
    allowed = {tmp_path / f"{DAY}_fills.csv", tmp_path / "positions_history.jsonl"}
    original_open = io.open

    def read_only(file, mode="r", *args, **kwargs):
        assert mode in ("r", "rb"), "endpoint attempted a write"
        assert Path(file) in allowed, "endpoint read an unrelated file"
        return original_open(file, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(io, "open", read_only)
        patch.setattr(builtins, "open", read_only)
        assert get_day(client).status_code == 200
        assert client.get(BASE + "/dates", headers=HEADERS).status_code == 200
    after = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir()}
    assert before == after


def test_missing_output_directory_not_created(client, tmp_path, monkeypatch):
    missing = tmp_path / "not-created"
    monkeypatch.setattr(ld, "OUTPUT_DIR", missing)
    assert get_day(client).status_code == 404
    assert client.get(BASE + "/dates", headers=HEADERS).json() == {"dates": [], "truncated": False}
    assert not missing.exists()


def test_symlink_escape_rejected(client, tmp_path):
    target = tmp_path / "external.csv"
    target.write_bytes(b"not allowed")
    link = tmp_path / f"{DAY}_fills.csv"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink privilege unavailable on Windows")
    assert get_day(client).status_code == 503
    assert client.get(BASE + "/dates", headers=HEADERS).json()["dates"] == []


def test_api_router_integration_without_runner_or_config(client, monkeypatch):
    # Existing config imports dotenv at module load; never read operator secrets.
    import dotenv
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: False)
    import api

    monkeypatch.setattr(api.Runner, "get", lambda: pytest.fail("read route accessed Runner"))
    monkeypatch.setattr(api, "load_config", lambda: pytest.fail("read route loaded config"))
    app = FastAPI()
    app.include_router(api.router)
    actual = TestClient(app)
    actual._trust_env = False
    assert actual.get(BASE + "/dates", headers=HEADERS).status_code == 200
    assert actual.get(BASE, params={"date": DAY}, headers=HEADERS).status_code == 404
    assert actual.get(BASE + "/dates").status_code == 401
