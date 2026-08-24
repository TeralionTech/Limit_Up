"""T30 全額交割/禁單名單 (2026-08-12 全額交割股狂送單事故的修復)。

合成 100-byte 定長記錄驗證 parser 與下單閘門:
    SETTYPE (byte 41) != '0'  → 排除 (全額交割)
    MARK-W  (byte 42) == '2'  → 排除 (每筆需 100% 預收)
"""
from test_session_money import make_session, _fire_chase, _fill
import t30


def _rec(stock_no: str, settype: str = "0", mark_w: str = "0") -> bytes:
    b = bytearray(b"0" * t30.RECORD_SIZE)
    b[0:6] = f"{stock_no:<6}".encode("ascii")
    b[41] = ord(settype)
    b[42] = ord(mark_w)
    return bytes(b)


def _write_t30(path, *records):
    path.write_bytes(b"".join(records))


class TestParseUntradable:
    def test_settype_and_markw2_excluded(self, tmp_path):
        f = tmp_path / "T30V.TSE"
        _write_t30(f,
                   _rec("1101"),                          # 普通股 → 不排除
                   _rec("9103", settype="1"),             # 全額交割
                   _rec("6547", settype="2"),             # 全額交割+分盤
                   _rec("3081", mark_w="1"),              # 第一次處置 → 可交易,不排除
                   _rec("2491", mark_w="2"))              # 每筆需 100% 預收 → 排除
        assert t30.parse_untradable(f) == {"9103", "6547", "2491"}

    def test_bad_size_raises(self, tmp_path):
        f = tmp_path / "T30V.TSE"
        f.write_bytes(b"0" * 150)                          # 非 100 倍數
        try:
            t30.parse_untradable(f)
            assert False, "應該 raise"
        except ValueError:
            pass


class TestLoadUntradable:
    def test_merges_tse_and_otc(self, tmp_path):
        _write_t30(tmp_path / "T30V.TSE", _rec("9103", settype="1"))
        _write_t30(tmp_path / "T30V.OTC", _rec("6547", mark_w="2"))
        untradable, meta = t30.load_untradable(tmp_path)
        assert untradable == {"9103", "6547"}
        assert meta["missing_all"] is False
        assert meta["files"]["T30V.TSE"]["ok"] is True
        assert meta["files"]["T30V.TSE"]["stale"] is False   # 剛寫的 → 今日檔

    def test_missing_all_flagged(self, tmp_path):
        untradable, meta = t30.load_untradable(tmp_path / "nonexistent")
        assert untradable == set()
        assert meta["missing_all"] is True

    def test_partial_missing_still_works(self, tmp_path):
        _write_t30(tmp_path / "T30V.TSE", _rec("9103", settype="1"))
        untradable, meta = t30.load_untradable(tmp_path)     # OTC 缺
        assert untradable == {"9103"}
        assert meta["missing_all"] is False
        assert meta["files"]["T30V.OTC"]["exists"] is False


class TestUntradableOrderGate:
    def test_pre_orders_skip_untradable(self):
        # 名單內的檔: 不掛單、不佔預算、標 full_cash_delivery;其他檔照常
        s = make_session()
        s.set_untradable({"9103"})
        s.place_pre_orders(["9103", "1101"], {"9103": 50.0, "1101": 50.0})
        assert s.trades["9103"].stopped_reason == "full_cash_delivery"
        assert not [c for c in s.broker.placed if c[1] == "9103"]
        assert [c[1] for c in s.broker.placed] == ["1101"]
        assert s.budget_used == 200_000                      # 只有 1101 的 4 張 × 50k

    def test_first_trade_does_not_chase_untradable(self):
        # 首筆成交來了也不市價追 — 狂送單根絕
        s = make_session()
        s.set_untradable({"9103"})
        s.place_pre_orders(["9103"], {"9103": 50.0})
        _fire_chase(s, "9103")
        assert s.broker.placed == []                         # 零委託
        assert s.broker.calls == []

    def test_roll_day_clears_list(self):
        s = make_session()
        s.set_untradable({"9103"})
        s.roll_day("2099-01-01")                             # 新交易日
        assert s.untradable == set()
