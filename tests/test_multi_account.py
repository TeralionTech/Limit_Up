"""多帳號收資料計畫 (2026-08-10: 三帳號 4/3/3,單帳號掛掉不再少收一半)。"""
from types import SimpleNamespace

from subscriber import Subscriber


def _cfg(**kw):
    base = dict(account_id="A", password="pa", pfx_path="a.pfx", pfx_password="",
                socket_count=5,
                account_id_2="", password_2="", pfx_path_2="", pfx_password_2="",
                account_id_3="", password_3="", pfx_path_3="", pfx_password_3="",
                socket_count_2=0, socket_count_3=0)
    base.update(kw)
    return SimpleNamespace(**base)


class TestAccountPlan:
    def test_three_accounts_433(self):
        cfg = _cfg(socket_count=4,
                   account_id_2="B", password_2="pb", pfx_path_2="b.pfx", socket_count_2=3,
                   account_id_3="C", password_3="pc", pfx_path_3="c.pfx", socket_count_3=3)
        plan = Subscriber._account_plan(cfg)
        assert [(p[0], p[5]) for p in plan] == \
            [("primary", 4), ("secondary", 3), ("tertiary", 3)]
        assert sum(p[5] for p in plan) == 10          # 10 sockets × 199 = 1990 容量
        assert plan[1][1] == "B" and plan[2][3] == "c.pfx"

    def test_two_accounts(self):
        cfg = _cfg(account_id_2="B", pfx_path_2="b.pfx")
        plan = Subscriber._account_plan(cfg)
        assert [p[0] for p in plan] == ["primary", "secondary"]
        assert plan[1][5] == 5                        # 未設 SOCKET_COUNT_2 → 同主帳號

    def test_primary_only(self):
        plan = Subscriber._account_plan(_cfg())
        assert [(p[0], p[5]) for p in plan] == [("primary", 5)]

    def test_third_without_pfx_skipped(self):
        # 只填帳號沒填憑證路徑 → 不納入 (登入必失敗,不浪費)
        cfg = _cfg(account_id_3="C")
        plan = Subscriber._account_plan(cfg)
        assert [p[0] for p in plan] == ["primary"]

    def test_none_cfg(self):
        assert Subscriber._account_plan(None) == []
