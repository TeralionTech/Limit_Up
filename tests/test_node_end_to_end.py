"""node 端到端核心 (中心過濾架構): Hub 快照 → seed state(含峰值)→ node book handler 續墊高峰值
→ 08:59:58 量減半(state.final_check_all)→ 存活 marked = 預掛清單。用真 State + 真 handler,不跑 runner。"""
from filter import make_node_book_handler
from state import State


def _book(price, size):
    return [{"price": price, "size": size}], []          # (bids, asks)


def _seed_from_snapshot(state, rows):
    """模擬 node 拉到 Hub 快照後 seed: mark 帶 max_bid_vol → state._max_bid_size = Hub 峰值。"""
    for sym, limit_up, max_bid_vol, first_tick in rows:
        state.mark(sym, limit_up, max_bid_vol, limit_up, first_tick=first_tick)


class TestNodeSeedAndFinalCheck:
    def test_final_check_drops_weakened_keeps_strong(self):
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 1000, True), ("B", 50.0, 800, False)])
        on_book = make_node_book_handler(st, {"A": 100.0, "B": 50.0})
        # node 08:59:50–58 收到當前量: A 900 (≥ 1000×0.5 保留)、B 300 (< 800×0.5 剔除)
        on_book("A", *_book(100.0, 900))
        on_book("B", *_book(50.0, 300))
        dropped = st.final_check_all()
        assert [d[0] for d in dropped] == ["B"]
        assert st.get_marked_list() == ["A"]              # 存活 = 預掛清單

    def test_node_extends_peak_in_last_8s(self):
        # 使用者補正: 峰值不凍結 — node 在最後 8 秒看到更高量 → 墊高峰值 → 量減半更嚴
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, False)])   # Hub 峰值 800
        on_book = make_node_book_handler(st, {"A": 100.0})
        on_book("A", *_book(100.0, 1200))                 # node 看到 1200 → 峰值墊到 1200
        assert st.get_max_bid_size("A") == 1200
        on_book("A", *_book(100.0, 500))                  # 當前掉到 500
        dropped = st.final_check_all()
        # 500 < 1200×0.5=600 → 剔除。(若峰值沒墊高、還是 800,則 500 ≥ 400 會留 → 就是差別所在)
        assert [d[0] for d in dropped] == ["A"]

    def test_node_book_ignores_non_limitup_level(self):
        # 開盤市價列 price=0 / 非漲停價那層不更新峰值 (避開 6243 型誤判)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("A", 100.0, 800, False)])
        on_book = make_node_book_handler(st, {"A": 100.0})
        on_book("A", *_book(0.0, 5))                      # 市價列 price=0 → 不更新
        on_book("A", *_book(99.0, 5))                     # 低於漲停 → 不更新
        assert st.get_max_bid_size("A") == 800            # 峰值不動

    def test_first_tick_priority_preserved(self):
        # 快照的開盤即鎖 → seed 後 get_marked_prioritized 仍把它排前面 (預掛送單優先)
        st = State(bid_drop_ratio=0.5)
        _seed_from_snapshot(st, [("6933", 260.0, 100, False), ("8105", 16.65, 500, True)])
        order = st.get_marked_prioritized()
        assert order.index("8105") < order.index("6933")   # 開盤即鎖 8105 優先
