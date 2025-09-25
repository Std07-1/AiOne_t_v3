"""Гвардійський тест: очікувані BUY/SELL рекомендації не повинні мапитись у NORMAL.

Якщо цей тест почне падати, це попередження про регресію у логіці map_reco_to_signal
або у списках BUY_SET/SELL_SET в config.
"""

from utils.utils import map_reco_to_signal


def test_buy_recommendations_map_to_alert_buy() -> None:
    # Мінімальний набір BUY‑рекомендацій, що гарантують ALERT_BUY
    for reco in ["STRONG_BUY", "BUY_IN_DIPS"]:
        sig = map_reco_to_signal(reco)
        assert sig == "ALERT_BUY", f"Очікували ALERT_BUY для {reco}, але отримали {sig}"


def test_sell_recommendations_map_to_alert_sell() -> None:
    # Мінімальний набір SELL‑рекомендацій, що гарантують ALERT_SELL
    for reco in ["STRONG_SELL", "SELL_ON_RALLIES"]:
        sig = map_reco_to_signal(reco)
        assert (
            sig == "ALERT_SELL"
        ), f"Очікували ALERT_SELL для {reco}, але отримали {sig}"


def test_unknown_or_wait_is_normal() -> None:
    # Для невідомих/нейтральних очікуємо NORMAL — sanity check
    for reco in [None, "", "WAIT", "AVOID", "UNCERTAIN"]:
        sig = map_reco_to_signal(reco)
        assert sig == "NORMAL"
