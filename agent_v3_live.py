# -*- coding: utf-8 -*-
#!/usr/bin/env python3
import requests, time, hmac, hashlib, base64, json, os, sys, threading
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

import logging as _log
_logger = _log.getLogger("agent")
_logger.setLevel(_log.INFO)
_logger.propagate = False
if not _logger.handlers:
    _fh = _log.FileHandler("/root/trading-agent/agent.log", encoding="utf-8")
    _fh.setFormatter(_log.Formatter("%(message)s"))
    _logger.addHandler(_fh)
def log(*args):
    msg = " ".join(str(a) for a in args)
    _logger.info(msg)


BITGET_API_KEY    = os.getenv("BITGET_API_KEY",    "")
BITGET_SECRET_KEY = os.getenv("BITGET_SECRET_KEY", "")
BITGET_PASSPHRASE = os.getenv("BITGET_PASSPHRASE", "")
WHALE_API_KEY     = ""
TONCENTER_API_KEY = ""
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN",    "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID",  "")
COINGLASS_API_KEY  = os.getenv("", "")
COINALYZE_API_KEY  = ""

# Маппинг символов для Coinalyze
_CZ_SYM = {
    "BTCUSDT": "BTCUSDT_PERP.A",
    "ETHUSDT": "ETHUSDT_PERP.A",
    "TONUSDT": "TONUSDT_PERP.A",
}
BITGET_BASE       = "https://api.bitget.com"

ASSETS = {
    "BTCUSDT": {"leverage": 15, "symbol": "BTCUSDT", "cg_id": "bitcoin",         "whale_sym": "btc", "bybit_sym": "BTCUSDT"},
    "ETHUSDT": {"leverage": 5,  "symbol": "ETHUSDT", "cg_id": "ethereum",         "whale_sym": "eth", "bybit_sym": "ETHUSDT"},
    "TONUSDT": {"leverage": 5,  "symbol": "TONUSDT", "cg_id": "the-open-network", "whale_sym": "ton", "bybit_sym": "TONUSDT"},
}


ANALYZE_BINANCE_SYMBOLS = {
    'SOLUSDT': 'SOLUSDT',
    'TRXUSDT': 'TRXUSDT',
    'LINKUSDT': 'LINKUSDT',
    'XRPUSDT': 'XRPUSDT',
    'LTCUSDT': 'LTCUSDT',
    'XLMUSDT': 'XLMUSDT',
    'OPUSDT': 'OPUSDT',
    'CAKEUSDT': 'CAKEUSDT',
    'BGBUSDT': 'BGBUSDT',
    'FFUSDT': 'FFUSDT',
}

NUM_ASSETS = len(ASSETS)
ANALYZE_ONLY = [
    'SOLUSDT',
    'TRXUSDT',
    'LINKUSDT',
    'XRPUSDT',
    'LTCUSDT',
    'XLMUSDT',
    'OPUSDT',
    'CAKEUSDT',
    'BGBUSDT',
    'FFUSDT',
]



TON_EXCHANGE_ADDRS = {
    "Gate.io": "EQC0lrj3O0af8GotieYsTXChA_wijIIVN7Sd_wkgYLwoH07q",
}

EMA_FAST         = 3
EMA_SLOW         = 18
RSI_PERIOD       = 14
RSI_OB           = 70
RSI_OS           = 35
THRESHOLD        = 3          # минимальный суммарный скор
MIN_ACTIVE_FACTORS = 5        # минимум факторов в одну сторону
SL_PCT           = 0.015
TP1_PCT          = 0.04
TP1_PCT_BTC      = 0.03
BE_TRIGGER       = 0.40
TRAILING_PCT_BTC = 0.015
TRAILING_PCT_DEF = 0.025
TAKER_FEE        = 0.001
ROUND_TRIP_FEE   = TAKER_FEE * 2
INTERVAL_SEC     = 2 * 3600
TRADE_WEEKENDS   = False
MONITOR_SEC      = 60
RSI_SHORT_MIN    = 45       # шорт только если RSI > этого значения
RSI_LONG_MAX     = 55       # лонг только если RSI < этого значения
COOLDOWN_SEC     = 2 * 3600 # минимум 2ч между сделками по одному активу
AGENT_CMD_FILE   = "/tmp/agent_command.json"

be_set = set()
last_trade_time: dict = {}  # sym -> timestamp последней открытой позиции

# ── BITGET ───────────────────────────────────────────────────────────

def _sign(ts, method, path, body=""):
    msg = ts + method + path + body
    return base64.b64encode(
        hmac.new(BITGET_SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()

def bitget_request(method, path, params=None, body=None):
    ts = str(int(time.time() * 1000))
    body_str = json.dumps(body) if body else ""
    if method.upper() == "GET" and params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        sign_path = path + "?" + qs
    else:
        sign_path = path
    sig = _sign(ts, method.upper(), sign_path, body_str)
    headers = {
        "ACCESS-KEY":        BITGET_API_KEY,
        "ACCESS-SIGN":       sig,
        "ACCESS-TIMESTAMP":  ts,
        "ACCESS-PASSPHRASE": BITGET_PASSPHRASE,
        "Content-Type":      "application/json",
    }
    url = BITGET_BASE + path
    if method.upper() == "GET":
        r = requests.get(url, params=params, headers=headers, timeout=10)
    else:
        r = requests.post(url, data=body_str, headers=headers, timeout=10)
    return r.json()

def get_futures_balance():
    resp = bitget_request("GET", "/api/v2/mix/account/accounts",
                          params={"productType": "USDT-FUTURES"})
    if resp.get("code") == "00000":
        for acc in resp["data"]:
            if acc.get("marginCoin") == "USDT":
                return float(acc.get("equity", acc.get("available", 0)))
    return None

def get_open_positions():
    resp = bitget_request("GET", "/api/v2/mix/position/all-position",
                          params={"productType": "USDT-FUTURES"})
    positions = {}
    if resp.get("code") == "00000":
        for p in resp.get("data", []):
            if float(p.get("total", 0)) > 0:
                sym = p["symbol"].replace("_UMCBL", "")
                positions[sym] = {
                    "side":         p["holdSide"],
                    "size":         float(p["total"]),
                    "entry":        float(p["openPriceAvg"]),
                    "unrealizedPL": float(p.get("unrealizedPL", 0)),
                    "leverage":     float(p.get("leverage", 1)),
                }
    return positions

def get_ticker(symbol):
    resp = bitget_request("GET", "/api/v2/mix/market/ticker",
                          params={"symbol": symbol, "productType": "USDT-FUTURES"})
    if resp.get("code") == "00000":
        data = resp.get("data", {})
        if isinstance(data, list) and len(data) > 0:
            return float(data[0].get("lastPr", data[0].get("last", 0)))
        elif isinstance(data, dict):
            return float(data.get("lastPr", data.get("last", 0)))
    return None

def place_order(symbol, side, size):
    ekb = pytz.timezone("Asia/Yekaterinburg")
    if datetime.now(ekb).weekday() == 5 and not TRADE_WEEKENDS:
        log("[BLOCK] Суббота — открытие позиций запрещено.")
        tg("Суббота: " + symbol)
        return {"code": "SATURDAY_BLOCK"}
    body = {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "marginMode": "crossed", "marginCoin": "USDT",
        "size": str(size), "side": side, "tradeSide": "open",
        "orderType": "market", "timeInForceValue": "normal",
    }
    return bitget_request("POST", "/api/v2/mix/order/place-order", body=body)

def close_position(symbol, side, size):
    r = bitget_request("POST", "/api/v2/mix/order/flash-close-position",
        body={"symbol": symbol, "productType": "USDT-FUTURES",
              "marginCoin": "USDT", "holdSide": side})
    if r.get("code") == "00000":
        return r
    close_side = "buy" if side == "long" else "sell"
    return bitget_request("POST", "/api/v2/mix/order/place-order", body={
        "symbol": symbol, "productType": "USDT-FUTURES",
        "marginMode": "crossed", "marginCoin": "USDT",
        "size": str(size), "side": close_side, "tradeSide": "close",
        "holdSide": side, "orderType": "market",
        "timeInForceValue": "normal", "reduceOnly": "YES"})

def cancel_all_plan_orders(symbol):
    try:
        r2 = bitget_request("POST", "/api/v2/mix/order/cancel-plan-order",
            body={"symbol": symbol, "productType": "USDT-FUTURES",
                  "marginCoin": "USDT", "planType": "moving_plan"})
        r3 = bitget_request("POST", "/api/v2/mix/order/cancel-plan-order",
            body={"symbol": symbol, "productType": "USDT-FUTURES",
                  "marginCoin": "USDT", "planType": "track_plan"})
        r4 = bitget_request("POST", "/api/v2/mix/order/cancel-plan-order",
            body={"symbol": symbol, "productType": "USDT-FUTURES",
                  "marginCoin": "USDT", "planType": "loss_plan"})
        log(f"  [cancel_plan] moving: {r2.get('code')} | track: {r3.get('code')} | loss: {r4.get('code')}")
    except Exception as e:
        log(f"  [cancel_plan] Ошибка: {e}")

def price_scale(symbol):
    if symbol == "BTCUSDT":   return 1
    elif symbol == "ETHUSDT": return 2
    else:                     return 4

def set_sl_tp(symbol, plan_type, trigger_price, hold_side="long", size=None):
    trigger_price = round(float(trigger_price), price_scale(symbol))
    body = {
        "symbol": symbol, "marginCoin": "USDT", "productType": "USDT-FUTURES",
        "planType": plan_type, "triggerPrice": str(trigger_price),
        "triggerType": "mark_price", "holdSide": hold_side,
    }
    if size is not None:
        body["size"] = str(size)
    return bitget_request("POST", "/api/v2/mix/order/place-tpsl-order", body=body)

def set_trailing_stop(symbol, hold_side, callback_rate, trigger_price, size=None):
    trigger_price = round(float(trigger_price), price_scale(symbol))
    close_side = "buy" if hold_side == "short" else "sell"
    body = {
        "symbol": symbol, "marginCoin": "USDT", "productType": "USDT-FUTURES",
        "marginMode": "isolated",
        "planType": "track_plan",
        "side": close_side,
        "tradeSide": "close",
        "orderType": "market",
        "callbackRatio": str(round(float(callback_rate) * 100, 2)),
        "triggerPrice": str(trigger_price),
        "triggerType": "mark_price",
        "holdSide": hold_side,
    }
    if size is not None:
        body["size"] = str(size)
    return bitget_request("POST", "/api/v2/mix/order/place-plan-order", body=body)

def calc_position_size(sym, balance, price, open_count):
    free_slots = NUM_ASSETS - open_count
    if free_slots <= 0:
        return 0
    capital  = (balance * 0.90) * 0.30  # 30% от 90% баланса (10% резерв)
    leverage = ASSETS[sym]["leverage"]
    notional = capital * leverage
    size     = notional / price
    if   price > 10000: size = round(size, 3)
    elif price > 100:   size = round(size, 2)
    elif price > 1:     size = round(size, 1)
    else:               size = round(size, 0)
    return size

# ── TELEGRAM ─────────────────────────────────────────────────────────

def tg(msg):
    try:
        requests.post(
            "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10)
    except:
        pass

# ── STATUS / CLOSE ────────────────────────────────────────────────────

def print_status():
    now = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    balance  = get_futures_balance()
    open_pos = get_open_positions()
    lines = ["<b>STATUS | " + now + "</b>"]
    lines.append("Баланс: <b>$" + str(round(balance, 2)) + "</b>" if balance else "Баланс: недоступен")
    if not open_pos:
        lines.append("Открытых позиций нет")
    else:
        total_pnl = 0.0
        for sym, pos in open_pos.items():
            side  = pos["side"].upper()
            size  = pos["size"]
            entry = pos["entry"]
            pnl   = pos["unrealizedPL"]
            lev   = int(pos["leverage"])
            total_pnl += pnl
            cfg = ASSETS.get(sym)
            cur_price = get_ticker(cfg["symbol"]) if cfg else None
            price_str = "$" + str(round(cur_price, 4)) if cur_price else "n/a"
            if cur_price and entry > 0:
                pct = ((cur_price - entry) / entry * 100 * lev
                       if pos["side"] == "long"
                       else (entry - cur_price) / entry * 100 * lev)
                pct_str = ("+" if pct >= 0 else "") + str(round(pct, 2)) + "%"
            else:
                pct_str = "n/a"
            emoji = "🟢" if pnl >= 0 else "🔴"
            close_fee = round(cur_price * size * TAKER_FEE, 3) if cur_price and size else 0.0
            net_pnl   = round(pnl - close_fee, 2)
            pnl_str   = ("+" if pnl >= 0 else "") + str(round(pnl, 2))
            net_str   = ("+" if net_pnl >= 0 else "") + str(net_pnl)
            be_status = " 🔒BE" if sym in be_set else ""
            lines.append(emoji + " <b>" + sym + "</b> " + side + " x" + str(lev) + be_status)
            lines.append("  Вход: $" + str(entry) + " | Сейчас: " + price_str)
            lines.append("  PnL: <b>" + pnl_str + " USDT</b> (" + pct_str + ") | Нетто: <b>" + net_str + " USDT</b>")
        total_emoji = "🟢" if total_pnl >= 0 else "🔴"
        total_str   = ("+" if total_pnl >= 0 else "") + str(round(total_pnl, 2))
        lines.append(total_emoji + " Итого PnL: <b>" + total_str + " USDT</b>")
    msg = "\n".join(lines)
    log(msg.replace("<b>", "").replace("</b>", ""))
    tg(msg)

def close_all_positions():
    log("Закрываю все позиции...")
    open_pos = get_open_positions()
    if not open_pos:
        log("Нет открытых позиций.")
        return
    total_pnl = 0.0
    for sym, pos in open_pos.items():
        cfg = ASSETS.get(sym)
        if not cfg:
            log("  " + sym + ": неизвестный актив, пропуск")
            continue
        size = pos["size"]
        side = pos["side"]
        pnl  = pos["unrealizedPL"]
        total_pnl += pnl
        cancel_all_plan_orders(cfg["symbol"])
        result = close_position(cfg["symbol"], side, size)
        if result.get("code") == "00000":
            pnl_str = ("+" if pnl >= 0 else "") + str(round(pnl, 2))
            log("  OK " + sym + " | PnL: " + pnl_str + " USDT")
            tg("ЗАКРЫТО " + sym + " | PnL: " + pnl_str + " USDT")
        else:
            log("  ERR " + sym + ": " + result.get("msg", "unknown"))
            tg("Ошибка закрытия " + sym + ": " + result.get("msg", "unknown"))
    total_str = ("+" if total_pnl >= 0 else "") + str(round(total_pnl, 2))
    log("Итого PnL: " + total_str + " USDT")
    tg("Все позиции закрыты | Итого PnL: " + total_str + " USDT")

# ── BREAKEVEN ─────────────────────────────────────────────────────────

def check_breakeven(sym, pos, current_price):
    entry = pos["entry"]
    side  = pos["side"]
    size  = pos.get("size")
    cfg   = ASSETS[sym]
    be_decimals = 0 if sym == "BTCUSDT" else 2 if sym == "ETHUSDT" else 4
    if side == "long":
        tp1        = entry * (1 + (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT))
        be_trigger = entry + (tp1 - entry) * BE_TRIGGER
        if current_price >= be_trigger:
            be_sl = round(entry * (1 + ROUND_TRIP_FEE), be_decimals)
            cancel_all_plan_orders(cfg["symbol"])
            time.sleep(0.3)
            r = set_sl_tp(cfg["symbol"], "loss_plan", be_sl, hold_side="long", size=size)
            log("  [BE] " + sym + " long: $" + str(be_sl) + " -> " + str(r.get("code")) + " " + r.get("msg",""))
            if r.get("code") == "00000":
                tg("BE SL " + sym + ": $" + str(be_sl))
                trail_pct = TRAILING_PCT_BTC if sym == "BTCUSDT" else TRAILING_PCT_DEF
                tp1 = entry * (1 + (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT))
                set_trailing_stop(cfg["symbol"], "long", trail_pct, tp1, size=size)
                return True
    else:
        tp1        = entry * (1 - (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT))
        be_trigger = entry - (entry - tp1) * BE_TRIGGER
        if current_price <= be_trigger:
            be_sl = round(entry * (1 - ROUND_TRIP_FEE), be_decimals)
            cancel_all_plan_orders(cfg["symbol"])
            time.sleep(0.3)
            r = set_sl_tp(cfg["symbol"], "loss_plan", be_sl, hold_side="short", size=size)
            log("  [BE] " + sym + " short: $" + str(be_sl) + " -> " + str(r.get("code")) + " " + r.get("msg",""))
            if r.get("code") == "00000":
                tg("BE SL " + sym + ": $" + str(be_sl))
                trail_pct = TRAILING_PCT_BTC if sym == "BTCUSDT" else TRAILING_PCT_DEF
                tp1 = entry * (1 - (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT))
                set_trailing_stop(cfg["symbol"], "short", trail_pct, tp1, size=size)
                return True
    return False

# ── SCORES ────────────────────────────────────────────────────────────

def get_whale_score_alert(whale_sym, lookback_sec=21600):
    try:
        since = int(time.time() - lookback_sec)
        r = requests.get(
            "https://api.whale-alert.io/v1/transactions",
            params={
                "apikey": WHALE_API_KEY,
                "minvalue": 500000,
                "start": since,
                "currency": whale_sym,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return 0
        txs = r.json().get("transactions", [])
        toex = fromex = 0
        for tx in txs:
            amt = tx.get("amount_usd", 0) or 0
            to_ = tx.get("to") or {}
            fr_ = tx.get("from") or {}
            if to_.get("owner_type") == "exchange":
                toex += amt
            if fr_.get("owner_type") == "exchange":
                fromex += amt
        net = fromex - toex
        if net >= 50_000_000:
            return 2
        elif net >= 10_000_000:
            return 1
        elif net <= -50_000_000:
            return -2
        elif net <= -10_000_000:
            return -1
        return 0
    except:
        return 0

def get_ton_whale_score(lookback_sec=21600):
    """TON whale score: приток/отток на биржи по крупным транзакциям."""
    try:
        since = int(time.time() - lookback_sec)
        to_ex = from_ex = 0
        headers = {"X-API-Key": TONCENTER_APIKEY}
        for name, addr in TON_EXCHANGE_ADDRS.items():
            r = requests.get("http://toncenter.com/api/v2/getTransactions",
                             params={"address": addr, "limit": 100},
                             headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            for tx in r.json().get("result", []):
                if tx.get("utime", 0) < since:
                    continue
                in_val = int(tx.get("in_msg", {}).get("value", 0)) / 1e9
                out_val = sum(int(m.get("value", 0)) for m in tx.get("out_msgs", [])) / 1e9
                if in_val > 10000:
                    to_ex += in_val
                if out_val > 10000:
                    from_ex += out_val
        net = from_ex - to_ex
        if   net >= 500000:  return  2
        elif net >= 100000:  return  1
        elif net <= -500000: return -2
        elif net <= -100000: return -1
        return 0
    except:
        return 0


STABLE_WHALE_SYMS = ["usdt", "usdc"]


def get_stable_whale_score(lookback_sec: int = 21600) -> int:
    if not WHALE_API_KEY:
        return 0
    try:
        since = int(time.time() - lookback_sec)
        toex = fromex = 0.0
        for sym in STABLE_WHALE_SYMS:
            r = requests.get(
                "https://api.whale-alert.io/v1/transactions",
                params={
                    "apikey": WHALE_API_KEY,
                    "minvalue": 500000,
                    "start": since,
                    "currency": sym,
                },
                timeout=10,
            )
            if r.status_code != 200:
                continue
            txs = r.json().get("transactions", [])
            for tx in txs:
                amt = tx.get("amount_usd", 0) or 0
                to_ = tx.get("to") or {}
                fr_ = tx.get("from") or {}
                if to_.get("owner_type") == "exchange":
                    toex += amt
                if fr_.get("owner_type") == "exchange":
                    fromex += amt
        net = fromex - toex
        if net >= 3_000_000_000:
            return 2
        elif net >= 1_000_000_000:
            return 1
        elif net <= -3_000_000_000:
            return -2
        elif net <= -1_000_000_000:
            return -1
        return 0
    except:
        return 0


def get_risk_on_score() -> int:
    btcw = get_whale_score_alert("btc")
    ethw = get_whale_score_alert("eth")
    stablew = get_stable_whale_score()
    usdt_d = get_usdt_dominance_score()
    fg = get_fear_greed_score()
    score = 0
    if btcw == 2 or ethw == 2:
        score += 2
    elif btcw == -2 or ethw == -2:
        score -= 2
    elif btcw == 1 or ethw == 1:
        score += 1
    elif btcw == -1 or ethw == -1:
        score -= 1
    if stablew >= 2:
        score -= 2
    elif stablew <= -2:
        score += 2
    elif stablew == 1:
        score -= 1
    elif stablew == -1:
        score += 1
    score += usdt_d
    score += fg
    if score > 4:
        score = 4
    elif score < -4:
        score = -4
    return score

def get_usdt_dominance_score():
    try:
        r = requests.get("https://api.coingecko.com/api/v3/global",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200: return 0
        usdt_d = r.json().get("data", {}).get("market_cap_percentage", {}).get("usdt", 0)
        if usdt_d > 7.0:   return -1
        elif usdt_d < 5.0: return 1
        return 0
    except: return 0

def get_cluster_score(sym):
    """Cluster score: buy/sell delta крупных сделок.
    Источники: Bybit Futures (x1), Bybit Spot (x2), Binance Spot (x2),
               Bitget Futures (x1), Bitget Spot (x1).
    Все объёмы приведены к USDT (qty * price).
    """
    min_usdt = {"BTCUSDT": 20000, "ETHUSDT": 5000, "TONUSDT": 500,
                "SOLUSDT": 2000, "XRPUSDT": 1000, "LINKUSDT": 500}
    threshold = min_usdt.get(sym, 500)

    scores = []

    def score_delta(buy_vol, sell_vol, weight=1):
        total = buy_vol + sell_vol
        if total <= 0:
            return
        d = (buy_vol - sell_vol) / total * 100
        if   d >= 70:  v = 2
        elif d >= 50:  v = 1
        elif d <= -70: v = -2
        elif d <= -50: v = -1
        else:          v = 0
        for _ in range(weight):
            scores.append(v)

    # --- Bybit Futures (вес x1) ---
    try:
        r = requests.get("https://api.bybit.com/v5/market/recent-trade",
            params={"category": "linear", "symbol": sym, "limit": "1000"}, timeout=10)
        if r.status_code == 200:
            buy_vol = sell_vol = 0.0
            for t in r.json().get("result", {}).get("list", []):
                qty   = float(t.get("size", 0))
                price = float(t.get("price", 0))
                usdt  = qty * price
                if usdt < threshold: continue
                if t.get("side") == "Buy": buy_vol += usdt
                else: sell_vol += usdt
            score_delta(buy_vol, sell_vol, weight=1)
    except: pass

    # --- Bybit Spot (вес x2) ---
    try:
        r = requests.get("https://api.bybit.com/v5/market/recent-trade",
            params={"category": "spot", "symbol": sym, "limit": "1000"}, timeout=10)
        if r.status_code == 200:
            buy_vol = sell_vol = 0.0
            for t in r.json().get("result", {}).get("list", []):
                qty   = float(t.get("size", 0))
                price = float(t.get("price", 0))
                usdt  = qty * price
                if usdt < threshold: continue
                if t.get("side") == "Buy": buy_vol += usdt
                else: sell_vol += usdt
            score_delta(buy_vol, sell_vol, weight=2)
    except: pass

    # --- Binance Spot (вес x2) ---
    try:
        r = requests.get("https://api.binance.com/api/v3/trades",
            params={"symbol": sym, "limit": 1000}, timeout=10)
        if r.status_code == 200:
            buy_vol = sell_vol = 0.0
            for t in r.json():
                qty   = float(t.get("qty", 0))
                price = float(t.get("price", 0))
                usdt  = qty * price
                if usdt < threshold: continue
                if not t.get("isBuyerMaker"): buy_vol += usdt
                else: sell_vol += usdt
            score_delta(buy_vol, sell_vol, weight=2)
    except: pass

    # --- Bitget Futures (вес x1) ---
    try:
        r = requests.get("https://api.bitget.com/api/v2/mix/market/fills",
            params={"symbol": sym, "productType": "USDT-FUTURES", "limit": "500"}, timeout=10)
        if r.status_code == 200:
            buy_vol = sell_vol = 0.0
            for t in r.json().get("data", []):
                qty   = float(t.get("size", 0))
                price = float(t.get("price", 0))
                usdt  = qty * price
                if usdt < threshold: continue
                if t.get("side") == "buy": buy_vol += usdt
                else: sell_vol += usdt
            score_delta(buy_vol, sell_vol, weight=1)
    except: pass

    # --- Bitget Spot (вес x1) ---
    try:
        r = requests.get("https://api.bitget.com/api/v2/spot/market/fills",
            params={"symbol": sym, "limit": "500"}, timeout=10)
        if r.status_code == 200:
            buy_vol = sell_vol = 0.0
            for t in r.json().get("data", []):
                qty   = float(t.get("size", 0))
                price = float(t.get("price", 0))
                usdt  = qty * price
                if usdt < threshold: continue
                if t.get("side") == "buy": buy_vol += usdt
                else: sell_vol += usdt
            score_delta(buy_vol, sell_vol, weight=1)
    except: pass

    if not scores: return 0
    avg = sum(scores) / len(scores)
    if   avg >= 1.5:  return 2
    elif avg >= 0.5:  return 1
    elif avg <= -1.5: return -2
    elif avg <= -0.5: return -1
    return 0

def get_cvd_score(symbol, window_hours=2):
    """CVD Futures (Binance fapi) за последние window_hours часов.
    Использует startTime/endTime для точного 2ч окна.
    Returns (score, delta_pct)."""
    try:
        import time as _time
        end_ms   = int(_time.time() * 1000)
        start_ms = end_ms - window_hours * 3600 * 1000
        buy_vol = sell_vol = 0.0
        from_id = None
        # Binance aggTrades: max 1000 за запрос, пагинируем по времени
        params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
        while True:
            r = requests.get("https://fapi.binance.com/fapi/v1/aggTrades",
                params=params, timeout=15)
            if r.status_code != 200: break
            trades = r.json()
            if not trades: break
            for t in trades:
                qty   = float(t["q"])
                price = float(t["p"])
                usdt  = qty * price
                if not t["m"]: buy_vol  += usdt
                else:          sell_vol += usdt
            if len(trades) < 1000: break
            # следующая страница: startTime = последний trade + 1ms
            params = {"symbol": symbol,
                      "startTime": trades[-1]["T"] + 1,
                      "endTime": end_ms, "limit": 1000}
        total = buy_vol + sell_vol
        if total == 0: return 0, 0.0
        delta_pct = (buy_vol - sell_vol) / total * 100
        if   delta_pct >  15: score =  2
        elif delta_pct >   5: score =  1
        elif delta_pct < -15: score = -2
        elif delta_pct <  -5: score = -1
        else:                 score =  0
        return score, round(delta_pct, 1)
    except: return 0, 0.0


def get_spot_cvd_score(symbol, window_hours=2):
    """CVD Spot (Binance spot) за последние window_hours часов.
    Использует startTime/endTime для точного 2ч окна.
    Returns (score, delta_pct)."""
    try:
        import time as _time
        end_ms   = int(_time.time() * 1000)
        start_ms = end_ms - window_hours * 3600 * 1000
        buy_vol = sell_vol = 0.0
        params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
        while True:
            r = requests.get("https://api.binance.com/api/v3/aggTrades",
                params=params, timeout=15)
            if r.status_code != 200: break
            trades = r.json()
            if not trades: break
            for t in trades:
                qty   = float(t["q"])
                price = float(t["p"])
                usdt  = qty * price
                if not t["m"]: buy_vol  += usdt
                else:          sell_vol += usdt
            if len(trades) < 1000: break
            params = {"symbol": symbol,
                      "startTime": trades[-1]["T"] + 1,
                      "endTime": end_ms, "limit": 1000}
        total = buy_vol + sell_vol
        if total == 0: return 0, 0.0
        delta_pct = (buy_vol - sell_vol) / total * 100
        if   delta_pct >  15: score =  2
        elif delta_pct >   5: score =  1
        elif delta_pct < -15: score = -2
        elif delta_pct <  -5: score = -1
        else:                 score =  0
        return score, round(delta_pct, 1)
    except: return 0, 0.0


def get_cvd_divergence_score(symbol):
    """
    CVD спот vs фьючерс за 2ч. Приоритет — споровые данные.
    Дивергенция: фьючи+/спот- → дистрибуция → -2
                 фьючи-/спот+ → накопление  → +2
    Совпадение:  оба+ → +2, оба- → -2
    Только спот: +1 / -1
    Только фьючи: 0 (не доверяем без спота)
    Returns (score, fut_pct, spot_pct).
    """
    fut,  fut_pct  = get_cvd_score(symbol)
    spot, spot_pct = get_spot_cvd_score(symbol)

    if fut > 0 and spot < 0:   score = -2
    elif fut < 0 and spot > 0: score =  2
    elif fut > 0 and spot > 0: score =  2
    elif fut < 0 and spot < 0: score = -2
    elif spot > 0 and fut == 0: score =  1
    elif spot < 0 and fut == 0: score = -1
    else:                       score =  0
    return score, fut_pct, spot_pct

def get_volume_score(symbol, periods=24):
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": "1h", "limit": periods + 1}, timeout=10)
        if r.status_code != 200: return 0
        klines = r.json()
        if len(klines) < 2: return 0
        vols     = [float(k[5]) for k in klines[:-1]]
        last_vol = float(klines[-1][5])
        avg_vol  = sum(vols) / len(vols) if vols else 1
        ratio    = last_vol / avg_vol if avg_vol > 0 else 1
        if   ratio > 3.0: return  2
        elif ratio > 1.8: return  1
        elif ratio < 0.4: return -1
        return 0
    except: return 0

_fg_cache = {"value": None, "ts": 0}

def get_fear_greed_score():
    global _fg_cache
    try:
        if time.time() - _fg_cache["ts"] < 3600 and _fg_cache["value"] is not None:
            return _fg_cache["value"]
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        if r.status_code != 200: return 0
        val = int(r.json()["data"][0]["value"])
        if   val <= 20: score = -2
        elif val <= 35: score = -1
        elif val >= 80: score =  2
        elif val >= 65: score =  1
        else:           score =  0
        _fg_cache = {"value": score, "ts": time.time()}
        log(f"  [F&G] Index={val} → score={score}")
        return score
    except: return 0


def get_oi_score_coinalyze(symbol):
    """OI score через Coinalyze (агрегат всех бирж) за 2ч.
    Returns (score, change_pct)."""
    cz_sym = _CZ_SYM.get(symbol)
    if not cz_sym:
        return 0, 0.0
    try:
        now = int(time.time())
        frm = now - 7200
        r = requests.get(
            "https://api.coinalyze.net/v1/open-interest-history",
            params={"symbols": cz_sym, "interval": "2hour",
                    "from": frm, "to": now, "convert_to_usd": "true",
                    "api_key": COINALYZE_API_KEY},
            timeout=10)
        if r.status_code != 200:
            return 0, 0.0
        hist = r.json()
        if not hist or not hist[0].get("history"):
            return 0, 0.0
        h = hist[0]["history"][0]
        oi_open  = float(h.get("o", 0))
        oi_close = float(h.get("c", 0))
        if oi_open <= 0:
            return 0, 0.0
        change = (oi_close - oi_open) / oi_open
        if   change >  0.005: score =  1
        elif change < -0.005: score = -1
        else:                 score =  0
        return score, round(change * 100, 2)
    except:
        return 0, 0.0


def get_liq_score_coinalyze(symbol):
    """Ликвидации через Coinalyze за 2ч.
    Лонг-ликвидации >> шорт -> медведи давят -> -1, иначе +1."""
    cz_sym = _CZ_SYM.get(symbol)
    if not cz_sym:
        return 0
    try:
        now = int(time.time())
        frm = now - 7200
        r = requests.get(
            "https://api.coinalyze.net/v1/liquidation-history",
            params={"symbols": cz_sym, "interval": "2hour",
                    "from": frm, "to": now, "convert_to_usd": "true",
                    "api_key": COINALYZE_API_KEY},
            timeout=10)
        if r.status_code != 200:
            return 0
        hist = r.json()
        if not hist or not hist[0].get("history"):
            return 0
        h = hist[0]["history"][0]
        liq_long  = float(h.get("l", 0))
        liq_short = float(h.get("s", 0))
        total = liq_long + liq_short
        if total <= 0:
            return 0
        ratio = liq_long / total
        if   ratio > 0.65: return -1
        elif ratio < 0.35: return  1
        return 0
    except:
        return 0


def get_ls_score_coinalyze(symbol):
    """Long/Short ratio через Coinalyze за 2ч.
    ratio > 1.05 -> лонги доминируют -> +1
    ratio < 0.95 -> шорты доминируют -> -1"""
    cz_sym = _CZ_SYM.get(symbol)
    if not cz_sym:
        return 0
    try:
        now = int(time.time())
        frm = now - 7200
        r = requests.get(
            "https://api.coinalyze.net/v1/long-short-ratio-history",
            params={"symbols": cz_sym, "interval": "2hour",
                    "from": frm, "to": now,
                    "api_key": COINALYZE_API_KEY},
            timeout=10)
        if r.status_code != 200:
            return 0
        hist = r.json()
        if not hist or not hist[0].get("history"):
            return 0
        h = hist[0]["history"][0]
        ratio = float(h.get("r", 1.0))
        if   ratio > 1.05: return  1
        elif ratio < 0.95: return -1
        return 0
    except:
        return 0

def get_liq_score(symbol):
    if not COINGLASS_API_KEY:
        return 0
    try:
        sym = symbol.replace("USDT", "")
        r = requests.get("https://open-api.coinglass.com/public/v2/liquidation_map",
            params={"symbol": sym, "interval": "1h"},
            headers={"coinglassSecret": COINGLASS_API_KEY}, timeout=10)
        if r.status_code != 200: return 0
        data = r.json().get("data", {})
        longs_above  = data.get("longLiquidationAbove",  0)
        shorts_below = data.get("shortLiquidationBelow", 0)
        if longs_above > shorts_below * 2:  return -1
        if shorts_below > longs_above * 2:  return  1
        return 0
    except: return 0

# Маппинг CoinGecko ID → Binance symbol (для get_ohlcv)
_CG_TO_BINANCE = {
    "bitcoin":  "BTCUSDT",
    "ethereum": "ETHUSDT",
    "the-open-network": "TONUSDT",
    "terra-luna-classic": "LUNCUSDT",
}

def get_ohlcv(cg_id, days=5):
    """Получает OHLCV через Binance Futures klines (4h свечи)."""
    try:
        symbol = _CG_TO_BINANCE.get(cg_id)
        if not symbol:
            return None
        limit = min(days * 6, 200)   # 4h свечей за days дней (6 свечей/день)
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": "4h", "limit": limit},
            timeout=15)
        if r.status_code != 200:
            return None
        raw = r.json()
        df = pd.DataFrame(raw, columns=[
            "ts", "open", "high", "low", "close", "volume",
            "close_ts", "quote_vol", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("dt")
        df = df[["open", "high", "low", "close", "volume"]].dropna()
        df.rename(columns={"volume": "fut_vol"}, inplace=True)
        df["fut_vol"] = df["fut_vol"].astype(float) * df["close"].astype(float)
        try:
            rs = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": "4h", "limit": limit},
                timeout=15)
            if rs.status_code == 200:
                raws = rs.json()
                dfs = pd.DataFrame(raws, columns=[
                    "ts","open","high","low","close","volume",
                    "close_ts","quote_vol","trades","taker_buy_base",
                    "taker_buy_quote","ignore"])
                dfs["volume"] = dfs["volume"].astype(float)
                dfs["close"] = dfs["close"].astype(float)
                dfs["spot_vol"] = dfs["volume"] * dfs["close"]
                dfs["dt"] = pd.to_datetime(dfs["ts"], unit="ms", utc=True)
                dfs = dfs.set_index("dt")[["spot_vol"]]
                df = df.join(dfs, how="left")
            else:
                df["spot_vol"] = df["fut_vol"]
        except Exception:
            df["spot_vol"] = df["fut_vol"]
        df["volume"] = df["spot_vol"].fillna(0) + df["fut_vol"].fillna(0)
        return df.reset_index()
    except Exception as e:
        log(f"[get_ohlcv] Ошибка: {e}")
        return None

def tech_score(df):
    """
    Технический скор по 4h свечам.
    df содержит: open, high, low, close, volume (USDT, spot+fut),
                 spot_vol, fut_vol — из get_ohlcv().

    RSIVOL считается трижды:
      1. от spot_vol  (приоритет 1)
      2. от fut_vol   (приоритет 2)
      3. от volume    (spot+fut, fallback)
    Итоговый rsivol = первый ненулевой из трёх.
    wt2 использует volume (USDT) — единый масштаб для всех активов.
    """
    c   = df["close"]
    v   = df["volume"]       # spot + fut, USDT
    h   = df["high"]
    lo  = df["low"]

    # Спотовый и фьючерсный объёмы (USDT)
    v_spot = df["spot_vol"] if "spot_vol" in df.columns else v
    v_fut  = df["fut_vol"]  if "fut_vol"  in df.columns else v

    # ── EMA ──────────────────────────────────────────────────────────
    ema_f = c.ewm(span=EMA_FAST).mean()
    ema_s = c.ewm(span=EMA_SLOW).mean()

    # ── Обычный RSI (для логов) ───────────────────────────────────────
    delta = c.diff()
    gain  = delta.clip(lower=0).rolling(RSI_PERIOD).mean()
    loss  = (-delta.clip(upper=0)).rolling(RSI_PERIOD).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - 100 / (1 + rs)

    # ── RSIVOL helper ─────────────────────────────────────────────────
    RVOL_LEN1 = 14   # EMA период для VWAP
    RVOL_LEN2 = 24   # rolling период для RSI от VWAP
    RVOL_OB   = 70
    RVOL_OS   = 30
    RVOL_TOP  = 83

    def calc_rsivol(vol_series):
        """RSI от VWAP, взвешенного по vol_series (USDT)."""
        vol = vol_series.replace(0, np.nan)
        vwap     = (vol * c).ewm(span=RVOL_LEN1).mean() / vol.ewm(span=RVOL_LEN1).mean()
        rv_delta = vwap.diff()
        rv_gain  = rv_delta.clip(lower=0).rolling(RVOL_LEN2).mean()
        rv_loss  = (-rv_delta.clip(upper=0)).rolling(RVOL_LEN2).mean()
        rv_rs    = rv_gain / rv_loss.replace(0, np.nan)
        return 100 - 100 / (1 + rv_rs)

    # Считаем RSIVOL от трёх источников
    rsivol_spot    = calc_rsivol(v_spot)
    rsivol_fut     = calc_rsivol(v_fut)
    rsivol_combined = calc_rsivol(v)

    # Выбираем приоритетный: спот → фьючерс → combined
    i_last = len(df) - 1
    def pick_rsivol():
        for rv in [rsivol_spot, rsivol_fut, rsivol_combined]:
            val = rv.iloc[i_last]
            if not np.isnan(val):
                return rv, val
        return rsivol_combined, 50.0

    rsivol, rv_val = pick_rsivol()

    # ── wt2 — объём на длину свечи (USDT, единый масштаб) ────────────
    WT_N1   = 30
    WT_N2   = 10
    dbar    = (h - lo).replace(0, np.nan)
    sr_dbar = dbar.ewm(span=WT_N2).mean()
    iss     = v / sr_dbar          # USDT / price_range → нормализовано
    ae_iss  = iss.ewm(span=WT_N1).mean()
    wd_iss  = 0.015 * (iss - ae_iss).abs().ewm(span=WT_N1).mean()
    ci_iss  = (iss - ae_iss) / wd_iss.replace(0, np.nan)
    wt2     = ci_iss.ewm(span=WT_N1 + WT_N2).mean().ewm(span=4).mean()

    # ── MACD ──────────────────────────────────────────────────────────
    macd_fast   = c.ewm(span=12).mean()
    macd_slow_l = c.ewm(span=26).mean()
    macd_line   = macd_fast - macd_slow_l
    signal_line = macd_line.ewm(span=9).mean()
    macd_hist   = macd_line - signal_line

    # ── Bollinger Bands ───────────────────────────────────────────────
    bb_mid = c.rolling(20).mean()
    bb_std = c.rolling(20).std()
    bb_up  = bb_mid + 2 * bb_std
    bb_low = bb_mid - 2 * bb_std

    # ── Ширина шапки RSIVOL (фильтр роста) ───────────────────────────
    above_top = (rsivol > RVOL_TOP).astype(int)
    grp       = (above_top != above_top.shift()).cumsum()
    hat_width = above_top * (above_top.groupby(grp).cumcount() + 1)
    prev_above = above_top.shift(1).fillna(0)
    hat_ends   = hat_width.shift(1)[(above_top == 0) & (prev_above == 1)]
    avg_hat    = hat_ends.mean() if len(hat_ends) > 0 else 5.0
    cur_hat    = hat_width.iloc[i_last]
    wide_hat   = bool(cur_hat > avg_hat and cur_hat > 0)

    i = i_last
    s = 0

    if i >= EMA_SLOW:
        # 1. EMA cross (±2)
        s += 2 if ema_f.iloc[i] > ema_s.iloc[i] else -2

        # 2. RSIVOL (±2) — приоритет: spot → fut → combined
        if rv_val < RVOL_OS:
            s += 2
        elif rv_val > RVOL_OB:
            s -= 2

        # 3. wt2 — аномальный объём на длину свечи (±1)
        wt      = wt2.iloc[i]     if not np.isnan(wt2.iloc[i])     else 0.0
        wt_prev = wt2.iloc[i - 1] if i >= 1 and not np.isnan(wt2.iloc[i - 1]) else 0.0
        if wt > 0 and wt > wt_prev:
            s += 1
        elif wt < 0 and wt < wt_prev:
            s -= 1

        # 4. MACD histogram direction (±1)
        if i >= 1:
            s += 1 if macd_hist.iloc[i] > macd_hist.iloc[i - 1] else -1

        # 5. MACD line vs signal (±1)
        s += 1 if macd_line.iloc[i] > signal_line.iloc[i] else -1

        # 6. Bollinger Bands (±1)
        price = c.iloc[i]
        if i >= 20:
            if price > bb_up.iloc[i]:
                s -= 1
            elif price < bb_low.iloc[i]:
                s += 1

        # 7. Фильтр шапки: широкая шапка RSIVOL → рост запрещён
        if wide_hat and s < 0:
            s = 0

    rsi_val    = round(rsi.iloc[i], 1) if i >= RSI_PERIOD else 50.0
    rsivol_val = round(rv_val, 1)
    return s, rsi_val, rsivol_val

def push_log_to_github():
    try:
        import base64, urllib.request, json as _json, subprocess
        import datetime as _dt
        def push_log_to_github():
    try:
        import base64, urllib.request, json as _json, subprocess
        import datetime as _dt
        GH_TOKEN = os.getenv("GITHUB_TOKEN")  # или "YOUR_GITHUB_TOKEN_HERE"
        GH_REPO  = "bashsolomon-sudo/trading-agent"
        GH_BASE  = "LOGS"
        MAX_BYTES = 20_000_000  # ~20MB — ротация до нового файла

        # Получаем новые строки из journald (последние 50 строк за цикл)
        result = subprocess.run(
            ["journalctl", "-u", "agent", "-n", "50", "--no-pager", "--output=short"],
            capture_output=True, text=True
        )
        new_lines = result.stdout.strip() or "(no logs)"
        now_str = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        def get_file(path):
            api = f"https://api.github.com/repos/{GH_REPO}/contents/{path}"
            req = urllib.request.Request(api,
                headers={"Authorization": f"token {GH_TOKEN}", "User-Agent": "agent"})
            try:
                with urllib.request.urlopen(req) as r:
                    data = _json.loads(r.read())
                    text = base64.b64decode(data["content"]).decode()
                    return text, data["sha"], api
            except:
                return "", None, api

        def push_file(api, content_str, sha, msg):
            content_b64 = base64.b64encode(content_str.encode()).decode()
            payload = {"message": msg, "content": content_b64}
            if sha:
                payload["sha"] = sha
            data = _json.dumps(payload).encode()
            req = urllib.request.Request(api, data=data, method="PUT",
                headers={"Authorization": f"token {GH_TOKEN}",
                         "Content-Type": "application/json", "User-Agent": "agent"})
            with urllib.request.urlopen(req) as r:
                r.read()

        # Определяем текущий файл (LOGS.md, LOGS_2.md, ...)
        index = 1
        while True:
            path = f"{GH_BASE}.md" if index == 1 else f"{GH_BASE}_{index}.md"
            existing, sha, api = get_file(path)
            if len(existing.encode()) < MAX_BYTES:
                break
            index += 1

        # Накапливаем
        bt = chr(96) * 3
        separator = f"\n\n---\n**{now_str}**\n```\n{new_lines}\n```\n"
        updated = existing + separator

        push_file(api, updated, sha, f"logs: {now_str}")
        log(f"[GitHub] Лог добавлен в {path}")
    except Exception as e:
        log(f"[GitHub] Ошибка пуша: {e}")

def run_cycle():
    ekb     = pytz.timezone("Asia/Yekaterinburg")
    now_ekb = datetime.now(ekb)
    if now_ekb.weekday() == 5 and not TRADE_WEEKENDS:
        open_pos = get_open_positions()
        if open_pos:
            log("Суббота — закрываю все позиции...")
            close_all_positions()
        else:
            log("Суббота — позиций нет.")
        return

    now = datetime.now(timezone.utc).strftime("%d.%m %H:%M")
    log("=" * 55)
    log("Цикл: " + now + " UTC")

    balance = get_futures_balance()
    if balance is None:
        balance = 300.0

    open_pos   = get_open_positions()
    open_count = len(open_pos)
    log("Баланс: $" + str(round(balance, 2)) + " | Позиций: " + str(open_count))

    # Чистим висячие план-ордера по символам без открытой позиции
    for sym_check in list(ASSETS.keys()):
        if sym_check not in open_pos:
            cancel_all_plan_orders(ASSETS[sym_check]["symbol"])

    # Глобальные факторы — считаем один раз
    usdt_d_score = get_usdt_dominance_score()
    fg_score     = get_fear_greed_score()
    busy_syms    = set(open_pos.keys())

    for sym, cfg in ASSETS.items():
        if sym in busy_syms:
            log("  " + sym + ": позиция открыта, пропуск")
            continue

        df = get_ohlcv(cfg["cg_id"])
        time.sleep(4)
        if df is None or len(df) < EMA_SLOW + 2:
            log("  " + sym + ": нет данных")
            continue

        t_score, rsi_val, rsivol_val = tech_score(df)
        w_score  = (get_ton_whale_score()
                    if sym == "TONUSDT"
                    else get_whale_score_alert(cfg["whale_sym"]))
        o_score, oi_pct        = get_oi_score_coinalyze(sym)
        c_score               = get_cluster_score(sym)
        cvd, fut_pct, spot_pct = get_cvd_divergence_score(sym)
        vol                   = get_volume_score(sym)
        liq_score             = get_liq_score_coinalyze(sym)
        ls_score              = get_ls_score_coinalyze(sym)
        total    = t_score + w_score + o_score + usdt_d_score + c_score + cvd + vol + fg_score + liq_score + ls_score

        # Считаем количество факторов в сторону сигнала
        all_factors = [t_score, w_score, o_score, usdt_d_score, c_score, cvd, vol, fg_score, liq_score, ls_score]
        if total > 0:
            active_factors = sum(1 for f in all_factors if f > 0)
        elif total < 0:
            active_factors = sum(1 for f in all_factors if f < 0)
        else:
            active_factors = 0

        price = float(df["close"].iloc[-1])

        log(f"  {sym}: tech={t_score} whale={w_score} "
              f"oi={o_score}({oi_pct:+.2f}%) usdt_d={usdt_d_score} cluster={c_score} "
              f"cvd={cvd}(fut={fut_pct:+.1f}%/spot={spot_pct:+.1f}%) vol={vol} fg={fg_score} "
              f"liq={liq_score} ls={ls_score} "
              f"-> score={total} | factors={active_factors}/{MIN_ACTIVE_FACTORS} | RSI={rsi_val} | RSIVOL={rsivol_val} | ${round(price, 4)}")

        # Двойной фильтр: суммарный скор + минимум 4 фактора
        if price <= 0:
            continue
        if abs(total) < THRESHOLD:
            log(f"  {sym}: скор {total} < порога {THRESHOLD} — пропуск")
            continue
        if active_factors < MIN_ACTIVE_FACTORS:
            log(f"  {sym}: только {active_factors} факторов (нужно {MIN_ACTIVE_FACTORS}) — пропуск")
            tg(f"⚠️ {sym}: скор {total} достаточный, но факторов {active_factors}/{MIN_ACTIVE_FACTORS} — сигнал отклонён")
            continue

        direction = "LONG" if total > 0 else "SHORT"

        # ── RSI/RSIVOL-фильтры ──────────────────────────────────────
        if direction == "SHORT" and rsivol_val < 60:
            log(f"  {sym}: RSIVOL={rsivol_val:.1f} < 60 — шорт отклонён (недостаточный объёмный импульс)")
            continue
        if direction == "LONG" and rsivol_val > 40:
            log(f"  {sym}: RSIVOL={rsivol_val:.1f} > 40 — лонг отклонён (объёмный импульс ещё перегрет)")
            continue

        # ── Доп. фильтр: узкий сигнал (мало факторов) ─────────────────────
        if abs(total) >= THRESHOLD and active_factors < 3:
            log(f"  {sym}: узкий сигнал — score={total}, factors={active_factors}/{MIN_ACTIVE_FACTORS}, пропуск")
            tg(f"⚠️ {sym}: узкий сигнал — score={total}, factors={active_factors}/{MIN_ACTIVE_FACTORS}, сделка отклонена")
            continue

        # ── Доп. фильтр: tech-only spike ────────────────────────────────
        if abs(total) <= 2 and abs(t_score) >= 4:
            log(f"  {sym}: tech-only spike — tech={t_score}, total={total}, пропуск")
            tg(f"⚠️ {sym}: tech-only spike — tech={t_score}, total={total}, сделка отклонена")
            continue

        # ── Cooldown 2ч между сделками по одному активу ───────────────
        now_ts = time.time()
        last_ts = last_trade_time.get(sym, 0)
        if now_ts - last_ts < COOLDOWN_SEC:
            remaining = int((COOLDOWN_SEC - (now_ts - last_ts)) / 60)
            log(f"  {sym}: cooldown активен, осталось {remaining} мин — пропуск")
            continue

        size = calc_position_size(sym, balance, price, open_count)
        if size <= 0:
            log("  " + sym + ": размер = 0, пропуск")
            continue

        price_dec = 0 if sym == "BTCUSDT" else 2 if sym == "ETHUSDT" else 4
        if direction == "LONG":
            sl_price      = round(price * (1 - SL_PCT), price_dec)
            tp1_price     = round(price * (1 + (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT)), price_dec)
            trail_pct     = TRAILING_PCT_BTC if sym == "BTCUSDT" else TRAILING_PCT_DEF
            side          = "buy"
            hold_side     = "long"
            trail_trigger = tp1_price  # трейлинг активируется на уровне TP1
        else:
            sl_price      = round(price * (1 + SL_PCT), price_dec)
            tp1_price     = round(price * (1 - (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT)), price_dec)
            trail_pct     = TRAILING_PCT_BTC if sym == "BTCUSDT" else TRAILING_PCT_DEF
            side          = "sell"
            hold_side     = "short"
            trail_trigger = tp1_price  # трейлинг активируется на уровне TP1

        result = place_order(cfg["symbol"], side, size)
        if result.get("code") != "00000":
            err = result.get("msg", str(result))
            log("  Ошибка ордера " + sym + ": " + err)
            tg("Ошибка ордера " + sym + ": " + err)
            continue

        open_count += 1
        last_trade_time[sym] = time.time()  # cooldown старт
        time.sleep(0.5)
        real_positions = get_open_positions()
        real_size = real_positions.get(sym, {}).get("size", size)

        cancel_all_plan_orders(cfg["symbol"])
        time.sleep(0.5)

        r_sl = set_sl_tp(cfg["symbol"], "loss_plan", sl_price,
                         hold_side=hold_side, size=real_size)
        sl_ok = r_sl.get("code") == "00000"
        log("  SL: " + ("OK $" + str(sl_price) if sl_ok else "ERR " + r_sl.get("msg", "")))

        r_tr = set_trailing_stop(cfg["symbol"], hold_side, trail_pct,
                                 trail_trigger, size=real_size)
        tr_ok = r_tr.get("code") == "00000"
        log("  Trailing: " + ("OK " + str(round(trail_pct*100,1)) + "%" if tr_ok else "ERR " + r_tr.get("msg", "")))

        factors_str = (f"tech={t_score} whale={w_score} "
                       f"oi={o_score}({oi_pct:+.2f}%) usdt_d={usdt_d_score} cluster={c_score} "
                       f"cvd={cvd}(fut={fut_pct:+.1f}%/spot={spot_pct:+.1f}%) vol={vol} fg={fg_score} "
                       f"liq={liq_score} ls={ls_score}")
        tg(f"СИГНАЛ {direction} | {sym}\n"
           f"Score: {total} | Факторов: {active_factors}/10\n"
           f"({factors_str})\n"
           f"Вход: ${round(price, 4)}\n"
           f"SL: ${sl_price} {'✅' if sl_ok else '❌'}\n"
           f"TP1(BE): ${tp1_price}\n"
           f"Trailing: {round(trail_pct*100,1)}% {'✅' if tr_ok else '❌'}\n"
           f"Баланс: ${round(balance, 2)}")

    log("Цикл завершён.")
    push_log_to_github()

# ── COMMANDS ──────────────────────────────────────────────────────────

def check_agent_commands():
    if not os.path.exists(AGENT_CMD_FILE):
        return
    try:
        with open(AGENT_CMD_FILE) as f:
            cmd = json.load(f)
        os.remove(AGENT_CMD_FILE)
        command = cmd.get("command")
        symbol  = (cmd.get("symbol") or "").upper()
        reason  = cmd.get("reason", "")
        if not symbol:
            return
        open_pos = get_open_positions()
        if sym_key not in open_pos:
            log("[CMD] " + sym_key + ": нет позиции, пропуск")
            return
        pos   = open_pos[sym_key]
        side  = pos["side"]
        size  = pos["size"]
        cfg   = ASSETS.get(sym_key)
        if not cfg: return
        price = get_ticker(cfg["symbol"])
        if not price: return
        if command == "tighten_sl":
            if side == "long":
                new_sl = round(price * 0.995, 4)
                if new_sl > pos["entry"]: new_sl = round(pos["entry"] * 1.001, 4)
            else:
                new_sl = round(price * 1.005, 4)
                if new_sl < pos["entry"]: new_sl = round(pos["entry"] * 0.999, 4)
            r = set_sl_tp(cfg["symbol"], "loss_plan", new_sl, hold_side=side, size=size)
            if r.get("code") == "00000":
                msg = "SL ужесточён " + sym_key + "\n" + reason + "\nНовый SL: $" + str(new_sl)
                log("[CMD] " + msg)
                tg(msg)
        elif command == "check_signal":
            f = calc_factors(sym_key, cfg)
            if not f: return
            total   = f["total"]
            rsi_val = f["rsi"]
            rsivol  = f["rsivol"]
            price   = f["price"]
            pnl     = pos["unrealizedPL"]
            pnl_str = ("+" if pnl >= 0 else "") + str(round(pnl, 2))
            msg = (f"ТРИГГЕР: {reason}\n"
                   f"{sym_key} | {side.upper()} | PnL: {pnl_str} USDT\n"
                   f"Скор: tech={f['t_score']} whale={f['w_score']} "
                   f"oi={f['o_score']}({f['oi_pct']:+.2f}%) "
                   f"usdt_d={f['usdt_d']} cluster={f['c_score']} "
                   f"cvd={f['cvd']}(fut={f['fut_pct']:+.1f}%/spot={f['spot_pct']:+.1f}%) "
                   f"vol={f['vol']} liq={f['liq']} ls={f['ls']} fg={f['fg']} -> {total}\n"
                   f"RSI: {rsi_val} | RSIVOL: {rsivol} | Цена: ${round(price, 4)}")
            log("[CMD] " + msg)
            tg(msg)
            if side == "long" and total <= -THRESHOLD:
                tg("Закрываю " + sym_key + " LONG — сигнал развернулся (" + str(total) + ")")
                close_position(cfg["symbol"], side, size)
            elif side == "short" and total >= THRESHOLD:
                tg("Закрываю " + sym_key + " SHORT — сигнал развернулся (" + str(total) + ")")
                close_position(cfg["symbol"], side, size)
    except Exception as e:
        log("[CMD] Ошибка: " + str(e))

# ── MONITOR ───────────────────────────────────────────────────────────

def price_monitor():
    global be_set
    be_set = set()
    log("[Monitor] Запущен, интервал: " + str(MONITOR_SEC) + "с")
    while True:
        try:
            open_pos = get_open_positions()
            if not open_pos:
                log("[Monitor] Нет открытых позиций")
            for sym, pos in open_pos.items():
                if sym not in ASSETS: continue
                price = get_ticker(ASSETS[sym]["symbol"])
                if price is None: continue
                log("[Monitor] " + sym +
                      " цена=$" + str(round(price, 4)) +
                      " entry=$" + str(pos["entry"]) +
                      " side=" + pos["side"] +
                      (" BE" if sym in be_set else ""))
                if sym not in be_set:
                    if check_breakeven(sym, pos, price):
                        be_set.add(sym)
        except Exception as e:
            log("[Monitor] Ошибка: " + str(e))
        try:
            check_agent_commands()
        except Exception as e:
            log("[Monitor] CMD ошибка: " + str(e))
        time.sleep(MONITOR_SEC)

# ── SIGNAL LOOP ───────────────────────────────────────────────────────

def signal_loop():
    log("Trading Agent v3.3 | Интервал: " + str(INTERVAL_SEC // 3600) + "ч")
    log(f"Фильтры: score>={THRESHOLD} AND factors>={MIN_ACTIVE_FACTORS}/10")
    while True:
        try:
            ekb     = pytz.timezone("Asia/Yekaterinburg")
            now_ekb = datetime.now(ekb)
            if now_ekb.weekday() == 5 and not TRADE_WEEKENDS:
                run_cycle()
                tomorrow = (now_ekb + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0)
                sleep_sec = int((tomorrow - now_ekb).total_seconds())
                log("Суббота — агент спит " + str(sleep_sec // 3600) + "ч")
                tg("Суббота — агент спит до воскресенья")
                time.sleep(sleep_sec)
                continue
            run_cycle()
        except Exception as e:
            log("Ошибка цикла: " + str(e))
            tg("Ошибка агента: " + str(e))
        time.sleep(INTERVAL_SEC)

# ── TELEGRAM BOT ──────────────────────────────────────────────────────





# ── UNIFIED FACTORS (для сигналов) ────────────────────────────────────

def calc_factors(sym_key, cfg):
    """
    Единый расчёт факторов в стиле боевого run_cycle.
    Возвращает dict или None.
    """
    df = get_ohlcv(cfg["cg_id"])
    if df is None or len(df) < EMA_SLOW + 2:
        return None

    t_score, rsi_val, rsivol_val = tech_score(df)
    usdt_d  = get_usdt_dominance_score()
    fg      = get_fear_greed_score()

    if sym_key == "TONUSDT":
        w_score = get_stable_whale_score()
    else:
        w_score = get_whale_score_alert(cfg["whale_sym"])

    o_score, oi_pct          = get_oi_score_coinalyze(sym_key)
    c_score                  = get_cluster_score(sym_key)
    cvd, fut_pct, spot_pct   = get_cvd_divergence_score(sym_key)
    vol                      = get_volume_score(sym_key)
    liq_score                = get_liq_score_coinalyze(sym_key)
    ls_score                 = get_ls_score_coinalyze(sym_key)

    total = (
        t_score + w_score + o_score + usdt_d + c_score +
        cvd + vol + fg + liq_score + ls_score
    )

    all_factors = [t_score, w_score, o_score, usdt_d, c_score, cvd, vol, fg, liq_score, ls_score]
    if total > 0:
        active = sum(1 for f in all_factors if f > 0)
    elif total < 0:
        active = sum(1 for f in all_factors if f < 0)
    else:
        active = 0

    price = float(df["close"].iloc[-1])

    return {
        "total":    total,
        "active":   active,
        "t_score":  t_score,
        "rsi":      rsi_val,
        "rsivol":   rsivol_val,
        "usdt_d":   usdt_d,
        "fg":       fg,
        "w_score":  w_score,
        "o_score":  o_score,
        "oi_pct":   oi_pct,
        "c_score":  c_score,
        "cvd":      cvd,
        "fut_pct":  fut_pct,
        "spot_pct": spot_pct,
        "vol":      vol,
        "liq":      liq_score,
        "ls":       ls_score,
        "price":    price,
    }


def _do_signal(reply, MAIN_KB, thread_id):
    import pytz
    from datetime import datetime

    ekb = pytz.timezone("Asia/Yekaterinburg")
    now_str = datetime.now(ekb).strftime("%d.%m.%Y %H:%M")

    signal_lines = []
    for sym_key, cfg in ASSETS.items():
        try:
            f = calc_factors(sym_key, cfg)
            if not f:
                continue

            total   = int(f["total"])
            active  = int(f["active"])
            rsi_val = f["rsi"]
            rsivol  = f["rsivol"]
            price   = f["price"]

            if total >= THRESHOLD:
                direction = "\u2b06\ufe0f \u041b\u041e\u041d\u0413"
            elif total <= -THRESHOLD:
                direction = "\u2b07\ufe0f \u0428\u041e\u0420\u0422"
            else:
                direction = "\u26aa\ufe0f \u041d\u0415\u0419\u0422\u0420\u0410\u041b\u042c\u041d\u041e"

            name = sym_key.replace("USDT", "")
            signal_lines.append(
                f"<b>{name}</b> | {direction} | \u0421\u043a\u043e\u0440: {total}\n"
                f"  \u0426\u0435\u043d\u0430: ${round(price, 4)} | RSI: {rsi_val} | RSIVOL: {rsivol}\n"
                f"  tech={f['t_score']} whale={f['w_score']} oi={f['o_score']}({f['oi_pct']:+.2f}%) "
                f"usdt_d={f['usdt_d']} cluster={f['c_score']} "
                f"cvd={f['cvd']}(fut={f['fut_pct']:+.1f}%/spot={f['spot_pct']:+.1f}%) "
                f"vol={f['vol']} liq={f['liq']} ls={f['ls']} fg={f['fg']}"
            )
        except Exception as e:
            signal_lines.append(f"{sym_key}: ошибка {e}")

    if not signal_lines:
        signal_lines.append("Нет данных по сигналам")

    post = (
        "\U0001f4ca <b>\u0421\u0438\u0433\u043d\u0430\u043b | "
        + now_str + " (EKB)</b>\n\n"
        + "\n\n".join(signal_lines)
    )
    reply(post, MAIN_KB, thread_id=thread_id)


def tg_bot_loop():
    global TRADE_WEEKENDS
    offset = 0
    url    = "https://api.telegram.org/bot" + TELEGRAM_TOKEN
    try:
        _r = requests.get(url + "/getUpdates", params={"offset": -1, "timeout": 0}, timeout=5)
        _upds = _r.json().get("result", [])
        if _upds:
            offset = _upds[-1]["update_id"] + 1
            requests.get(url + "/getUpdates", params={"offset": offset, "timeout": 0}, timeout=5)
    except Exception:
        pass
    try:
        requests.post(url + "/setMyCommands", json={"commands": [
            {"command": "status",    "description": "Позиции и PnL"},
            {"command": "close",     "description": "Закрыть все позиции"},
            {"command": "pause",     "description": "Пауза"},
            {"command": "resume",    "description": "Возобновить"},
            {"command": "stop",      "description": "Остановить агент"},
            {"command": "start",     "description": "Запустить агент"},
            {"command": "restart",   "description": "Перезапустить"},
            {"command": "stopclose", "description": "Стоп + закрыть"},
            {"command": "weekend",   "description": "Вкл/выкл выходные"},
            {"command": "breakeven", "description": "SL в безубыток с комиссиями"},
            {"command": "help",      "description": "Список команд"},
            {"command": "signal",    "description": "Сигнал для поста"},
        ]}, timeout=10)
        log("[Bot] Команды зарегистрированы")
    except Exception as e:
        log("[Bot] Ошибка: " + str(e))

    MAIN_KB = {"keyboard": [
        [{"text": "/status"},  {"text": "/close"}],
        [{"text": "/pause"},   {"text": "/resume"}],
        [{"text": "/restart"}, {"text": "/stopclose"}],
        [{"text": "/help"},    {"text": "/stop"}],
        [{"text": "/weekend"}, {"text": "/sl BTC 00000"}],
        [{"text": "/breakeven"}, {"text": "/signal"}],
    ], "resize_keyboard": True, "persistent": True}

    def reply(text, keyboard=None, thread_id=None):
        try:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
            if keyboard:
                payload["reply_markup"] = keyboard
            if thread_id:
                payload["message_thread_id"] = thread_id
            requests.post(url + "/sendMessage", json=payload, timeout=10)
        except Exception:
            pass

    while True:
        try:
            r = requests.get(url + "/getUpdates",
                             params={"offset": offset, "timeout": 30}, timeout=35)
            updates = r.json().get("result", [])
            for upd in updates:
                offset  = upd["update_id"] + 1
                msg     = upd.get("message", {})
                chat_id   = str(msg.get("chat", {}).get("id", ""))
                thread_id = msg.get("message_thread_id")
                text      = msg.get("text", "").strip()
                if text: log("[Bot] Получено:", repr(text), "chat:", chat_id)
                if chat_id != TELEGRAM_CHAT_ID:
                    continue
                if text == "/status":
                    print_status()
                elif text == "/close":
                    reply("Закрываю все позиции...", MAIN_KB, thread_id=thread_id)
                    close_all_positions()
                    reply("Все позиции закрыты", MAIN_KB, thread_id=thread_id)
                elif text == "/stop":
                    reply("Останавливаю...", MAIN_KB, thread_id=thread_id)
                    threading.Timer(1.5, lambda: os.system("systemctl stop agent")).start()
                elif text == "/start":
                    reply("Запускаю...", MAIN_KB, thread_id=thread_id)
                    os.system("systemctl start agent")
                elif text == "/restart":
                    reply("Перезапускаю...", MAIN_KB, thread_id=thread_id)
                    os.system("systemctl restart agent")
                elif text == "/stopclose":
                    reply("Стоп + закрытие...", MAIN_KB, thread_id=thread_id)
                    close_all_positions()
                    reply("Позиции закрыты. Останавливаю агент...", MAIN_KB, thread_id=thread_id)
                    threading.Timer(1.5, lambda: os.system("systemctl stop agent")).start()
                elif text == "/pause":
                    reply("Пауза. Для возобновления: /resume", MAIN_KB, thread_id=thread_id)
                    threading.Timer(1.5, lambda: (os.system("systemctl stop agent"), os.system("systemctl disable agent"))).start()
                elif text == "/resume":
                    reply("Возобновляю...", MAIN_KB, thread_id=thread_id)
                    os.system("systemctl enable agent")
                    os.system("systemctl start agent")
                    reply("Агент запущен", MAIN_KB, thread_id=thread_id)
                elif text == "/weekend":
                    TRADE_WEEKENDS = not TRADE_WEEKENDS
                    status = "ВКЛ" if TRADE_WEEKENDS else "ВЫКЛ"
                    reply("Торговля в выходные: " + status, MAIN_KB, thread_id=thread_id)
                elif text.startswith("/sl "):
                    parts = text.split()
                    if len(parts) == 3:
                        sym = parts[1].upper()
                        if not sym.endswith("USDT"): sym += "USDT"
                        try:
                            new_sl   = float(parts[2])
                            open_pos = get_open_positions()
                            if sym in open_pos:
                                pos       = open_pos[sym]
                                hold_side = pos["side"]
                                pos_size  = pos["size"]
                                r2 = set_sl_tp(sym, "loss_plan", new_sl,
                                               hold_side=hold_side, size=pos_size)
                                if r2.get("code") == "00000":
                                    reply("SL по " + sym + " -> " + str(new_sl), MAIN_KB, thread_id=thread_id)
                                else:
                                    reply("Ошибка: " + r2.get("msg", str(r2)), MAIN_KB, thread_id=thread_id)
                            else:
                                reply("Позиция " + sym + " не найдена", MAIN_KB, thread_id=thread_id)
                        except ValueError:
                            reply("Формат: /sl BTCUSDT 83000", MAIN_KB, thread_id=thread_id)
                    else:
                        reply("Формат: /sl BTCUSDT 83000", MAIN_KB, thread_id=thread_id)

                elif text == "/breakeven":
                    open_pos = get_open_positions()
                    log("[BE] open_pos:", open_pos)
                    if not open_pos:
                        reply("Нет открытых позиций", MAIN_KB, thread_id=thread_id)
                    else:
                        results = []
                        for sym, pos in open_pos.items():
                            entry     = float(pos["entry"])
                            side      = pos["side"]
                            size      = pos["size"]
                            # комиссия входа (тейкер) + выхода (тейкер) = 0.2%
                            fee = 0.002
                            if side == "short":
                                be_price = round(entry * (1 - fee), 6)
                            else:
                                be_price = round(entry * (1 + fee), 6)
                            log("[BE] Вызов set_sl_tp:", sym, be_price, side, size)
                            r2 = set_sl_tp(sym, "loss_plan", be_price,
                                           hold_side=side, size=size)
                            log("[BE] Результат:", r2)
                            if r2.get("code") == "00000":
                                results.append(sym + " SL -> " + str(be_price) + " ✅")
                            else:
                                results.append(sym + " ошибка: " + r2.get("msg", str(r2)) + " ❌")
                        reply("<b>Безубыток:</b>\n" + "\n".join(results), MAIN_KB, thread_id=thread_id)
                elif text == "/signal":
                    threading.Thread(target=_do_signal, args=(reply, MAIN_KB, thread_id), daemon=True).start()
                elif text == "/help":
                    reply(
                        "<b>Команды:</b>\n"
                        "/status — позиции и PnL\n"
                        "/close — закрыть все\n"
                        "/stop — остановить агент\n"
                        "/start — запустить агент\n"
                        "/restart — перезапустить\n"
                        "/stopclose — стоп + закрыть\n"
                        "/pause — пауза\n"
                        "/resume — возобновить\n"
                        "/weekend — вкл/выкл выходные\n"
                        "/breakeven — SL в безубыток\n"
                        "/signal — сигнал для поста\n"
                        "/sl SYM PRICE — перенести SL",
                        MAIN_KB, thread_id=thread_id)
        except Exception as e:
            log("[Bot] EXCEPTION:", e)
            # exponential backoff: 5s, 10s, 20s, 40s, max 60s
            _bot_retry = getattr(tg_bot_loop, '_retry_count', 0) + 1
            tg_bot_loop._retry_count = _bot_retry
            _sleep = min(5 * (2 ** (_bot_retry - 1)), 60)
            log(f"[Bot] retry #{_bot_retry}, sleep {_sleep}s")
            time.sleep(_sleep)
        else:
            tg_bot_loop._retry_count = 0

# ── ENTRY POINT ───────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "status":
        print_status()
    elif cmd == "close":
        close_all_positions()
    elif cmd == "stop":
        os.system("systemctl stop agent")
        log("Агент остановлен.")
    elif cmd == "start":
        os.system("systemctl start agent")
        log("Агент запущен.")
    elif cmd == "restart":
        os.system("systemctl restart agent")
        log("Агент перезапущен.")
    elif cmd == "stopclose":
        os.system("systemctl stop agent")
        time.sleep(2)
        close_all_positions()
    else:
        # Восстановление план-ордеров при старте
        log("[Init] Проверка и восстановление план-ордеров...")
        open_pos = get_open_positions()
        for sym, pos in open_pos.items():
            cfg = ASSETS.get(sym)
            if not cfg: continue
            side  = pos["side"]
            entry = pos["entry"]
            size  = pos["size"]
            lev   = int(pos["leverage"])
            sl_pct   = SL_PCT
            tp1_pct  = TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT
            trail_pct = TRAILING_PCT_BTC if sym == "BTCUSDT" else TRAILING_PCT_DEF
            cur_price = get_ticker(cfg["symbol"])
            if side == "short":
                sl_price      = round(entry * (1 + sl_pct),   price_scale(cfg["symbol"]))
                tp1_price     = round(entry * (1 - tp1_pct),  price_scale(cfg["symbol"]))
                # если TP1 уже пройден — активируем трейлинг от текущей цены
                trail_trigger = tp1_price if (cur_price is None or cur_price > tp1_price) else round(cur_price * 0.999, price_scale(cfg["symbol"]))
            else:
                sl_price      = round(entry * (1 - sl_pct),   price_scale(cfg["symbol"]))
                tp1_price     = round(entry * (1 + tp1_pct),  price_scale(cfg["symbol"]))
                trail_trigger = tp1_price if (cur_price is None or cur_price < tp1_price) else round(cur_price * 1.001, price_scale(cfg["symbol"]))
            # Проверяем BE: если цена уже прошла BE уровень — ставим BE SL
            be_trigger_pct = (TP1_PCT_BTC if sym == "BTCUSDT" else TP1_PCT) * BE_TRIGGER
            if side == "short" and cur_price is not None and cur_price <= entry * (1 - be_trigger_pct):
                sl_price = round(entry * (1 - ROUND_TRIP_FEE), price_scale(cfg["symbol"]))
                log(f"  {sym}: BE уровень пройден, SL -> BE ${sl_price}")
            elif side == "long" and cur_price is not None and cur_price >= entry * (1 + be_trigger_pct):
                sl_price = round(entry * (1 + ROUND_TRIP_FEE), price_scale(cfg["symbol"]))
                log(f"  {sym}: BE уровень пройден, SL -> BE ${sl_price}")
            cancel_all_plan_orders(cfg["symbol"])
            r_sl = set_sl_tp(cfg["symbol"], "loss_plan", sl_price, hold_side=side, size=size)
            r_tr = set_trailing_stop(cfg["symbol"], side, trail_pct, trail_trigger, size=size)
            sl_ok = r_sl.get("code") == "00000"
            tr_ok = r_tr.get("code") == "00000"
            log(f"  {sym}: SL={'OK' if sl_ok else 'ERR'} ${sl_price} | Trail={'OK' if tr_ok else 'ERR'} {round(trail_pct*100,1)}%")
        bot_thread     = threading.Thread(target=tg_bot_loop,   daemon=True, name="TgBot")
        monitor_thread = threading.Thread(target=price_monitor, daemon=True, name="Monitor")
        bot_thread.start()
        monitor_thread.start()
        signal_loop()

# ── UNIFIED FACTORS (для сигналов) ────────────────────────────────────





