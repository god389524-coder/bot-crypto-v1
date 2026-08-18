
import telebot
import requests
import time
import json
import os
import math
import sqlite3
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


# ============================================================
#  CRYPTO SPOT MOMENTUM BOT — SINGLE FILE EDITION
# ============================================================
#
# Tujuan:
# - Scan Top 100 crypto berdasarkan market cap
# - Fokus Binance Spot USDT
# - Cari Support Bounce + Breakout/Retest
# - Konfirmasi multi-timeframe 15m / 1H / 4H
# - RSI / EMA / MACD / ATR / ADX / Volume
# - Market regime BTC 4H
# - Support/resistance berbasis pivot + clustering
# - Risk / reward
# - Entry sekarang atau tunggu candle berikutnya
# - Stop loss / TP1 / TP2
# - Alert Telegram otomatis
# - Paper tracking otomatis
# - SQLite journal
#
# TIDAK ADA:
# - Futures
# - Leverage
# - Short
# - Auto buy / auto sell
#
# CATATAN:
# Tidak ada sistem yang menjamin profit. Bot ini sengaja menolak
# banyak setup agar tidak memaksakan sinyal.
# ============================================================


# ============================================================
# TOKEN
# ============================================================

# TOKEN YANG KAMU BERIKAN.
# Karena token Telegram sudah terekspos di percakapan, sebaiknya
# setelah bot berhasil diuji kamu ROTATE token ini via BotFather.
TOKEN = "8779539825:AAE2l-6ZyuaVE_yDFTkgqb7p00MJQAUesr0"

bot = telebot.TeleBot(TOKEN)


# ============================================================
# CONFIG
# ============================================================

COINGECKO_API_KEY = ""

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_24H_TICKER = "https://api.binance.com/api/v3/ticker/24hr"

TIMEOUT = 15

# Scanner bekerja setiap 2 menit.
SCAN_INTERVAL = 120

# 100 coin top market cap.
TOP_N = 100

# Sinyal hanya dikirim jika score minimal.
MIN_SCORE = 82

# Minimum reward/risk ke TP1.
MIN_RR = 1.8

# Minimum volume ratio.
MIN_VOLUME_RATIO = 1.15

# Cooldown satu coin/setup.
ALERT_COOLDOWN = 4 * 60 * 60

# Batasi jumlah worker agar tidak terlalu agresif ke API.
MAX_WORKERS_FAST = 8
MAX_WORKERS_DEEP = 5

# Minimum quote volume 24H di Binance agar lebih likuid.
MIN_QUOTE_VOLUME_USDT = 2_000_000

# Database lokal.
DB_FILE = "crypto_momentum.db"

# File tambahan untuk state ringan.
STATE_FILE = "bot_state.json"

# ============================================================
# FILTER COIN
# ============================================================

ABAIKAN = {
    "usdt",
    "usdc",
    "usde",
    "dai",
    "fdusd",
    "tusd",
    "usdd",
    "wbtc",
    "steth",
    "wsteth",
    "weeth",
    "wrapped-steth",
}

# ============================================================
# STATE DEFAULT
# ============================================================

state = {
    "chat_ids": [],
    "subscribed": True,
    "alerts": {},
    "active_trades": {},
}

state_lock = threading.Lock()
db_lock = threading.Lock()

# Symbol Binance Spot
BINANCE_SYMBOLS = set()

# Ticker 24H cache
BINANCE_TICKER_CACHE = {}

# Kline cache:
# {(symbol, interval, limit): (timestamp, raw_data)}
KLINE_CACHE = {}
KLINE_CACHE_TTL = {
    "15m": 45,
    "1h": 120,
    "4h": 300,
    "1d": 600,
}

# Session HTTP
SESSION = requests.Session()

# API limiter sederhana
API_LOCK = threading.Lock()
LAST_API_TIME = 0.0
MIN_API_GAP = 0.08


# ============================================================
# STATE
# ============================================================

def load_state():
    global state

    if not os.path.exists(STATE_FILE):
        return

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        if isinstance(loaded, dict):
            state.update(loaded)

        if not isinstance(state.get("chat_ids"), list):
            state["chat_ids"] = []

        if not isinstance(state.get("alerts"), dict):
            state["alerts"] = {}

        if not isinstance(state.get("active_trades"), dict):
            state["active_trades"] = {}

    except Exception as e:
        print("Gagal load state:", e)


def save_state():
    try:
        tmp = STATE_FILE + ".tmp"

        with state_lock:
            snapshot = json.loads(json.dumps(state))

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)

        os.replace(tmp, STATE_FILE)

    except Exception as e:
        print("Gagal save state:", e)


load_state()


# ============================================================
# DATABASE
# ============================================================

def init_database():
    with db_lock:
        conn = sqlite3.connect(DB_FILE)

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                setup TEXT NOT NULL,
                score REAL NOT NULL,
                candle_time TEXT NOT NULL,
                entry REAL NOT NULL,
                stop REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                result TEXT DEFAULT 'OPEN'
            );

            CREATE INDEX IF NOT EXISTS idx_alert_symbol_candle
            ON alerts(symbol, candle_time);

            CREATE INDEX IF NOT EXISTS idx_alert_created
            ON alerts(created_at);

            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER,
                symbol TEXT NOT NULL,
                entry REAL NOT NULL,
                stop REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT DEFAULT 'OPEN',
                exit_price REAL,
                result_pct REAL,
                closed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_paper_status
            ON paper_trades(status);
            """
        )

        conn.commit()
        conn.close()


init_database()


# ============================================================
# HTTP HELPERS
# ============================================================

def api_wait():
    global LAST_API_TIME

    with API_LOCK:
        now = time.time()
        diff = now - LAST_API_TIME

        if diff < MIN_API_GAP:
            time.sleep(MIN_API_GAP - diff)

        LAST_API_TIME = time.time()


def get_json(url, params=None, headers=None, retries=4):
    last_error = None

    for attempt in range(retries):
        try:
            api_wait()

            response = SESSION.get(
                url,
                params=params,
                headers=headers,
                timeout=TIMEOUT,
            )

            if response.status_code == 429:
                wait = min(10, 2 ** attempt)
                print(f"Rate limit API. Tunggu {wait}s...")
                time.sleep(wait)
                continue

            if response.status_code in (418, 403):
                print(
                    f"API menolak request: HTTP {response.status_code}"
                )
                return None

            if response.status_code != 200:
                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{response.text[:150]}"
                )
                time.sleep(0.8 * (attempt + 1))
                continue

            return response.json()

        except Exception as e:
            last_error = str(e)
            time.sleep(0.8 * (attempt + 1))

    print("API error:", last_error)
    return None


# ============================================================
# FORMAT
# ============================================================

def fmt_price(price):
    if price is None:
        return "-"

    if price >= 1000:
        return f"${price:,.2f}"

    if price >= 1:
        return f"${price:,.4f}"

    if price >= 0.01:
        return f"${price:.5f}"

    if price >= 0.0001:
        return f"${price:.7f}"

    return f"${price:.10f}"


def fmt_percent(value):
    if value is None:
        return "-"

    return f"{value:+.2f}%"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default=None):
    try:
        number = float(value)

        if not math.isfinite(number):
            return default

        return number

    except Exception:
        return default


# ============================================================
# TOP 100 COIN
# ============================================================

def get_top_100():
    headers = {}

    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h",
    }

    data = get_json(
        COINGECKO_URL,
        params,
        headers,
    )

    if not data:
        return []

    result = []

    for coin in data:
        symbol = str(
            coin.get("symbol", "")
        ).lower()

        if not symbol:
            continue

        if symbol in ABAIKAN:
            continue

        result.append(
            {
                "id": coin.get("id"),
                "name": coin.get("name") or symbol.upper(),
                "symbol": symbol.upper(),
                "market_cap_rank": coin.get(
                    "market_cap_rank"
                ),
                "market_cap": safe_float(
                    coin.get("market_cap"),
                    0,
                ),
                "volume": safe_float(
                    coin.get("total_volume"),
                    0,
                ),
                "change_1h": safe_float(
                    coin.get(
                        "price_change_percentage_1h_in_currency"
                    ),
                    0,
                ),
                "change_24h": safe_float(
                    coin.get(
                        "price_change_percentage_24h"
                    ),
                    0,
                ),
            }
        )

    return result


# ============================================================
# BINANCE SYMBOLS
# ============================================================

def refresh_binance_symbols():
    global BINANCE_SYMBOLS

    data = get_json(
        BINANCE_EXCHANGE_INFO,
        {
            "permissions": "SPOT",
        },
    )

    if not data:
        return False

    symbols = set()

    for item in data.get("symbols", []):
        try:
            if (
                item.get("status") == "TRADING"
                and item.get("quoteAsset") == "USDT"
                and item.get("isSpotTradingAllowed") is True
            ):
                symbols.add(
                    item.get("symbol")
                )
        except Exception:
            continue

    BINANCE_SYMBOLS = symbols

    print(
        f"✅ Binance Spot symbols: "
        f"{len(BINANCE_SYMBOLS)}"
    )

    return True


# ============================================================
# BINANCE 24H TICKERS
# ============================================================

def refresh_24h_tickers():
    global BINANCE_TICKER_CACHE

    data = get_json(
        BINANCE_24H_TICKER
    )

    if not data or not isinstance(data, list):
        return False

    cache = {}

    for item in data:
        symbol = item.get("symbol")

        if not symbol:
            continue

        if not symbol.endswith("USDT"):
            continue

        quote_volume = safe_float(
            item.get("quoteVolume"),
            0,
        )

        if quote_volume is None:
            quote_volume = 0

        cache[symbol] = {
            "price": safe_float(
                item.get("lastPrice"),
                0,
            ),
            "quote_volume": quote_volume,
            "change_24h": safe_float(
                item.get("priceChangePercent"),
                0,
            ),
            "high": safe_float(
                item.get("highPrice"),
                0,
            ),
            "low": safe_float(
                item.get("lowPrice"),
                0,
            ),
        }

    BINANCE_TICKER_CACHE = cache

    print(
        f"✅ 24H ticker cache: "
        f"{len(cache)} pair"
    )

    return True


# ============================================================
# KLINES
# ============================================================

def get_klines(
    symbol,
    interval,
    limit=200,
):
    key = (
        symbol,
        interval,
        limit,
    )

    ttl = KLINE_CACHE_TTL.get(
        interval,
        60,
    )

    cached = KLINE_CACHE.get(key)

    if cached:
        saved_time, data = cached

        if time.time() - saved_time < ttl:
            return data

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    data = get_json(
        BINANCE_KLINES_URL,
        params,
    )

    if data:
        KLINE_CACHE[key] = (
            time.time(),
            data,
        )

    return data


def candle_arrays(data):
    if not data or len(data) < 5:
        return None

    # Buang candle terakhir karena biasanya masih berjalan.
    candles = data[:-1]

    if len(candles) < 5:
        return None

    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    times = []

    for candle in candles:
        try:
            times.append(candle[0])
            opens.append(float(candle[1]))
            highs.append(float(candle[2]))
            lows.append(float(candle[3]))
            closes.append(float(candle[4]))
            volumes.append(float(candle[5]))
        except Exception:
            return None

    return {
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
        "time": times,
    }


# ============================================================
# INDICATORS
# ============================================================

def calculate_ema(values, period):
    if not values or len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    value = sum(
        values[:period]
    ) / period

    for price in values[period:]:
        value = (
            (price - value)
            * multiplier
            + value
        )

    return value


def calculate_rsi(values, period=14):
    if not values or len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = (
            values[i]
            - values[i - 1]
        )

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(
                abs(change)
            )

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(
        period,
        len(gains),
    ):
        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


def calculate_atr(
    highs,
    lows,
    closes,
    period=14,
):
    if (
        not highs
        or len(closes) <= period
    ):
        return None

    true_ranges = []

    for i in range(
        1,
        len(closes),
    ):
        tr = max(
            highs[i] - lows[i],
            abs(
                highs[i]
                - closes[i - 1]
            ),
            abs(
                lows[i]
                - closes[i - 1]
            ),
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return (
        sum(
            true_ranges[-period:]
        )
        / period
    )


def calculate_macd(values):
    if len(values) < 35:
        return None, None, None

    # Build a MACD history so signal line is meaningful.
    history = []

    for i in range(
        26,
        len(values) + 1,
    ):
        e12 = calculate_ema(
            values[:i],
            12,
        )
        e26 = calculate_ema(
            values[:i],
            26,
        )

        if e12 is None or e26 is None:
            continue

        history.append(
            e12 - e26
        )

    if len(history) < 9:
        return None, None, None

    signal = calculate_ema(
        history,
        9,
    )

    line = history[-1]

    if signal is None:
        return line, None, None

    return (
        line,
        signal,
        line - signal,
    )


def calculate_adx(
    highs,
    lows,
    closes,
    period=14,
):
    if len(closes) < period * 2 + 2:
        return None

    tr_values = []
    plus_dm = []
    minus_dm = []

    for i in range(1, len(closes)):
        up = (
            highs[i]
            - highs[i - 1]
        )

        down = (
            lows[i - 1]
            - lows[i]
        )

        tr = max(
            highs[i] - lows[i],
            abs(
                highs[i]
                - closes[i - 1]
            ),
            abs(
                lows[i]
                - closes[i - 1]
            ),
        )

        tr_values.append(tr)
        plus_dm.append(
            up if up > down and up > 0
            else 0
        )
        minus_dm.append(
            down if down > up and down > 0
            else 0
        )

    if len(tr_values) < period * 2:
        return None

    atr_value = (
        sum(tr_values[:period])
        / period
    )

    plus_value = (
        sum(plus_dm[:period])
        / period
    )

    minus_value = (
        sum(minus_dm[:period])
        / period
    )

    dx_history = []

    for i in range(
        period,
        len(tr_values),
    ):
        atr_value = (
            (
                atr_value
                * (period - 1)
            )
            + tr_values[i]
        ) / period

        plus_value = (
            (
                plus_value
                * (period - 1)
            )
            + plus_dm[i]
        ) / period

        minus_value = (
            (
                minus_value
                * (period - 1)
            )
            + minus_dm[i]
        ) / period

        if atr_value <= 0:
            continue

        plus_di = (
            100
            * plus_value
            / atr_value
        )

        minus_di = (
            100
            * minus_value
            / atr_value
        )

        denominator = (
            plus_di
            + minus_di
        )

        if denominator <= 0:
            continue

        dx = (
            abs(
                plus_di
                - minus_di
            )
            / denominator
            * 100
        )

        dx_history.append(dx)

    if len(dx_history) < period:
        return None

    adx = (
        sum(
            dx_history[:period]
        )
        / period
    )

    for dx in dx_history[period:]:
        adx = (
            (
                adx
                * (period - 1)
            )
            + dx
        ) / period

    return adx


# ============================================================
# CANDLE FEATURES
# ============================================================

def candle_features(data):
    if not data:
        return None

    o = data["open"][-1]
    h = data["high"][-1]
    l = data["low"][-1]
    c = data["close"][-1]

    previous_o = data["open"][-2]
    previous_c = data["close"][-2]

    candle_range = h - l

    if candle_range <= 0:
        return {
            "range": 0,
            "body": 0,
            "upper_wick": 0,
            "lower_wick": 0,
            "bullish": False,
            "bearish": False,
            "strong_close": False,
            "hammer": False,
            "bullish_engulfing": False,
        }

    body = abs(
        c - o
    )

    upper_wick = (
        h - max(o, c)
    )

    lower_wick = (
        min(o, c) - l
    )

    strong_close = (
        (c - l)
        / candle_range
        >= 0.70
    )

    hammer = (
        lower_wick >= body * 1.4
        and upper_wick
        <= candle_range * 0.35
        and lower_wick
        / candle_range
        >= 0.45
    )

    bullish_engulfing = (
        c > o
        and previous_c < previous_o
        and c >= previous_o
        and o <= previous_c
    )

    return {
        "range": candle_range,
        "body": body,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "bullish": c > o,
        "bearish": c < o,
        "strong_close": strong_close,
        "hammer": hammer,
        "bullish_engulfing": bullish_engulfing,
    }


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(
    symbol,
    interval,
    limit=220,
):
    data = get_klines(
        symbol,
        interval,
        limit,
    )

    arrays = candle_arrays(
        data
    )

    if not arrays:
        return None

    closes = arrays["close"]
    highs = arrays["high"]
    lows = arrays["low"]
    volumes = arrays["volume"]

    if len(closes) < 50:
        return None

    current = closes[-1]

    ema20 = calculate_ema(
        closes,
        20,
    )

    ema50 = calculate_ema(
        closes,
        50,
    )

    ema200 = calculate_ema(
        closes,
        200,
    )

    rsi = calculate_rsi(
        closes,
        14,
    )

    atr = calculate_atr(
        highs,
        lows,
        closes,
        14,
    )

    macd_line, macd_signal, macd_hist = (
        calculate_macd(
            closes
        )
    )

    adx = calculate_adx(
        highs,
        lows,
        closes,
        14,
    )

    average_volume = (
        sum(
            volumes[-21:-1]
        )
        / 20
        if len(volumes) >= 21
        else 0
    )

    volume_ratio = (
        volumes[-1]
        / average_volume
        if average_volume > 0
        else 0
    )

    features = candle_features(
        arrays
    )

    return {
        "symbol": symbol,
        "interval": interval,
        "open": arrays["open"][-1],
        "high": arrays["high"][-1],
        "low": arrays["low"][-1],
        "close": current,
        "previous_close": closes[-2],
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "atr": atr,
        "macd": macd_line,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "adx": adx,
        "volume": volumes[-1],
        "volume_ratio": volume_ratio,
        "features": features,
        "candle_time": arrays["time"][-1],
        "closes": closes,
        "highs": highs,
        "lows": lows,
    }


# ============================================================
# PIVOT LEVELS
# ============================================================

def pivot_lows(
    lows,
    left=3,
    right=3,
):
    result = []

    if len(lows) < (
        left + right + 1
    ):
        return result

    for i in range(
        left,
        len(lows) - right,
    ):
        center = lows[i]

        left_min = min(
            lows[
                i - left : i
            ]
        )

        right_min = min(
            lows[
                i + 1 : i + right + 1
            ]
        )

        if (
            center <= left_min
            and center <= right_min
        ):
            result.append(
                (
                    i,
                    center,
                )
            )

    return result


def pivot_highs(
    highs,
    left=3,
    right=3,
):
    result = []

    if len(highs) < (
        left + right + 1
    ):
        return result

    for i in range(
        left,
        len(highs) - right,
    ):
        center = highs[i]

        left_max = max(
            highs[
                i - left : i
            ]
        )

        right_max = max(
            highs[
                i + 1 : i + right + 1
            ]
        )

        if (
            center >= left_max
            and center >= right_max
        ):
            result.append(
                (
                    i,
                    center,
                )
            )

    return result


def cluster_levels(
    values,
    tolerance_pct=0.45,
    min_hits=2,
):
    if not values:
        return []

    values = sorted(
        float(v)
        for v in values
    )

    clusters = [
        [
            values[0]
        ]
    ]

    for value in values[1:]:
        current = clusters[-1]

        center = (
            sum(current)
            / len(current)
        )

        difference = (
            abs(value - center)
            / center
            * 100
        )

        if (
            difference
            <= tolerance_pct
        ):
            current.append(
                value
            )
        else:
            clusters.append(
                [value]
            )

    result = []

    for cluster in clusters:
        if len(cluster) < min_hits:
            continue

        result.append(
            {
                "level": (
                    sum(cluster)
                    / len(cluster)
                ),
                "hits": len(cluster),
                "low": min(cluster),
                "high": max(cluster),
            }
        )

    return result


def support_levels(analysis):
    lows = analysis["lows"]

    pivots = pivot_lows(
        lows,
        3,
        3,
    )

    values = [
        value
        for _, value in pivots
    ]

    return cluster_levels(
        values,
        tolerance_pct=0.60,
        min_hits=2,
    )


def resistance_levels(analysis):
    highs = analysis["highs"]

    pivots = pivot_highs(
        highs,
        3,
        3,
    )

    values = [
        value
        for _, value in pivots
    ]

    return cluster_levels(
        values,
        tolerance_pct=0.60,
        min_hits=2,
    )


def nearest_support(
    levels,
    price,
):
    candidates = [
        x
        for x in levels
        if x["level"] < price
    ]

    if not candidates:
        return None

    # Semakin dekat + semakin banyak hit = lebih menarik.
    return max(
        candidates,
        key=lambda x: (
            x["level"],
            x["hits"],
        ),
    )


def nearest_resistance(
    levels,
    price,
):
    candidates = [
        x
        for x in levels
        if x["level"] > price
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda x: (
            x["level"],
            -x["hits"],
        ),
    )


# ============================================================
# MARKET REGIME BTC
# ============================================================

def get_btc_regime():
    btc_4h = analyze_timeframe(
        "BTCUSDT",
        "4h",
        220,
    )

    if not btc_4h:
        return {
            "name": "UNKNOWN",
            "score": 0,
            "reasons": [],
        }

    close = btc_4h["close"]
    ema20 = btc_4h["ema20"]
    ema50 = btc_4h["ema50"]
    ema200 = btc_4h["ema200"]
    rsi = btc_4h["rsi"]
    adx = btc_4h["adx"]

    score = 0
    reasons = []

    if (
        ema20
        and ema50
        and close > ema20 > ema50
    ):
        score += 40
        reasons.append(
            "BTC 4H trend bullish"
        )

    elif (
        ema20
        and ema50
        and close > ema20
    ):
        score += 18
        reasons.append(
            "BTC 4H di atas EMA20"
        )

    elif (
        ema20
        and ema50
        and close < ema20 < ema50
    ):
        score -= 40
        reasons.append(
            "BTC 4H trend bearish"
        )

    elif close < ema20:
        score -= 20
        reasons.append(
            "BTC 4H di bawah EMA20"
        )

    if ema200:
        if close > ema200:
            score += 15
            reasons.append(
                "BTC di atas EMA200"
            )
        else:
            score -= 15
            reasons.append(
                "BTC di bawah EMA200"
            )

    if rsi is not None:
        if 50 <= rsi <= 68:
            score += 15
        elif rsi < 42:
            score -= 12
        elif rsi > 75:
            score -= 8

    if adx is not None:
        if adx >= 25:
            score += 10
        elif adx < 15:
            score -= 5

    if score >= 45:
        name = "BULLISH"

    elif score <= -40:
        name = "BEARISH"

    else:
        name = "NETRAL"

    return {
        "name": name,
        "score": score,
        "rsi": rsi,
        "adx": adx,
        "reasons": reasons,
    }


# ============================================================
# TECHNICAL SCORE ENGINE
# ============================================================

def build_support_bounce_signal(
    coin,
    a15,
    a1h,
    a4h,
    btc_regime,
):
    """
    Membuat sinyal hanya bila kondisi cukup ketat.

    Maksimum score secara konseptual 100.
    Score bukan probabilitas.
    """

    if not a15 or not a1h or not a4h:
        return None

    if not a15["atr"]:
        return None

    price = a15["close"]
    support_info = nearest_support(
        support_levels(a15),
        price,
    )

    resistance_info = nearest_resistance(
        resistance_levels(a15),
        price,
    )

    if not support_info:
        return None

    if not resistance_info:
        return None

    support = support_info["level"]
    resistance = resistance_info["level"]

    # --------------------------------------------------------
    # SUPPORT INTERACTION
    # --------------------------------------------------------

    atr = a15["atr"]

    zone_size = max(
        atr * 0.70,
        price * 0.0025,
    )

    zone_low = (
        support
        - zone_size * 1.25
    )

    zone_high = (
        support
        + zone_size
    )

    candle_low = a15["low"]

    touched = (
        zone_low
        <= candle_low
        <= zone_high
    )

    # Reclaim harus jelas.
    reclaim = (
        a15["close"]
        > support
    )

    if not touched or not reclaim:
        return None

    features = a15["features"]

    if not features:
        return None

    rejection = (
        features["lower_wick"]
        >= max(
            features["body"] * 1.25,
            features["range"] * 0.20,
        )
    )

    bullish = (
        features["bullish"]
    )

    strong_close = (
        features["strong_close"]
    )

    hammer = (
        features["hammer"]
    )

    engulfing = (
        features["bullish_engulfing"]
    )

    # --------------------------------------------------------
    # TREND
    # --------------------------------------------------------

    trend_1h = False
    trend_4h = False

    if (
        a1h["ema20"]
        and a1h["ema50"]
        and a1h["close"]
        > a1h["ema20"]
        > a1h["ema50"]
    ):
        trend_1h = True

    if (
        a4h["ema20"]
        and a4h["ema50"]
        and a4h["close"]
        > a4h["ema20"]
        > a4h["ema50"]
    ):
        trend_4h = True

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score = 0.0
    reasons = []
    penalties = []

    score += 15
    reasons.append(
        "Harga menyentuh zona support"
    )

    if support_info["hits"] >= 3:
        score += 5
        reasons.append(
            f"Support punya {support_info['hits']} reaksi"
        )

    if reclaim:
        score += 12
        reasons.append(
            "Harga reclaim support"
        )

    if rejection:
        score += 10
        reasons.append(
            "Lower-wick rejection kuat"
        )

    if bullish:
        score += 5
        reasons.append(
            "Candle bullish"
        )

    if strong_close:
        score += 5
        reasons.append(
            "Close berada di area atas candle"
        )

    if hammer:
        score += 4
        reasons.append(
            "Struktur hammer/rejection"
        )

    if engulfing:
        score += 5
        reasons.append(
            "Bullish engulfing"
        )

    volume_ratio = (
        a15["volume_ratio"]
    )

    if volume_ratio >= 2.0:
        score += 10
        reasons.append(
            f"Volume spike {volume_ratio:.2f}x"
        )

    elif volume_ratio >= 1.5:
        score += 7
        reasons.append(
            f"Volume kuat {volume_ratio:.2f}x"
        )

    elif volume_ratio >= MIN_VOLUME_RATIO:
        score += 4
        reasons.append(
            f"Volume {volume_ratio:.2f}x"
        )

    else:
        penalties.append(
            "Volume belum cukup kuat"
        )

    rsi15 = a15["rsi"]

    if rsi15 is not None:
        if 48 <= rsi15 <= 68:
            score += 7
            reasons.append(
                f"RSI 15m sehat {rsi15:.1f}"
            )

        elif 42 <= rsi15 < 48:
            score += 2
            reasons.append(
                f"RSI 15m mulai pulih {rsi15:.1f}"
            )

        elif rsi15 > 74:
            penalties.append(
                "RSI 15m terlalu panas"
            )

        elif rsi15 < 32:
            penalties.append(
                "RSI sangat lemah"
            )

    # MACD
    macd_hist = a15["macd_hist"]

    if macd_hist is not None:
        if macd_hist > 0:
            score += 5
            reasons.append(
                "MACD histogram bullish"
            )
        else:
            penalties.append(
                "MACD histogram bearish"
            )

    # ADX 1H
    if a1h["adx"] is not None:
        if a1h["adx"] >= 25:
            score += 5
            reasons.append(
                f"ADX 1H kuat {a1h['adx']:.1f}"
            )
        elif a1h["adx"] < 15:
            penalties.append(
                "ADX 1H lemah/choppy"
            )

    # Trend alignment
    if trend_1h:
        score += 8
        reasons.append(
            "Trend 1H bullish"
        )
    else:
        score -= 8
        penalties.append(
            "Trend 1H belum bullish"
        )

    if trend_4h:
        score += 8
        reasons.append(
            "Trend 4H bullish"
        )
    else:
        score -= 10
        penalties.append(
            "Trend 4H belum bullish"
        )

    # BTC regime
    if btc_regime["name"] == "BULLISH":
        score += 5
        reasons.append(
            "Market regime BTC mendukung"
        )

    elif btc_regime["name"] == "BEARISH":
        score -= 15
        penalties.append(
            "BTC 4H bearish"
        )

    else:
        score += 1
        reasons.append(
            "BTC 4H netral"
        )

    # --------------------------------------------------------
    # HARD FILTERS
    # --------------------------------------------------------

    # Falling knife prevention.
    if (
        a15["ema20"]
        and price < a15["ema20"] * 0.985
    ):
        score -= 7
        penalties.append(
            "Harga masih terlalu jauh di bawah EMA20"
        )

    # Over-extension.
    if (
        a15["ema20"]
        and atr
        and price > (
            a15["ema20"]
            + atr * 2.5
        )
    ):
        return None

    # Resistance should have enough room.
    upside = (
        (
            resistance
            - price
        )
        / price
        * 100
    )

    if upside < 1.0:
        return None

    if upside > 35:
        # Resistance yang terlalu jauh bisa berarti struktur tidak jelas.
        return None

    # --------------------------------------------------------
    # RISK
    # --------------------------------------------------------

    stop = (
        support
        - max(
            atr * 0.85,
            price * 0.004,
        )
    )

    risk = (
        price
        - stop
    )

    if risk <= 0:
        return None

    risk_pct = (
        risk
        / price
        * 100
    )

    if risk_pct > 6:
        return None

    tp1 = min(
        resistance,
        price + risk * 1.8,
    )

    tp2 = min(
        resistance,
        price + risk * 2.6,
    )

    rr1 = (
        tp1 - price
    ) / risk

    rr2 = (
        tp2 - price
    ) / risk

    if rr1 < MIN_RR:
        return None

    # --------------------------------------------------------
    # FINAL SCORE
    # --------------------------------------------------------

    score = max(
        0,
        min(
            100,
            score,
        ),
    )

    if score < MIN_SCORE:
        return None

    # --------------------------------------------------------
    # DECISION
    # --------------------------------------------------------

    # Sangat ketat untuk "BELI SEKARANG".
    buy_now = (
        score >= 91
        and trend_1h
        and trend_4h
        and volume_ratio >= 1.5
        and (
            rejection
            or hammer
            or engulfing
        )
        and btc_regime["name"] != "BEARISH"
        and (rsi15 is None or rsi15 < 72)
    )

    if buy_now:
        decision = "🟢 BELI SEKARANG"
        quality = "A+"

    else:
        decision = "🟡 TUNGGU 1 CANDLE"
        quality = "A"

    # Jika trend lebih besar belum sepakat, jangan langsung beli.
    if not trend_4h:
        decision = "🟡 TUNGGU 1 CANDLE"

    return {
        "name": coin["name"],
        "symbol": coin["symbol"] + "USDT",
        "setup": "SUPPORT_BOUNCE",
        "score": round(
            score,
            1,
        ),
        "quality": quality,
        "decision": decision,
        "price": price,
        "support": support,
        "support_hits": support_info["hits"],
        "zone_low": zone_low,
        "zone_high": zone_high,
        "resistance": resistance,
        "resistance_hits": resistance_info["hits"],
        "upside": upside,
        "stop": stop,
        "risk_pct": risk_pct,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": rr1,
        "rr2": rr2,
        "rsi15": rsi15,
        "rsi1h": a1h["rsi"],
        "rsi4h": a4h["rsi"],
        "adx1h": a1h["adx"],
        "volume_ratio": volume_ratio,
        "trend1h": trend_1h,
        "trend4h": trend_4h,
        "btc_regime": btc_regime["name"],
        "rejection": rejection,
        "bullish": bullish,
        "reasons": reasons,
        "penalties": penalties,
        "candle_time": a15["candle_time"],
    }


# ============================================================
# BREAKOUT / RETEST
# ============================================================

def build_breakout_retest_signal(
    coin,
    a15,
    a1h,
    a4h,
    btc_regime,
):
    if not a15 or not a1h or not a4h:
        return None

    if not a15["atr"]:
        return None

    price = a15["close"]

    resistance_info = nearest_resistance(
        resistance_levels(a15),
        price,
    )

    if not resistance_info:
        return None

    level = resistance_info["level"]
    atr = a15["atr"]

    # Ambil dua candle terakhir yang sudah close.
    closes = a15["closes"]

    highs = a15["highs"]
    lows = a15["lows"]

    if len(closes) < 5:
        return None

    previous_close = closes[-2]

    broke = (
        previous_close > level
        or highs[-2] > level
    )

    retest = (
        lows[-1]
        <= level + atr * 0.35
    )

    held = (
        price > level
    )

    if not (
        broke
        and retest
        and held
    ):
        return None

    bullish = (
        a15["features"]
        and a15["features"]["bullish"]
    )

    if not bullish:
        return None

    volume_ratio = a15["volume_ratio"]

    if volume_ratio < 1.3:
        return None

    trend1 = (
        a1h["ema20"]
        and a1h["ema50"]
        and a1h["close"]
        > a1h["ema20"]
        > a1h["ema50"]
    )

    trend4 = (
        a4h["ema20"]
        and a4h["ema50"]
        and a4h["close"]
        > a4h["ema20"]
        > a4h["ema50"]
    )

    if not trend1 or not trend4:
        return None

    score = 84
    reasons = [
        "Resistance berhasil ditembus",
        "Level breakout berhasil diretest",
        "Harga bertahan di atas level",
        f"Volume {volume_ratio:.2f}x",
        "Trend 1H bullish",
        "Trend 4H bullish",
    ]

    if a15["rsi"] is not None:
        if 50 <= a15["rsi"] <= 70:
            score += 5
            reasons.append(
                "RSI mendukung"
            )
        elif a15["rsi"] > 75:
            return None

    if (
        a15["macd_hist"] is not None
        and a15["macd_hist"] > 0
    ):
        score += 4
        reasons.append(
            "MACD bullish"
        )

    if (
        a1h["adx"] is not None
        and a1h["adx"] >= 20
    ):
        score += 3
        reasons.append(
            "ADX 1H mendukung"
        )

    if btc_regime["name"] == "BEARISH":
        score -= 12

    score = min(
        100,
        score,
    )

    if score < MIN_SCORE + 2:
        return None

    stop = (
        level
        - max(
            atr * 0.9,
            price * 0.004,
        )
    )

    risk = (
        price
        - stop
    )

    if risk <= 0:
        return None

    # Cari resistance berikutnya yang lebih tinggi.
    resistances = sorted(
        [
            item["level"]
            for item in resistance_levels(a15)
            if item["level"]
            > price * 1.01
        ]
    )

    if resistances:
        target = resistances[0]
    else:
        target = (
            price
            + risk * 2.2
        )

    upside = (
        target - price
    ) / price * 100

    rr = (
        target - price
    ) / risk

    if rr < MIN_RR:
        return None

    tp1 = min(
        target,
        price + risk * 1.8,
    )

    tp2 = target

    return {
        "name": coin["name"],
        "symbol": coin["symbol"] + "USDT",
        "setup": "BREAKOUT_RETEST",
        "score": round(
            score,
            1,
        ),
        "quality": "A+",
        "decision": "🟢 BELI SEKARANG",
        "price": price,
        "support": level,
        "support_hits": resistance_info["hits"],
        "zone_low": level - atr * 0.35,
        "zone_high": level + atr * 0.35,
        "resistance": target,
        "resistance_hits": 0,
        "upside": upside,
        "stop": stop,
        "risk_pct": (
            risk / price * 100
        ),
        "tp1": tp1,
        "tp2": tp2,
        "rr1": (
            tp1 - price
        ) / risk,
        "rr2": (
            tp2 - price
        ) / risk,
        "rsi15": a15["rsi"],
        "rsi1h": a1h["rsi"],
        "rsi4h": a4h["rsi"],
        "adx1h": a1h["adx"],
        "volume_ratio": volume_ratio,
        "trend1h": trend1,
        "trend4h": trend4,
        "btc_regime": btc_regime["name"],
        "rejection": False,
        "bullish": True,
        "reasons": reasons,
        "penalties": [],
        "candle_time": a15["candle_time"],
    }


# ============================================================
# DEEP ANALYSIS
# ============================================================

def deep_analyze_coin(
    coin,
    btc_regime,
):
    symbol = coin["symbol"] + "USDT"

    ticker = BINANCE_TICKER_CACHE.get(
        symbol
    )

    if not ticker:
        return None

    if (
        ticker["quote_volume"]
        < MIN_QUOTE_VOLUME_USDT
    ):
        return None

    a15 = analyze_timeframe(
        symbol,
        "15m",
        220,
    )

    if not a15:
        return None

    a1h = analyze_timeframe(
        symbol,
        "1h",
        220,
    )

    if not a1h:
        return None

    a4h = analyze_timeframe(
        symbol,
        "4h",
        220,
    )

    if not a4h:
        return None

    candidates = []

    support_signal = build_support_bounce_signal(
        coin,
        a15,
        a1h,
        a4h,
        btc_regime,
    )

    if support_signal:
        candidates.append(
            support_signal
        )

    breakout_signal = build_breakout_retest_signal(
        coin,
        a15,
        a1h,
        a4h,
        btc_regime,
    )

    if breakout_signal:
        candidates.append(
            breakout_signal
        )

    if not candidates:
        return None

    # Pilih setup terbaik.
    return max(
        candidates,
        key=lambda x: x["score"],
    )


# ============================================================
# ALERT CONTROL
# ============================================================

def alert_allowed(
    symbol,
    setup,
    candle_time,
):
    now = time.time()

    key = (
        symbol
        + "_"
        + setup
    )

    with state_lock:
        old = state["alerts"].get(
            key
        )

        if old:
            if (
                now - old["time"]
                < ALERT_COOLDOWN
            ):
                return False

            # Jika candle masih sama, jangan ulang.
            if str(
                old.get("candle")
            ) == str(candle_time):
                return False

        state["alerts"][key] = {
            "time": now,
            "candle": candle_time,
        }

    save_state()

    return True


# ============================================================
# FORMAT ALERT
# ============================================================

def create_alert(signal):
    action = signal["decision"]

    message = ""

    message += (
        "🚨 *MOMENTUM TERDETEKSI*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
    )

    message += (
        f"🪙 *{signal['name']} "
        f"({signal['symbol']})*\n"
    )

    message += (
        f"🎯 Setup: *{signal['setup']}*\n"
        f"🏆 Score: *{signal['score']}/100*"
        f"  | Quality: *{signal['quality']}*\n\n"
    )

    message += (
        "💰 *HARGA & ENTRY*\n"
    )

    message += (
        f"• Harga: *{fmt_price(signal['price'])}*\n"
    )

    if action == "🟢 BELI SEKARANG":
        message += (
            "• Entry: *BELI SEKARANG*\n"
        )
    else:
        message += (
            "• Entry: *TUNGGU CANDLE 15M BERIKUTNYA*\n"
        )

    message += (
        "\n📍 *LEVEL*\n"
    )

    message += (
        f"• Support: *{fmt_price(signal['support'])}*\n"
        f"• Zona: {fmt_price(signal['zone_low'])} - "
        f"{fmt_price(signal['zone_high'])}\n"
        f"• Resistance/Target: "
        f"*{fmt_price(signal['resistance'])}*\n"
        f"• Potensi ke target: "
        f"*+{signal['upside']:.2f}%*\n"
    )

    message += (
        "\n🎯 *TARGET*\n"
        f"• TP1: *{fmt_price(signal['tp1'])}* "
        f"(RR {signal['rr1']:.2f})\n"
        f"• TP2: *{fmt_price(signal['tp2'])}* "
        f"(RR {signal['rr2']:.2f})\n"
    )

    message += (
        "\n🛡️ *RISK*\n"
        f"• Stop Loss: *{fmt_price(signal['stop'])}*\n"
        f"• Risiko: *-{signal['risk_pct']:.2f}%*\n"
    )

    message += (
        "\n📊 *KONFIRMASI*\n"
        f"• RSI 15M: "
        f"*{signal['rsi15']:.1f}*\n"
        if signal["rsi15"] is not None
        else "\n📊 *KONFIRMASI*\n• RSI 15M: -*\n"
    )

    if signal["rsi1h"] is not None:
        message += (
            f"• RSI 1H: "
            f"*{signal['rsi1h']:.1f}*\n"
        )

    if signal["rsi4h"] is not None:
        message += (
            f"• RSI 4H: "
            f"*{signal['rsi4h']:.1f}*\n"
        )

    if signal["adx1h"] is not None:
        message += (
            f"• ADX 1H: "
            f"*{signal['adx1h']:.1f}*\n"
        )

    message += (
        f"• Volume 15M: "
        f"*{signal['volume_ratio']:.2f}x*\n"
    )

    message += (
        "• Trend 1H: "
        + (
            "🟢 BULLISH"
            if signal["trend1h"]
            else "🔴 BELUM BULLISH"
        )
        + "\n"
    )

    message += (
        "• Trend 4H: "
        + (
            "🟢 BULLISH"
            if signal["trend4h"]
            else "🔴 BELUM BULLISH"
        )
        + "\n"
    )

    message += (
        f"• BTC Regime: "
        f"*{signal['btc_regime']}*\n"
    )

    message += (
        "\n🎯 *KEPUTUSAN*\n"
        f"*{action}*\n"
    )

    message += (
        "\n✅ *ALASAN UTAMA*\n"
    )

    for reason in signal["reasons"][:10]:
        message += (
            f"• {reason}\n"
        )

    if signal["penalties"]:
        message += (
            "\n⚠️ *HAL YANG PERLU DIPERHATIKAN*\n"
        )

        for item in signal["penalties"][:5]:
            message += (
                f"• {item}\n"
            )

    message += (
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ *Sinyal teknikal, bukan jaminan profit.*\n"
        "Bot tidak melakukan pembelian otomatis."
    )

    return message


# ============================================================
# SEND TELEGRAM
# ============================================================

def send_alert_to_all(
    message,
):
    with state_lock:
        chat_ids = list(
            state["chat_ids"]
        )

    for chat_id in chat_ids:
        try:
            bot.send_message(
                chat_id,
                message,
                parse_mode="Markdown",
            )

        except Exception as e:
            print(
                "Gagal kirim Telegram:",
                chat_id,
                e,
            )


# ============================================================
# PAPER DATABASE
# ============================================================

def save_alert_to_db(signal):
    created = now_iso()

    with db_lock:
        conn = sqlite3.connect(
            DB_FILE
        )

        cursor = conn.execute(
            """
            INSERT INTO alerts(
                created_at,
                symbol,
                setup,
                score,
                candle_time,
                entry,
                stop,
                tp1,
                tp2
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                created,
                signal["symbol"],
                signal["setup"],
                signal["score"],
                str(
                    signal["candle_time"]
                ),
                signal["price"],
                signal["stop"],
                signal["tp1"],
                signal["tp2"],
            ),
        )

        alert_id = cursor.lastrowid

        conn.execute(
            """
            INSERT INTO paper_trades(
                alert_id,
                symbol,
                entry,
                stop,
                tp1,
                tp2,
                opened_at
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                alert_id,
                signal["symbol"],
                signal["price"],
                signal["stop"],
                signal["tp1"],
                signal["tp2"],
                created,
            ),
        )

        conn.commit()
        conn.close()

    return alert_id


def get_open_paper_trades():
    with db_lock:
        conn = sqlite3.connect(
            DB_FILE
        )

        rows = conn.execute(
            """
            SELECT
                id,
                alert_id,
                symbol,
                entry,
                stop,
                tp1,
                tp2,
                opened_at
            FROM paper_trades
            WHERE status='OPEN'
            """
        ).fetchall()

        conn.close()

    return rows


def close_paper_trade(
    trade_id,
    status,
    exit_price,
):
    with db_lock:
        conn = sqlite3.connect(
            DB_FILE
        )

        row = conn.execute(
            """
            SELECT entry
            FROM paper_trades
            WHERE id=?
            """,
            (
                trade_id,
            ),
        ).fetchone()

        if not row:
            conn.close()
            return

        entry = float(
            row[0]
        )

        result_pct = (
            (
                exit_price
                - entry
            )
            / entry
            * 100
        )

        conn.execute(
            """
            UPDATE paper_trades
            SET
                status=?,
                exit_price=?,
                result_pct=?,
                closed_at=?
            WHERE id=?
            """,
            (
                status,
                exit_price,
                result_pct,
                now_iso(),
                trade_id,
            ),
        )

        conn.commit()
        conn.close()


# ============================================================
# PAPER TRACKER
# ============================================================

def check_open_trades():
    rows = get_open_paper_trades()

    if not rows:
        return

    for row in rows:
        (
            trade_id,
            alert_id,
            symbol,
            entry,
            stop,
            tp1,
            tp2,
            opened_at,
        ) = row

        try:
            data = get_klines(
                symbol,
                "15m",
                10,
            )

            arrays = candle_arrays(
                data
            )

            if not arrays:
                continue

            high = arrays["high"][-1]
            low = arrays["low"][-1]

            # Konservatif:
            # jika satu candle menyentuh SL dan TP,
            # anggap SL kena lebih dulu karena urutan intrabar
            # tidak diketahui dari OHLC.
            if low <= stop:
                close_paper_trade(
                    trade_id,
                    "SL",
                    stop,
                )

                send_alert_to_all(
                    (
                        f"🛑 *STOP LOSS TERKENA*\n\n"
                        f"🪙 {symbol}\n"
                        f"SL: {fmt_price(stop)}\n"
                        f"Entry: {fmt_price(entry)}\n\n"
                        "Paper trade ditutup."
                    )
                )

                with state_lock:
                    state["active_trades"].pop(
                        symbol,
                        None,
                    )

                save_state()
                continue

            if high >= tp2:
                close_paper_trade(
                    trade_id,
                    "TP2",
                    tp2,
                )

                send_alert_to_all(
                    (
                        f"💰 *TP2 TERCAPAI*\n\n"
                        f"🪙 {symbol}\n"
                        f"TP2: {fmt_price(tp2)}\n"
                        f"Entry: {fmt_price(entry)}\n\n"
                        "Paper trade ditutup."
                    )
                )

                with state_lock:
                    state["active_trades"].pop(
                        symbol,
                        None,
                    )

                save_state()
                continue

            if high >= tp1:
                # Jangan ditutup dulu jika TP1 tersentuh.
                # Tandai agar tidak spam.
                key = f"tp1_{symbol}"

                with state_lock:
                    already = (
                        state["active_trades"]
                        .get(symbol, {})
                        .get("tp1_hit", False)
                    )

                    if not already:
                        state["active_trades"].setdefault(
                            symbol,
                            {},
                        )["tp1_hit"] = True

                if not already:
                    send_alert_to_all(
                        (
                            f"🎯 *TP1 TERCAPAI*\n\n"
                            f"🪙 {symbol}\n"
                            f"TP1: {fmt_price(tp1)}\n"
                            f"Entry: {fmt_price(entry)}\n\n"
                            "Pertimbangkan mengamankan sebagian profit."
                        )
                    )

                    save_state()

        except Exception as e:
            print(
                "Paper tracker error:",
                symbol,
                e,
            )


# ============================================================
# PERFORMANCE
# ============================================================

def performance_stats():
    with db_lock:
        conn = sqlite3.connect(
            DB_FILE
        )

        rows = conn.execute(
            """
            SELECT result_pct
            FROM paper_trades
            WHERE status!='OPEN'
            AND result_pct IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()

        conn.close()

    values = [
        float(row[0])
        for row in rows
    ]

    if not values:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "average": 0,
            "profit_factor": 0,
            "max_drawdown": 0,
        }

    wins = [
        value
        for value in values
        if value > 0
    ]

    losses = [
        value
        for value in values
        if value <= 0
    ]

    gross_profit = sum(
        wins
    )

    gross_loss = abs(
        sum(losses)
    )

    equity = 0
    peak = 0
    max_drawdown = 0

    for value in values:
        equity += value
        peak = max(
            peak,
            equity,
        )
        drawdown = (
            peak
            - equity
        )
        max_drawdown = max(
            max_drawdown,
            drawdown,
        )

    return {
        "trades": len(values),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (
            len(wins)
            / len(values)
            * 100
        ),
        "average": (
            sum(values)
            / len(values)
        ),
        "profit_factor": (
            gross_profit
            / gross_loss
            if gross_loss > 0
            else float("inf")
        ),
        "max_drawdown": max_drawdown,
    }


# ============================================================
# SCANNER
# ============================================================

def scan_one_coin(
    coin,
    btc_regime,
):
    symbol = (
        coin["symbol"]
        + "USDT"
    )

    if symbol not in BINANCE_SYMBOLS:
        return None

    ticker = BINANCE_TICKER_CACHE.get(
        symbol
    )

    if not ticker:
        return None

    if (
        ticker["quote_volume"]
        < MIN_QUOTE_VOLUME_USDT
    ):
        return None

    try:
        result = deep_analyze_coin(
            coin,
            btc_regime,
        )

        return result

    except Exception as e:
        print(
            "Error scan",
            symbol,
            e,
        )
        return None


def run_scan():
    print(
        "\n"
        "====================================================\n"
        "🔎 SCAN TOP 100 DIMULAI\n"
        "===================================================="
    )

    # Refresh basic market data.
    if not BINANCE_SYMBOLS:
        refresh_binance_symbols()

    refresh_24h_tickers()

    coins = get_top_100()

    if not coins:
        print(
            "❌ Top 100 tidak tersedia."
        )
        return

    btc_regime = get_btc_regime()

    print(
        "BTC Regime:",
        btc_regime["name"],
        btc_regime["score"],
    )

    # Preliminary universe.
    universe = []

    for coin in coins:
        symbol = (
            coin["symbol"]
            + "USDT"
        )

        if symbol not in BINANCE_SYMBOLS:
            continue

        ticker = BINANCE_TICKER_CACHE.get(
            symbol
        )

        if not ticker:
            continue

        if (
            ticker["quote_volume"]
            < MIN_QUOTE_VOLUME_USDT
        ):
            continue

        universe.append(
            coin
        )

    print(
        f"Universe aktif: {len(universe)}"
    )

    results = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS_DEEP
    ) as executor:

        future_map = {
            executor.submit(
                scan_one_coin,
                coin,
                btc_regime,
            ): coin
            for coin in universe
        }

        for future in as_completed(
            future_map
        ):
            try:
                result = future.result()

                if result:
                    results.append(
                        result
                    )

            except Exception as e:
                print(
                    "Worker error:",
                    e,
                )

    # Sort terbaik dulu.
    results.sort(
        key=lambda x: x["score"],
        reverse=True,
    )

    print(
        "Sinyal kandidat:",
        len(results),
    )

    for signal in results:
        if not alert_allowed(
            signal["symbol"],
            signal["setup"],
            signal["candle_time"],
        ):
            continue

        # Simpan
        save_alert_to_db(
            signal
        )

        # Active state
        with state_lock:
            state["active_trades"][
                signal["symbol"]
            ] = {
                "entry": signal["price"],
                "stop_loss": signal["stop"],
                "tp1": signal["tp1"],
                "tp2": signal["tp2"],
                "tp1_hit": False,
                "created": time.time(),
            }

        save_state()

        message = create_alert(
            signal
        )

        print(
            "🚨 SIGNAL:",
            signal["symbol"],
            signal["score"],
            signal["setup"],
        )

        send_alert_to_all(
            message
        )

    check_open_trades()

    print(
        "✅ SCAN SELESAI"
    )


# ============================================================
# MANUAL ANALYSIS
# ============================================================

def analyze_single_coin(
    user_symbol,
):
    symbol = user_symbol.upper()

    if symbol.endswith(
        "USDT"
    ):
        pair = symbol
        short_symbol = symbol[
            :-4
        ]
    else:
        pair = symbol + "USDT"
        short_symbol = symbol

    if pair not in BINANCE_SYMBOLS:
        refresh_binance_symbols()

    if pair not in BINANCE_SYMBOLS:
        return (
            "❌ Pair "
            + pair
            + " tidak tersedia "
            "di Binance Spot."
        )

    refresh_24h_tickers()

    ticker = BINANCE_TICKER_CACHE.get(
        pair
    )

    if not ticker:
        return (
            "❌ Data ticker "
            "tidak tersedia."
        )

    coin = {
        "name": short_symbol,
        "symbol": short_symbol,
    }

    btc_regime = get_btc_regime()

    result = deep_analyze_coin(
        coin,
        btc_regime,
    )

    if not result:
        return (
            f"📊 *{pair}*\n\n"
            "Tidak ada setup dengan kualitas "
            "yang cukup tinggi sekarang.\n\n"
            "✅ Bot tidak memaksakan BUY."
        )

    return create_alert(
        result
    )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

@bot.message_handler(
    commands=["start"]
)
def start_command(message):
    chat_id = str(
        message.chat.id
    )

    with state_lock:
        if chat_id not in state["chat_ids"]:
            state["chat_ids"].append(
                chat_id
            )

    save_state()

    bot.reply_to(
        message,
        (
            "🤖 *CRYPTO SPOT MOMENTUM BOT*\n\n"
            "✅ Kamu sudah terdaftar untuk alert.\n\n"
            "*Bot mencari:*\n"
            "• Support bounce\n"
            "• Breakout + retest\n"
            "• Rejection candle\n"
            "• Volume confirmation\n"
            "• RSI\n"
            "• EMA 20/50/200\n"
            "• MACD\n"
            "• ADX\n"
            "• Trend 1H + 4H\n"
            "• BTC market regime\n"
            "• Risk/reward\n\n"
            "*Perintah:*\n"
            "/on\n"
            "/off\n"
            "/status\n"
            "/scan\n"
            "/analyze BTC\n"
            "/perf\n"
            "/health"
        ),
        parse_mode="Markdown",
    )


@bot.message_handler(
    commands=["on"]
)
def on_command(message):
    chat_id = str(
        message.chat.id
    )

    with state_lock:
        if chat_id not in state["chat_ids"]:
            state["chat_ids"].append(
                chat_id
            )

    save_state()

    bot.reply_to(
        message,
        "🟢 *Alert AKTIF.*",
        parse_mode="Markdown",
    )


@bot.message_handler(
    commands=["off"]
)
def off_command(message):
    chat_id = str(
        message.chat.id
    )

    with state_lock:
        state["chat_ids"] = [
            x
            for x in state["chat_ids"]
            if x != chat_id
        ]

    save_state()

    bot.reply_to(
        message,
        "🔴 *Alert dimatikan untuk chat ini.*",
        parse_mode="Markdown",
    )


@bot.message_handler(
    commands=["status"]
)
def status_command(message):
    chat_id = str(
        message.chat.id
    )

    with state_lock:
        active = len(
            state["active_trades"]
        )
        subscribed = (
            chat_id in
            state["chat_ids"]
        )

    bot.reply_to(
        message,
        (
            "🤖 *STATUS BOT*\n\n"
            f"Alert kamu: "
            f"{'🟢 ON' if subscribed else '🔴 OFF'}\n"
            f"Active paper setup: {active}\n"
            f"Top N: {TOP_N}\n"
            f"Scan interval: "
            f"{SCAN_INTERVAL}s\n"
            f"Min score: {MIN_SCORE}\n"
            f"Min RR: {MIN_RR}\n"
            f"Min volume: "
            f"${MIN_QUOTE_VOLUME_USDT:,.0f}\n"
            "Mode: *SPOT ONLY*"
        ),
        parse_mode="Markdown",
    )


@bot.message_handler(
    commands=["scan"]
)
def scan_command(message):
    chat_id = str(
        message.chat.id
    )

    with state_lock:
        if chat_id not in state["chat_ids"]:
            state["chat_ids"].append(
                chat_id
            )

    save_state()

    bot.reply_to(
        message,
        (
            "🔎 *Scan manual dimulai...*\n"
            "Bot akan mengirim alert hanya jika "
            "ada setup yang lolos filter."
        ),
        parse_mode="Markdown",
    )

    threading.Thread(
        target=run_scan,
        daemon=True,
    ).start()


@bot.message_handler(
    commands=["analyze"]
)
def analyze_command(message):
    parts = (
        message.text
        .split()
    )

    if len(parts) < 2:
        bot.reply_to(
            message,
            (
                "Contoh:\n"
                "/analyze BTC\n"
                "/analyze ETH\n"
                "/analyze SOL"
            ),
        )
        return

    symbol = parts[1]

    loading = bot.reply_to(
        message,
        f"🔎 Analisis {symbol.upper()}...",
    )

    try:
        result = analyze_single_coin(
            symbol
        )

        bot.edit_message_text(
            result,
            message.chat.id,
            loading.message_id,
            parse_mode="Markdown",
        )

    except Exception as e:
        bot.edit_message_text(
            (
                "❌ Gagal menganalisis.\n\n"
                f"Error: {e}"
            ),
            message.chat.id,
            loading.message_id,
        )


@bot.message_handler(
    commands=["perf"]
)
def perf_command(message):
    stats = performance_stats()

    pf = stats[
        "profit_factor"
    ]

    if math.isinf(pf):
        pf_text = "∞"
    else:
        pf_text = f"{pf:.2f}"

    bot.reply_to(
        message,
        (
            "📊 *PAPER PERFORMANCE*\n\n"
            f"Trades: {stats['trades']}\n"
            f"Wins: {stats['wins']}\n"
            f"Losses: {stats['losses']}\n"
            f"Win rate: "
            f"{stats['win_rate']:.2f}%\n"
            f"Average result: "
            f"{stats['average']:+.2f}%\n"
            f"Profit factor: {pf_text}\n"
            f"Max drawdown: "
            f"{stats['max_drawdown']:.2f}%\n\n"
            "⚠️ Statistik belum berarti jika "
            "jumlah sample terlalu kecil."
        ),
        parse_mode="Markdown",
    )


@bot.message_handler(
    commands=["health"]
)
def health_command(message):
    bot.reply_to(
        message,
        "🩺 Mengecek koneksi...",
    )

    try:
        symbols_ok = refresh_binance_symbols()
        tickers_ok = refresh_24h_tickers()
        coins = get_top_100()

        btc = get_btc_regime()

        bot.send_message(
            message.chat.id,
            (
                "🩺 *HEALTH CHECK*\n\n"
                f"Binance symbols: "
                f"{len(BINANCE_SYMBOLS)}\n"
                f"Binance exchange info: "
                f"{'✅' if symbols_ok else '❌'}\n"
                f"Binance ticker: "
                f"{'✅' if tickers_ok else '❌'}\n"
                f"CoinGecko Top100: "
                f"{len(coins)} coin\n"
                f"BTC regime: "
                f"{btc['name']}\n"
                f"Database: "
                f"{'✅' if os.path.exists(DB_FILE) else '❌'}\n"
                "Order execution: *DISABLED*\n"
                "Leverage/futures: *DISABLED*"
            ),
            parse_mode="Markdown",
        )

    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"❌ Health check error: {e}",
        )


# ============================================================
# STARTUP
# ============================================================

def startup():
    print(
        "=============================================="
    )

    print(
        "🚀 CRYPTO SPOT MOMENTUM BOT"
    )

    print(
        "=============================================="
    )

    if not refresh_binance_symbols():
        print(
            "⚠️ Binance symbol list gagal."
        )

    refresh_24h_tickers()

    btc = get_btc_regime()

    print(
        "BTC regime:",
        btc["name"],
    )

    print(
        "Bot siap."
    )


# ============================================================
# BACKGROUND SCANNER
# ============================================================

def scanner_loop():
    startup()

    while True:
        start_time = time.time()

        try:
            run_scan()

        except Exception as e:
            print(
                "SCANNER ERROR:",
                e,
            )

        elapsed = (
            time.time()
            - start_time
        )

        sleep_for = max(
            10,
            SCAN_INTERVAL
            - elapsed,
        )

        print(
            f"⏱️ Next scan "
            f"{sleep_for:.0f}s lagi..."
        )

        time.sleep(
            sleep_for
        )


# ============================================================
# TELEGRAM POLLING
# ============================================================

def telegram_loop():
    while True:
        try:
            print(
                "🤖 Telegram polling..."
            )

            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
            )

        except Exception as e:
            print(
                "Telegram polling error:",
                e,
            )

            time.sleep(5)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    scanner_thread = threading.Thread(
        target=scanner_loop,
        daemon=True,
    )

    scanner_thread.start()

    telegram_loop()
