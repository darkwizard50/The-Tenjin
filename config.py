"""Configuration for the F&O scanner bot."""

# --- Telegram ---
BOT_TOKEN = "8441817784:AAHwY0LeaWdR4krFwnDysFLpBh7c7N3Ch6M"
CHAT_ID = "-1003530455702"

# --- RSI engine ---
RSI_PERIOD = 14
RSI_UPPER_THRESHOLD = 59
RSI_LOWER_THRESHOLD = 41
EMA_PERIOD = 20
MTF_INTERVAL = "15m"
MTF_PERIOD = "5d"

# --- Volume filter ---
VOLUME_SPIKE_MULTIPLIER = 1.8
VOLUME_LOOKBACK = 20

# --- Cooldowns (seconds) ---
RSI_COOLDOWN_SECONDS = 30 * 60
NEWS_COOLDOWN_SECONDS = 15 * 60

# --- Loop intervals (seconds) ---
RSI_SCAN_INTERVAL = 60
NEWS_SCAN_INTERVAL = 300

# --- Yahoo Finance ---
CANDLE_INTERVAL = "5m"
CANDLE_PERIOD = "5d"
BATCH_SIZE = 50

# --- Upstox (optional secondary confirmation) ---
UPSTOX_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)
UPSTOX_CANDLES_URL = (
    "https://api.upstox.com/v3/historical-candle/intraday/{key}/minutes/5"
)
UPSTOX_CACHE_TTL_SECONDS = 60

# --- News feeds (multi-source) ---
NEWS_FEED_URL = "https://www.livemint.com/rss/markets"  # legacy single source
NEWS_SOURCES = [
    ("MINT", "https://www.livemint.com/rss/markets"),
    ("MINT-COMPANIES", "https://www.livemint.com/rss/companies"),
    ("ECONOMIC-TIMES", "https://economictimes.indiatimes.com/markets/stocks/news/rssfeeds/2146842.cms"),
    ("ET-EARNINGS", "https://economictimes.indiatimes.com/markets/earnings/rssfeeds/1977021501.cms"),
    ("MONEYCONTROL", "https://www.moneycontrol.com/rss/MCtopnews.xml"),
    ("MONEYCONTROL-BIZ", "https://www.moneycontrol.com/rss/business.xml"),
    ("BUSINESS-STANDARD", "https://www.business-standard.com/rss/markets-106.rss"),
    ("BSE-NOTICES", "https://www.bseindia.com/data/xml/notices.xml"),
    # NSE filings: requires a JS-rendered session — left as a future hook.
    # Reuters India: discontinued public RSS — covered indirectly via wires above.
]
NEWS_FEED_TIMEOUT = 8

# --- Pre-market summary ---
PRE_MARKET_HOUR = 8
PRE_MARKET_MINUTE = 15
PRE_MARKET_LOOKBACK_HOURS = 16
PRE_MARKET_DEDUPE_RATIO = 80
PRE_MARKET_MAX_HIGH = 8
PRE_MARKET_MAX_MEDIUM = 6
PRE_MARKET_MAX_WATCHLIST = 8

BULLISH_KEYWORDS = [
    "rally", "surge", "soar", "jump", "rise", "gain", "record high",
    "buyback", "order win", "upgrade", "outperform", "beats estimates",
    "profit jumps", "growth", "bullish",
]
BEARISH_KEYWORDS = [
    "fall", "drop", "plunge", "tumble", "slump", "decline", "loss",
    "downgrade", "miss estimates", "fraud", "raid", "probe", "warning",
    "bearish", "selloff",
]

EARNINGS_KEYWORDS = [
    "earnings", "results", "quarterly",
    "q1 results", "q2 results", "q3 results", "q4 results",
]
FILING_KEYWORDS = [
    "filing", "intimation", "disclosure", "announcement",
    "sebi", "exchange filing", "nse filing", "bse filing",
    "fraud", "raid", "merger", "acquisition", "stake sale",
    "fund raising", "dividend", "order win",
]

# --- Scoring weights ---
# RSI floor (base + volume + fno) = 40 + 30 + 20 = 90 → always CRITICAL.
SCORE_RSI_CROSSOVER = 40
SCORE_VOLUME_SPIKE = 30
SCORE_DUAL_SOURCE = 10
SCORE_MTF_ALIGNED = 5
SCORE_EMA_ALIGNED = 5
SCORE_EARNINGS = 50
SCORE_FILING = 40
SCORE_FNO_STOCK = 20

# --- Priority thresholds ---
PRIORITY_CRITICAL = 80
PRIORITY_HIGH = 50
PRIORITY_MEDIUM = 30
