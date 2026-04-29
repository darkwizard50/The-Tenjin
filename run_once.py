from app.cooldowns import CooldownManager
from app.db import AlertStore
from app.news_engine import NewsEngine
from app.rsi_engine import RSIEngine
from app.fno_universe import FNO_STOCKS
from app.summary_engine import SummaryEngine

store = AlertStore()
cooldowns = CooldownManager(store=store)
summary = SummaryEngine()

news = NewsEngine(cooldowns, summary=summary, store=store)
rsi = RSIEngine(FNO_STOCKS, cooldowns, store=store)

print("Running news scan...")
news.scan()

print("Running RSI scan...")
rsi.scan()

print("Done.")
