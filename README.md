# Market News Telegram Bot

A small Python script that polls the Mint Markets RSS feed every 5 minutes,
deduplicates headlines, classifies them by priority (CRITICAL / HIGH / MEDIUM),
and forwards new ones to a Telegram chat.

## Run

```bash
python -m app.main
```

The script runs in a loop until you stop it.

## Configuration

`BOT_TOKEN` and `CHAT_ID` are currently set inside `app/main.py`. For better
security you can move them to environment variables.
