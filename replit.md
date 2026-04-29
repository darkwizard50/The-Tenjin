# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Python boilerplate

A minimal Python CLI starter lives at the project root:

- `app/main.py` — CLI entry point (`python -m app.main --name Replit`)
- `tests/test_main.py` — pytest tests
- `requirements.txt` / `requirements-dev.txt` — runtime / dev dependencies
- Python 3.11 is installed as a Replit module

## Market News Bot (deployed artifact)

24/7 Telegram alerting bot lives in `app/` at the project root and is registered as the worker artifact `artifacts/market-bot`.

- Run command: `python -u -m app.main` (executed from project root with `PYTHONPATH=/home/runner/workspace`).
- Health endpoint: `GET /healthz` on port 8090, also reachable via the path-routed proxy at `/__market-bot`.
- State (cooldowns, dedup cache, RSI snapshots, alert history, health log) is persisted in the workspace Postgres DB and warm-loaded on every boot.
- Dev workflow: `artifacts/market-bot: Market News Bot`. Production deployment target: **Reserved VM** (always-on; the bot has no autoscale-friendly request shape).

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server locally

See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details.
