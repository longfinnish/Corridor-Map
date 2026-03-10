# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project
Corridor Map (app.thecorridor.io) — proprietary infrastructure intelligence map for DC site selection.
GitHub: longfinnish/Corridor-Map, branch: master
Deployment: Cloudflare Pages (auto-deploys on push to master)

## Key Files
- `index.html` — main application (single file, all JS inline)
- `data/gas_interconnects.json` — 36 pipelines, 6,642 interconnection points
- `data/ioc_contracts.json` — 32 pipelines, 8,251 IOC contracts
- `data/unsub_capacity.json` — 23 pipelines, unsubscribed capacity
- `data/planned_transmission.json` — 873 planned TX projects
- `data/ercot_lmp.json` — ERCOT LMP pricing nodes
- `data/miso_lmp.json` — MISO LMP pricing nodes
- `scripts/fetch_gas_data.py` — daily gas refresh (GitHub Actions)
- `scripts/fetch_ioc_data.py` — automated IOC refresh
- `scripts/fetch_lmp_data.py` — daily LMP refresh
- `.github/workflows/gas-refresh.yml` — daily automation
- `.github/workflows/ioc-reminder.yml` — quarterly reminder

## Conventions
- JavaScript validation: extract script blocks with Python regex, run `node -c`
- Cache busting: `?v=XX` on URLs
- All spatial queries: ArcGIS REST bbox pattern
- Git push from this directory
