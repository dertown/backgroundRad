# Background Radiation

Splunk app that keeps your home Geiger dashboard and compares it with live public government radiation networks.

Home readings stay in `index=radsensor` (`cpm`, `usvh`). Government feeds are written to the same index as `sourcetype=gov:radiation:json`.

## What it compares

All live values are normalized to **µSv/h**.

| Network | Agency | What you get |
| --- | --- | --- |
| Germany BfS ODL-Info | Federal Office for Radiation Protection | Hourly ambient dose rate, total (cosmic + terrestrial) |
| Canada FPS | Health Canada | Latest terrestrial gamma (nSv/h ÷ 1000). Usually lower than total-dose networks |
| Finland STUK | Radiation and Nuclear Safety Authority via FMI | Latest outdoor dose rate |
| Combined | All of the above | World median used for the home comparison |
| Published typicals | UNSCEAR / national reports | Lookup table if a live feed is down |

EPA RadNet near-real-time files are public, but they are per-station ZIP downloads rather than a bulk JSON API, so they are not polled automatically. US context is included via the typical-background lookup.

## Install in Splunk

1. Copy this folder to `$SPLUNK_HOME/etc/apps/background_radiation`
2. Restart Splunk, or reload the app
3. Confirm the scripted input is enabled: **Settings → Data inputs → Scripts → `fetch_gov_radiation.py`**
4. Open **Background Radiation**
5. Wait one 15-minute poll, or run the collector once:

```bash
$SPLUNK_HOME/bin/splunk cmd python3 etc/apps/background_radiation/bin/fetch_gov_radiation.py --summary-only
```

To import only the dashboard, paste `dashboards/background_radiation.json` into Dashboard Studio.

## Tabs

- **Home sensor** — your original CPM / µSv/h charts, with latest and period averages
- **World vs home** — live medians, home−world delta, home/world ratio, map, and highest stations

## Notes

- Indoor home Geigers often read differently from outdoor government monitors
- Canada FPS is terrestrial-only; Germany and Finland are closer to total ambient dose
- Typical outdoor gamma is roughly **0.05–0.15 µSv/h**; that is background, not an alarm
- No API keys or credentials are used. The collector calls public HTTPS open-data endpoints with the system certificate store
