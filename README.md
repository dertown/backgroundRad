# Background Radiation

Splunk app that keeps your home Geiger dashboard and compares it with live public government radiation networks.

Home readings stay in `index=radsensor` (`cpm`, `usvh`). Government feeds are written to the same index as `sourcetype=gov:radiation:json`.

## What it compares

All live values are normalized to **µSv/h**.

| Country | Network / agency | What you get |
| --- | --- | --- |
| Germany | BfS ODL-Info | Hourly ambient dose rate, total (cosmic + terrestrial), ~1,600 stations |
| Finland | STUK via FMI | 10-minute outdoor dose rate, ~250 stations |
| Austria | Strahlenfrühwarnsystem (BMLUK) | 10-minute dose rate (nSv/h), ~110 stations; coordinates from the public EURDEP station list |
| Switzerland | NADAM (NEOC/NAZ) | Previous day's mean dose rate (nSv/h), ~75 stations; coordinates from MeteoSwiss station metadata |
| Japan | NRA RAMIS | Monitoring-post air dose rate, ~1,100 stations |
| Australia | ARPANSA ERMS | 10-minute dose rate from the ERMS detectors |
| United States | EPA RadNet | Hourly dose equivalent rate (nSv/h) for 30 major cities (hourly input) |
| Canada | Health Canada FPS | Latest terrestrial gamma (nSv/h ÷ 1000). Usually lower than total-dose networks |
| World | All of the above | Median of the country medians, used for the home comparison |
| Published typicals | UNSCEAR / national reports | Lookup table, shown for countries with no live data |

Station readings older than 12 hours are dropped, so an outage shows up as missing data, not as stale "live" values. Brazil and the UK have no public live feed polled here; they appear through the typical-background lookup only.

Run a single source with `--sources`, e.g. `--sources japan,austria`. The default run polls everything except `radnet`, which has its own hourly input.

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
- **Live world vs home** — world median, home−world delta, home/world ratio, station map (colored by µSv/h), per-country table vs home, highest stations, and feed status per network

## Notes

- Indoor home Geigers often read differently from outdoor government monitors
- Canada FPS is terrestrial-only; the other networks are closer to total ambient dose
- Japan includes posts near Fukushima Daiichi, so its maximum can be well above 1 µSv/h while the national median stays low
- If **Feed status by network** shows `CERTIFICATE_VERIFY_FAILED`, the Python running the script lacks a CA certificate for that site; the BfS and FMI servers need a complete system CA store
- Typical outdoor gamma is roughly **0.05–0.15 µSv/h**; that is background, not an alarm
- No API keys or credentials are used. The collector calls public HTTPS open-data endpoints with the system certificate store
