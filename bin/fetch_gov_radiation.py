#!/usr/bin/env python3
"""Poll public government radiation APIs and emit Splunk JSON events.

Sources (no API keys):
  - Germany BfS ODL-Info WFS (hourly ambient dose rate, uSv/h)
  - Canada Health Canada Fixed Point Surveillance (nSv/h terrestrial)
  - Finland STUK via FMI WFS (10-minute outdoor dose rate, uSv/h)

All measurements are normalized to usvh for comparison with index=radsensor.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

USER_AGENT = "background-radiation-splunk/1.0 (open government data collector)"
HTTP_TIMEOUT = 90

GML_NS = "http://www.opengis.net/gml/3.2"
GMLCOV_NS = "http://www.opengis.net/gmlcov/1.0"

BFS_URL = (
    "https://www.imis.bfs.de/ogc/opendata/ows"
    "?service=WFS&version=1.1.0&request=GetFeature"
    "&typeName=opendata:odlinfo_odl_1h_latest"
    "&outputFormat=application/json"
)
CANADA_URL = (
    "https://maps-cartes.services.geo.ca/server_serveur/rest/services"
    "/HC/fpsn_en/MapServer/1/query"
    "?where=1%3D1&outFields=*&returnGeometry=true&outSR=4326"
    "&f=geojson&resultRecordCount=20000"
)
STUK_URL = (
    "https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0"
    "&request=GetFeature"
    "&storedquery_id=stuk::observations::external-radiation::latest::multipointcoverage"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_to_iso(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        return text if text else None
    if raw > 1e12:
        raw = raw / 1000.0
    try:
        return datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return None


def http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read()


def to_usvh(value: Optional[float], unit: str) -> Optional[float]:
    if value is None:
        return None
    unit_norm = (unit or "").strip().lower().replace(" ", "")
    unit_norm = unit_norm.replace("µ", "u").replace("μ", "u")
    if unit_norm in {"usv/h", "usvh", "microsievert/h", "microsieverts/h"}:
        return value
    if unit_norm in {"nsv/h", "nsvh", "nsv/hr", "nanosievert/h", "nanosieverts/h"}:
        return value / 1000.0
    if unit_norm in {"msv/h", "msvh"}:
        return value * 1000.0
    if unit_norm in {"ngy/h", "ngyh"}:
        return value / 1000.0
    return value


def percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * p))
    return ordered[max(0, min(idx, len(ordered) - 1))]


def summarize(values: Iterable[float]) -> Dict[str, Optional[float]]:
    data = [v for v in values if v is not None]
    if not data:
        return {
            "station_count": 0,
            "min_usvh": None,
            "max_usvh": None,
            "avg_usvh": None,
            "median_usvh": None,
            "p10_usvh": None,
            "p90_usvh": None,
        }
    avg = statistics.fmean(data) if hasattr(statistics, "fmean") else (sum(data) / float(len(data)))
    return {
        "station_count": len(data),
        "min_usvh": round(min(data), 6),
        "max_usvh": round(max(data), 6),
        "avg_usvh": round(avg, 6),
        "median_usvh": round(statistics.median(data), 6),
        "p10_usvh": round(percentile(data, 0.10), 6),
        "p90_usvh": round(percentile(data, 0.90), 6),
    }


def station_event(**fields: Any) -> Dict[str, Any]:
    event = {
        "origin": "government",
        "event_type": "station",
        "collected_at": utc_now(),
    }
    event.update({k: v for k, v in fields.items() if v is not None})
    return event


def summary_event(source_network: str, country: str, country_code: str, agency: str, source_url: str, values: List[float], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    event = {
        "origin": "government",
        "event_type": "network_summary",
        "collected_at": utc_now(),
        "measured_at": utc_now(),
        "source_network": source_network,
        "country": country,
        "country_code": country_code,
        "source_agency": agency,
        "source_url": source_url,
    }
    event.update(summarize(values))
    if extra:
        event.update(extra)
    return event


def fetch_bfs() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(BFS_URL))
    events: List[Dict[str, Any]] = []
    values: List[float] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        if props.get("site_status") not in (None, 1):
            continue
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        usvh = to_usvh(props.get("value"), props.get("unit") or "uSv/h")
        if usvh is None:
            continue
        values.append(usvh)
        events.append(
            station_event(
                source_network="Germany BfS ODL-Info",
                source_agency="Federal Office for Radiation Protection (BfS)",
                source_url="https://odlinfo.bfs.de/ODL/EN/service/data-interface/data-interface_node.html",
                country="Germany",
                country_code="DE",
                station_id=props.get("id") or props.get("kenn"),
                station_name=props.get("name"),
                lat=coords[1],
                lon=coords[0],
                usvh=usvh,
                original_value=props.get("value"),
                original_unit=props.get("unit") or "uSv/h",
                dose_component="total",
                measured_at=props.get("end_measure") or props.get("start_measure"),
            )
        )
    events.append(
        summary_event(
            "Germany BfS ODL-Info",
            "Germany",
            "DE",
            "Federal Office for Radiation Protection (BfS)",
            "https://odlinfo.bfs.de/ODL/EN/service/data-interface/data-interface_node.html",
            values,
            extra={"dose_component": "total"},
        )
    )
    return events


def fetch_canada() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(CANADA_URL))
    events: List[Dict[str, Any]] = []
    values: List[float] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        nsvh = props.get("latest_value_nSvHr")
        usvh = to_usvh(nsvh, "nSv/h")
        if usvh is None:
            continue
        values.append(usvh)
        events.append(
            station_event(
                source_network="Canada Health Canada FPS",
                source_agency="Health Canada Radiation Surveillance Division",
                source_url="https://open.canada.ca/data/en/dataset/f0d1c3a9-cf78-4b07-af55-9e8d4303449e",
                country="Canada",
                country_code="CA",
                station_id=props.get("location_name") or str(props.get("OBJECTID")),
                station_name=props.get("location_name"),
                lat=coords[1],
                lon=coords[0],
                usvh=usvh,
                original_value=nsvh,
                original_unit="nSv/h",
                dose_component="terrestrial",
                measured_at=epoch_to_iso(props.get("time")),
            )
        )
    events.append(
        summary_event(
            "Canada Health Canada FPS",
            "Canada",
            "CA",
            "Health Canada Radiation Surveillance Division",
            "https://open.canada.ca/data/en/dataset/f0d1c3a9-cf78-4b07-af55-9e8d4303449e",
            values,
            extra={"dose_component": "terrestrial", "note": "Health Canada FPS latest values are terrestrial gamma only"},
        )
    )
    return events


def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def fetch_stuk() -> List[Dict[str, Any]]:
    root = ET.fromstring(http_get(STUK_URL))
    locations: Dict[str, Dict[str, Any]] = {}
    for point in root.iter("{%s}Point" % GML_NS):
        ident = (point.attrib.get("{%s}id" % GML_NS) or point.attrib.get("id") or "").split("-")[-1]
        name_el = point.find("{%s}name" % GML_NS)
        pos_el = point.find("{%s}pos" % GML_NS)
        if pos_el is None or not (pos_el.text or "").strip():
            continue
        parts = pos_el.text.strip().split()
        lat, lon = float(parts[0]), float(parts[1])
        locations[pos_el.text.strip()] = {
            "id": ident,
            "name": name_el.text if name_el is not None else ident,
            "lat": lat,
            "lon": lon,
        }

    tuple_text = ""
    for el in root.iter("{%s}doubleOrNilReasonTupleList" % GML_NS):
        tuple_text = el.text or ""
        break
    raw_vals = tuple_text.split()
    dose_values: List[Optional[float]] = []
    for i in range(0, len(raw_vals), 2):
        token = raw_vals[i]
        if token in {"NaN", "nil", ""}:
            dose_values.append(None)
        else:
            try:
                dose_values.append(float(token))
            except ValueError:
                dose_values.append(None)

    positions_text = ""
    for el in root.iter("{%s}positions" % GMLCOV_NS):
        if el.text and el.text.strip():
            positions_text = el.text
            break
    if not positions_text:
        for el in root.iter():
            if _localname(el.tag) == "positions" and el.text and el.text.strip():
                positions_text = el.text
                break
    pos_tokens = positions_text.split()
    triples = [pos_tokens[i : i + 3] for i in range(0, len(pos_tokens) - 2, 3)]

    events: List[Dict[str, Any]] = []
    values: List[float] = []
    for i, triple in enumerate(triples):
        if i >= len(dose_values) or dose_values[i] is None:
            continue
        lat = float(triple[0])
        lon = float(triple[1])
        key = "%s %s" % (triple[0], triple[1])
        loc = locations.get(key) or {
            "id": "stuk-%s-%s" % (triple[0], triple[1]),
            "name": key,
            "lat": lat,
            "lon": lon,
        }
        usvh = float(dose_values[i])
        values.append(usvh)
        events.append(
            station_event(
                source_network="Finland STUK",
                source_agency="Radiation and Nuclear Safety Authority (STUK)",
                source_url="https://en.ilmatieteenlaitos.fi/open-data",
                country="Finland",
                country_code="FI",
                station_id=loc["id"],
                station_name=loc["name"],
                lat=loc["lat"],
                lon=loc["lon"],
                usvh=usvh,
                original_value=usvh,
                original_unit="uSv/h",
                dose_component="total",
                measured_at=epoch_to_iso(triple[2]),
            )
        )
    events.append(
        summary_event(
            "Finland STUK",
            "Finland",
            "FI",
            "Radiation and Nuclear Safety Authority (STUK)",
            "https://en.ilmatieteenlaitos.fi/open-data",
            values,
            extra={"dose_component": "total"},
        )
    )
    return events


def emit(event: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=True, separators=(",", ":")) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch government background radiation readings")
    parser.add_argument("--summary-only", action="store_true", help="Emit network summaries, not every station")
    args = parser.parse_args()

    fetchers = [
        ("Germany BfS ODL-Info", fetch_bfs),
        ("Canada Health Canada FPS", fetch_canada),
        ("Finland STUK", fetch_stuk),
    ]
    all_events: List[Dict[str, Any]] = []
    ok_sources: List[str] = []
    failed_sources: List[Dict[str, str]] = []
    world_values: List[float] = []

    for name, fn in fetchers:
        try:
            events = fn()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, ET.ParseError, ValueError, OSError) as exc:
            failed_sources.append({"source_network": name, "error": str(exc)})
            emit(
                {
                    "origin": "government",
                    "event_type": "source_error",
                    "collected_at": utc_now(),
                    "measured_at": utc_now(),
                    "source_network": name,
                    "error": str(exc),
                }
            )
            continue
        ok_sources.append(name)
        for event in events:
            all_events.append(event)
            if event.get("event_type") == "station" and event.get("usvh") is not None:
                world_values.append(float(event["usvh"]))

    combined = summary_event(
        "All government networks",
        "World",
        "WORLD",
        "Combined public government feeds",
        "https://odlinfo.bfs.de/",
        world_values,
        extra={"sources_ok": ok_sources, "dose_component": "mixed"},
    )
    all_events.append(combined)
    all_events.append(
        {
            "origin": "government",
            "event_type": "collector_run",
            "collected_at": utc_now(),
            "measured_at": utc_now(),
            "sources_ok": ok_sources,
            "sources_failed": failed_sources,
            "station_count": sum(1 for e in all_events if e.get("event_type") == "station"),
        }
    )

    for event in all_events:
        if args.summary_only and event.get("event_type") == "station":
            continue
        emit(event)
    return 0 if ok_sources else 1


if __name__ == "__main__":
    sys.exit(main())
