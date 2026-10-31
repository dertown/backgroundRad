#!/usr/bin/env python3
"""Poll public government radiation APIs and emit Splunk JSON events.

Sources (no API keys):
  - Germany BfS ODL-Info WFS (hourly ambient dose rate, uSv/h)
  - Canada Health Canada Fixed Point Surveillance (nSv/h terrestrial)
  - Finland STUK via FMI WFS (10-minute outdoor dose rate, uSv/h)
  - Austria Strahlenfruehwarnsystem (BMLUK, 10-minute dose rate, nSv/h)
  - Switzerland NADAM via NAZ (daily mean dose rate, nSv/h)
  - Japan NRA RAMIS monitoring posts (air dose rate, uSv/h)
  - Australia ARPANSA ERMS (10-minute dose rate, uSv/h)
  - United States EPA RadNet (hourly dose equivalent rate, nSv/h)

All measurements are normalized to usvh for comparison with index=radsensor.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import re
import statistics
import os
import ssl
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# Every event carries this so dashboards can ignore data from older collector versions.
COLLECTOR_ID = "background_radiation-1.1"
USER_AGENT = "background-radiation-splunk/1.1 (open government data collector)"
HTTP_TIMEOUT = 90
# Station readings older than this are dropped so a dead feed cannot pose as live.
MAX_STATION_AGE_HOURS = 12

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
AUSTRIA_URL = "https://mb.strahlenschutz.gv.at/api/current"
# Public EURDEP station list (code, lat, lng); used only for Austrian station coordinates.
EURDEP_STATIONS_URL = "https://remap.jrc.ec.europa.eu/api/maps/eurdep"
NADAM_URL = "https://www.naz.ch/shared/DAY/dataset_en.js"
METEOSWISS_STATIONS_URL = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv"
JAPAN_URL = "https://www.ramis.nra.go.jp/api/v1/map/map-means-data-public?data_type=1"
ARPANSA_URL = (
    "https://gisserver.arpansa.gov.au/arcgis/rest/services"
    "/Environmental_Radiation_Measurements___Latest_Readings_Public/MapServer/1/query"
    "?where=1%3D1&outFields=*&returnGeometry=true&outSR=4326&f=geojson"
)
RADNET_URL = "https://radnet.epa.gov/cdx-radnet-rest/api/rest/csv/{year}/fixed/{path}"
# Public intermediate that some radnet.epa.gov servers omit from their chain; the root still
# comes from the system trust store.
RADNET_INTERMEDIATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "radnet_intermediate.pem")

# NADAM stations whose names do not match a MeteoSwiss station entry.
NADAM_EXTRA_COORDS = {
    "villigen psi": (47.538, 8.231),
    "gutsch ob andermatt": (46.652, 8.616),
    "san bernardino": (46.463, 9.185),
    "santa maria": (46.598, 10.425),
}

# RadNet publishes one CSV per city (~650 KB each), so poll a spread of major cities.
RADNET_STATIONS: List[Tuple[str, str, float, float]] = [
    ("AK/FAIRBANKS", "Fairbanks, AK", 64.84, -147.72),
    ("AL/BIRMINGHAM", "Birmingham, AL", 33.52, -86.80),
    ("AZ/PHOENIX", "Phoenix, AZ", 33.45, -112.07),
    ("CA/LOS ANGELES", "Los Angeles, CA", 34.05, -118.24),
    ("CA/SAN DIEGO", "San Diego, CA", 32.72, -117.16),
    ("CA/SAN FRANCISCO", "San Francisco, CA", 37.77, -122.42),
    ("CO/DENVER", "Denver, CO", 39.74, -104.99),
    ("DC/WASHINGTON", "Washington, DC", 38.90, -77.04),
    ("FL/ORLANDO", "Orlando, FL", 28.54, -81.38),
    ("fl/miami", "Miami, FL", 25.76, -80.19),
    ("GA/ATLANTA", "Atlanta, GA", 33.75, -84.39),
    ("HI/HONOLULU", "Honolulu, HI", 21.31, -157.86),
    ("ID/BOISE", "Boise, ID", 43.62, -116.20),
    ("il/chicago", "Chicago, IL", 41.88, -87.63),
    ("MA/BOSTON", "Boston, MA", 42.36, -71.06),
    ("MI/DETROIT", "Detroit, MI", 42.33, -83.05),
    ("MN/ST. PAUL", "St. Paul, MN", 44.95, -93.09),
    ("mo/st. louis", "St. Louis, MO", 38.63, -90.20),
    ("nm/albuquerque", "Albuquerque, NM", 35.08, -106.65),
    ("nv/las vegas", "Las Vegas, NV", 36.17, -115.14),
    ("ny/buffalo", "Buffalo, NY", 42.89, -78.88),
    ("NY/SYRACUSE", "Syracuse, NY", 43.05, -76.15),
    ("oh/cleveland", "Cleveland, OH", 41.50, -81.69),
    ("or/portland", "Portland, OR", 45.52, -122.68),
    ("pa/philadelphia", "Philadelphia, PA", 39.95, -75.17),
    ("TN/MEMPHIS", "Memphis, TN", 35.15, -90.05),
    ("tx/austin", "Austin, TX", 30.27, -97.74),
    ("tx/dallas", "Dallas, TX", 32.78, -96.80),
    ("ut/salt lake city", "Salt Lake City, UT", 40.76, -111.89),
    ("wa/seattle", "Seattle, WA", 47.61, -122.33),
]


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


def iso_to_utc(value: Optional[str]) -> Optional[str]:
    """Normalize an ISO-8601 timestamp with offset to the collector's UTC format."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_fresh(measured_at: Optional[str], max_age_hours: float = MAX_STATION_AGE_HOURS) -> bool:
    if not measured_at:
        return True
    try:
        when = datetime.strptime(measured_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - when <= timedelta(hours=max_age_hours)


def http_get(url: str, context: Optional[ssl.SSLContext] = None) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*", "Accept-Encoding": "gzip"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=context) as resp:
        body = resp.read()
        if resp.headers.get("Content-Encoding", "").lower() == "gzip" or body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        return body


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
    # measured_at goes first: props.conf takes _time from the first "measured_at" in the event.
    event = {
        "measured_at": fields.pop("measured_at", None) or utc_now(),
        "origin": "government",
        "collector": COLLECTOR_ID,
        "event_type": "station",
        "collected_at": utc_now(),
    }
    event.update({k: v for k, v in fields.items() if v is not None})
    return event


def summary_event(source_network: str, country: str, country_code: str, agency: str, source_url: str, values: List[float], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    event = {
        "measured_at": utc_now(),
        "origin": "government",
        "collector": COLLECTOR_ID,
        "event_type": "network_summary",
        "collected_at": utc_now(),
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


def finish_network(stations: List[Dict[str, Any]], source_network: str, country: str, country_code: str, agency: str, source_url: str, dose_component: str, extra: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Drop stale stations and append the network summary."""
    fresh = [e for e in stations if is_fresh(e.get("measured_at"))]
    summary_extra = {"dose_component": dose_component, "stale_dropped": len(stations) - len(fresh)}
    if extra:
        summary_extra.update(extra)
    fresh.append(
        summary_event(
            source_network,
            country,
            country_code,
            agency,
            source_url,
            [e["usvh"] for e in fresh],
            extra=summary_extra,
        )
    )
    return fresh


def fetch_bfs() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(BFS_URL))
    events: List[Dict[str, Any]] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        if props.get("site_status") not in (None, 1):
            continue
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        usvh = to_usvh(props.get("value"), props.get("unit") or "uSv/h")
        if usvh is None:
            continue
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
                measured_at=iso_to_utc(props.get("end_measure") or props.get("start_measure")),
            )
        )
    return finish_network(
        events,
        "Germany BfS ODL-Info",
        "Germany",
        "DE",
        "Federal Office for Radiation Protection (BfS)",
        "https://odlinfo.bfs.de/ODL/EN/service/data-interface/data-interface_node.html",
        "total",
    )


def fetch_canada() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(CANADA_URL))
    events: List[Dict[str, Any]] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        nsvh = props.get("latest_value_nSvHr")
        usvh = to_usvh(nsvh, "nSv/h")
        if usvh is None:
            continue
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
    return finish_network(
        events,
        "Canada Health Canada FPS",
        "Canada",
        "CA",
        "Health Canada Radiation Surveillance Division",
        "https://open.canada.ca/data/en/dataset/f0d1c3a9-cf78-4b07-af55-9e8d4303449e",
        "terrestrial",
        extra={"note": "Health Canada FPS latest values are terrestrial gamma only"},
    )


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
    return finish_network(
        events,
        "Finland STUK",
        "Finland",
        "FI",
        "Radiation and Nuclear Safety Authority (STUK)",
        "https://en.ilmatieteenlaitos.fi/open-data",
        "total",
    )


def fetch_austria() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(AUSTRIA_URL))
    # The portal only gives map pixel positions; take real coordinates from EURDEP by station code.
    coords: Dict[str, Tuple[float, float]] = {}
    try:
        for row in json.loads(http_get(EURDEP_STATIONS_URL)):
            if row.get("cnt") == "AT" and row.get("code"):
                coords[row["code"]] = (row.get("lat"), row.get("lng"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        coords = {}
    measured_at = epoch_to_iso(payload.get("time"))
    events: List[Dict[str, Any]] = []
    for row in payload.get("data") or []:
        # Offline stations report "nv" (nicht verfuegbar) instead of a number.
        try:
            nsvh = float(row.get("messwert"))
        except (TypeError, ValueError):
            continue
        code = row.get("nummer")
        lat, lon = coords.get(code, (None, None))
        events.append(
            station_event(
                source_network="Austria SFWS",
                source_agency="Federal Ministry BMLUK Radiation Early Warning System",
                source_url="https://mb.strahlenschutz.gv.at/",
                country="Austria",
                country_code="AT",
                station_id=code,
                station_name=row.get("name"),
                lat=lat,
                lon=lon,
                usvh=to_usvh(nsvh, "nSv/h"),
                original_value=nsvh,
                original_unit="nSv/h",
                dose_component="total",
                measured_at=measured_at,
            )
        )
    return finish_network(
        events,
        "Austria SFWS",
        "Austria",
        "AT",
        "Federal Ministry BMLUK Radiation Early Warning System",
        "https://mb.strahlenschutz.gv.at/",
        "total",
    )


def _norm_name(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _meteoswiss_coords() -> Dict[str, Tuple[float, float]]:
    raw = http_get(METEOSWISS_STATIONS_URL)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
    coords: Dict[str, Tuple[float, float]] = {}
    for row in csv.DictReader(io.StringIO(text), delimiter=";"):
        try:
            coords[_norm_name(row["station_name"])] = (
                float(row["station_coordinates_wgs84_lat"]),
                float(row["station_coordinates_wgs84_lon"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
    coords.update(NADAM_EXTRA_COORDS)
    return coords


def _match_swiss(name: str, coords: Dict[str, Tuple[float, float]]) -> Tuple[Optional[float], Optional[float]]:
    base = re.sub(r"\s+[A-Z]{2}\s*$", "", name).strip()
    key = _norm_name(base)
    if key in coords:
        return coords[key]
    for part in re.split(r"\s*[/,]\s*", base):
        part_key = _norm_name(part)
        if not part_key:
            continue
        for candidate, value in coords.items():
            if candidate == part_key or candidate.startswith(part_key + " "):
                return value
    return (None, None)


def fetch_switzerland() -> List[Dict[str, Any]]:
    text = http_get(NADAM_URL).decode("utf-8", "replace")
    match = re.search(r"dataSet\s*=\s*(\[.*?\]);", text, re.S)
    if not match:
        raise ValueError("NADAM dataset not found")
    rows = json.loads(match.group(1))
    date_match = re.search(r"strDate\s*=\s*'([^']+)'", text)
    value_date = date_match.group(1) if date_match else None
    try:
        coords = _meteoswiss_coords()
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        coords = {}
    events: List[Dict[str, Any]] = []
    for row in rows:
        if len(row) < 3:
            continue
        name = str(row[0]).strip()
        value_match = re.match(r"\s*([0-9.]+)\s*(\S+)", str(row[2]))
        if not value_match:
            continue
        value = float(value_match.group(1))
        unit = value_match.group(2)
        lat, lon = _match_swiss(name, coords)
        events.append(
            station_event(
                source_network="Switzerland NADAM",
                source_agency="National Emergency Operations Centre (NEOC/NAZ)",
                source_url="https://www.naz.ch/en/aktuell/tagesmittelwerte",
                country="Switzerland",
                country_code="CH",
                station_id="NADAM-" + _norm_name(name).replace(" ", "-"),
                station_name=name,
                lat=lat,
                lon=lon,
                usvh=to_usvh(value, unit),
                original_value=value,
                original_unit=unit,
                dose_component="total",
                # Daily mean of the previous day; stamped at collection so it stays in recent time ranges.
                sample_period="daily_mean",
                value_date=value_date,
            )
        )
    return finish_network(
        events,
        "Switzerland NADAM",
        "Switzerland",
        "CH",
        "National Emergency Operations Centre (NEOC/NAZ)",
        "https://www.naz.ch/en/aktuell/tagesmittelwerte",
        "total",
        extra={"sample_period": "daily_mean", "value_date": value_date},
    )


def fetch_japan() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(JAPAN_URL))
    events: List[Dict[str, Any]] = []
    for row in payload.get("data") or []:
        usvh = row.get("air_dose_rate")
        if usvh is None:
            continue
        events.append(
            station_event(
                source_network="Japan NRA RAMIS",
                source_agency="Nuclear Regulation Authority (NRA)",
                source_url="https://www.ramis.nra.go.jp/",
                country="Japan",
                country_code="JP",
                station_id=row.get("obs_station_unique_code") or row.get("id"),
                station_name=row.get("display_name_roman") or row.get("id"),
                lat=row.get("latitude"),
                lon=row.get("longitude"),
                usvh=float(usvh),
                original_value=usvh,
                original_unit="uSv/h",
                dose_component="total",
                measured_at=iso_to_utc(row.get("meas_datetime")),
            )
        )
    return finish_network(
        events,
        "Japan NRA RAMIS",
        "Japan",
        "JP",
        "Nuclear Regulation Authority (NRA)",
        "https://www.ramis.nra.go.jp/",
        "total",
    )


def fetch_australia() -> List[Dict[str, Any]]:
    payload = json.loads(http_get(ARPANSA_URL))
    events: List[Dict[str, Any]] = []
    for feature in payload.get("features") or []:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]
        nsvh = props.get("doseRate_nSvh")
        if nsvh is None:
            continue
        events.append(
            station_event(
                source_network="Australia ARPANSA ERMS",
                source_agency="Australian Radiation Protection and Nuclear Safety Agency (ARPANSA)",
                source_url="https://www.arpansa.gov.au/understanding-radiation/radmap-aus/environmental-radiation-monitoring-system-erms",
                country="Australia",
                country_code="AU",
                station_id=str(props.get("MIid") or props.get("stationId")),
                station_name=props.get("stationLabel") or props.get("stationId"),
                lat=coords[1],
                lon=coords[0],
                usvh=to_usvh(float(nsvh), "nSv/h"),
                original_value=nsvh,
                original_unit="nSv/h",
                dose_component="total",
                measured_at=epoch_to_iso(props.get("endTime")),
            )
        )
    return finish_network(
        events,
        "Australia ARPANSA ERMS",
        "Australia",
        "AU",
        "Australian Radiation Protection and Nuclear Safety Agency (ARPANSA)",
        "https://www.arpansa.gov.au/understanding-radiation/radmap-aus/environmental-radiation-monitoring-system-erms",
        "total",
    )


def _radnet_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    if os.path.exists(RADNET_INTERMEDIATE):
        context.load_verify_locations(cafile=RADNET_INTERMEDIATE)
    return context


def _radnet_latest(station: Tuple[str, str, float, float]) -> Optional[Dict[str, Any]]:
    path, name, lat, lon = station
    year = datetime.now(timezone.utc).year
    url = RADNET_URL.format(year=year, path=urllib.parse.quote(path))
    text = None
    for _ in range(3):
        try:
            text = http_get(url, _radnet_context()).decode("utf-8", "replace")
            break
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
    if text is None:
        return None
    latest: Optional[Tuple[str, float]] = None
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 3 or not row[2].strip() or row[0] == "LOCATION_NAME":
            continue
        try:
            latest = (row[1], float(row[2]))
        except ValueError:
            continue
    if latest is None:
        return None
    try:
        measured_at = datetime.strptime(latest[0], "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        measured_at = None
    return station_event(
        source_network="United States EPA RadNet",
        source_agency="US Environmental Protection Agency (EPA)",
        source_url="https://www.epa.gov/radnet/radnet-csv-file-downloads",
        country="United States",
        country_code="US",
        station_id="RADNET-" + path.upper().replace(" ", "_"),
        station_name=name,
        lat=lat,
        lon=lon,
        usvh=to_usvh(latest[1], "nSv/h"),
        original_value=latest[1],
        original_unit="nSv/h",
        dose_component="total",
        measured_at=measured_at,
    )


def fetch_radnet() -> List[Dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(_radnet_latest, RADNET_STATIONS))
    events = [e for e in results if e is not None]
    if not events:
        raise ValueError("no RadNet station returned a dose rate")
    return finish_network(
        events,
        "United States EPA RadNet",
        "United States",
        "US",
        "US Environmental Protection Agency (EPA)",
        "https://www.epa.gov/radnet/radnet-csv-file-downloads",
        "total",
    )


FETCHERS: Dict[str, Tuple[str, Callable[[], List[Dict[str, Any]]]]] = {
    "germany": ("Germany BfS ODL-Info", fetch_bfs),
    "canada": ("Canada Health Canada FPS", fetch_canada),
    "finland": ("Finland STUK", fetch_stuk),
    "austria": ("Austria SFWS", fetch_austria),
    "switzerland": ("Switzerland NADAM", fetch_switzerland),
    "japan": ("Japan NRA RAMIS", fetch_japan),
    "australia": ("Australia ARPANSA ERMS", fetch_australia),
    "radnet": ("United States EPA RadNet", fetch_radnet),
}
# RadNet is hourly and heavy, so it runs from its own hourly input (see default/inputs.conf).
DEFAULT_SOURCES = [k for k in FETCHERS if k != "radnet"]


def emit(event: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=True, separators=(",", ":")) + "\n")


def run_fetcher(key: str) -> Tuple[str, Optional[List[Dict[str, Any]]], Optional[str]]:
    name, fn = FETCHERS[key]
    try:
        return name, fn(), None
    except Exception as exc:  # one broken feed must never stop the others from being indexed
        return name, None, "%s: %s" % (type(exc).__name__, exc)


def log(message: str) -> None:
    """Write to stderr, which Splunk records in splunkd.log under ExecProcessor."""
    sys.stderr.write("fetch_gov_radiation: %s\n" % message)
    sys.stderr.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch government background radiation readings")
    parser.add_argument("--summary-only", action="store_true", help="Emit network summaries, not every station")
    parser.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help="Comma-separated sources to poll (%s)" % ", ".join(FETCHERS),
    )
    # Tolerate stale arguments from older inputs.conf stanzas instead of exiting with no events.
    args, extra = parser.parse_known_args()
    if extra:
        log("ignoring unknown argument(s): %s" % " ".join(extra))

    keys = [k.strip().lower() for k in args.sources.split(",") if k.strip()]
    unknown = [k for k in keys if k not in FETCHERS]
    if unknown:
        log("ignoring unknown source(s): %s" % ", ".join(unknown))
    keys = [k for k in keys if k in FETCHERS] or list(DEFAULT_SOURCES)

    all_events: List[Dict[str, Any]] = []
    ok_sources: List[str] = []
    failed_sources: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=len(keys) or 1) as pool:
        results = list(pool.map(run_fetcher, keys))

    for name, events, error in results:
        if events is None:
            failed_sources.append({"source_network": name, "error": error or "unknown error"})
            all_events.append(
                {
                    "measured_at": utc_now(),
                    "origin": "government",
                    "collector": COLLECTOR_ID,
                    "event_type": "source_error",
                    "collected_at": utc_now(),
                    "source_network": name,
                    "error": error,
                }
            )
            continue
        ok_sources.append(name)
        all_events.extend(events)

    all_events.append(
        {
            "measured_at": utc_now(),
            "origin": "government",
            "collector": COLLECTOR_ID,
            "event_type": "collector_run",
            "collected_at": utc_now(),
            "sources_ok": ok_sources,
            "sources_failed": failed_sources,
            "station_count": sum(1 for e in all_events if e.get("event_type") == "station"),
        }
    )

    log(
        "%s: %d stations, ok=%s, failed=%s"
        % (COLLECTOR_ID, all_events[-1]["station_count"], ok_sources, [f["source_network"] for f in failed_sources])
    )
    for event in all_events:
        if args.summary_only and event.get("event_type") == "station":
            continue
        emit(event)
    return 0 if ok_sources else 1


if __name__ == "__main__":
    sys.exit(main())
