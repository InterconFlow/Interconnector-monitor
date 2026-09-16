import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

try:
    import entsoe
    from entsoe import EntsoePandasClient
except ImportError:
    sys.exit("Missing dependency. Run: pip install entsoe-py pandas python-dateutil")

# ENTSO-E migrated their REST API to a new host; the entsoe-py library
# (as of the version available via pip) still defaults to the old one,
# which is now unstable/returns 503s. Point it at the new endpoint.
entsoe.entsoe.URL = "https://external-api.tp.entsoe.eu/api"

# ---------------------------------------------------------------------------
# Border list — (link_id, from_country, to_country) using entsoe-py's
# country/bidding-zone codes. Where a country has multiple bidding zones,
# a specific zone code is used (e.g. 'IT_NORD', 'NO_2', 'SE_4', 'DK_1').
# This mirrors the LINKS array in interconnector-load-monitor.html — keep
# link_id values in sync with the "route" field there if you edit either.
# ---------------------------------------------------------------------------
BORDERS = [
    ("FR–ES", "FR", "ES"),
    ("FR–DE", "FR", "DE_LU"),
    ("FR–BE", "FR", "BE"),
    ("FR–IT (AC ties)", "FR", "IT_NORD"),
    # Note: the Savoie-Piemonte HVDC merchant line isn't separately
    # published by ENTSO-E's Transparency API (only the FR-IT zone
    # border total is) — that entry in the app will stay on the
    # simulated model even after a successful fetch, which is expected.
    ("FR–CH", "FR", "CH"),
    ("FR–GB (IFA)", "FR", "GB"),
    ("DE–PL", "DE_LU", "PL"),
    ("DE–DK1", "DE_LU", "DK_1"),
    ("DE–NL", "DE_LU", "NL"),
    ("DE–AT", "DE_LU", "AT"),
    ("DE–CH", "DE_LU", "CH"),
    ("DE–CZ", "DE_LU", "CZ"),
    ("DE–BE (ALEGrO)", "DE_LU", "BE"),
    ("NL–GB (BritNed)", "NL", "GB"),
    ("NL–BE", "NL", "BE"),
    ("NL–NO (NorNed)", "NL", "NO_2"),
    ("NL–DK (COBRAcable)", "NL", "DK_1"),
    ("BE–GB (Nemo)", "BE", "GB"),
    ("NO–DK (Skagerrak)", "NO_2", "DK_1"),
    ("NO–DE (NordLink)", "NO_2", "DE_LU"),
    ("NO–GB (North Sea Link)", "NO_2", "GB"),
    ("NO–SE", "NO_1", "SE_3"),
    ("SE–DK2 (Öresund)", "SE_4", "DK_2"),
    ("SE–DK1 (Konti-Skan)", "SE_3", "DK_1"),
    ("SE–FI (Fenno-Skan)", "SE_3", "FI"),
    ("SE–PL (SwePol)", "SE_4", "PL"),
    ("SE–DE (Baltic Cable)", "SE_4", "DE_LU"),
    ("SE–LT (NordBalt)", "SE_4", "LT"),
    ("FI–EE (Estlink)", "FI", "EE"),
    ("PL–LT (LitPol)", "PL", "LT"),
    ("PL–CZ", "PL", "CZ"),
    ("PL–SK", "PL", "SK"),
    ("LT–LV", "LT", "LV"),
    ("LV–EE", "LV", "EE"),
    ("CZ–AT", "CZ", "AT"),
    ("CZ–SK", "CZ", "SK"),
    ("AT–CH", "AT", "CH"),
    ("AT–IT", "AT", "IT_NORD"),
    ("AT–HU", "AT", "HU"),
    ("AT–SI", "AT", "SI"),
    ("CH–IT", "CH", "IT_NORD"),
    ("IT–SI", "IT_NORD", "SI"),
    ("IT–GR", "IT_GR", "GR"),
    ("IT–ME", "IT_SUD", "ME"),
    ("SI–HR", "SI", "HR"),
    ("SI–HU", "SI", "HU"),
    ("HR–HU", "HR", "HU"),
    ("HR–RS", "HR", "RS"),
    ("HR–BA", "HR", "BA"),
    ("HU–SK", "HU", "SK"),
    ("HU–RO", "HU", "RO"),
    ("HU–RS", "HU", "RS"),
    ("RO–RS", "RO", "RS"),
    ("RO–BG", "RO", "BG"),
    ("BG–GR", "BG", "GR"),
    ("BG–RS", "BG", "RS"),
    ("BG–MK", "BG", "MK"),
    ("GR–MK", "GR", "MK"),
    ("GR–AL", "GR", "AL"),
    ("RS–BA", "RS", "BA"),
    ("RS–ME", "RS", "ME"),
    ("RS–MK", "RS", "MK"),
    ("ES–PT", "ES", "PT"),
    ("GB–IE (EWIC)", "GB", "IE"),
    ("DK–GB (Viking Link)", "DK_1", "GB"),
    # Newer additions — ENTSO-E area codes for MD/XK are less consistently
    # populated than core-EU zones, so these are more likely than most to
    # show up as "no data" / "failed" and fall back to simulation. Worth
    # trying anyway since they're real, if lower-priority, interconnections.
    ("MD–RO", "MD", "RO"),
    ("MD–UA", "MD", "UA_IPS"),
    ("XK–AL", "XK", "AL"),
    ("XK–ME", "XK", "ME"),
    ("XK–MK", "XK", "MK"),
    ("XK–RS", "XK", "RS"),
]

# entsoe-py methods this script tries, in order of preference, for flow data.
def fetch_flow(client, code_from, code_to, start, end):
    return client.query_crossborder_flows(code_from, code_to, start=start, end=end)

def fetch_ntc(client, code_from, code_to, start, end):
    try:
        return client.query_net_transfer_capacity_dayahead(code_from, code_to, start=start, end=end)
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", default=os.environ.get("ENTSOE_TOKEN"))
    ap.add_argument("--days", type=int, default=90, help="How many days back to fetch (max 90 to match the mockup's date picker)")
    ap.add_argument("--out", default="entsoe_data.json")
    ap.add_argument("--pace", type=float, default=2.5, help="Seconds to sleep between API calls")
    args = ap.parse_args()

    if not args.token:
        sys.exit("Provide --token YOUR_TOKEN or set ENTSOE_TOKEN env var.")

    client = EntsoePandasClient(api_key=args.token)

    end = pd.Timestamp.now(tz="Europe/Brussels").floor("h")
    start = end - pd.Timedelta(days=args.days)

    results = {}
    ok, failed = [], []

    for i, (link_id, c_from, c_to) in enumerate(BORDERS):
        print(f"[{i+1}/{len(BORDERS)}] {link_id} ({c_from} -> {c_to}) ...", end=" ", flush=True)
        try:
            flow = fetch_flow(client, c_from, c_to, start, end)
            time.sleep(args.pace)
            ntc = fetch_ntc(client, c_from, c_to, start, end)
            time.sleep(args.pace)

            if flow is None or flow.empty:
                print("no data")
                failed.append(link_id)
                continue

            # Resample to 15-minute intervals, place original values as anchors,
            # and linearly interpolate intermediate points instead of flat forward-filling.
            flow_q = flow.resample("15min").asfreq().interpolate(method="linear")

            ntc_series = []
            ntc_fallback_mean = None
            if ntc is not None and not ntc.empty:
                ntc_q = ntc.resample("15min").asfreq().interpolate(method="linear")
                for ts, val in ntc_q.items():
                    if pd.isna(val):
                        continue
                    ntc_series.append({
                        "t": ts.tz_convert("UTC").isoformat(),
                        "ntc_mw": round(float(val), 1)
                    })
                if ntc.dropna().size:
                    ntc_fallback_mean = round(float(ntc.dropna().mean()), 1)
            
            series = []
            for ts, val in flow_q.items():
                if pd.isna(val):
                    continue
                series.append({
                    "t": ts.tz_convert("UTC").isoformat(),
                    "flow_mw": round(float(val), 1)
                })

            results[link_id] = {
                "from": c_from, "to": c_to,
                "ntc_mw": ntc_fallback_mean,       # single average, used if no per-time NTC below
                "ntc_series": ntc_series,          # time-varying NTC, preferred when present
                "series": series
            }
            print(f"ok ({len(series)} 15-min points{', NTC: ' + str(len(ntc_series)) + ' pts' if ntc_series else ', no NTC data'})")
            ok.append(link_id)

        except Exception as e:
            print(f"failed: {e}")
            failed.append(link_id)
            time.sleep(args.pace)
            continue

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "days_requested": args.days,
        "links": results
    }

    with open(args.out, "w") as f:
        json.dump(payload, f)

    print("\n" + "=" * 60)
    print(f"Done. {len(ok)}/{len(BORDERS)} borders retrieved, {len(failed)} skipped.")
    if failed:
        print("Skipped (no data via API, or need a different bidding-zone code):")
        for x in failed:
            print("  -", x)
    print(f"Wrote {args.out}")
    print("Drop this file into the mockup using its 'Load real data' button.")


if __name__ == "__main__":
    main()
