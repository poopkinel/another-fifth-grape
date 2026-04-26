"""Direct-source verification — bypass the library and inspect raw XMLs.

Verifies the source-side claims in `data_source_issues.md` against a chain's
*own* published files. Takes direct .gz URLs (or local paths) for one StoresFull
and one PriceFull file, downloads + gunzips + parses the XML, and reports
findings aligned to each tracker issue.

We don't try to auto-discover URLs from portal index pages — every chain's
portal has a different UI (Shufersal uses a category dropdown, publishprice
portals embed files in <script>, others use FTP). It's faster and more reliable
for the operator to paste a direct file URL than for this tool to crawl every
portal type.

Find a file URL by hand (example for Shufersal): visit https://prices.shufersal.co.il/,
choose category=Stores or category=Price from the dropdown, copy a .gz link.
For publishprice portals (Carrefour, etc.): visit the portal and copy a link
from the table.

Usage:
    python scripts/verify_source.py STORES_URL PRICE_URL [--expected-chain-id ID] [--out DIR]
    python scripts/verify_source.py /path/to/StoresFull.gz /path/to/PriceFull.gz ...

Examples:
    python scripts/verify_source.py \\
        'https://pricesprodpublic.blob.core.windows.net/.../Stores7290027600007-001-...gz?sig=...' \\
        'https://pricesprodpublic.blob.core.windows.net/.../Price7290027600007-001-...gz?sig=...' \\
        --expected-chain-id 7290027600007 --out /tmp/shufersal-evidence
"""

import argparse
import datetime as dt
import gzip
import os
import re
import sys
import urllib.request
import urllib.error
from collections import Counter
from xml.etree import ElementTree as ET

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
)
TIMEOUT = 60


def fetch_bytes(url_or_path: str) -> bytes:
    """Local path or URL → raw bytes."""
    if os.path.exists(url_or_path):
        with open(url_or_path, "rb") as f:
            return f.read()
    req = urllib.request.Request(url_or_path, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()


def gunzip_if_needed(data: bytes, hint_name: str) -> bytes:
    if hint_name.endswith(".gz") or data[:2] == b"\x1f\x8b":
        return gzip.decompress(data)
    return data


def text_or_empty(elem, tag: str) -> str:
    if elem is None:
        return ""
    child = elem.find(tag)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def inspect_stores_xml(xml_bytes: bytes) -> dict:
    """Issue #1, #2, #3 checks against StoresFull XML."""
    tree = ET.fromstring(xml_bytes)

    chain_id = text_or_empty(tree, ".//ChainId") or text_or_empty(tree, "ChainId")
    chain_name = text_or_empty(tree, ".//ChainName") or text_or_empty(tree, "ChainName")

    sub_chains_seen: Counter = Counter()
    for sc in tree.iter("SubChain"):
        sid = text_or_empty(sc, "SubChainId")
        snm = text_or_empty(sc, "SubChainName")
        sub_chains_seen[(sid, snm)] += 1

    stores = list(tree.iter("Store"))
    n_stores = len(stores)
    n_empty_addr = sum(1 for s in stores if not text_or_empty(s, "Address"))
    n_empty_city = sum(1 for s in stores if not text_or_empty(s, "City"))

    sample = []
    for s in stores[:5]:
        sample.append({
            "store_id":     text_or_empty(s, "StoreId"),
            "store_name":   text_or_empty(s, "StoreName"),
            "address":      text_or_empty(s, "Address"),
            "city":         text_or_empty(s, "City"),
            "sub_chain_id": text_or_empty(s, "SubChainId"),
        })

    sub_chain_ids_in_stores = Counter(
        text_or_empty(s, "SubChainId") for s in stores
    )

    return {
        "root_chain_id":            chain_id,
        "root_chain_name":          chain_name,
        "subchains_distinct":       [
            {"id": sid, "name": snm, "count": cnt}
            for (sid, snm), cnt in sub_chains_seen.most_common()
        ],
        "stores_total":             n_stores,
        "stores_empty_address":     n_empty_addr,
        "stores_empty_city":        n_empty_city,
        "stores_sample":            sample,
        "store_sub_chain_id_counts": dict(sub_chain_ids_in_stores),
    }


def inspect_pricefull_xml(xml_bytes: bytes, item_limit: int = 200_000) -> dict:
    """Issue #6 (empty <ItemName>) check against PriceFull XML."""
    tree = ET.fromstring(xml_bytes)
    chain_id = text_or_empty(tree, ".//ChainId") or text_or_empty(tree, "ChainId")
    sub_chain_id = (
        text_or_empty(tree, ".//SubChainId") or text_or_empty(tree, "SubChainId")
    )

    n_items = 0
    n_empty_name = 0
    sample_empty: list[str] = []
    for it in tree.iter("Item"):
        n_items += 1
        if not text_or_empty(it, "ItemName"):
            n_empty_name += 1
            if len(sample_empty) < 5:
                sample_empty.append(text_or_empty(it, "ItemCode"))
        if n_items >= item_limit:
            break

    return {
        "root_chain_id":             chain_id,
        "root_sub_chain_id":         sub_chain_id,
        "items_inspected":           n_items,
        "items_empty_name":          n_empty_name,
        "sample_empty_item_codes":   sample_empty,
        "items_inspected_capped_at": item_limit if n_items >= item_limit else None,
    }


def fmt_pct(n: int, total: int) -> str:
    return f"{n}/{total} ({100*n/total:.2f}%)" if total else "0/0"


def render_report(
    expected_chain_id: str | None,
    stores_url: str,
    stores_summary: dict,
    price_url: str,
    price_summary: dict,
) -> str:
    when = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = []
    lines.append("# Direct-source verification")
    lines.append(f"_Run at {when}_")
    if expected_chain_id:
        lines.append(f"_Expected `<ChainId>` = `{expected_chain_id}`_")
    lines.append("")

    lines.append("## StoresFull")
    lines.append(f"- Source: `{stores_url}`")
    rcid = stores_summary["root_chain_id"]
    if expected_chain_id:
        verdict = "MATCHES" if rcid == expected_chain_id else "*** MISMATCH ***"
        lines.append(f"- Root `<ChainId>`: `{rcid}` (`{stores_summary['root_chain_name']}`) — **{verdict}**")
    else:
        lines.append(f"- Root `<ChainId>`: `{rcid}` (`{stores_summary['root_chain_name']}`)")
    if stores_summary["subchains_distinct"]:
        lines.append("- SubChains declared at root:")
        for sc in stores_summary["subchains_distinct"]:
            lines.append(f"    - id=`{sc['id']}` name=`{sc['name']}` (×{sc['count']})")
    if stores_summary["store_sub_chain_id_counts"]:
        lines.append("- Per-store SubChainId distribution:")
        for sid, cnt in sorted(stores_summary["store_sub_chain_id_counts"].items(),
                               key=lambda kv: -kv[1]):
            lines.append(f"    - `{sid or '(empty)'}` × {cnt}")
    total = stores_summary["stores_total"]
    lines.append(f"- Stores total: **{total}**")
    lines.append(f"- Empty `<Address>`: **{fmt_pct(stores_summary['stores_empty_address'], total)}** *(Issue #3)*")
    lines.append(f"- Empty `<City>`: **{fmt_pct(stores_summary['stores_empty_city'], total)}** *(Issue #2)*")
    lines.append("- First 5 stores:")
    for s in stores_summary["stores_sample"]:
        lines.append(f"    - `{s['store_id']}` / `{s['store_name']}` — "
                     f"`{s['address']}`, `{s['city']}` (sub_chain `{s['sub_chain_id']}`)")
    lines.append("")

    lines.append("## PriceFull")
    lines.append(f"- Source: `{price_url}`")
    lines.append(f"- Root `<ChainId>`: `{price_summary['root_chain_id']}`, "
                 f"`<SubChainId>`: `{price_summary['root_sub_chain_id']}`")
    n_items = price_summary["items_inspected"]
    cap = price_summary.get("items_inspected_capped_at")
    cap_note = f" (capped at {cap})" if cap else ""
    lines.append(f"- Items inspected: **{n_items}**{cap_note}")
    lines.append(f"- Empty `<ItemName>`: **{fmt_pct(price_summary['items_empty_name'], n_items)}** "
                 f"*(was Issue #6, retracted as our parser bug — non-zero here means at least some portion is genuinely source-side)*")
    if price_summary["sample_empty_item_codes"]:
        lines.append(f"  - Sample ItemCodes with empty name: `{price_summary['sample_empty_item_codes']}`")
    lines.append("")
    lines.append("## Cross-checks for Issue #1 (Carrefour-under-Yeinot-Bitan)")
    lines.append(
        f"- If root `<ChainId>` = `7290055700007` but `<SubChainName>` lists "
        f"`קרפור` / `Carrefour`, the source-side mislabel is confirmed: "
        f"the publisher (Yeinot Bitan, GS1 prefix `7290055700007`) is tagging "
        f"Carrefour-branded stores with its own ChainId, with the brand "
        f"distinction relegated to the SubChain table only."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Direct-source verification of CPFTA chain feeds.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("stores", help="StoresFull URL or local path (.gz or .xml)")
    ap.add_argument("price",  help="PriceFull URL or local path (.gz or .xml)")
    ap.add_argument("--expected-chain-id", default=None,
                    help="13-digit ChainId expected at root; flags MISMATCH if different")
    ap.add_argument("--out", default=None,
                    help="directory to save raw bytes + verification.md")
    args = ap.parse_args()

    out_dir = args.out or os.path.join(
        "/tmp",
        f"verify_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )
    os.makedirs(out_dir, exist_ok=True)

    print(f"→ Fetching stores: {args.stores}", file=sys.stderr)
    try:
        stores_raw = fetch_bytes(args.stores)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"FAIL: stores fetch: {e}", file=sys.stderr)
        return 2
    stores_xml = gunzip_if_needed(stores_raw, args.stores)
    with open(os.path.join(out_dir, "stores.xml"), "wb") as f:
        f.write(stores_xml)

    print(f"→ Fetching price:  {args.price}", file=sys.stderr)
    try:
        price_raw = fetch_bytes(args.price)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"FAIL: price fetch: {e}", file=sys.stderr)
        return 2
    price_xml = gunzip_if_needed(price_raw, args.price)
    with open(os.path.join(out_dir, "price.xml"), "wb") as f:
        f.write(price_xml)

    stores_summary = inspect_stores_xml(stores_xml)
    price_summary  = inspect_pricefull_xml(price_xml)

    report = render_report(
        args.expected_chain_id,
        args.stores, stores_summary,
        args.price,  price_summary,
    )
    report_path = os.path.join(out_dir, "verification.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n✓ Wrote {report_path}\n", file=sys.stderr)
    print(report)

    if args.expected_chain_id and stores_summary["root_chain_id"] != args.expected_chain_id:
        return 4  # mismatch
    return 0


if __name__ == "__main__":
    sys.exit(main())
