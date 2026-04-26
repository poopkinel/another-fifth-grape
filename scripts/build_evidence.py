"""Build a complaint-grade evidence bundle from chain XML files.

Takes one or more downloaded chain feed files (.gz or .xml) plus a sidecar JSON
of fetch metadata (URL, timestamp, HTTP headers), and produces an evidence
bundle directory containing:

  bundle/
    manifest.json                     # SHA-256 of every file + fetch metadata
    README.md                         # human-readable overview + per-file index
    files/
      <chain>_<basename>.gz           # original byte-for-byte
      <chain>_<basename>.xml          # gunzipped (when applicable)
    evidence/
      <chain>_<basename>.md           # issue-by-issue findings with line numbers
                                      # and verbatim XML quotes

The evidence Markdown calls out, per file, exactly which tracker issues
(data_source_issues.md §1 / §2 / §3 / §4 / §8 / §9) are confirmed by the
content, with quoted XML lines and `lxml` line numbers.

Usage:
    python scripts/build_evidence.py FILE [FILE ...] --out BUNDLE_DIR \\
        [--metadata FILE.json]

Where each FILE is a local .gz or .xml. For Wayback Machine corroboration,
submit each source URL to https://web.archive.org/save/<url> separately and
record the resulting archive URL in the metadata JSON.

Metadata JSON shape (one entry per input file, keyed by absolute path):
    {
      "/abs/path/to/StoresFull.gz": {
        "chain":           "yeinot_bitan_carrefour",
        "url":             "https://prices.carrefour.co.il/...",
        "fetched_at":      "2026-04-26T15:32:11Z",
        "http_headers":    {"Server": "...", "Last-Modified": "..."},
        "wayback_url":     "https://web.archive.org/web/.../..."   // optional
      }
    }

If --metadata is omitted, manifest.json is still produced with hashes and
local file metadata, and per-file evidence is generated; the regulator just
won't have the fetch-time witness chain.
"""

import argparse
import dataclasses
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
from collections import Counter
from typing import Iterable

from lxml import etree  # for sourceline


# ────────────────────────────────────────────────────────────────────────────
# File handling
# ────────────────────────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def is_gzip(path: str) -> bool:
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"


def gunzip_to(src_gz: str, dst_xml: str) -> None:
    with gzip.open(src_gz, "rb") as f_in, open(dst_xml, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


# ────────────────────────────────────────────────────────────────────────────
# XML helpers (case-insensitive, namespace-stripped, line-number-aware)
# ────────────────────────────────────────────────────────────────────────────

def local(elem) -> str:
    tag = elem.tag
    if isinstance(tag, str) and "}" in tag:
        tag = tag.split("}", 1)[1]
    return tag.lower() if isinstance(tag, str) else ""


def child_text_ci(elem, tag: str) -> str:
    target = tag.lower()
    for child in elem:
        if local(child) == target:
            return (child.text or "").strip()
    return ""


def iter_local_ci(tree, *tags: str) -> Iterable:
    targets = {t.lower() for t in tags}
    for elem in tree.iter():
        if local(elem) in targets:
            yield elem


# ────────────────────────────────────────────────────────────────────────────
# Per-issue evidence functions
# ────────────────────────────────────────────────────────────────────────────

@dataclasses.dataclass
class FileFindings:
    issues_confirmed: list[str]
    issues_absent: list[str]
    sections: list[str]   # rendered Markdown sections


def _xml_excerpt(elem) -> str:
    """Serialize one element back to XML for verbatim quoting."""
    return etree.tostring(elem, pretty_print=True, encoding="unicode").rstrip()


def _ev_root_chainid(tree) -> tuple[str, str | None]:
    """Return (root chain_id, root chain_name)."""
    cid = ""
    name = ""
    for e in iter_local_ci(tree, "chainid"):
        if e.text and e.text.strip():
            cid = e.text.strip()
            break
    for e in iter_local_ci(tree, "chainname"):
        if e.text and e.text.strip():
            name = e.text.strip()
            break
    return cid, name


def _ev_carrefour_under_yb(tree, file_kind: str) -> str | None:
    """§1 — root ChainId is Yeinot Bitan's GS1 (7290055700007) but stores carry
    Carrefour-branded names. Returns Markdown section or None if not applicable.
    """
    if file_kind != "stores":
        return None
    root_cid, root_name = _ev_root_chainid(tree)
    if root_cid != "7290055700007":
        return None
    # Find stores whose StoreName mentions קרפור or carrefour
    matches = []
    for s in iter_local_ci(tree, "store"):
        sname = child_text_ci(s, "storename")
        if "קרפור" in sname or "carrefour" in sname.lower():
            matches.append({
                "line":    s.sourceline,
                "store_id": child_text_ci(s, "storeid"),
                "name":    sname,
                "address": child_text_ci(s, "address"),
                "city":    child_text_ci(s, "city"),
            })
    if not matches:
        return (
            "### §1 Carrefour-under-Yeinot-Bitan ChainId — not directly observed\n"
            f"Root `<ChainId>` is `{root_cid}` (Yeinot Bitan Ltd.'s GS1 prefix), but no\n"
            "StoreName in this file contains 'קרפור' or 'Carrefour'. The mislabel may\n"
            "still apply via SubChainName; that's recorded separately in the tracker.\n"
        )
    lines = [
        "### §1 Carrefour-under-Yeinot-Bitan ChainId — **CONFIRMED**",
        "",
        f"Root `<ChainId>` is `{root_cid}` (Yeinot Bitan Ltd.'s GS1 company prefix), but the",
        f"file lists **{len(matches)}** store(s) whose `<StoreName>` identifies them as",
        f"Carrefour-branded. The publisher (Yeinot Bitan, the legal owner since acquiring",
        f"Mega/Carrefour Israel) is using its own ChainId for stores operating under a",
        f"different consumer-facing brand. First few:",
        "",
    ]
    for m in matches[:5]:
        lines.append(
            f"- line `{m['line']}` — StoreId `{m['store_id']}` · "
            f"`{m['name']}` · `{m['address']}, {m['city']}`"
        )
    if len(matches) > 5:
        lines.append(f"- … and {len(matches) - 5} more")
    return "\n".join(lines) + "\n"


def _ev_empty_city(tree, file_kind: str) -> str | None:
    """§2 — stores with empty <City>."""
    if file_kind != "stores":
        return None
    empties = []
    total = 0
    for s in iter_local_ci(tree, "store"):
        total += 1
        if not child_text_ci(s, "city"):
            empties.append({
                "line":     s.sourceline,
                "store_id": child_text_ci(s, "storeid"),
                "name":     child_text_ci(s, "storename"),
                "address":  child_text_ci(s, "address"),
            })
    if not empties:
        return f"### §2 Empty `<City>` — none observed (0 / {total})\n"
    lines = [
        f"### §2 Empty `<City>` — **CONFIRMED**: {len(empties)} of {total} stores",
        "",
        "Stores with `<City>` empty cannot be reliably geocoded; same-named streets in",
        "different cities resolve unpredictably. First few:",
        "",
    ]
    for e in empties[:10]:
        lines.append(
            f"- line `{e['line']}` — StoreId `{e['store_id']}` · "
            f"`{e['name']}` · address `{e['address']}` · **City empty**"
        )
    if len(empties) > 10:
        lines.append(f"- … and {len(empties) - 10} more")
    return "\n".join(lines) + "\n"


def _ev_empty_address(tree, file_kind: str) -> str | None:
    if file_kind != "stores":
        return None
    empties = []
    total = 0
    for s in iter_local_ci(tree, "store"):
        total += 1
        if not child_text_ci(s, "address"):
            empties.append({
                "line":     s.sourceline,
                "store_id": child_text_ci(s, "storeid"),
                "name":     child_text_ci(s, "storename"),
                "city":     child_text_ci(s, "city"),
            })
    if not empties:
        return f"### §3 Empty `<Address>` — none observed (0 / {total})\n"
    lines = [
        f"### §3 Empty `<Address>` — **CONFIRMED**: {len(empties)} of {total} stores",
        "",
    ]
    for e in empties[:10]:
        lines.append(
            f"- line `{e['line']}` — StoreId `{e['store_id']}` · "
            f"`{e['name']}` · city `{e['city']}` · **Address empty**"
        )
    if len(empties) > 10:
        lines.append(f"- … and {len(empties) - 10} more")
    return "\n".join(lines) + "\n"


def _ev_empty_everywhere(tree, file_kind: str) -> str | None:
    """§9A — store records with every metadata field blank."""
    if file_kind != "stores":
        return None
    placeholders = []
    for s in iter_local_ci(tree, "store"):
        if (not child_text_ci(s, "storeid") and
            not child_text_ci(s, "storename") and
            not child_text_ci(s, "address") and
            not child_text_ci(s, "city")):
            placeholders.append({
                "line": s.sourceline,
                "raw":  _xml_excerpt(s),
            })
    if not placeholders:
        return None  # not flagged — no evidence
    lines = [
        f"### §9A Placeholder store record (all fields empty) — **CONFIRMED**: {len(placeholders)}",
        "",
        "Store record(s) with `StoreId`, `StoreName`, `Address`, and `City` all empty.",
        "These provide no information that lets a consumer identify or locate the store.",
        "",
    ]
    for p in placeholders[:5]:
        lines.append(f"- line `{p['line']}`:")
        lines.append("  ```xml")
        for ln in p["raw"].splitlines():
            lines.append(f"  {ln}")
        lines.append("  ```")
    return "\n".join(lines) + "\n"


def _ev_schema_deviation(tree, file_kind: str) -> str | None:
    """§9B — non-standard root / item element names in PriceFull."""
    if file_kind != "price":
        return None
    root_local = local(tree)
    item_tags: Counter = Counter()
    for it in iter_local_ci(tree, "item", "product"):
        item_tags[local(it)] += 1
    notes = []
    if root_local not in {"root", "envelope", "pricefull"}:
        notes.append(f"Root element is `<{tree.tag}>` (local name `{root_local}`); "
                     f"common convention is `<Root>` / `<Envelope>`.")
    if item_tags and "product" in item_tags and "item" not in item_tags:
        notes.append(f"Items are wrapped in `<Product>` (×{item_tags['product']}) "
                     f"instead of the more common `<Item>`.")
    if not notes:
        return None
    return "\n".join([
        "### §9B Non-standard PriceFull schema — **CONFIRMED**",
        "",
        "Mahsani-style schema deviations force every consumer of this feed to write",
        "bespoke parsing code. Observed:",
        "",
        *(f"- {n}" for n in notes),
        "",
    ]) + "\n"


def _ev_root_chainid_match(tree, file_kind: str, expected_chain_id: str | None) -> str:
    cid, name = _ev_root_chainid(tree)
    if expected_chain_id and cid and cid != expected_chain_id:
        return (
            f"### Root identification — *** MISMATCH ***\n"
            f"Expected `<ChainId>` `{expected_chain_id}`, found `{cid}` (`{name}`).\n"
        )
    return f"### Root identification\nRoot `<ChainId>`: `{cid}` (`{name}`).\n"


# ────────────────────────────────────────────────────────────────────────────
# Per-file evidence
# ────────────────────────────────────────────────────────────────────────────

def detect_kind(name: str) -> str:
    """Best-effort: 'stores' or 'price' from filename."""
    n = name.lower()
    if "stores" in n or "store" in n:
        return "stores"
    return "price"


def evidence_for_file(
    src_path: str,
    xml_path: str | None,
    file_kind: str,
    metadata: dict,
) -> FileFindings:
    """Build per-file evidence Markdown. xml_path may be None for malformed files."""
    sections: list[str] = []
    confirmed: list[str] = []
    absent: list[str] = []

    sections.append(f"# Evidence — `{os.path.basename(src_path)}`\n")
    sections.append(f"- **Source URL:** `{metadata.get('url', '(not recorded)')}`")
    sections.append(f"- **Fetched:** `{metadata.get('fetched_at', '(not recorded)')}`")
    sections.append(f"- **Bytes (compressed):** {os.path.getsize(src_path)}")
    sections.append(f"- **SHA-256 (compressed):** `{sha256_file(src_path)}`")
    if xml_path and os.path.exists(xml_path):
        sections.append(f"- **Bytes (XML):** {os.path.getsize(xml_path)}")
        sections.append(f"- **SHA-256 (XML):** `{sha256_file(xml_path)}`")
    if metadata.get("wayback_url"):
        sections.append(f"- **Wayback Machine archive:** {metadata['wayback_url']}")
    if metadata.get("http_headers"):
        sections.append("- **HTTP headers at fetch:**")
        for k, v in metadata["http_headers"].items():
            sections.append(f"    - `{k}`: `{v}`")
    sections.append(f"- **File kind (inferred):** `{file_kind}`\n")

    # Parse
    if not xml_path or not os.path.exists(xml_path):
        sections.append("## Parse status — could not gunzip\n")
        return FileFindings(confirmed, absent, sections)

    parser = etree.XMLParser(recover=False)
    try:
        tree = etree.parse(xml_path, parser).getroot()
    except etree.XMLSyntaxError as e:
        # §8 — malformed XML
        sections.append(
            "## Parse status — **MALFORMED XML AT SOURCE (§8 confirmed)**\n\n"
            f"`lxml.etree.XMLParser` (strict) raised: `{e}`\n\n"
            "This file is not parseable by any standards-compliant XML parser. "
            "Quoting the last 600 bytes of the raw file as evidence of truncation:\n\n"
            "```\n"
        )
        with open(xml_path, "rb") as f:
            f.seek(max(0, os.path.getsize(xml_path) - 600))
            tail = f.read().decode("utf-8", errors="replace")
        sections.append(tail)
        sections.append("\n```\n")
        confirmed.append("§8")
        return FileFindings(confirmed, absent, sections)

    sections.append("## Parse status — OK (lxml strict mode accepted the file)\n")

    # Issue checks
    expected = metadata.get("expected_chain_id")
    sections.append(_ev_root_chainid_match(tree, file_kind, expected))

    for fn, label in [
        (_ev_carrefour_under_yb, "§1"),
        (_ev_empty_city,         "§2"),
        (_ev_empty_address,      "§3"),
        (_ev_empty_everywhere,   "§9A"),
        (_ev_schema_deviation,   "§9B"),
    ]:
        out = fn(tree, file_kind)
        if out is None:
            absent.append(label)
            continue
        sections.append(out)
        if "**CONFIRMED**" in out:
            confirmed.append(label)
        elif "not observed" in out or "not directly observed" in out:
            absent.append(label)

    return FileFindings(confirmed, absent, sections)


# ────────────────────────────────────────────────────────────────────────────
# Bundle build
# ────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("files", nargs="+", help="local .gz or .xml chain feed files")
    ap.add_argument("--out", required=True, help="evidence bundle directory (will be created)")
    ap.add_argument("--metadata", default=None,
                    help="optional JSON file mapping abs-path → {url, fetched_at, http_headers, expected_chain_id, wayback_url}")
    args = ap.parse_args()

    bundle = os.path.abspath(args.out)
    files_dir = os.path.join(bundle, "files")
    ev_dir    = os.path.join(bundle, "evidence")
    os.makedirs(files_dir, exist_ok=True)
    os.makedirs(ev_dir,    exist_ok=True)

    metadata_db: dict = {}
    if args.metadata:
        with open(args.metadata, "r") as f:
            metadata_db = json.load(f)

    manifest_entries = []
    readme_index_rows = []

    for src in args.files:
        src_abs = os.path.abspath(src)
        meta = metadata_db.get(src_abs, {})
        chain_label = meta.get("chain") or "unknown"
        base = os.path.basename(src_abs)
        out_basename = f"{chain_label}__{base}"

        # Copy original byte-for-byte
        copied_src = os.path.join(files_dir, out_basename)
        shutil.copy2(src_abs, copied_src)

        # Decompress if gzip
        xml_out = None
        if is_gzip(copied_src):
            xml_out = copied_src.replace(".gz", "") if copied_src.endswith(".gz") else copied_src + ".xml"
            try:
                gunzip_to(copied_src, xml_out)
            except OSError as e:
                print(f"[warn] {base}: gunzip failed: {e}", file=sys.stderr)
                xml_out = None
        else:
            xml_out = copied_src   # already XML

        kind = meta.get("kind") or detect_kind(base)
        findings = evidence_for_file(copied_src, xml_out, kind, meta)

        ev_path = os.path.join(ev_dir, f"{chain_label}__{base}.md")
        with open(ev_path, "w", encoding="utf-8") as f:
            f.write("\n".join(findings.sections) + "\n")

        manifest_entries.append({
            "input_path":      src_abs,
            "bundled_compressed": os.path.relpath(copied_src, bundle),
            "bundled_xml":     (os.path.relpath(xml_out, bundle)
                                if xml_out and os.path.exists(xml_out) else None),
            "chain":           chain_label,
            "kind":            kind,
            "sha256_compressed": sha256_file(copied_src),
            "sha256_xml":      (sha256_file(xml_out)
                                if xml_out and os.path.exists(xml_out) else None),
            "metadata":        meta,
            "issues_confirmed": findings.issues_confirmed,
            "evidence_md":     os.path.relpath(ev_path, bundle),
        })
        readme_index_rows.append((chain_label, base, findings.issues_confirmed,
                                  os.path.relpath(ev_path, bundle)))

    # manifest.json
    manifest = {
        "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tool":         "scripts/build_evidence.py",
        "files":        manifest_entries,
    }
    with open(os.path.join(bundle, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    # README.md — overview + index
    readme_lines = [
        "# CPFTA chain-feed evidence bundle",
        "",
        f"_Generated {manifest['generated_at']} by `scripts/build_evidence.py`_",
        "",
        "Each file in this bundle was downloaded from a supermarket chain's CPFTA-",
        "mandated price/store transparency portal. `manifest.json` records SHA-256",
        "hashes for every file (compressed and decompressed) and any fetch metadata",
        "(URL, timestamp, HTTP headers) provided when the bundle was built. The",
        "`evidence/` directory contains a per-file Markdown report identifying which",
        "issues from `data_source_issues.md` are confirmed by the file's content,",
        "with line numbers and verbatim XML quotes.",
        "",
        "## Authenticity & re-verification",
        "",
        "- Every `.gz` is the byte-for-byte file we downloaded. Re-hashing it should",
        "  produce the SHA-256 in `manifest.json`.",
        "- Source URLs in `manifest.json` are public and belong to the chains'",
        "  publication portals. Re-fetching them should produce a current snapshot;",
        "  if the bytes differ, the chain has republished — record the new hash too,",
        "  not as evidence of tampering on our side.",
        "- For independent witness, each source URL can be submitted to the Wayback",
        "  Machine (`https://web.archive.org/save/<url>`); the resulting archive URL",
        "  belongs in the metadata JSON's `wayback_url` field.",
        "",
        "## Index",
        "",
        "| chain | file | issues confirmed | evidence |",
        "|-------|------|------------------|----------|",
    ]
    for chain, base, confirmed, ev_rel in readme_index_rows:
        confirmed_s = ", ".join(confirmed) if confirmed else "(none from this file)"
        readme_lines.append(f"| `{chain}` | `{base}` | {confirmed_s} | [{ev_rel}]({ev_rel}) |")
    with open(os.path.join(bundle, "README.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(readme_lines) + "\n")

    print(f"\n✓ bundle written to {bundle}", file=sys.stderr)
    print(f"  files:    {len(manifest_entries)}", file=sys.stderr)
    print(f"  manifest: {os.path.join(bundle, 'manifest.json')}", file=sys.stderr)
    print(f"  index:    {os.path.join(bundle, 'README.md')}",       file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
