"""Classify every cell of a parser-produced CSV against its source XML, to
distinguish RLE-masked emptiness (parser library bug) from genuine source-side
emptiness — and to surface any other parser losses or mismatches.

Background: `il_supermarket_parser`'s `XmlDataFrameConverter.reduce_size` masks
any cell equal to the previous row's cell to NaN (row-wise RLE on every column),
then `to_csv` writes NaN as empty. So in the CSV, "RLE-masked" and "source was
empty" look identical. Our pipeline's workaround is to ffill all columns; this
recovers RLE losses but mis-propagates genuinely-empty values.

This tool reads the source XML directly, replays the library's row construction
(direct children of the row element, lower-cased tags, `build_value` semantics
from utils/xml_utils.py:147), matches rows to the parsed CSV positionally
(grouped by file_name when present), and classifies each cell:

  - MATCH:           csv value == source value (non-empty)
  - GENUINE_EMPTY:   csv empty, source empty too
  - RLE_MASKED:      csv empty, source non-empty AND equal to previous row's
                     source — i.e., the library masked it; ffill will recover it
  - UNEXPECTED_LOSS: csv empty, source non-empty AND DIFFERENT from previous —
                     real parser bug, ffill will mis-recover (and propagate
                     stale prior value)
  - MISMATCH:        csv non-empty but != source value (type coercion, encoding,
                     other parser bug)
  - STRUCTURED:      source element has child structure (build_value returns a
                     dict); skipped from cell comparison

Usage:
    # Single source XML against single parsed CSV
    python scripts/verify_rle.py --xml /tmp/StoresFull-7290...-001.xml \\
        --csv /tmp/parsed/store.csv --row-tag Store

    # Whole dump folder against parsed CSV (matched by file_name column)
    python scripts/verify_rle.py --xml-dir /tmp/dumps/HaziHinam \\
        --csv /tmp/parsed/store.csv --row-tag Store --sample 10

The CSV is read with dtype=str, keep_default_na=False so empty cells stay as ""
(not NaN) — which is exactly what we need to distinguish from "missing".
"""

import argparse
import gzip
import os
import sys
from collections import Counter, defaultdict
from xml.etree import ElementTree as ET

import pandas as pd


# ───────────────────────── XML reading ─────────────────────────

def _read_xml_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        data = f.read()
    if path.endswith(".gz") or data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def _local_tag(elem) -> str:
    """Strip XML namespace, lowercase. Matches library's `name.tag.lower()`."""
    tag = elem.tag
    if isinstance(tag, str) and "}" in tag:
        tag = tag.split("}", 1)[1]
    return tag.lower() if isinstance(tag, str) else ""


def _iter_row_elements(tree, row_tag: str):
    """Yield every element whose local-name matches row_tag (case-insensitive)."""
    target = row_tag.lower()
    for elem in tree.iter():
        if _local_tag(elem) == target:
            yield elem


def _build_value(elem):
    """Mirror the library's build_value semantics for our cell-comparison.

    Library (utils/xml_utils.py:147): leaf text → name.text; if text falsy →
    no_content (we use ""); if "\\n" in text → return a nested dict.
    For verification we only care about leaf text vs. "structured" — we don't
    try to reproduce the dict's CSV repr.
    """
    text = elem.text
    if not text:
        return ""
    if "\n" in text:
        return _STRUCTURED  # sentinel
    return text


_STRUCTURED = object()


def _row_to_dict(row_elem) -> dict:
    """Direct children → {tag_lower: value-or-sentinel}.

    If the same tag appears twice as a direct child, the LAST wins, mirroring
    the library's dict-comprehension build (xml_dataframe_parser.py:139).
    """
    out: dict = {}
    for child in row_elem:
        out[_local_tag(child)] = _build_value(child)
    return out


def collect_source_rows(xml_paths: list[str], row_tag: str) -> dict[str, list[dict]]:
    """Return {file_basename: [row_dict, ...]} preserving document order."""
    by_file: dict[str, list[dict]] = {}
    for path in xml_paths:
        xml_bytes = _read_xml_bytes(path)
        try:
            tree = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            print(f"  ! parse error in {path}: {e}", file=sys.stderr)
            continue
        rows = [_row_to_dict(r) for r in _iter_row_elements(tree, row_tag)]
        # The library's `file_name` column is the original (uncompressed) name
        # — strip a single .gz suffix to match what the CSV records.
        base = os.path.basename(path)
        if base.endswith(".gz"):
            base = base[:-3]
        by_file[base] = rows
    return by_file


# ───────────────────────── classification ─────────────────────────

CATEGORIES = (
    "MATCH",
    "GENUINE_EMPTY",
    "RLE_MASKED",
    "UNEXPECTED_LOSS",
    "MISMATCH",
    "STRUCTURED",
)


def classify_cell(csv_val: str, src_val, prev_src_val) -> str:
    """Per-cell classification given current and previous-row source values.

    `prev_src_val` is None for the first row; the library's mask never fires on
    row 0 because shift() yields NaN there.
    """
    if src_val is _STRUCTURED:
        return "STRUCTURED"
    csv_empty = (csv_val == "")
    src_empty = (src_val == "")
    if csv_empty and src_empty:
        return "GENUINE_EMPTY"
    if csv_empty and not src_empty:
        if prev_src_val is _STRUCTURED:
            # Can't tell — treat as RLE-ish "expected to be reduced"
            return "RLE_MASKED"
        if prev_src_val is not None and src_val == prev_src_val:
            return "RLE_MASKED"
        return "UNEXPECTED_LOSS"
    if not csv_empty and src_empty:
        return "MISMATCH"
    return "MATCH" if csv_val == src_val else "MISMATCH"


# ───────────────────────── verification ─────────────────────────

def verify(
    csv_path: str,
    sources_by_file: dict[str, list[dict]],
    sample: int = 5,
    skip_cols: tuple[str, ...] = ("found_folder", "file_name"),
) -> dict:
    """Compare CSV to source rows; return aggregated stats + samples."""
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[])
    df.columns = [c.strip().lower() for c in df.columns]

    # Group by file_name if present so multi-file CSVs match the right XML.
    if "file_name" in df.columns:
        groups = list(df.groupby("file_name", sort=False))
    else:
        if len(sources_by_file) != 1:
            print("! CSV has no file_name column but multiple XML sources given;"
                  " can't disambiguate. Pass exactly one --xml.", file=sys.stderr)
            sys.exit(2)
        only_file = next(iter(sources_by_file))
        groups = [(only_file, df)]

    # Per-column counters across whole CSV.
    counts: dict[str, Counter] = defaultdict(Counter)
    samples: dict[str, list[dict]] = defaultdict(list)
    rowcount_mismatches: list[tuple[str, int, int]] = []
    unmatched_files: list[str] = []

    columns_to_check = [c for c in df.columns if c not in skip_cols]

    for fname, gdf in groups:
        src_rows = sources_by_file.get(fname)
        if src_rows is None:
            unmatched_files.append(fname)
            continue
        if len(gdf) != len(src_rows):
            rowcount_mismatches.append((fname, len(gdf), len(src_rows)))
            # We still verify the overlapping prefix.
        n = min(len(gdf), len(src_rows))
        gdf = gdf.iloc[:n].reset_index(drop=True)
        src_rows = src_rows[:n]

        # Per-column "previous source value" tracker.
        prev_src: dict[str, object] = {c: None for c in columns_to_check}

        for i in range(n):
            csv_row = gdf.iloc[i]
            src_row = src_rows[i]
            for col in columns_to_check:
                src_val = src_row.get(col, "")  # CSV may have cols not in XML row
                csv_val = csv_row.get(col, "")
                if csv_val is None:
                    csv_val = ""
                cat = classify_cell(csv_val, src_val, prev_src[col])
                counts[col][cat] += 1
                if (cat in ("RLE_MASKED", "UNEXPECTED_LOSS", "MISMATCH")
                        and len(samples[cat]) < sample):
                    samples[cat].append({
                        "file": fname,
                        "row": i,
                        "col": col,
                        "csv": csv_val,
                        "source": "<structured>" if src_val is _STRUCTURED else src_val,
                        "prev_source": (
                            "<structured>" if prev_src[col] is _STRUCTURED
                            else prev_src[col]
                        ),
                    })
                # Track prev only when source had a real value; RLE in the
                # library compares against post-fillna shift, so source-empty
                # also "counts" as a previous value of "". We mirror that.
                prev_src[col] = src_val

    return {
        "counts": {k: dict(v) for k, v in counts.items()},
        "samples": {k: v for k, v in samples.items()},
        "rowcount_mismatches": rowcount_mismatches,
        "unmatched_files": unmatched_files,
        "total_rows_checked": sum(
            min(len(g), len(sources_by_file.get(f, [])))
            for f, g in groups
        ),
    }


# ───────────────────────── reporting ─────────────────────────

def render_report(result: dict) -> str:
    lines: list[str] = []
    lines.append("# Parser CSV ↔ source XML verification")
    lines.append(f"_Total rows compared: {result['total_rows_checked']}_")
    lines.append("")

    if result["unmatched_files"]:
        lines.append("## CSV files with no matching source XML")
        for f in result["unmatched_files"]:
            lines.append(f"  - {f}")
        lines.append("")
    if result["rowcount_mismatches"]:
        lines.append("## Row-count mismatches (csv_rows vs xml_rows) — only overlap verified")
        for f, c, s in result["rowcount_mismatches"]:
            lines.append(f"  - {f}: csv={c} xml={s}")
        lines.append("")

    # Per-column table
    lines.append("## Per-column classification")
    header_cells = ["column"] + list(CATEGORIES) + ["total"]
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join("---" for _ in header_cells) + "|")
    cols = sorted(result["counts"].keys())
    totals = Counter()
    for col in cols:
        c = result["counts"][col]
        row = [col] + [str(c.get(cat, 0)) for cat in CATEGORIES]
        total = sum(c.get(cat, 0) for cat in CATEGORIES)
        row.append(str(total))
        for cat in CATEGORIES:
            totals[cat] += c.get(cat, 0)
        totals["total"] += total
        lines.append("| " + " | ".join(row) + " |")
    lines.append(
        "| **TOTAL** | " + " | ".join(str(totals[c]) for c in CATEGORIES)
        + f" | {totals['total']} |"
    )
    lines.append("")

    # Headline metric
    rle = totals["RLE_MASKED"]
    loss = totals["UNEXPECTED_LOSS"]
    mismatch = totals["MISMATCH"]
    genuine = totals["GENUINE_EMPTY"]
    lines.append("## Headline")
    lines.append(f"  - RLE_MASKED      : {rle}   (recovered correctly by full-column ffill)")
    lines.append(f"  - GENUINE_EMPTY   : {genuine}   (ffill will MIS-propagate these)")
    lines.append(f"  - UNEXPECTED_LOSS : {loss}   (real parser bug, not RLE — ffill propagates wrong value)")
    lines.append(f"  - MISMATCH        : {mismatch}   (csv ≠ source where both non-empty)")
    lines.append("")

    # Samples for the cases worth eyeballing
    for cat in ("RLE_MASKED", "UNEXPECTED_LOSS", "MISMATCH"):
        ss = result["samples"].get(cat, [])
        if not ss:
            continue
        lines.append(f"## Sample {cat}")
        for s in ss:
            lines.append(
                f"  - {s['file']} row={s['row']} col={s['col']}: "
                f"csv={s['csv']!r} source={s['source']!r} "
                f"prev_source={s['prev_source']!r}"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


# ───────────────────────── CLI ─────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Classify parser-CSV cells vs source XML to separate RLE "
                    "artifacts from genuine emptiness and other losses.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--xml", help="single source XML or .gz")
    g.add_argument("--xml-dir", help="dir of source XMLs (.xml/.gz, non-recursive)")
    ap.add_argument("--csv", required=True, help="parsed CSV produced by il_supermarket_parser")
    ap.add_argument("--row-tag", default="Store",
                    help="XML row element name (Store / Item / Promotion). Case-insensitive.")
    ap.add_argument("--sample", type=int, default=5,
                    help="number of sample cells to show per interesting category")
    args = ap.parse_args()

    if args.xml:
        xml_paths = [args.xml]
    else:
        xml_paths = sorted(
            os.path.join(args.xml_dir, f)
            for f in os.listdir(args.xml_dir)
            if f.endswith(".xml") or f.endswith(".gz")
        )
        if not xml_paths:
            print(f"! no .xml/.gz files in {args.xml_dir}", file=sys.stderr)
            return 2

    print(f"→ Reading {len(xml_paths)} XML file(s) (row tag = {args.row_tag})", file=sys.stderr)
    sources_by_file = collect_source_rows(xml_paths, args.row_tag)
    n_rows = sum(len(v) for v in sources_by_file.values())
    print(f"  parsed {n_rows} <{args.row_tag}> rows from source", file=sys.stderr)

    print(f"→ Reading CSV: {args.csv}", file=sys.stderr)
    result = verify(args.csv, sources_by_file, sample=args.sample)

    print(render_report(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
