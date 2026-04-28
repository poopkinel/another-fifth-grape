"""Patch il_supermarket_parser's lossy RLE compression so RLE-masked cells
are distinguishable from genuinely-empty source cells in the parsed CSVs.

`XmlDataFrameConverter.reduce_size` in the upstream library row-wise-RLE-
compresses every column (xml_dataframe_parser.py:17-28): cells equal to the
previous row's cell become NaN, then `to_csv` writes NaN with no `na_rep` so
they render as empty cells — indistinguishable from cells whose source XML
was actually empty.

This patch swaps the NaN replacement for an explicit sentinel string. After
the patch:
    - empty CSV cell      → source XML was genuinely empty
    - sentinel CSV cell   → RLE-masked (source had a value equal to prev row)

Recovery in CSV consumers:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.replace(RLE_SENTINEL, pd.NA).ffill()
ffill only fills the sentinels (now NaN), leaving genuine "" cells untouched.

Apply once at import time of any module that runs the parser. Idempotent.

Drop this patch when upstream il_supermarket_parser starts writing CSVs with
a non-empty `na_rep` (or otherwise distinguishes masked-vs-source-empty).
"""

from il_supermarket_parsers.documents.xml_dataframe_parser import (
    XmlDataFrameConverter,
)

# U+E000 is in the Unicode Private Use Area — guaranteed never to appear in
# legitimate text. The "RLE" letters in the middle keep it greppable in raw
# CSVs and logs.
RLE_SENTINEL = "RLE"

_ORIGINAL_REDUCE_SIZE = XmlDataFrameConverter.reduce_size
_PATCHED = False


def _reduce_size_with_sentinel(self, data):
    """Drop-in replacement for XmlDataFrameConverter.reduce_size that emits
    RLE_SENTINEL where the original would have emitted NaN."""
    if len(data) == 0:
        return data
    data = data.fillna("", inplace=False)
    for col in data.columns:
        if data[col].notna().any():
            data[col] = data[col].mask(
                data[col] == data[col].shift(),
                other=RLE_SENTINEL,
            )
    return data


def apply_parser_patch() -> None:
    """Replace XmlDataFrameConverter.reduce_size with the sentinel version.
    Safe to call multiple times."""
    global _PATCHED
    if _PATCHED:
        return
    XmlDataFrameConverter.reduce_size = _reduce_size_with_sentinel
    _PATCHED = True
