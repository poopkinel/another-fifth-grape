"""Chain definitions for the top ~10 Israeli supermarket chains."""

from il_supermarket_scarper import ScraperFactory

# Maps our internal chain_id → (ScraperFactory enum name, display name)
CHAINS: dict[str, tuple[str, str]] = {
    # Original 10 (live-portal scrape works for shufersal from this VPS;
    # the rest are geo-blocked but the live entries are kept so SCRAPE_SOURCE
    # can be flipped per-chain if a future host can reach them).
    "shufersal":             (ScraperFactory.SHUFERSAL.name,                 "שופרסל"),
    "rami_levy":             (ScraperFactory.RAMI_LEVY.name,                 "רמי לוי"),
    "victory":               (ScraperFactory.VICTORY.name,                   "ויקטורי"),
    "yohananof":             (ScraperFactory.YOHANANOF.name,                 "יוחננוף"),
    "osher_ad":              (ScraperFactory.OSHER_AD.name,                  "אושר עד"),
    "tiv_taam":              (ScraperFactory.TIV_TAAM.name,                  "טיב טעם"),
    "yeinot_bitan":          (ScraperFactory.YAYNO_BITAN_AND_CARREFOUR.name, "יינות ביתן"),
    "hazi_hinam":            (ScraperFactory.HAZI_HINAM.name,                "חצי חינם"),
    "mahsani_hashuk":        (ScraperFactory.MAHSANI_ASHUK.name,             "מחסני השוק"),
    "super_pharm":           (ScraperFactory.SUPER_PHARM.name,               "סופר-פארם"),
    # Bonus chains added via the Kaggle source. ScraperFactory entries exist
    # for all of them, so they'd also work under SCRAPE_SOURCE=live from a
    # host that can reach the chain portals. Excluded from the original 23
    # in the dataset:
    #   cofix         — Rami Levy acquired the grocery line; treated as rami_levy.
    #   het_cohen     — kaggle file present but chainid/chainname empty (corrupt).
    #   meshmat_yosef_{1,2} — fragmentary, two rows each.
    "bareket":               (ScraperFactory.BAREKET.name,                   "סופר ברקת"),
    "city_market_kiryatgat": (ScraperFactory.CITY_MARKET_KIRYATGAT.name,     "סיטי צפריר"),
    "city_market_shops":     (ScraperFactory.CITY_MARKET_SHOPS.name,         "סיטי מרקט"),
    "dor_alon":              (ScraperFactory.DOR_ALON.name,                  "דור אלון"),
    "good_pharm":            (ScraperFactory.GOOD_PHARM.name,                "גוד פארם"),
    "keshet":                (ScraperFactory.KESHET.name,                   "קשת טעמים"),
    "king_store":            (ScraperFactory.KING_STORE.name,               "קינג סטור"),
    "maayan_2000":           (ScraperFactory.MAAYAN_2000.name,              "מעיין 2000"),
    "netiv_hased":           (ScraperFactory.NETIV_HASED.name,              "נתיב החסד"),
    "polizer":               (ScraperFactory.POLIZER.name,                  "פוליצר"),
    "salach_dabach":         (ScraperFactory.SALACH_DABACH.name,            "סאלח דבאח"),
    "shefa_barcart_ashem":   (ScraperFactory.SHEFA_BARCART_ASHEM.name,      "שפע ברכת השם"),
    "shuk_ahir":             (ScraperFactory.SHUK_AHIR.name,                "שוק העיר"),
    "stop_market":           (ScraperFactory.STOP_MARKET.name,              "סטופ מרקט"),
    "super_sapir":           (ScraperFactory.SUPER_SAPIR.name,              "סופר ספיר"),
    "super_yuda":            (ScraperFactory.SUPER_YUDA.name,               "סופר יודה"),
    "wolt":                  (ScraperFactory.WOLT.name,                     "וולט מרקט"),
    "yellow":                (ScraperFactory.YELLOW.name,                   "יילו"),
    "zol_vebegadol":         (ScraperFactory.ZOL_VEBEGADOL.name,            "זול ובגדול"),
}

# Maps our chain_id → file-name stem in the Kaggle dataset
# (https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024).
# Files in the dataset are named e.g. store_file_<stem>.csv,
# price_full_file_<stem>.csv, promo_full_file_<stem>.csv.
# Chains in CHAINS but missing here have no Kaggle source — for those,
# SCRAPE_SOURCE=kaggle skips them with a warning.
KAGGLE_FILE_STEM: dict[str, str] = {
    "shufersal":             "shufersal",
    "rami_levy":             "rami_levy",
    "victory":               "victory",
    "yohananof":             "yohananof",
    "osher_ad":              "osher_ad",
    "tiv_taam":              "tiv_taam",
    "yeinot_bitan":          "yayno_bitan_and_carrefour",
    "hazi_hinam":            "hazi_hinam",
    "mahsani_hashuk":        "mahsani_ashuk",
    # super_pharm: not in the Kaggle dataset
    "bareket":               "bareket",
    "city_market_kiryatgat": "city_market_kiryatgat",
    "city_market_shops":     "city_market_shops",
    "dor_alon":              "dor_alon",
    "good_pharm":            "good_pharm",
    "keshet":                "keshet",
    "king_store":            "king_store",
    "maayan_2000":           "maayan_2000",
    "netiv_hased":           "netiv_hased",
    "polizer":               "polizer",
    "salach_dabach":         "salach_dabach",
    "shefa_barcart_ashem":   "shefa_barcart_ashem",
    "shuk_ahir":             "shuk_ahir",
    "stop_market":           "stop_market",
    "super_sapir":           "super_sapir",
    "super_yuda":            "super_yuda",
    "wolt":                  "wolt",
    "yellow":                "yellow",
    "zol_vebegadol":         "zol_vebegadol",
}


def get_scraper_names() -> list[str]:
    """Return ScraperFactory enum names for all enabled chains."""
    return [scraper_name for scraper_name, _ in CHAINS.values()]


def chain_display_name(chain_id: str) -> str:
    return CHAINS[chain_id][1]


def scraper_name_to_chain_id(scraper_name: str) -> str | None:
    """Reverse-lookup: ScraperFactory name → our chain_id."""
    for cid, (sname, _) in CHAINS.items():
        if sname == scraper_name:
            return cid
    return None
