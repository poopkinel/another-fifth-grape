"""Chain definitions for the top ~10 Israeli supermarket chains."""

from il_supermarket_scarper import ScraperFactory

# Maps our internal chain_id → (ScraperFactory enum name, display name)
CHAINS: dict[str, tuple[str, str]] = {
    "shufersal":    (ScraperFactory.SHUFERSAL.name,                    "שופרסל"),
    "rami_levy":    (ScraperFactory.RAMI_LEVY.name,                    "רמי לוי"),
    "victory":      (ScraperFactory.VICTORY.name,                      "ויקטורי"),
    "yohananof":    (ScraperFactory.YOHANANOF.name,                    "יוחננוף"),
    "osher_ad":     (ScraperFactory.OSHER_AD.name,                     "אושר עד"),
    "tiv_taam":     (ScraperFactory.TIV_TAAM.name,                     "טיב טעם"),
    "yeinot_bitan": (ScraperFactory.YAYNO_BITAN_AND_CARREFOUR.name,    "יינות ביתן"),
    "hazi_hinam":   (ScraperFactory.HAZI_HINAM.name,                   "חצי חינם"),
    "mahsani_hashuk": (ScraperFactory.MAHSANI_ASHUK.name,              "מחסני השוק"),
    "super_pharm":  (ScraperFactory.SUPER_PHARM.name,                  "סופר-פארם"),
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
