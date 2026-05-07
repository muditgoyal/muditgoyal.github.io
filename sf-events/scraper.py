"""
SF Events Scraper — pulls real events from multiple public sources.

Sources:
  1. SF FunCheap (RSS)
  2. Eventbrite (public search, JSON-LD)
  3. Luma SF (public API + HTML fallback)
  4. SFStation (RSS)
  5. SF.gov Open Data (Socrata API)
  6. SFJAZZ (calendar page, JSON-LD)
  7. 19hz.info (Bay Area electronic music)
  8. The Chapel SF (calendar page)
  9. Meetup (public search, JSON-LD)
  10. Do415 (RSS)
  11. SF Symphony (calendar, JSON-LD)
  12. SFMOMA (events page)

Run:  uv run scraper.py
Output: events.json (consumed by index.html)
"""

from __future__ import annotations

import json
import re
import hashlib
import concurrent.futures
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, asdict, field

import httpx
from bs4 import BeautifulSoup
import feedparser


PACIFIC = timezone(timedelta(hours=-7))  # PDT
OUTPUT = Path(__file__).parent / "events.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "music": [
        "concert", "live music", "dj set", "jazz", "band", "symphony", "opera",
        "singer", "songwriter", "album release", "beats", "electronic music",
        "hip hop", "r&b", "rock", "indie", "folk", "classical", "quartet",
        "orchestra", "recital", "sfjazz", "philharmonic", "ensemble",
    ],
    "comedy": [
        "comedy", "stand-up", "standup", "improv", "comedian", "laugh",
        "funny", "open mic comedy", "comedy show", "comedic",
    ],
    "film": [
        "film", "movie", "screening", "cinema", "documentary", "short film",
        "35mm", "matinee", "premiere", "film festival",
    ],
    "art": [
        "art", "gallery", "exhibition", "museum", "painting", "sculpture",
        "mural", "installation", "sfmoma", "de young", "contemporary art",
        "art opening", "photography exhibit",
    ],
    "tech": [
        "tech", "startup", "hackathon", "coding", "developer", "ai ",
        "machine learning", "data science", "software", "python", "javascript",
        "web3", "crypto", "blockchain", "saas", "devops",
    ],
    "food": [
        "food", "wine", "beer", "cocktail", "tasting", "dinner", "brunch",
        "chef", "culinary", "farmers market", "cooking class", "sake",
        "food truck", "pop-up dinner", "supper club", "hot dog",
    ],
    "outdoor": [
        "hike", "bike", "run ", "running", "walk", "trail", "park", "beach",
        "garden", "nature", "kayak", "sailing", "picnic", "bonfire",
        "outdoor", "sunrise", "sunset",
    ],
    "nightlife": [
        "club night", "dance party", "nightlife", "rave", "after dark",
        "late night", "drag", "disco", "house music", "techno", "dj",
        "warehouse", "dance floor", "afterparty",
    ],
    "theater": [
        "theater", "theatre", "play", "musical", "stage", "broadway",
        "off-broadway", "one-man show", "monologue", "a.c.t.",
    ],
    "culture": [
        "cultural", "heritage", "history", "festival", "community", "market",
        "fair", "parade", "street fair", "block party", "fundraiser", "benefit",
    ],
    "lectures": [
        "lecture", "talk", "author", "book", "reading", "panel", "workshop",
        "seminar", "class", "course", "speaker", "conversation with",
        "fireside chat", "q&a",
    ],
    "sports": [
        "sport", "game", "race", "tournament", "match", "baseball",
        "basketball", "soccer", "football", "giants", "warriors", "49ers",
    ],
}

SF_NEIGHBORHOODS = {
    "mission district": "Mission", "the mission": "Mission", "mission": "Mission",
    "soma": "SoMa", "south of market": "SoMa",
    "castro": "Castro", "hayes valley": "Hayes Valley",
    "north beach": "North Beach", "chinatown": "Chinatown",
    "richmond": "Richmond", "inner richmond": "Inner Richmond",
    "outer richmond": "Outer Richmond",
    "sunset": "Sunset", "inner sunset": "Inner Sunset", "outer sunset": "Outer Sunset",
    "marina": "Marina", "pacific heights": "Pacific Heights",
    "nob hill": "Nob Hill", "tenderloin": "Tenderloin",
    "financial district": "Financial District", "fidi": "Financial District",
    "embarcadero": "Embarcadero", "presidio": "Presidio",
    "golden gate park": "Golden Gate Park",
    "potrero hill": "Potrero Hill", "dogpatch": "Dogpatch",
    "civic center": "Civic Center", "union square": "Union Square",
    "haight": "Haight-Ashbury", "lower haight": "Lower Haight",
    "western addition": "Western Addition", "fillmore": "Western Addition",
    "bernal heights": "Bernal Heights", "noe valley": "Noe Valley",
    "glen park": "Glen Park", "bayview": "Bayview",
    "excelsior": "Excelsior", "japantown": "Japantown",
    "cole valley": "Cole Valley", "twin peaks": "Twin Peaks",
    "ocean beach": "Outer Sunset", "fort mason": "Marina",
    "mission bay": "Mission Bay", "mid-market": "Mid-Market",
}

VENUE_HOODS: dict[str, str] = {
    "sfjazz": "Hayes Valley", "the fillmore": "Western Addition",
    "the chapel": "Mission", "bottom of the hill": "Potrero Hill",
    "great american music hall": "Tenderloin", "the warfield": "Mid-Market",
    "roxie": "Mission", "balboa theatre": "Richmond",
    "castro theatre": "Castro", "alamo drafthouse": "Mission",
    "sfmoma": "SoMa", "de young": "Golden Gate Park",
    "asian art museum": "Civic Center", "legion of honor": "Lincoln Park",
    "yerba buena": "SoMa", "a.c.t.": "Union Square",
    "geary theater": "Union Square", "sf playhouse": "Union Square",
    "magic theatre": "Marina", "dna lounge": "SoMa",
    "public works": "Mission", "monarch": "SoMa",
    "city lights": "North Beach", "ferry building": "Embarcadero",
    "cobb's comedy": "North Beach", "punch line": "Financial District",
    "rickshaw stop": "Hayes Valley", "the independent": "Western Addition",
    "bimbo's": "North Beach", "the regency": "Mid-Market",
    "august hall": "Mid-Market", "hotel utah": "SoMa",
    "amnesia": "Mission", "the midway": "Dogpatch",
    "fort mason": "Marina", "palace of fine arts": "Marina",
    "chase center": "Mission Bay", "oracle park": "SoMa",
    "sf symphony": "Civic Center", "davies symphony": "Civic Center",
    "war memorial opera": "Civic Center", "the masonic": "Nob Hill",
}


@dataclass
class Event:
    title: str
    description: str
    venue: str
    neighborhood: str
    category: str
    start_time: str  # ISO 8601
    price: str
    url: str
    source: str
    image_url: str = ""
    id: str = field(default="")

    def __post_init__(self):
        if not self.id:
            raw = f"{self.title}|{self.start_time}|{self.venue}"
            self.id = hashlib.md5(raw.encode()).hexdigest()[:12]


# ── Helpers ──

def classify_event(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    scores: dict[str, int] = {}
    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scores[category] = score
    return max(scores, key=scores.get) if scores else "culture"


def guess_neighborhood(text: str, venue: str = "") -> str:
    v_lower = venue.lower()
    for key, hood in VENUE_HOODS.items():
        if key in v_lower:
            return hood
    lower = text.lower()
    for key, name in SF_NEIGHBORHOODS.items():
        if key in lower:
            return name
    return "San Francisco"


def clean_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text)[:300]


def extract_date_from_title(title: str) -> str | None:
    now = datetime.now(PACIFIC)
    m = re.match(r"(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?", title)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = int(m.group(3)) if m.group(3) else now.year
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day, 19, 0, tzinfo=PACIFIC).isoformat()
        except ValueError:
            pass
    m = re.match(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\s*,?\s*"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{1,2})",
        title, re.IGNORECASE,
    )
    if m:
        months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                   "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
        month = months.get(m.group(1).lower()[:3], 0)
        day = int(m.group(2))
        if month:
            try:
                return datetime(now.year, month, day, 19, 0, tzinfo=PACIFIC).isoformat()
            except ValueError:
                pass
    return None


def parse_iso_or_fallback(datestr: str, fallback: str = "") -> str:
    if not datestr:
        return fallback
    if re.match(r"\d{4}-\d{2}-\d{2}", datestr):
        return datestr
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m/%d/%y"]:
        try:
            return datetime.strptime(datestr.strip(), fmt).replace(tzinfo=PACIFIC).isoformat()
        except ValueError:
            continue
    return fallback


def extract_price(offers) -> str:
    if not offers:
        return "See listing"
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        low = offers.get("lowPrice", offers.get("price", ""))
        high = offers.get("highPrice", "")
        if low and high and str(low) != str(high):
            return f"${low}\u2013${high}"
        if low:
            return f"${low}" if str(low) != "0" else "Free"
    return "See listing"


def get_client() -> httpx.Client:
    return httpx.Client(headers=HEADERS, timeout=20, follow_redirects=True)


# ══════════════════════════════════════════════════════════════
# Source 1: SF FunCheap (RSS)
# ══════════════════════════════════════════════════════════════

def fetch_funcheap() -> list[Event]:
    print("  Fetching SF FunCheap RSS...")
    events = []
    try:
        feed = feedparser.parse("https://sf.funcheap.com/feed/")
        for entry in feed.entries[:50]:
            title = entry.get("title", "")
            desc = clean_html(entry.get("summary", ""))
            link = entry.get("link", "")

            event_date = extract_date_from_title(title)
            if not event_date:
                pub = entry.get("published_parsed")
                event_date = datetime(*pub[:6], tzinfo=PACIFIC).isoformat() if pub else datetime.now(PACIFIC).isoformat()

            price_match = re.search(r"\$\d+", f"{title} {desc}")
            price = price_match.group() if price_match else ("Free" if "free" in title.lower() else "See listing")

            image = ""
            if "media_content" in entry:
                image = entry.media_content[0].get("url", "")
            elif "enclosures" in entry and entry.enclosures:
                image = entry.enclosures[0].get("href", "")

            events.append(Event(
                title=re.sub(r"^\d+/\d+(/\d+)?:\s*", "", title),
                description=desc, venue="",
                neighborhood=guess_neighborhood(f"{title} {desc}"),
                category=classify_event(title, desc),
                start_time=event_date, price=price, url=link,
                source="FunCheap", image_url=image,
            ))
    except Exception as e:
        print(f"  [WARN] FunCheap failed: {e}")
    print(f"  Got {len(events)} from FunCheap")
    return events


# ══════════════════════════════════════════════════════════════
# Source 2: Eventbrite (public search, JSON-LD)
# ══════════════════════════════════════════════════════════════

def fetch_eventbrite() -> list[Event]:
    print("  Fetching Eventbrite SF events...")
    events = []
    try:
        with get_client() as client:
            urls = [
                "https://www.eventbrite.com/d/ca--san-francisco/events--this-week/",
                "https://www.eventbrite.com/d/ca--san-francisco/music--events--this-week/",
                "https://www.eventbrite.com/d/ca--san-francisco/food-and-drink--events--this-week/",
                "https://www.eventbrite.com/d/ca--san-francisco/arts--events--this-week/",
            ]
            for url in urls:
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, "html.parser")
                    _parse_jsonld_events(soup, events, "Eventbrite")
                    if not events:
                        _parse_eventbrite_html(soup, events)
                except Exception:
                    continue
    except Exception as e:
        print(f"  [WARN] Eventbrite failed: {e}")
    print(f"  Got {len(events)} from Eventbrite")
    return events


def _parse_jsonld_events(soup: BeautifulSoup, events: list[Event], source: str,
                          default_venue: str = "", default_hood: str = "San Francisco",
                          default_category: str = ""):
    """Generic JSON-LD Event parser, reused across sources."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and data.get("@type") == "ItemList":
                items = data.get("itemListElement", [])
            elif isinstance(data, dict) and data.get("@type") in ("Event", "MusicEvent"):
                items = [data]

            for item in items:
                evt = item.get("item", item) if "item" in item else item
                if not isinstance(evt, dict) or evt.get("@type") not in ("Event", "MusicEvent"):
                    continue

                title = evt.get("name", "")
                desc = clean_html(evt.get("description", ""))
                url = evt.get("url", "")
                start = evt.get("startDate", "")
                image = evt.get("image", "")
                if isinstance(image, list) and image:
                    image = image[0]

                location = evt.get("location", {})
                venue, neighborhood = default_venue, default_hood
                if isinstance(location, dict):
                    venue = location.get("name", "") or default_venue
                    addr = location.get("address", {})
                    if isinstance(addr, dict):
                        locality = addr.get("addressLocality", "")
                        if locality:
                            neighborhood = guess_neighborhood(f"{venue} {locality}", venue)

                category = default_category or classify_event(title, desc)

                events.append(Event(
                    title=title, description=desc, venue=venue,
                    neighborhood=neighborhood, category=category,
                    start_time=start, price=extract_price(evt.get("offers")),
                    url=url, source=source,
                    image_url=image if isinstance(image, str) else "",
                ))
        except json.JSONDecodeError:
            continue


def _parse_eventbrite_html(soup: BeautifulSoup, events: list[Event]):
    cards = soup.select(
        "[data-testid='event-card'], .search-event-card-wrapper, "
        ".eds-event-card-content, [data-testid='search-event-card']"
    )
    for card in cards[:30]:
        title_el = card.select_one("h2, h3, [data-testid='event-card-title']")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        link_el = card.select_one("a[href*='eventbrite.com/e/']")
        url = link_el["href"] if link_el else ""
        desc_el = card.select_one("[data-testid='event-card-details']")
        desc = desc_el.get_text(strip=True) if desc_el else ""
        venue_el = card.select_one("[data-testid='event-card-location']")
        venue = venue_el.get_text(strip=True) if venue_el else ""

        events.append(Event(
            title=title, description=desc, venue=venue,
            neighborhood=guess_neighborhood(f"{title} {desc} {venue}", venue),
            category=classify_event(title, desc),
            start_time=datetime.now(PACIFIC).isoformat(),
            price="See listing", url=url, source="Eventbrite",
        ))


# ══════════════════════════════════════════════════════════════
# Source 3: Luma (lu.ma)
# ══════════════════════════════════════════════════════════════

def fetch_luma() -> list[Event]:
    print("  Fetching Luma SF events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get(
                "https://api.lu.ma/public/v2/event/search",
                params={"query": "San Francisco", "pagination_limit": 50},
                headers={**HEADERS, "Accept": "application/json"},
            )
            if resp.status_code == 200 and "json" in resp.headers.get("content-type", ""):
                data = resp.json()
                for entry in data.get("entries", data.get("events", [])):
                    evt = entry.get("event", entry)
                    api_id = evt.get("api_id", "")
                    geo = evt.get("geo_address_info", {}) or {}
                    venue_name = evt.get("location_name", "") or geo.get("full_address", "")
                    events.append(Event(
                        title=evt.get("name", ""),
                        description=clean_html(evt.get("description", "")),
                        venue=venue_name,
                        neighborhood=guess_neighborhood(f"{venue_name} {geo.get('city','')}", venue_name),
                        category=classify_event(evt.get("name",""), evt.get("description","")),
                        start_time=evt.get("start_at", ""),
                        price="See listing",
                        url=f"https://lu.ma/{api_id}" if api_id else "",
                        source="Luma", image_url=evt.get("cover_url", ""),
                    ))
            else:
                resp2 = client.get("https://lu.ma/sf")
                if resp2.status_code == 200:
                    _parse_luma_html(resp2.text, events)
    except Exception as e:
        print(f"  [WARN] Luma failed: {e}")
    print(f"  Got {len(events)} from Luma")
    return events


def _parse_luma_html(html: str, events: list[Event]):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", id="__NEXT_DATA__"):
        try:
            data = json.loads(script.string)
            page_props = data.get("props", {}).get("pageProps", {})
            for key in ["initialData", "data"]:
                for entry in page_props.get(key, {}).get("entries", []):
                    evt = entry.get("event", {})
                    api_id = evt.get("api_id", "")
                    events.append(Event(
                        title=evt.get("name", ""),
                        description=clean_html(evt.get("description", "")),
                        venue="", neighborhood="San Francisco",
                        category=classify_event(evt.get("name",""), evt.get("description","")),
                        start_time=evt.get("start_at", ""), price="See listing",
                        url=f"https://lu.ma/{api_id}" if api_id else "",
                        source="Luma",
                    ))
        except json.JSONDecodeError:
            continue


# ══════════════════════════════════════════════════════════════
# Source 4: SFStation (RSS) — skip "Win Tickets" giveaway posts
# ══════════════════════════════════════════════════════════════

def fetch_sfstation() -> list[Event]:
    print("  Fetching SFStation events...")
    events = []
    try:
        feed = feedparser.parse("https://www.sfstation.com/feed")
        for entry in feed.entries[:40]:
            title = entry.get("title", "")
            if re.match(r"win tickets", title, re.IGNORECASE):
                continue
            desc = clean_html(entry.get("summary", ""))
            link = entry.get("link", "")
            pub = entry.get("published_parsed")
            iso = datetime(*pub[:6], tzinfo=PACIFIC).isoformat() if pub else datetime.now(PACIFIC).isoformat()
            image = ""
            if "media_content" in entry:
                image = entry.media_content[0].get("url", "")
            events.append(Event(
                title=title, description=desc, venue="",
                neighborhood=guess_neighborhood(f"{title} {desc}"),
                category=classify_event(title, desc),
                start_time=iso, price="See listing", url=link,
                source="SFStation", image_url=image,
            ))
    except Exception as e:
        print(f"  [WARN] SFStation failed: {e}")
    print(f"  Got {len(events)} from SFStation")
    return events


# ══════════════════════════════════════════════════════════════
# Source 5: SF.gov Open Data (Socrata API)
# ══════════════════════════════════════════════════════════════

def fetch_sfgov() -> list[Event]:
    print("  Fetching SF.gov events...")
    events = []
    try:
        today = datetime.now(PACIFIC).strftime("%Y-%m-%d")
        with get_client() as client:
            resp = client.get(
                "https://data.sfgov.org/resource/yitu-d5am.json",
                params={"$where": f"start_date >= '{today}'", "$limit": 50, "$order": "start_date ASC"},
            )
            if resp.status_code == 200:
                for item in resp.json():
                    title = item.get("title", item.get("event_name", ""))
                    if not title:
                        continue
                    venue = item.get("location", item.get("facility_name", ""))
                    desc = item.get("description", "")
                    events.append(Event(
                        title=title, description=clean_html(desc),
                        venue=venue, neighborhood=guess_neighborhood(f"{title} {venue}", venue),
                        category=classify_event(title, desc),
                        start_time=item.get("start_date", ""),
                        price="Free" if item.get("free") else "See listing",
                        url=item.get("url", ""), source="SF.gov",
                    ))
    except Exception as e:
        print(f"  [WARN] SF.gov failed: {e}")
    print(f"  Got {len(events)} from SF.gov")
    return events


# ══════════════════════════════════════════════════════════════
# Source 6: SFJAZZ (calendar page, JSON-LD)
# ══════════════════════════════════════════════════════════════

def fetch_sfjazz() -> list[Event]:
    print("  Fetching SFJAZZ events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get("https://www.sfjazz.org/events/")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                _parse_jsonld_events(soup, events, "SFJAZZ",
                                     default_venue="SFJAZZ Center",
                                     default_hood="Hayes Valley",
                                     default_category="music")
                if not events:
                    for card in soup.select(".event-card, .performance-card, [class*='event']")[:20]:
                        title_el = card.select_one("h2, h3, .event-title, .event-card__title")
                        if not title_el:
                            continue
                        link_el = card.select_one("a[href]")
                        href = link_el["href"] if link_el else ""
                        url = href if href.startswith("http") else f"https://www.sfjazz.org{href}"
                        date_el = card.select_one(".event-date, time, [datetime]")
                        start = ""
                        if date_el:
                            start = date_el.get("datetime", "") or parse_iso_or_fallback(date_el.get_text(strip=True))
                        events.append(Event(
                            title=title_el.get_text(strip=True), description="",
                            venue="SFJAZZ Center", neighborhood="Hayes Valley",
                            category="music", start_time=start,
                            price="See listing", url=url, source="SFJAZZ",
                        ))
    except Exception as e:
        print(f"  [WARN] SFJAZZ failed: {e}")
    print(f"  Got {len(events)} from SFJAZZ")
    return events


# ══════════════════════════════════════════════════════════════
# Source 7: 19hz.info (Bay Area electronic music)
# ══════════════════════════════════════════════════════════════

def fetch_19hz() -> list[Event]:
    print("  Fetching 19hz.info events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get("https://19hz.info/eventlisting_BayArea.php")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for row in soup.select("tr")[:80]:
                    cells = row.find_all("td")
                    if len(cells) < 4:
                        continue
                    date_text = cells[0].get_text(strip=True)
                    title = cells[1].get_text(strip=True)
                    venue_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    location_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                    full_loc = f"{venue_text} {location_text}".lower()
                    if not any(kw in full_loc for kw in [
                        "san francisco", " sf", "soma", "mission", "hayes",
                        "castro", "dogpatch", "potrero",
                    ]):
                        continue

                    link_el = cells[1].select_one("a[href]")
                    url = link_el["href"] if link_el else ""
                    start = parse_iso_or_fallback(date_text) or extract_date_from_title(date_text) or ""

                    events.append(Event(
                        title=title, description="Electronic music event",
                        venue=venue_text, neighborhood=guess_neighborhood(full_loc, venue_text),
                        category="nightlife", start_time=start,
                        price="See listing", url=url, source="19hz",
                    ))
    except Exception as e:
        print(f"  [WARN] 19hz failed: {e}")
    print(f"  Got {len(events)} from 19hz")
    return events


# ══════════════════════════════════════════════════════════════
# Source 8: The Chapel SF (calendar)
# ══════════════════════════════════════════════════════════════

def fetch_the_chapel() -> list[Event]:
    print("  Fetching The Chapel SF events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get("https://www.thechapelsf.com/calendar/")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                _parse_jsonld_events(soup, events, "The Chapel",
                                     default_venue="The Chapel",
                                     default_hood="Mission",
                                     default_category="music")
                if not events:
                    for card in soup.select(".event-listing, .tw-section, [class*='event']")[:20]:
                        title_el = card.select_one("h1, h2, h3, .event-name, .tw-name")
                        if not title_el:
                            continue
                        link_el = card.select_one("a[href]")
                        href = link_el["href"] if link_el else ""
                        url = href if href.startswith("http") else f"https://www.thechapelsf.com{href}"
                        date_el = card.select_one(".event-date, time, .tw-event-date")
                        start = ""
                        if date_el:
                            start = date_el.get("datetime", "") or parse_iso_or_fallback(date_el.get_text(strip=True))
                        price_el = card.select_one(".event-cost, .tw-price")
                        events.append(Event(
                            title=title_el.get_text(strip=True), description="",
                            venue="The Chapel", neighborhood="Mission",
                            category="music", start_time=start,
                            price=price_el.get_text(strip=True) if price_el else "See listing",
                            url=url, source="The Chapel",
                        ))
    except Exception as e:
        print(f"  [WARN] The Chapel failed: {e}")
    print(f"  Got {len(events)} from The Chapel")
    return events


# ══════════════════════════════════════════════════════════════
# Source 9: Meetup (public search, JSON-LD)
# ══════════════════════════════════════════════════════════════

def fetch_meetup() -> list[Event]:
    print("  Fetching Meetup SF events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get(
                "https://www.meetup.com/find/",
                params={"location": "us--ca--San Francisco", "source": "EVENTS"},
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                _parse_jsonld_events(soup, events, "Meetup")
                if not events:
                    for card in soup.select("[data-testid='categoryResults-eventCard'], .event-listing")[:20]:
                        title_el = card.select_one("h2, h3, [data-testid='event-name']")
                        if not title_el:
                            continue
                        link_el = card.select_one("a[href*='meetup.com/']")
                        events.append(Event(
                            title=title_el.get_text(strip=True), description="",
                            venue="", neighborhood="San Francisco",
                            category=classify_event(title_el.get_text(strip=True), ""),
                            start_time=datetime.now(PACIFIC).isoformat(),
                            price="Free", url=link_el["href"] if link_el else "",
                            source="Meetup",
                        ))
    except Exception as e:
        print(f"  [WARN] Meetup failed: {e}")
    print(f"  Got {len(events)} from Meetup")
    return events


# ══════════════════════════════════════════════════════════════
# Source 10: Do415 (RSS)
# ══════════════════════════════════════════════════════════════

def fetch_do415() -> list[Event]:
    print("  Fetching Do415 events...")
    events = []
    try:
        feed = feedparser.parse("https://do415.com/feed")
        if not feed.entries:
            feed = feedparser.parse("https://do415.com/rss")
        for entry in feed.entries[:40]:
            title = entry.get("title", "")
            desc = clean_html(entry.get("summary", ""))
            link = entry.get("link", "")
            pub = entry.get("published_parsed")
            iso = datetime(*pub[:6], tzinfo=PACIFIC).isoformat() if pub else (
                extract_date_from_title(title) or datetime.now(PACIFIC).isoformat()
            )
            events.append(Event(
                title=title, description=desc, venue="",
                neighborhood=guess_neighborhood(f"{title} {desc}"),
                category=classify_event(title, desc),
                start_time=iso, price="See listing", url=link, source="Do415",
            ))
    except Exception as e:
        print(f"  [WARN] Do415 failed: {e}")
    print(f"  Got {len(events)} from Do415")
    return events


# ══════════════════════════════════════════════════════════════
# Source 11: SF Symphony
# ══════════════════════════════════════════════════════════════

def fetch_sf_symphony() -> list[Event]:
    print("  Fetching SF Symphony events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get("https://www.sfsymphony.org/Buy-Tickets")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                _parse_jsonld_events(soup, events, "SF Symphony",
                                     default_venue="Davies Symphony Hall",
                                     default_hood="Civic Center",
                                     default_category="music")
    except Exception as e:
        print(f"  [WARN] SF Symphony failed: {e}")
    print(f"  Got {len(events)} from SF Symphony")
    return events


# ══════════════════════════════════════════════════════════════
# Source 12: SFMOMA (events page)
# ══════════════════════════════════════════════════════════════

def fetch_sfmoma() -> list[Event]:
    print("  Fetching SFMOMA events...")
    events = []
    try:
        with get_client() as client:
            resp = client.get("https://www.sfmoma.org/events/")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                _parse_jsonld_events(soup, events, "SFMOMA",
                                     default_venue="SFMOMA",
                                     default_hood="SoMa",
                                     default_category="art")
                if not events:
                    for card in soup.select(".card--event, .event-card, [class*='event-listing']")[:15]:
                        title_el = card.select_one("h2, h3, .card__title")
                        if not title_el:
                            continue
                        link_el = card.select_one("a[href]")
                        href = link_el["href"] if link_el else ""
                        url = href if href.startswith("http") else f"https://www.sfmoma.org{href}"
                        events.append(Event(
                            title=title_el.get_text(strip=True), description="",
                            venue="SFMOMA", neighborhood="SoMa",
                            category="art", start_time="",
                            price="See listing", url=url, source="SFMOMA",
                        ))
    except Exception as e:
        print(f"  [WARN] SFMOMA failed: {e}")
    print(f"  Got {len(events)} from SFMOMA")
    return events


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

ALL_SOURCES = [
    ("FunCheap", fetch_funcheap),
    ("Eventbrite", fetch_eventbrite),
    ("Luma", fetch_luma),
    ("SFStation", fetch_sfstation),
    ("SF.gov", fetch_sfgov),
    ("SFJAZZ", fetch_sfjazz),
    ("19hz", fetch_19hz),
    ("The Chapel", fetch_the_chapel),
    ("Meetup", fetch_meetup),
    ("Do415", fetch_do415),
    ("SF Symphony", fetch_sf_symphony),
    ("SFMOMA", fetch_sfmoma),
]


def dedup_events(events: list[Event]) -> list[Event]:
    seen: set[str] = set()
    unique = []
    for e in events:
        key = re.sub(r"\W+", "", e.title.lower())[:50]
        if key not in seen and len(key) > 3:
            seen.add(key)
            unique.append(e)
    return unique


def filter_future_events(events: list[Event]) -> list[Event]:
    yesterday = (datetime.now(PACIFIC) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    cutoff = yesterday.isoformat()[:10]
    result = []
    for e in events:
        if not e.start_time:
            continue
        try:
            if e.start_time[:10] >= cutoff:
                result.append(e)
        except (TypeError, ValueError):
            result.append(e)
    return result


def main():
    print("SF Events Scraper")
    print("=" * 50)
    print(f"  Time: {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"  Sources: {len(ALL_SOURCES)}")
    print()

    all_events: list[Event] = []

    # Fetch from all sources concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_name = {
            executor.submit(fetcher): name for name, fetcher in ALL_SOURCES
        }
        for future in concurrent.futures.as_completed(future_to_name):
            name = future_to_name[future]
            try:
                all_events.extend(future.result())
            except Exception as e:
                print(f"  [ERROR] {name} crashed: {e}")

    all_events = dedup_events(all_events)
    all_events = filter_future_events(all_events)
    all_events.sort(key=lambda e: e.start_time or "9999")

    by_source: dict[str, int] = {}
    for e in all_events:
        by_source[e.source] = by_source.get(e.source, 0) + 1
    print("\nResults by source:")
    for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {source}: {count}")
    print(f"\nTotal unique future events: {len(all_events)}")

    output = {
        "scraped_at": datetime.now(PACIFIC).isoformat(),
        "event_count": len(all_events),
        "sources": sorted(by_source.keys()),
        "events": [asdict(e) for e in all_events],
    }
    OUTPUT.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"Written to {OUTPUT}")


if __name__ == "__main__":
    main()
