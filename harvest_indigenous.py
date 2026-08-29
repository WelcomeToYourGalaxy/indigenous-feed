#!/usr/bin/env python3
"""
harvest_indigenous.py — the invasion wire for Indigenous peoples: territory,
rights, settler-state policy, and what is done to homelands, worldwide.

Self-contained: fetching, feed parsing, word-edge matching and deduplication are
all in this file. Reads sources_invasion.json, writes wire_invasion.json.
Standard library only — no dependencies, no API keys, no model calls.

Two directions, one feed. Ground is taken — concessions, roads, dams, mines,
settlement, eviction, contamination, criminalised protest — and ground is
returned or held: titling, demarcation, treaty settlements, court wins,
co-management, repatriation. Both are marked, and either can be read alone.

The scope is territory and the peoples whose territory it is: Indigenous,
tribal, First Nations, Aboriginal and traditional communities anywhere,
including peoples living in isolation. Environmental harm counts when it lands
on a homeland — a spill in a river a community drinks from is this feed's
business, a spill elsewhere is not.

    python3 harvest_indigenous.py
    python3 harvest_indigenous.py --dry-run
    python3 harvest_indigenous.py --fixtures DIR
"""

import argparse
import gzip
import html
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES_PATH = os.path.join(HERE, "sources_indigenous.json")
OUT_PATH = os.path.join(HERE, "wire_indigenous.json")

RETAIN_DAYS = 45
MAX_ITEMS = 1200
WORKERS = 10         # a few hundred wires now
NOTABLE_SCORE = 3       # at or above this a story is marked as well documented

# --------------------------------------------------------------------------
# Plumbing: fetching, feed parsing, word-edge matching, fingerprints.
# --------------------------------------------------------------------------
USER_AGENT = ("Mozilla/5.0 (compatible; space-life-news/1.0; "
              "+https://github.com/WelcomeToYourGalaxy/space-life-news)")

TIMEOUT = 25

SNIPPET_CHARS = 240

TAG_RE = re.compile(r"<[^>]+>")

WS_RE = re.compile(r"\s+")

PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)

def build_gnews_url(loc):
    # the wire keeps 45 days, so ask the search for the same span rather than 30
    q = loc["query"] + " when:45d"
    return ("https://news.google.com/rss/search?q=" + urllib.parse.quote(q) +
            "&hl=" + loc["hl"] + "&gl=" + loc["gl"] + "&ceid=" + loc["ceid"])

def fetch(url, tries=3):
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
                "Accept-Encoding": "gzip",
                "Accept-Language": "*",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return raw
        except Exception as exc:                       # noqa: BLE001 — report, don't crash the run
            last = exc
            time.sleep(1.5 * (attempt + 1))
    print("  ! unreachable: %s (%s)" % (url[:90], last), file=sys.stderr)
    return None

def strip_ns(tag):
    return tag.split("}", 1)[1] if "}" in tag else tag

def text_of(el):
    return WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", el.text or ""))).strip() if el is not None else ""

def child(node, *names):
    for kid in node:
        if strip_ns(kid.tag) in names:
            return kid
    return None

def parse_date(raw):
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None

def parse_feed(raw, src):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # Some publishers serve a stray byte before the declaration.
        try:
            root = ET.fromstring(raw[raw.index(b"<"):])
        except Exception:  # noqa: BLE001
            return []

    nodes = [n for n in root.iter() if strip_ns(n.tag) == "item"]
    atom = False
    if not nodes:
        nodes = [n for n in root.iter() if strip_ns(n.tag) == "entry"]
        atom = True

    out = []
    for n in nodes:
        title = text_of(child(n, "title"))
        if atom:
            link = ""
            for kid in n:
                if strip_ns(kid.tag) == "link" and kid.get("rel", "alternate") == "alternate":
                    link = kid.get("href", "")
                    break
        else:
            link_el = child(n, "link")
            link = (link_el.text or "").strip() if link_el is not None else ""
            if not link:
                link = text_of(child(n, "guid"))
        if not title or not link:
            continue

        outlet_el = child(n, "source")
        outlet = text_of(outlet_el) if outlet_el is not None else ""
        if outlet and title.endswith(" - " + outlet):
            title = title[: -(len(outlet) + 3)].strip()
        elif not outlet and src["name"].startswith("Google News") and " - " in title:
            # Google News appends the outlet to the headline when it omits <source>.
            head, _, tail = title.rpartition(" - ")
            if head and 2 <= len(tail) <= 45:
                title, outlet = head.strip(), tail.strip()

        stamp = parse_date(text_of(child(n, "pubDate", "published", "updated", "date")))
        snippet = text_of(child(n, "description", "summary", "content"))[:SNIPPET_CHARS]

        out.append({
            "t": title,
            "u": link,
            "o": outlet or src["name"].replace("Google News · ", ""),
            "g": src["lang"],
            "r": src["region"],
            "k": src.get("kind", "news"),
            "d": stamp,
            "s": snippet,
            "w": src["name"],
        })
    return out

def _compile(term):
    if any(ord(ch) > 0x24F for ch in term):        # non-Latin script
        # substring matching is already prefix-like in scripts without word
        # breaks, so a trailing * is a no-op — strip it rather than search for
        # a literal asterisk, which is what used to happen.
        return term[:-1] if term.endswith("*") else term
    if term.endswith("*"):
        return re.compile(r"(?<![a-z0-9])" + re.escape(term[:-1]) + r"[a-z0-9\-]*", re.I)
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", re.I)

def _compile_all(terms):
    return [_compile(t) for t in terms]

def hit(text, compiled):
    """True when any compiled term matches."""
    for c in compiled:
        if isinstance(c, str):
            if c in text:
                return True
        elif c.search(text):
            return True
    return False

def fingerprint(title):
    norm = PUNCT_RE.sub(" ", title.lower())
    return " ".join(WS_RE.sub(" ", norm).strip().split()[:9])

def canon_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parts.query)
        query = [(k, v) for k, v in query if not k.lower().startswith("utm_")]
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"),
                                        urllib.parse.urlencode(query), ""))
    except Exception:  # noqa: BLE001
        return url


# --------------------------------------------------------------------------
# Where the story is.  This is the region the finding concerns, not the region
# the wire was read from — a Japanese outlet reporting on the Amazon files
# under Latin America.  A story with global scope files under Global, and one
# can carry several: a study spanning Africa and South Asia files under both.
# --------------------------------------------------------------------------
GEO = [
    ("africa", "Africa", [
        ("africa*", None), ("sahel", None), ("congo basin", None), ("nigeria*", None),
        ("kenya*", None), ("ethiopia*", None), ("democratic republic of congo", None),
        ("drc", None), ("ghana", None), ("tanzania*", None), ("uganda*", None),
        ("south africa*", None), ("zimbabwe*", None), ("zambia*", None), ("mozambique", None),
        ("angola*", None), ("senegal", None), ("mali", ["africa", "sahel", "bamako", "drought"]),
        ("chad", ["lake", "africa", "sahel", "basin"]), ("sudan*", None), ("somalia*", None),
        ("madagascar", None), ("cameroon", None), ("côte d'ivoire", None), ("ivory coast", None),
        ("botswana", None), ("namibia", None), ("malawi", None), ("rwanda", None),
        ("okavango", None), ("lake victoria", None), ("serengeti", None), ("kalahari", None),
        ("horn of africa", None), ("afrique", None), ("áfrica", None), ("afrika", None),
        ("非洲", None), ("アフリカ", None), ("африк*", None), ("أفريقيا", None), ("अफ्रीका", None),
    ]),
    ("mena", "Middle East & North Africa", [
        ("middle east*", None), ("egypt*", None), ("morocco", None), ("algeria*", None),
        ("tunisia*", None), ("libya*", None), ("saudi arabia", None), ("emirates", None),
        ("qatar", None), ("kuwait", None), ("oman", None), ("yemen*", None), ("iraq*", None),
        ("iran*", None), ("israel*", None), ("palestin*", None), ("gaza", None), ("jordan", None),
        ("lebanon", None), ("syria*", None), ("turkey", ["drought", "climate", "pollution", "earthquake", "istanbul", "anatolia"]),
        ("türkiye", None), ("persian gulf", None), ("red sea", None), ("euphrates", None),
        ("tigris", None), ("dead sea", None), ("sahara", None), ("الشرق الأوسط", None),
        ("中东", None), ("北アフリカ", None),
    ]),
    ("asia", "Asia", [
        ("asia*", None), ("china", None), ("chinese", ["government", "province", "coal", "emissions", "cities"]),
        ("japan*", None), ("korea*", None), ("india", None), ("indian", ["ocean", "government", "farmers", "cities", "monsoon", "state"]),
        ("pakistan*", None), ("bangladesh*", None), ("nepal*", None), ("sri lanka", None),
        ("indonesia*", None), ("vietnam*", None), ("thailand", None), ("philippines", None),
        ("malaysia*", None), ("myanmar", None), ("cambodia*", None), ("laos", None),
        ("mongolia*", None), ("kazakhstan", None), ("uzbekistan", None), ("central asia", None),
        ("himalaya*", None), ("mekong", None), ("ganges", None), ("yangtze", None),
        ("brahmaputra", None), ("tibet*", None), ("borneo", None), ("sumatra", None),
        ("aral sea", None), ("gobi", None), ("siberia*", None), ("アジア", None), ("亚洲", None),
        ("아시아", None), ("एशिया", None), ("азия", None),
    ]),
    ("europe", "Europe", [
        ("europe*", ["union", "countries", "climate", "commission", "continent", "wide", "study", "across"]),
        ("european union", None), ("european commission", None), ("brussels", None),
        ("eu", ["deforestation", "regulation", "law", "directive", "commission", "member states",
                "emissions", "green deal", "farm", "policy", "ban", "target"]),
        ("united kingdom", None), ("britain", None), ("england", None),
        ("scotland", None), ("wales", ["climate", "flood", "farm", "coast"]), ("ireland", None),
        ("france", None), ("germany", None), ("spain", None), ("portugal", None), ("italy", None),
        ("greece", None), ("netherlands", None), ("belgium", None), ("poland", None),
        ("ukraine", None), ("russia*", None), ("sweden", None), ("norway", None), ("finland", None),
        ("denmark", None), ("switzerland", None), ("austria", None), ("romania", None),
        ("hungary", None), ("czech*", None), ("balkans", None), ("danube", None), ("alps", None),
        ("mediterranean", None), ("baltic", None), ("北欧", None), ("欧洲", None), ("ヨーロッパ", None),
        ("유럽", None), ("европ*", None), ("أوروبا", None),
    ]),
    ("latam", "Latin America & Caribbean", [
        ("latin america*", None), ("south america*", None), ("central america*", None),
        ("brazil*", None), ("brasil", None), ("amazon", None), ("amazônia", None), ("amazonía", None),
        ("argentina", None), ("chile", None), ("peru", None), ("colombia*", None),
        ("venezuela*", None), ("ecuador", None), ("bolivia*", None), ("paraguay", None),
        ("uruguay", None), ("mexico", None), ("méxico", None), ("guatemala", None),
        ("honduras", None), ("nicaragua", None), ("costa rica", None), ("panama", None),
        ("cuba", None), ("haiti", None), ("dominican republic", None), ("caribbean", None),
        ("patagonia", None), ("andes", None), ("cerrado", None), ("pantanal", None),
        ("gran chaco", None), ("orinoco", None), ("américa latina", None), ("拉丁美洲", None),
        ("ラテンアメリカ", None), ("латинская америка", None),
    ]),
    ("northam", "North America", [
        ("united states", None), ("u.s.", None), ("usa", None), ("american", ["government", "cities", "states", "west", "farmers", "midwest", "coast"]),
        ("canada", None), ("canadian", None), ("alaska*", None), ("california", None),
        ("texas", None), ("florida", None), ("great lakes", None), ("colorado river", None),
        ("mississippi", None), ("appalachia*", None), ("quebec", None), ("ontario", None),
        ("british columbia", None), ("gulf of mexico", None), ("états-unis", None),
        ("estados unidos", None), ("美国", None), ("加拿大", None), ("アメリカ合衆国", None),
        ("미국", None), ("сша", None),
    ]),
    ("oceania", "Oceania", [
        ("australia*", None), ("new zealand", None), ("aotearoa", None), ("papua", None),
        ("pacific island*", None), ("fiji", None), ("samoa", None), ("tonga", None),
        ("vanuatu", None), ("solomon islands", None), ("kiribati", None), ("tuvalu", None),
        ("great barrier reef", None), ("tasmania*", None), ("murray-darling", None),
        ("オセアニア", None), ("大洋洲", None), ("océanie", None),
    ]),
    ("polar", "Arctic & Antarctic", [
        ("arctic", None), ("antarctic*", None), ("greenland", None), ("svalbard", None),
        ("north pole", None), ("south pole", None), ("tundra", None), ("北極", None),
        ("南極", None), ("арктик*", None), ("antártic*", None), ("arctique", None),
    ]),
    ("ocean", "Oceans & high seas", [
        ("pacific ocean", None), ("atlantic ocean", None), ("indian ocean", None),
        ("southern ocean", None), ("high seas", None), ("open ocean", None),
        ("coral triangle", None), ("mariana", None), ("deep sea", None), ("north sea", None),
        ("bering sea", None), ("south china sea", None), ("océan pacifique", None),
        ("公海", None), ("深海", None),
    ]),
]


# --------------------------------------------------------------------------
# Subjects
# --------------------------------------------------------------------------
TOPICS = [
    ("territory", "Territory & titling", [
        ("land title*", None), ("titling", None), ("demarcat*", None), ("land claim*", None),
        ("native title", None), ("ancestral domain", None), ("ancestral land*", None),
        ("customary land", None), ("traditional territor*", None), ("land back", None),
        ("land returned", None), ("reserve boundar*", None), ("communal title", None),
        ("terra indígena", None), ("demarcação", None), ("tierras ancestrales", None),
        ("titulación", ["comunidad", "indígena", "nativa"]), ("territorio indígena", None),
        ("hutan adat", None), ("tanah adat", None), ("传统领域", None), ("傳統領域", None),
        ("территори", ["коренн", "традиционного природопользования"]),
    ]),
    ("extraction", "Extraction & infrastructure", [
        ("mining", ["indigenous", "territory", "reserve", "ancestral", "adat", "tribal", "community land"]),
        ("miner*", ["illegal", "indigenous", "territory", "reserve", "invade", "yanomami", "garimpo"]),
        ("mine", ["indigenous", "territory", "sacred", "traditional owners", "adivasi"]),
        ("oil block*", None), ("drilling", ["indigenous", "territory", "reserve", "ancestral"]),
        ("pipeline", ["first nations", "indigenous", "treaty", "reserve", "territory"]),
        ("logging", ["indigenous", "territory", "customary", "adat", "reserve"]),
        ("dam", ["indigenous", "displac", "territory", "flood", "resettle"]),
        ("plantation", ["customary", "adat", "indigenous", "community land"]),
        ("garimpo", None), ("orpaillage", ["autochtone", "guyane"]),
        ("concession*", ["indigenous", "customary", "adat", "ancestral", "community"]),
        ("carretera", ["indígena", "territorio"]), ("rodovia", ["indígena", "terra"]),
    ]),
    ("law", "Courts, treaties & policy", [
        ("court", ["indigenous", "native title", "tribal", "ancestral", "aboriginal", "first nations"]),
        ("ruling", ["indigenous", "native title", "tribal", "land rights"]),
        ("treaty", ["nation", "indigenous", "waitangi", "rights", "settlement"]),
        ("waitangi", None), ("marco temporal", None), ("consulta previa", None),
        ("case", ["court", "supreme", "tribunal", "rights", "land", "appeal"]),
        ("tribunal", ["waitangi", "indigenous", "land", "rights"]),
        ("ilo 169", None), ("undrip", None), ("declaration on the rights of indigenous", None),
        ("free prior and informed consent", None), ("fpic", None),
        ("forest rights act", None), ("gram sabha", None), ("ipra", ["law", "philippines"]),
        ("self-determination agreement", None), ("truth and reconciliation", None),
        ("sentencia", ["indígena", "consulta"]), ("fallo", ["indígena", "consulta"]),
        ("inter-american court", None), ("african court", ["ogiek", "indigenous"]),
    ]),
    ("defenders", "Defenders & criminalisation", [
        ("land defender*", None), ("environmental defender*", None),
        ("killed", ["defender", "indigenous leader", "activist", "community leader"]),
        ("murder*", ["defender", "indigenous", "leader"]), ("assassinat*", ["líder", "defensor", "indígena"]),
        ("criminalis*", ["protest", "indigenous", "defender"]), ("criminaliz*", ["protest", "indigenous", "defender"]),
        ("arrested", ["protest", "blockade", "indigenous", "land"]),
        ("blockade", ["indigenous", "first nations", "territory", "pipeline"]),
        ("threats", ["defender", "indigenous", "community leader"]),
        ("global witness", None), ("front line defenders", None),
    ]),
    ("isolation", "Peoples in isolation", [
        ("uncontacted", None), ("isolated peoples", None), ("voluntary isolation", None),
        ("first contact", ["tribe", "people", "indigenous"]), ("aislamiento voluntario", None),
        ("pueblos en aislamiento", None), ("indígenas isolados", None), ("povos isolados", None),
        ("pia*", ["aislamiento"]), ("no contactados", None),
    ]),
    ("environment", "Homelands & environment", [
        ("contaminat*", ["indigenous", "community", "river*", "reserve", "territory"]),
        ("mercury", ["indigenous", "river*", "community", "fish", "territory", "mining"]),
        ("oil spill", ["indigenous", "community", "territory", "river"]),
        ("water contamination", None), ("boil water advisory", None),
        ("deforestation", ["indigenous", "territory", "customary", "adat"]),
        ("climate relocation", None), ("erosion", ["village", "community", "coastal", "relocat"]),
        ("fishing rights", None), ("hunting rights", None), ("reindeer herding", None),
        ("caribou", ["herd", "hunt", "first nations"]), ("salmon", ["treaty", "tribe", "fishery"]),
        ("contaminación", ["indígena", "comunidad", "río"]), ("derrame", ["comunidad", "indígena"]),
    ]),
    ("displacement", "Eviction & displacement", [
        ("evict*", ["indigenous", "forest", "park", "community", "tribal"]),
        ("displac*", ["indigenous", "community", "tribal", "village"]),
        ("resettle*", ["indigenous", "village", "community", "dam"]),
        ("forced removal", None), ("fortress conservation", None),
        ("conservation eviction", None), ("carbon offset", ["indigenous", "community land", "territory"]),
        ("30x30", ["indigenous", "rights"]), ("protected area", ["eviction", "indigenous", "created over"]),
        ("desalojo", ["indígena", "comunidad"]), ("pengusiran", ["adat", "masyarakat"]),
    ]),
    ("culture", "Culture, heritage & language", [
        ("sacred site*", None), ("rock art", None), ("burial ground*", None),
        ("repatriation", ["remains", "ancestors", "artefacts", "artifacts"]),
        ("language revital*", None), ("endangered language*", None),
        ("heritage act", None), ("cultural heritage", ["destroyed", "damaged", "protection", "act"]),
        ("boarding school*", ["indigenous", "native", "residential"]), ("residential school*", None),
        ("patrimonio", ["indígena", "sagrado"]), ("遺骨返還", None), ("アイヌ", ["遺骨", "権利", "文化"]),
    ]),
    ("health", "Health & wellbeing", [
        ("health outcomes", ["indigenous", "first nations", "aboriginal", "tribal"]),
        ("life expectancy gap", None), ("epidemic", ["indigenous", "isolated", "tribe", "village"]),
        ("malnutrition", ["indigenous", "community", "tribal"]),
        ("suicide", ["indigenous", "first nations", "community", "youth"]),
        ("clinic", ["reserve", "indigenous", "remote community"]),
        ("food sovereignty", None), ("traditional medicine", ["rights", "protection", "knowledge"]),
    ]),
    ("recognition", "Recognition & self-rule", [
        ("self-government", ["indigenous", "first nations", "agreement"]),
        ("co-management", None), ("indigenous protected area", None),
        ("voice to parliament", None), ("reserved seats", ["indigenous", "parliament"]),
        ("apology", ["indigenous", "first nations", "aboriginal", "government"]),
        ("compensation", ["indigenous", "first nations", "land", "settlement"]),
        ("land trust", ["tribal", "indigenous"]), ("nation-to-nation", None),
        ("autonomía indígena", None), ("autonomia indígena", None), ("pengakuan masyarakat adat", None),
    ]),
]

# --------------------------------------------------------------------------
# The gate.
#
# PEOPLE  — the story is about Indigenous, tribal or traditional peoples, in any
#           of the feed's languages. Nothing enters without this.
# GROUND  — territory, rights, extraction, eviction, environment, heritage: the
#           subject matter this feed follows. Required alongside PEOPLE so that
#           a festival listing or a sports mascot row does not qualify.
# BLOCK   — "native" in its software, advertising, linguistic and botanical
#           senses, plus the team names that carry these words.
# --------------------------------------------------------------------------
PEOPLE = [
    "indigenous", "indigenous peoples", "indigenous community", "indigenous nation*",
    "first nation*", "métis", "metis", "inuit", "inuk", "aboriginal", "torres strait",
    "traditional owner*", "native title", "native american*", "american indian",
    "alaska native*", "native hawaiian", "tribal nation*", "tribal council", "tribe*",
    "māori", "maori", "iwi", "hapū", "sámi", "sami people", "saami", "adivasi",
    "scheduled tribe*", "particularly vulnerable tribal group", "orang asli",
    "masyarakat adat", "dayak", "papuan*", "ainu", "kanak", "garifuna", "quilombola*",
    "batwa", "san people", "maasai", "ogiek", "mapuche", "guaraní", "guarani", "yanomami",
    "kayapó", "munduruku", "shipibo", "awajún", "wayúu", "shuar", "nasa people",
    "uncontacted", "isolated peoples", "hill tribe*", "ethnic minority village",
    "lumad", "igorot", "moro", "waitangi", "treaty of waitangi", "girjas",
    "ancestral domain", "ancestral land*", "customary land", "native title",
    "land defender*", "indigenous leader*", "traditional authorities", "comunidad nativa",
    "comunidades nativas", "terra indígena", "tierras indígenas", "reserva indígena",
    "pueblos indígenas", "pueblo indígena", "comunidad indígena", "comunidades indígenas",
    "povos indígenas", "povo indígena", "indígena*", "peuples autochtones", "autochtone*",
    "premières nations", "amérindien*", "indigene völker", "ureinwohner", "indigene gemeinschaft",
    "popoli indigeni", "inheemse volken", "urfolk", "samer", "ludy tubylcze", "rdzenni",
    "коренные народы", "коренных народов", "корінні народи", "yerli halklar",
    "الشعوب الأصلية", "السكان الأصليين", "مردمان بومی", "आदिवासी", "जनजाति*", "আদিবাসী",
    "người bản địa", "dân tộc thiểu số", "ชนเผ่าพื้นเมือง", "先住民", "原住民", "原住民族",
    "土著", "원주민", "선주민", "jamii za asili", "watu wa asili", "αυτόχθονες", "ιθαγενείς",
]

GROUND = [
    "land", "lands", "territor*", "title", "titling", "demarcat*", "treaty", "rights",
    "claim*", "reserve", "reservation", "eviction", "evict*", "displac*", "resettle*",
    "mining", "mine", "miner*", "oil", "gas", "drilling", "pipeline", "logging", "logger*",
    "dam", "plantation", "concession*", "deforestation", "contamination", "spill",
    "mercury", "pollution", "water", "river", "forest", "sacred", "heritage", "repatriation",
    "language", "consultation", "consent", "court", "ruling", "law", "bill", "policy",
    "protest", "blockade", "defender*", "killed", "criminalis*", "criminaliz*",
    "sovereignty", "self-determination", "self-government", "autonomy", "compensation",
    "settlement", "apology", "boarding school*", "residential school*", "health", "clinic",
    "hunting", "fishing", "herding", "conservation", "carbon", "offset", "relocation",
    "tierra*", "territorio", "consulta", "despojo", "desalojo", "terra", "demarcação",
    "garimpo", "terre*", "droits", "landrechte", "vertreibung", "земл", "прав",
    "土地", "権利", "土著權", "토지", "권리", "أرض", "حقوق", "भूमि", "अधिकार", "tanah", "hak",
    "傳統領域", "传统领域", "領域", "司法", "訴訟", "판결", "先住権", "遺骨",
]

BLOCK = [
    # "native" and "tribal" in their other senses
    "native app*", "native advertising", "native ads", "cloud native", "native speaker*",
    "native code", "native language support", "native plant*", "native species",
    "native vegetation", "invasive species", "tribal tattoo*", "tribal print", "tribal fusion",
    "tribal council vote", "survivor tribal", "reality show",
    # team names and mascots, which flood any query with these words
    "kansas city chiefs", "atlanta braves", "chicago blackhawks", "cleveland guardians",
    "florida state seminoles", "washington commanders", "nfl", "nba", "mlb draft",
    # commercial and horoscope noise
    "gift guide", "best deals", "coupon", "horoscope", "astrolog*", "zodiac", "tarot",
    "box office", "streaming series", "season finale", "video game", "casino promotion",
]

# --------------------------------------------------------------------------
# Direction. Ground taken, or ground held and returned. Most stories are one or
# the other, some are both, and the difference is the first thing a reader wants.
# --------------------------------------------------------------------------
TAKEN = [
    "eviction", "evicted", "displaced", "removed", "cleared", "invaded", "encroach*",
    "concession granted", "licence granted", "license granted", "permit approved",
    "approved despite", "overruled", "struck down protections", "rolled back",
    "killed", "murdered", "arrested", "criminalis*", "criminaliz*", "raid*", "threatened",
    "contaminated", "polluted", "spill", "deforest*", "logging", "illegal mining",
    "without consent", "consent violated", "protest suppressed", "demolished", "destroyed",
    "desalojo", "despojo", "invasão", "garimpo", "desmatamento", "vertreibung",
]
HELD = [
    "titled", "title granted", "demarcation", "demarcated", "recognised", "recognized",
    "returned", "win", "wins", "won the case", "upholding", "upheld", "in favour of",
    "in favor of", "rejects the challenge", "granted to the community",
    "restored", "handed back", "land back", "won", "victory", "ruled in favour",
    "ruled in favor", "upheld the rights", "settlement reached", "agreement signed",
    "co-management", "self-government", "compensation awarded", "apology", "repatriated",
    "protected area created with", "veto upheld", "consultation ordered", "moratorium",
    "titulación", "demarcada", "reconocido", "devuelta", "vitória", "homologada",
]


# --------------------------------------------------------------------------
# Evidence signals for the pressure score.
# --------------------------------------------------------------------------
DOCUMENTED = [
    "ruling", "ruled", "court found", "verdict", "judgment", "judgement", "signed",
    "granted", "approved", "titled", "demarcated", "gazetted", "revoked", "evicted",
    "killed", "arrested", "charged", "convicted", "seized", "raid", "spill", "outbreak",
    "construction begins", "concession granted", "licence granted", "license granted",
    "returned", "handed back", "settlement reached", "agreement signed", "repatriated",
    "sentencia", "fallo", "homologada", "demarcada", "titulación", "desalojo",
]
INSTITUTIONAL = [
    "united nations", "ohchr", "special rapporteur", "ilo", "inter-american court",
    "african court", "supreme court", "constitutional court", "human rights watch",
    "amnesty international", "global witness", "iwgia", "cultural survival",
    "survival international", "world bank", "census", "official figures",
    "government data", "peer-reviewed", "study finds", "report finds", "monitoring data",
    "satellite data", "land registry", "gazette",
]
MEASURED = [
    "hectares", "square kilometres", "square kilometers", "km2", "km²", "acres",
    "per cent", "percent", "%", "families", "households", "communities", "villages",
    "thousands of", "millions of", "number of", "declined by", "rose by", "since 19",
]
PROJECTED = [
    "projected", "expected to", "due to decide", "hearing scheduled", "deadline",
    "will be decided", "under review", "consultation ordered", "proposed", "draft law",
    "by 2030", "next year", "in the coming months",
]


PEOPLE_C = _compile_all(PEOPLE)
GROUND_C = _compile_all(GROUND)
BLOCK_C = _compile_all(BLOCK)
TAKEN_C = _compile_all(TAKEN)
HELD_C = _compile_all(HELD)
DOCUMENTED_C = _compile_all(DOCUMENTED)
INSTITUTIONAL_C = _compile_all(INSTITUTIONAL)
MEASURED_C = _compile_all(MEASURED)
PROJECTED_C = _compile_all(PROJECTED)
TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]
GEO_C = [(gid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
         for gid, label, terms in GEO]


def relevant(text):
    """A people and a matter of ground. Either alone is not this feed."""
    if hit(text, BLOCK_C):
        return False
    return hit(text, PEOPLE_C) and hit(text, GROUND_C)


def kind_of(text):
    """Ground taken, ground held, or both."""
    kinds = []
    if hit(text, TAKEN_C):
        kinds.append("taken")
    if hit(text, HELD_C):
        kinds.append("held")
    return kinds or ["taken"]


def pressure(text, standing, placed):
    total, reasons = 0, []
    if hit(text, DOCUMENTED_C):
        total += 2
        reasons.append("documented")
    if hit(text, INSTITUTIONAL_C):
        total += 2
        reasons.append("institutional")
    if hit(text, MEASURED_C):
        total += 1
        reasons.append("measured")
    if hit(text, PROJECTED_C):
        total += 1
        reasons.append("projected")
    if placed:
        total += 1
        reasons.append("located")
    if standing in ("official", "research", "indigenous"):
        total += 1
        reasons.append("primary source")
    return total, reasons


def topics_for(text):
    hits = []
    for tid, _label, terms in TOPICS_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(tid)
            break
    return hits


def regions_for(text):
    hits = []
    for gid, _label, terms in GEO_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(gid)
            break
    return hits or ["unlocated"]


def load_sources():
    with open(SOURCES_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)
    srcs = []
    for s in cfg.get("direct", []):
        srcs.append({"name": s["name"], "lang": s["lang"], "standing": s["standing"],
                     "region": s["standing"], "kind": s.get("kind", "news"), "url": s["url"]})
    for block, prefix in (("gnews", "Google News · "), ("events", "Events · ")):
        for loc in cfg.get(block, []):
            srcs.append({"name": prefix + loc["label"], "lang": loc["lang"],
                         "standing": loc["standing"], "region": loc["standing"],
                         "kind": "news", "url": build_gnews_url(loc)})
    return srcs, cfg


def run(dry_run=False, fixtures=None):
    sources, cfg = load_sources()
    print("Reading %d wires…" % len(sources))

    def read(src):
        if fixtures:
            path = os.path.join(fixtures, re.sub(r"[^\w.-]", "_", src["name"]) + ".xml")
            if not os.path.exists(path):
                return src, None
            with open(path, "rb") as fh:
                return src, fh.read()
        return src, fetch(src["url"])

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for src, raw in pool.map(read, sources):
            results.append((src, raw))

    previous = []
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH, encoding="utf-8") as fh:
                previous = json.load(fh).get("items", [])
        except Exception:  # noqa: BLE001
            previous = []

    seen_fp, seen_url, items = set(), set(), []

    def absorb(row):
        fp = fingerprint(row["t"])
        cu = canon_url(row["u"])
        if fp in seen_fp or cu in seen_url:
            return False
        seen_fp.add(fp)
        seen_url.add(cu)
        items.append(row)
        return True

    stats, ok_count, refused = [], 0, 0
    for src, raw in results:
        stat = {"name": src["name"], "lang": src["lang"], "standing": src["standing"],
                "region": src["standing"], "kept": 0, "refused": 0, "ok": False}
        if raw:
            stat["ok"] = True
            ok_count += 1
            for row in parse_feed(raw, src):
                text = (row["t"] + " " + row["s"]).lower()
                if hit(text, BLOCK_C):
                    stat["refused"] += 1
                    refused += 1
                    continue
                if not relevant(text):
                    continue
                places = regions_for(text)
                total, reasons = pressure(text, src["standing"], places != ["unlocated"])
                row["x"] = topics_for(text) or ["territory"]
                row["w"] = places
                row["p"] = total
                row["y"] = reasons
                row["st"] = src["standing"]
                row["k"] = kind_of(text)
                if absorb(row):
                    stat["kept"] += 1
        stats.append(stat)
        print("  %-36s %s" % (src["name"][:36],
                              "unreachable" if not raw
                              else "%d kept, %d refused" % (stat["kept"], stat["refused"])))

    fresh_urls = {canon_url(i["u"]) for i in items}
    for row in previous:
        if "x" in row:
            absorb(row)

    cutoff = int(time.time() * 1000) - RETAIN_DAYS * 86400000
    items = [i for i in items if (i.get("d") or cutoff + 1) >= cutoff]
    items.sort(key=lambda i: i.get("d") or 0, reverse=True)
    items = items[:MAX_ITEMS]
    fresh = sum(1 for i in items if canon_url(i["u"]) in fresh_urls)

    languages = {}
    for loc in cfg.get("gnews", []):
        languages.setdefault(loc["lang"], re.sub(r"\s*·.*$|\s*\(.*$|\s+\d+$", "", loc["label"]).strip())
    languages.setdefault("en", "English")

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counts": {"stories": len(items), "new_this_run": fresh,
                   "languages": len({i["g"] for i in items}),
                   "notable": sum(1 for i in items if i.get("p", 0) >= NOTABLE_SCORE),
                   "taken": sum(1 for i in items if "taken" in i.get("k", [])),
                   "held": sum(1 for i in items if "held" in i.get("k", [])),
                   "refused": refused,
                   "wires_ok": ok_count, "wires_total": len(sources)},
        "notable_score": NOTABLE_SCORE,
        "languages": languages,
        "kinds": [
            {"id": "taken", "label": "Ground taken"},
            {"id": "held", "label": "Ground held or returned"},
        ],
        "standings": [
            {"id": "official", "label": "Bodies & courts"},
            {"id": "indigenous", "label": "Indigenous media & orgs"},
            {"id": "research", "label": "Research & rights groups"},
            {"id": "field", "label": "Field press"},
            {"id": "press", "label": "Press"},
        ],
        "topics": [{"id": tid, "label": label} for tid, label, _ in TOPICS],
        "geo": ([{"id": gid, "label": label} for gid, label, _ in GEO] +
                [{"id": "unlocated", "label": "No single region"}]),
        "sources": stats,
        "items": items,
    }

    print("\n%d stories (%d new, %d well documented) · %d ground taken, %d ground held · %d refused · %d languages · %d/%d wires answered"
          % (len(items), fresh, payload["counts"]["notable"], payload["counts"]["taken"],
             payload["counts"]["held"], refused, payload["counts"]["languages"],
             ok_count, len(sources)))

    if dry_run:
        print("\n--dry-run: wire_indigenous.json not written")
        return payload

    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    print("Wrote %s (%.0f KB)" % (OUT_PATH, os.path.getsize(OUT_PATH) / 1024))
    return payload


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fixtures")
    args = ap.parse_args()
    run(dry_run=args.dry_run, fixtures=args.fixtures)
