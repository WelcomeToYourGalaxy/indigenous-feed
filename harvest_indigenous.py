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

# The shared gazetteer. Placement used to be each wire's own short country
# table, which put most of every wire in a counter marked "unplaced"; this is
# the fleet's common one, and it is optional at import so a harvest still runs
# if the data file has not been fetched yet.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import galaxy_places
    _GAZETTEER = True
except Exception as _exc:                       # noqa: BLE001
    print("  ! gazetteer unavailable (%s); falling back to the local table"
          % _exc, file=sys.stderr)
    galaxy_places = None
    _GAZETTEER = False

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

READ_BUDGET_MIN = 40          # minutes spent reading wires

# The wall-clock budget for reading wires. Past it the remaining sources are
# recorded unreachable and the harvest finishes on what it has, because the
# wire is only written at the end of run() and a job killed by the workflow
# timeout commits nothing at all — which is how a feed gets stuck stale.
DEADLINE = None


def out_of_time():
    return DEADLINE is not None and time.monotonic() > DEADLINE


def fetch(url, tries=3):
    last = None
    for attempt in range(tries):
        if out_of_time():
            return None
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
        except urllib.error.HTTPError as exc:
            last = exc
            # Being rate-limited or refused is an answer, not a hiccup. Trying
            # the same query twice more against the same limiter spends eighty
            # seconds of a worker slot to be told the same thing, and deepens
            # the throttle for every other query in the run.
            if exc.code in (403, 429, 451):
                time.sleep(1.5)
                break
            time.sleep(1.5 * (attempt + 1))
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
# Where the story is, in three levels: region, subregion, place. A story naming
# Peru files under Peru, the Andes and Latin America at once, so the page can
# open a continent and drill into it rather than offering ten flat buckets.
# --------------------------------------------------------------------------
# region → subregion → country, with the terms that match each country.
# Matching a country implies its subregion and its region, so a story naming
# Peru files under Peru, South America and Latin America at once.
GEO3 = [
 ("africa", "Africa", [
   ("africa-e", "East Africa", [
     ("ke","Kenya",["kenya","kenyan","nairobi","ogiek","maasai","samburu","turkana"]),
     ("tz","Tanzania",["tanzania","tanzanian","ngorongoro","hadza","serengeti"]),
     ("ug","Uganda",["uganda","ugandan","batwa uganda","karamoja"]),
     ("et","Ethiopia",["ethiopia","ethiopian","omo valley","oromia"]),
     ("so","Somalia",["somalia","somali","somaliland"]),
     ("rw","Rwanda",["rwanda","rwandan"]),
     ("bi","Burundi",["burundi"]),
     ("sd","Sudan",["sudan","sudanese","darfur"]),
     ("ss","South Sudan",["south sudan","dinka","nuer"]),
     ("mg","Madagascar",["madagascar","malagasy"]),
     ("mz","Mozambique",["mozambique","cabo delgado"]),
     ("zm","Zambia",["zambia","zambian"]),
     ("zw","Zimbabwe",["zimbabwe","zimbabwean"]),
     ("mw","Malawi",["malawi"]),
   ]),
   ("africa-w", "West Africa", [
     ("ng","Nigeria",["nigeria","nigerian","ogoni","niger delta","ijaw"]),
     ("gh","Ghana",["ghana","ghanaian"]),
     ("ci","Côte d'Ivoire",["côte d'ivoire","ivory coast","ivorian"]),
     ("sn","Senegal",["senegal","senegalese","casamance"]),
     ("ml","Mali",["mali","malian","bamako","tuareg"]),
     ("bf","Burkina Faso",["burkina faso"]),
     ("ne","Niger",["niger republic","nigerien"]),
     ("lr","Liberia",["liberia","liberian"]),
     ("sl","Sierra Leone",["sierra leone"]),
     ("gn","Guinea",["guinea conakry","guinean"]),
     ("cm","Cameroon",["cameroon","cameroonian","baka"]),
   ]),
   ("africa-c", "Central Africa", [
     ("cd","DR Congo",["democratic republic of congo","drc","congolese","kivu","batwa"]),
     ("cg","Congo-Brazzaville",["republic of congo","brazzaville"]),
     ("ga","Gabon",["gabon","gabonese"]),
     ("cf","Central African Republic",["central african republic"]),
     ("td","Chad",["chad","chadian"]),
   ]),
   ("africa-s", "Southern Africa", [
     ("za","South Africa",["south africa","south african","khoisan","khoi","xolobeni"]),
     ("bw","Botswana",["botswana","san people","central kalahari"]),
     ("na","Namibia",["namibia","namibian","himba","ovahimba"]),
     ("ao","Angola",["angola","angolan"]),
     ("ls","Lesotho",["lesotho"]),
   ]),
   ("africa-n", "North Africa", [
     ("ma","Morocco",["morocco","moroccan","amazigh","berber","western sahara","sahrawi"]),
     ("dz","Algeria",["algeria","algerian","kabyle"]),
     ("tn","Tunisia",["tunisia"]),
     ("ly","Libya",["libya","libyan","tuareg libya"]),
     ("eg","Egypt",["egypt","egyptian","nubian"]),
   ]),
 ]),
 ("americas-n", "North America", [
   ("na-us", "United States", [
     ("us-ak","Alaska",["alaska","alaskan","inupiat","yupik","gwich'in"]),
     ("us-sw","US Southwest",["navajo","diné","hopi","apache","arizona tribe","new mexico pueblo","tohono o'odham"]),
     ("us-pl","US Plains & Midwest",["standing rock","lakota","dakota access","oglala","cheyenne river","ojibwe","anishinaabe"]),
     ("us-pnw","US Pacific Northwest",["yakama","nez perce","puyallup","lummi","columbia river treaty","klamath"]),
     ("us-e","US East & South",["cherokee","seminole","lumbee","penobscot","wampanoag","mashpee"]),
     ("us-hi","Hawai'i",["native hawaiian","kanaka maoli","mauna kea","hawaii"]),
   ]),
   ("na-ca", "Canada", [
     ("ca-bc","British Columbia",["british columbia","wet'suwet'en","haida","coastal gitxsan","secwepemc"]),
     ("ca-pr","Prairies",["alberta","saskatchewan","manitoba","treaty 8","treaty 6"]),
     ("ca-on","Ontario & Quebec",["ontario first nation","quebec","grassy narrows","innu","cree quebec","atikamekw"]),
     ("ca-n","Northern Canada",["nunavut","northwest territories","yukon","inuit nunangat","dene"]),
     ("ca-at","Atlantic Canada",["mi'kmaq","nova scotia","new brunswick","newfoundland","innu labrador"]),
   ]),
   ("na-mx", "Mexico", [
     ("mx-s","Southern Mexico",["chiapas","oaxaca","zapatista","zapoteco","mixe","tren maya","yucatán","maya"]),
     ("mx-n","Northern Mexico",["yaqui","rarámuri","tarahumara","sonora","chihuahua"]),
   ]),
 ]),
 ("americas-s", "Latin America & Caribbean", [
   ("la-amz", "Amazon Basin", [
     ("br-amz","Brazilian Amazon",["yanomami","munduruku","kayapó","xingu","terra indígena","amazônia","rondônia","pará"]),
     ("pe-amz","Peruvian Amazon",["loreto","ucayali","madre de dios","awajún","shipibo","kakataibo"]),
     ("co-amz","Colombian Amazon",["amazonas colombia","putumayo","caquetá"]),
     ("ec-amz","Ecuadorian Amazon",["yasuní","waorani","sarayaku","sucumbíos","achuar"]),
     ("bo-amz","Bolivian Amazon",["tipnis","beni","chiquitano","bolivian amazon"]),
     ("ve-amz","Venezuelan Amazon",["arco minero","amazonas venezuela","pemón"]),
   ]),
   ("la-and", "Andes & Southern Cone", [
     ("cl","Chile",["chile","chilean","mapuche","araucanía","wallmapu"]),
     ("ar","Argentina",["argentina","argentine","patagonia","qom","wichí"]),
     ("pe","Peru",["peru","peruvian","quechua","aymara peru"]),
     ("bo","Bolivia",["bolivia","bolivian","aymara","quechua bolivia"]),
     ("py","Paraguay",["paraguay","ayoreo","chaco paraguayo"]),
     ("uy","Uruguay",["uruguay"]),
   ]),
   ("la-ca", "Central America", [
     ("gt","Guatemala",["guatemala","guatemalan","ixil","k'iche'","q'eqchi'"]),
     ("hn","Honduras",["honduras","garífuna","lenca","berta cáceres"]),
     ("ni","Nicaragua",["nicaragua","miskito","bosawás"]),
     ("cr","Costa Rica",["costa rica","bribri","térraba"]),
     ("pa","Panama",["panama","guna","ngäbe","emberá"]),
     ("bz","Belize",["belize","maya belize"]),
     ("sv","El Salvador",["el salvador"]),
   ]),
   ("la-car", "Caribbean & Guianas", [
     ("gy","Guyana",["guyana","wapichan","rupununi"]),
     ("sr","Suriname",["suriname","saamaka","maroon suriname","kaliña"]),
     ("gf","French Guiana",["guyane","french guiana","wayana"]),
     ("do","Caribbean islands",["dominica kalinago","caribbean indigenous","taino","haiti","jamaica","puerto rico"]),
   ]),
   ("la-br", "Brazil (other)", [
     ("br-ne","Brazil northeast & cerrado",["cerrado","bahia indígena","maranhão","quilombola","pataxó","guarani-kaiowá","mato grosso do sul"]),
   ]),
 ]),
 ("asia-s", "South Asia", [
   ("sa-in", "India", [
     ("in-c","Central India",["chhattisgarh","jharkhand","odisha","madhya pradesh","hasdeo","niyamgiri","bastar"]),
     ("in-ne","Northeast India",["assam","manipur","nagaland","mizoram","meghalaya","arunachal"]),
     ("in-s","South & West India",["kerala adivasi","tamil nadu tribal","karnataka tribal","gujarat adivasi","maharashtra adivasi"]),
     ("in-h","Himalayan India",["ladakh","uttarakhand","himachal","sikkim"]),
   ]),
   ("sa-oth", "Rest of South Asia", [
     ("bd","Bangladesh",["bangladesh","chittagong hill tracts","jumma","chakma"]),
     ("np","Nepal",["nepal","tharu","newar","chepang"]),
     ("pk","Pakistan",["pakistan","balochistan","kalash"]),
     ("lk","Sri Lanka",["sri lanka","vedda"]),
     ("bt","Bhutan",["bhutan"]),
   ]),
 ]),
 ("asia-se", "Southeast Asia", [
   ("se-mar", "Maritime Southeast Asia", [
     ("id","Indonesia",["indonesia","indonesian","masyarakat adat","papua","west papua","kalimantan","dayak","sulawesi","sumatra","mentawai"]),
     ("ph","Philippines",["philippines","filipino","lumad","igorot","mindanao","cordillera","ancestral domain"]),
     ("my","Malaysia",["malaysia","sarawak","sabah","penan","orang asli","bakun"]),
     ("tl","Timor-Leste",["timor-leste","east timor"]),
     ("pg-ind","Papua New Guinea",["papua new guinea","bougainville","porgera"]),
   ]),
   ("se-main", "Mainland Southeast Asia", [
     ("th","Thailand",["thailand","karen thailand","bangkloi","chao lay","hill tribe"]),
     ("mm","Myanmar",["myanmar","burma","karen state","kachin","chin state","rakhine"]),
     ("vn","Vietnam",["vietnam","montagnard","central highlands vietnam"]),
     ("kh","Cambodia",["cambodia","bunong","ratanakiri"]),
     ("la","Laos",["laos","hmong laos"]),
   ]),
 ]),
 ("asia-e", "East & Central Asia", [
   ("ea-e", "East Asia", [
     ("tw","Taiwan",["taiwan","原住民族","傳統領域","amis","atayal","bunun"]),
     ("jp","Japan",["japan","ainu","hokkaido","okinawa","ryukyu"]),
     ("cn","China",["china","tibet","tibetan","xinjiang","uyghur","inner mongolia","yunnan minority"]),
     ("kr","Korea",["korea","korean"]),
     ("mn","Mongolia",["mongolia","mongolian","dukha","tsaatan"]),
   ]),
   ("ea-c", "Central Asia & Siberia", [
     ("ru-sib","Siberia & Russian North",["siberia","evenki","nenets","khanty","yamal","sakha","chukotka","коренные малочисленные"]),
     ("kz","Kazakhstan",["kazakhstan"]),
     ("kg","Kyrgyzstan",["kyrgyzstan"]),
     ("uz","Uzbekistan",["uzbekistan"]),
   ]),
 ]),
 ("mena", "Middle East & North Africa", [
   ("me-lev", "Levant & Gulf", [
     ("il","Israel & Palestine",["bedouin","negev","naqab","palestinian land","israel","west bank"]),
     ("jo","Jordan",["jordan","bedouin jordan"]),
     ("iq","Iraq",["iraq","marsh arabs","yazidi","kurdistan iraq"]),
     ("ir","Iran",["iran","qashqai","bakhtiari","ahwazi"]),
     ("sa","Gulf states",["saudi arabia","uae","oman","qatar","kuwait"]),
     ("tr","Turkey",["turkey","türkiye","kurdish","hasankeyf","alevi"]),
   ]),
 ]),
 ("europe", "Europe", [
   ("eu-n", "Nordic & Arctic Europe", [
     ("no","Norway",["norway","norwegian","sápmi","fosen","finnmark"]),
     ("se","Sweden",["sweden","swedish","girjas","gällivare","kiruna","samer"]),
     ("fi","Finland",["finland","finnish","inari","sámi parliament"]),
     ("gl","Greenland",["greenland","kalaallit","nuuk"]),
     ("ru-eu","Russian Karelia & Kola",["kola peninsula","karelia","murmansk sami"]),
   ]),
   ("eu-o", "Rest of Europe", [
     ("ua","Ukraine",["ukraine","crimean tatars","krym"]),
     ("ru","Russia (European)",["russia","russian federation"]),
     ("eu","European Union",["european union","european commission","brussels"]),
     ("uk","United Kingdom",["united kingdom","britain","scotland","wales"]),
     ("es","Spain",["spain","spanish"]),
     ("fr","France",["france","french"]),
     ("de","Germany",["germany","german"]),
   ]),
 ]),
 ("oceania", "Oceania", [
   ("oc-au", "Australia", [
     ("au-n","Northern Australia",["northern territory","arnhem land","kimberley","juukan gorge","tiwi","gulf country"]),
     ("au-w","Western Australia",["western australia","pilbara","noongar","yindjibarndi"]),
     ("au-e","Eastern Australia",["queensland","new south wales","victoria aboriginal","wiradjuri","gunditjmara","adani","carmichael"]),
     ("au-c","Central & South Australia",["south australia","adnyamathanha","arrernte","alice springs","olympic dam"]),
   ]),
   ("oc-nz", "Aotearoa New Zealand", [
     ("nz","Aotearoa",["new zealand","aotearoa","māori","maori","iwi","waitangi","ngāi tahu","tainui"]),
   ]),
   ("oc-pac", "Pacific Islands", [
     ("fj","Fiji",["fiji","fijian","itaukei"]),
     ("nc","Kanaky New Caledonia",["new caledonia","kanaky","kanak","nouméa"]),
     ("sb","Solomon Islands",["solomon islands"]),
     ("vu","Vanuatu",["vanuatu","ni-vanuatu"]),
     ("ws","Polynesia & Micronesia",["samoa","tonga","tuvalu","kiribati","marshall islands","palau","guam","chamorro","tahiti","rapa nui","easter island"]),
   ]),
 ]),
 ("polar", "Arctic & Antarctic", [
   ("pol-arc", "Circumpolar", [
     ("arctic","Arctic Council region",["arctic council","circumpolar","inuit circumpolar","arctic indigenous"]),
   ]),
 ]),
]

# --------------------------------------------------------------------------
# Subjects
# --------------------------------------------------------------------------
TOPICS = [
    ("territory", "Territory & titling", [
        ("land right*", None), ("rights to land", None), ("land dispute*", None), ("land tenure", None),
        ("derechos territoriales", None), ("derecho a la tierra", None),
        ("direitos territoriais", None), ("hak ulayat", None), ("hak tanah", None),
        ("hak orang asli", None), ("droits fonciers", None), ("на землю", ["коренн", "традиционн"]),
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
        ("royalt*", ["mining", "indigenous", "tribe*", "community", "resource*"]),
        ("lote", ["petrolero", "indígena", "territorio", "amazon"]),
        ("bloque petrolero", None), ("concesión", ["indígena", "territorio", "comunidad"]),
        ("benefit sharing", None), ("revenue sharing", ["indigenous", "community", "tribe*"]),
        ("transnational investment*", None),
        ("investment*", ["indigenous land*", "ancestral", "territor*", "customary"]),
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
        ("treaty", ["indigenous", "first nation*", "nation*", "signed", "rights", "obligation*"]),
        ("exclusion", ["indigenous", "aboriginal", "tribe*", "native"]),
        ("indigenous rights law", None), ("rights act", ["indigenous", "aboriginal", "native"]),
        ("repeal", ["indigenous", "rights law", "treaty", "act"]),
        ("declaration on the rights of indigenous peoples", None), ("undrip", None),
        ("expert mechanism", ["indigenous", "rights"]), ("special rapporteur", ["indigenous"]),
        ("ilo 169", None), ("convention 169", None),
        ("ley indígena", None), ("ley de comunidades", None),
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
        # The section names them one by one — shot at a checkpoint, stabbed at
        # home, ambushed on the river — and then says the killings are the
        # reported part. What follows is the unreported part, in its words:
        # surveillance, informants, harassment, smear campaigns,
        # stigmatisation, intimidation, criminalisation, death threats,
        # kidnappings, non-lethal attacks, sexual violence.
        ("shot dead", ["indigenous", "leader", "defender", "activist", "community"]),
        ("stabbed", ["indigenous", "leader", "defender", "activist"]),
        ("ambushed", ["indigenous", "leader", "defender", "activist", "expert"]),
        ("found dead", ["indigenous", "leader", "defender", "activist"]),
        ("illegal logger*", None), ("illegal miner*", None), ("garimpeiro*", None),
        ("intimidat*", ["indigenous", "community", "leader", "defender", "first nation*"]),
        ("harass*", ["indigenous", "community", "leader", "defender"]),
        ("smear campaign", None), ("death threat*", ["indigenous", "leader", "defender", "community"]),
        ("surveill*", ["indigenous", "defender", "activist", "community"]),
        ("kidnap*", ["indigenous", "leader", "defender"]),
        ("reprisal*", ["indigenous", "defender", "community"]),
        ("guardians of the forest", None), ("uncontacted", ["threat", "logger", "miner", "contact"]),
        ("defensor*", ["ambiental", "territorio", "indígena", "asesinad*", "amenaz*", "atacad*"]),
        ("líder indígena", ["asesinad*", "amenaz*", "atacad*", "muert*", "denunci*"]),
        ("lideresa indígena", ["asesinad*", "amenaz*", "atacad*"]),
        ("asesinad*", ["indígena", "defensor*", "líder", "territorio", "ambiental"]),
        ("liderança indígena", ["assassinad*", "ameaç*", "atacad*", "mort*"]),
        ("assassinad*", ["indígena", "defensor*", "liderança", "ambiental", "terra"]),
    ]),
    ("consultation", "Consent, and consultation without it", [
        # The largest single thing the wire was catching and filing under
        # "territory" because nothing else would take it. Consultation is the
        # mechanism the section's dispossession runs through — held, suspended,
        # skipped, or held and then ignored.
        ("free, prior and informed consent", None), ("fpic", None),
        ("prior consultation", None), ("indigenous consultation", None),
        ("consultation process", ["indigenous", "first nation*", "aboriginal", "tribe*"]),
        ("duty to consult", None), ("consent", ["indigenous", "first nation*", "community", "territory"]),
        ("consulta", ["indígena", "pueblos", "comunidad", "ley", "previa"]),
        ("consulta previa", None), ("consulta indígena", None), ("consulta libre", None),
        ("consulta prévia", None), ("consentimento livre", None),
        ("consultation préalable", None), ("consentement libre", None),
        ("konsultasi", ["adat", "masyarakat adat"]), ("padiatapa", None),
        ("предварительное согласие", ["коренн"]),
    ]),
    ("subsistence", "Hunting, fishing and gathering rights", [
        # Also being swallowed: the rules that govern whether people can still
        # feed themselves off their own country.
        ("hunting right*", ["indigenous", "treaty", "first nation*", "aboriginal", "native"]),
        ("fishing right*", ["indigenous", "treaty", "first nation*", "aboriginal", "native"]),
        ("subsistence", ["hunting", "fishing", "harvest", "indigenous", "traditional"]),
        ("traditional harvest*", None), ("gathering right*", ["traditional", "indigenous"]),
        ("whaling", ["subsistence", "indigenous", "aboriginal"]),
        ("reindeer herding", None), ("pastoralist*", ["evict*", "grazing right*", "displac*"]),
        ("традиционн", ["рыбал", "рыболов", "охот", "промысл", "природопользован"]),
        ("коренны", ["рыбач", "рыбал", "охот", "промысл", "квот"]),
        ("pesca tradicional", ["indígena", "comunidad"]), ("caza tradicional", None),
    ]),
    ("mobilisation", "Assembly, march and refusal", [
        ("rally", ["indigenous", "adivasi", "tribal", "first nation*", "aboriginal", "native"]),
        ("march", ["indigenous", "adivasi", "tribal", "first nation*", "land rights"]),
        ("protest*", ["indigenous", "adivasi", "tribal", "first nation*", "aboriginal"]),
        ("assembly", ["indigenous", "tribal", "first nation*", "peoples"]),
        ("mobilis*", ["indigenous", "community", "tribal"]), ("mobiliz*", ["indigenous", "community", "tribal"]),
        ("आदिवासी", None), ("adivasi", None), ("masyarakat adat", None),
        ("movilizaci*", ["indígena", "comunidad"]), ("marcha", ["indígena", "territorio"]),
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
        ("desalojo*", None), ("desalojar", None), ("despojo", ["tierra*", "territorio", "indígena"]),
        ("despejo", ["indígena", "terra", "comunidade"]), ("expulsion", ["indigenous", "community", "land"]),
        ("relocat*", ["indigenous", "community", "village", "forcibly"]),
        ("evict*", ["indigenous", "forest", "park", "community", "tribal"]),
        ("displac*", ["indigenous", "community", "tribal", "village"]),
        ("resettle*", ["indigenous", "village", "community", "dam"]),
        ("forced removal", None), ("fortress conservation", None),
        ("conservation eviction", None), ("carbon offset", ["indigenous", "community land", "territory"]),
        ("30x30", ["indigenous", "rights"]), ("protected area", ["eviction", "indigenous", "created over"]),
        ("desalojo", ["indígena", "comunidad"]), ("pengusiran", ["adat", "masyarakat"]),
    ]),
    ("culture", "Culture, heritage & language", [
        ("language", ["revival", "revitalis*", "revitaliz*", "extinct", "last speaker*",
                      "indigenous", "reclaim*"]),
        ("heritage", ["reclaim*", "repatriat*", "indigenous", "ancestral", "sacred"]),
        ("sacred site*", None), ("repatriat*", ["remains", "artefact*", "artifact*", "ancestral"]),
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
# ------------------------------------------------------------------
# Subjects this wire had a name for and never asked about.
#
# The terms below were already here and were well written; what was
# missing was any query aimed at them, so they held zero stories
# however much the world published. These are the phrases from the
# queries now added, so what is fetched can be filed.
# ------------------------------------------------------------------
FILL_TERMS = {
    "health": [
        ("agua potable comunidades", None), ("brecha de salud", None),
        ("desigualdade de saúde", None), ("eau potable communautés", None),
        ("esterilización forzada mujeres", None), ("esterilização forçada mulheres", None),
        ("gesundheitslücke indigene bevölkerung", None), ("stérilisation forcée femmes", None),
        ("zwangssterilisation indigener frauen", None), ("água potável comunidades", None),
        ("écart de santé", None), ("разрыв в здоровье", None),
        ("先住民 健康格差 報告", None), ("先住民 女性 強制不妊", None),
        ("原住民 健康 差距", None), ("原住民 妇女 强制绝育", None),
    ],
}

for _tid, _label, _terms in TOPICS:
    _terms.extend(FILL_TERMS.get(_tid, []))


# --------------------------------------------------------------------------
# The same subjects in the languages this wire's own queries ask in, derived
# from those queries and filed under the subject each query's label names. The
# gate above was written in English; the queries were translated and it was
# not, so three quarters of what the wire fetched could not be recognised once
# it arrived. Generated — edit topics_multilingual.json, or delete the file to
# turn this off.
# --------------------------------------------------------------------------
_EXTRA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "topics_multilingual.json")
if os.path.exists(_EXTRA_PATH):
    with open(_EXTRA_PATH, encoding="utf-8") as _fh:
        _EXTRA = json.load(_fh)
    TOPICS = [(tid, label, terms + [(t, g) for t, g in _EXTRA.get(tid, [])])
              for tid, label, terms in TOPICS]

TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]
GEO3_C = [(rid, rlabel, [(sid, slabel, [(pid, plabel, _compile_all(terms))
                                        for pid, plabel, terms in places])
                        for sid, slabel, places in subs])
          for rid, rlabel, subs in GEO3]


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


def places_for(text):
    """Returns (regions, subregions, places). Naming a place implies the
    subregion and region above it."""
    regions, subs, places = [], [], []
    for rid, _rl, sublist in GEO3_C:
        for sid, _sl, plist in sublist:
            for pid, _pl, terms in plist:
                if not hit(text, terms):
                    continue
                if pid not in places:
                    places.append(pid)
                if sid not in subs:
                    subs.append(sid)
                if rid not in regions:
                    regions.append(rid)
    return (regions or ["unlocated"], subs or ["unlocated"], places or ["unlocated"])


# ---------------------------------------------------------------- placement
# Coordinates for the gazetteer, a table of named places, and the routine that
# resolves a story to the most specific point it names.
#
# Nothing here decides what a story is ABOUT. That belongs to this feed's own
# relevant() and topics_for(), and must never be shadowed by anything arriving
# alongside the coordinates: a later definition wins in Python, and one
# careless slice once handed five feeds the conflict wire's vocabulary.

COORDS = {
 # --- regions ---
 "africa": [1.5, 20.0], "americas-n": [45.0, -100.0], "americas-s": [-12.0, -60.0],
 "asia-s": [22.0, 79.0], "asia-se": [2.0, 112.0], "asia-e": [40.0, 100.0],
 "mena": [28.0, 42.0], "europe": [52.0, 15.0], "oceania": [-25.0, 140.0], "polar": [78.0, 0.0],
 # --- subregions ---
 "africa-e": [1.0, 37.0], "africa-w": [10.0, -2.0], "africa-c": [0.0, 20.0],
 "africa-s": [-24.0, 24.0], "africa-n": [28.0, 12.0],
 "na-us": [39.0, -98.0], "na-ca": [58.0, -100.0], "na-mx": [23.0, -102.0],
 "la-amz": [-4.0, -62.0], "la-and": [-25.0, -68.0], "la-ca": [14.0, -87.0],
 "la-car": [8.0, -60.0], "la-br": [-13.0, -47.0],
 "sa-in": [22.0, 79.0], "sa-oth": [27.0, 85.0],
 "se-mar": [-2.0, 118.0], "se-main": [16.0, 101.0],
 "ea-e": [35.0, 118.0], "ea-c": [50.0, 80.0],
 "me-lev": [32.0, 40.0],
 "eu-n": [65.0, 20.0], "eu-o": [50.0, 15.0],
 "oc-au": [-25.0, 134.0], "oc-nz": [-41.0, 174.0], "oc-pac": [-15.0, 170.0],
 "pol-arc": [80.0, 0.0],
 # --- places: Africa ---
 "ke": [0.2, 37.9], "tz": [-6.4, 34.9], "ug": [1.4, 32.3], "et": [9.1, 40.5],
 "so": [5.2, 46.2], "rw": [-1.9, 29.9], "bi": [-3.4, 29.9], "sd": [15.6, 30.2],
 "ss": [7.9, 30.0], "mg": [-18.8, 46.9], "mz": [-18.7, 35.5], "zm": [-13.1, 27.8],
 "zw": [-19.0, 29.2], "mw": [-13.3, 34.3],
 "ng": [9.1, 8.7], "gh": [7.9, -1.0], "ci": [7.5, -5.5], "sn": [14.5, -14.5],
 "ml": [17.6, -4.0], "bf": [12.2, -1.6], "ne": [17.6, 8.1], "lr": [6.4, -9.4],
 "sl": [8.5, -11.8], "gn": [9.9, -9.7], "cm": [7.4, 12.4], "sahel": [15.0, 2.0], "horn": [8.0, 45.0],
 "cd": [-4.0, 21.8], "cg": [-0.2, 15.8], "ga": [-0.8, 11.6], "cf": [6.6, 20.9], "td": [15.5, 18.7],
 "za": [-30.6, 22.9], "bw": [-22.3, 24.7], "na": [-22.9, 18.5], "ao": [-11.2, 17.9], "ls": [-29.6, 28.2],
 "ma": [31.8, -7.1], "dz": [28.0, 1.7], "tn": [33.9, 9.5], "ly": [26.3, 17.2], "eg": [26.8, 30.8],
 # --- places: North America ---
 "us-ak": [64.0, -152.0], "us-sw": [34.5, -110.0], "us-pl": [44.0, -100.0],
 "us-pnw": [46.5, -121.0], "us-e": [35.5, -80.0], "us-hi": [20.8, -156.3],
 "ca-bc": [54.0, -125.0], "ca-pr": [52.0, -106.0], "ca-on": [49.0, -80.0],
 "ca-n": [64.0, -105.0], "ca-at": [46.5, -63.0],
 "mx-s": [17.0, -94.0], "mx-n": [28.5, -108.0],
 # --- places: Latin America ---
 "br-amz": [-4.5, -60.0], "pe-amz": [-6.0, -75.0], "co-amz": [-1.0, -72.0],
 "ec-amz": [-1.5, -76.5], "bo-amz": [-14.5, -65.0], "ve-amz": [5.0, -65.0],
 "cl": [-35.7, -71.5], "ar": [-38.4, -63.6], "pe": [-9.2, -75.0], "bo": [-16.3, -63.6],
 "py": [-23.4, -58.4], "uy": [-32.5, -55.8],
 "gt": [15.8, -90.2], "hn": [15.2, -86.2], "ni": [12.9, -85.2], "cr": [9.7, -83.8],
 "pa": [8.5, -80.8], "bz": [17.2, -88.5], "sv": [13.8, -88.9],
 "gy": [4.9, -58.9], "sr": [3.9, -56.0], "gf": [3.9, -53.1], "do": [18.7, -70.2],
 "br-ne": [-10.0, -45.0],
 # --- places: South Asia ---
 "in-c": [21.5, 82.0], "in-ne": [26.0, 93.0], "in-s": [13.0, 77.5], "in-h": [32.0, 78.0],
 "bd": [23.7, 90.4], "np": [28.4, 84.1], "pk": [30.4, 69.3], "lk": [7.9, 80.8], "bt": [27.5, 90.4],
 # --- places: Southeast Asia ---
 "id": [-2.5, 118.0], "ph": [12.9, 121.8], "my": [4.2, 109.5], "tl": [-8.9, 125.7], "pg-ind": [-6.3, 143.9],
 "th": [15.9, 100.99], "mm": [21.9, 95.96], "vn": [14.1, 108.3], "kh": [12.6, 104.99], "la": [19.9, 102.5],
 # --- places: East & Central Asia ---
 "tw": [23.7, 121.0], "jp": [36.2, 138.3], "cn": [35.9, 104.2], "kr": [36.5, 127.9], "mn": [46.9, 103.8],
 "ru-sib": [62.0, 105.0], "kz": [48.0, 66.9], "kg": [41.2, 74.8], "uz": [41.4, 64.6],
 # --- places: MENA ---
 "il": [31.5, 35.0], "jo": [30.6, 36.2], "iq": [33.2, 43.7], "ir": [32.4, 53.7],
 "sa": [24.0, 45.0], "tr": [39.0, 35.2],
 # --- places: Europe ---
 "no": [64.6, 12.0], "se": [62.0, 15.0], "fi": [64.0, 26.0], "gl": [71.7, -42.6], "ru-eu": [67.5, 35.0],
 "ua": [48.4, 31.2], "ru": [56.0, 40.0], "eu": [50.8, 4.4], "uk": [54.0, -2.5],
 "es": [40.2, -3.7], "fr": [46.6, 2.4], "de": [51.2, 10.4],
 # --- places: Oceania ---
 "au-n": [-15.0, 133.0], "au-w": [-25.0, 121.0], "au-e": [-30.0, 148.0], "au-c": [-29.0, 135.0],
 "nz": [-41.0, 174.0], "fj": [-17.7, 178.0], "nc": [-21.3, 165.5], "sb": [-9.6, 160.2],
 "vu": [-15.4, 166.9], "ws": [-13.8, -172.1],
 # --- polar ---
 "arctic": [80.0, 0.0],
}

PRECISE = {
 # --- Ukraine & Russia ---
 "kyiv": ("Kyiv", 50.45, 30.52), "kiev": ("Kyiv", 50.45, 30.52),
 "kharkiv": ("Kharkiv", 49.99, 36.23), "odesa": ("Odesa", 46.48, 30.73),
 "odessa": ("Odesa", 46.48, 30.73), "lviv": ("Lviv", 49.84, 24.03),
 "dnipro": ("Dnipro", 48.46, 35.05), "zaporizhzhia": ("Zaporizhzhia", 47.84, 35.14),
 "kherson": ("Kherson", 46.64, 32.61), "mykolaiv": ("Mykolaiv", 46.98, 31.99),
 "donetsk": ("Donetsk", 48.02, 37.80), "luhansk": ("Luhansk", 48.57, 39.31),
 "donbas": ("Donbas", 48.30, 38.20), "mariupol": ("Mariupol", 47.10, 37.55),
 "bakhmut": ("Bakhmut", 48.60, 38.00), "avdiivka": ("Avdiivka", 48.14, 37.75),
 "pokrovsk": ("Pokrovsk", 48.28, 37.18), "kupiansk": ("Kupiansk", 49.71, 37.62),
 "sumy": ("Sumy", 50.91, 34.80), "chernihiv": ("Chernihiv", 51.49, 31.29),
 "crimea": ("Crimea", 45.30, 34.40), "sevastopol": ("Sevastopol", 44.62, 33.53),
 "moscow": ("Moscow", 55.75, 37.62), "kremlin": ("Moscow", 55.75, 37.62),
 "belgorod": ("Belgorod", 50.60, 36.59), "kursk region": ("Kursk", 51.73, 36.19),
 "rostov": ("Rostov-on-Don", 47.24, 39.71), "novorossiysk": ("Novorossiysk", 44.72, 37.77),
 "st petersburg": ("St Petersburg", 59.94, 30.31), "vladivostok": ("Vladivostok", 43.12, 131.89),
 # --- Israel, Palestine, Lebanon, Syria ---
 "gaza city": ("Gaza City", 31.51, 34.45), "gaza strip": ("Gaza", 31.42, 34.35),
 "gaza": ("Gaza", 31.42, 34.35), "rafah": ("Rafah", 31.29, 34.25),
 "khan younis": ("Khan Younis", 31.34, 34.30), "deir al-balah": ("Deir al-Balah", 31.42, 34.35),
 "west bank": ("West Bank", 31.95, 35.30), "jenin": ("Jenin", 32.46, 35.30),
 "nablus": ("Nablus", 32.22, 35.26), "hebron": ("Hebron", 31.53, 35.10),
 "ramallah": ("Ramallah", 31.90, 35.21), "jerusalem": ("Jerusalem", 31.78, 35.22),
 "tel aviv": ("Tel Aviv", 32.09, 34.78), "haifa": ("Haifa", 32.79, 34.99),
 "golan": ("Golan Heights", 32.95, 35.75), "sderot": ("Sderot", 31.52, 34.60),
 "beirut": ("Beirut", 33.89, 35.50), "south lebanon": ("South Lebanon", 33.30, 35.40),
 "tyre": ("Tyre", 33.27, 35.20), "baalbek": ("Baalbek", 34.01, 36.21),
 "damascus": ("Damascus", 33.51, 36.29), "aleppo": ("Aleppo", 36.20, 37.13),
 "idlib": ("Idlib", 35.93, 36.63), "homs": ("Homs", 34.73, 36.71),
 "latakia": ("Latakia", 35.52, 35.79), "deir ez-zor": ("Deir ez-Zor", 35.34, 40.14),
 "hasakah": ("Hasakah", 36.50, 40.75), "rojava": ("North-east Syria", 36.40, 40.70),
 # --- Iraq, Iran, Gulf, Yemen ---
 "baghdad": ("Baghdad", 33.31, 44.36), "mosul": ("Mosul", 36.35, 43.13),
 "erbil": ("Erbil", 36.19, 44.01), "basra": ("Basra", 30.51, 47.78),
 "fallujah": ("Fallujah", 33.35, 43.78), "kirkuk": ("Kirkuk", 35.47, 44.39),
 "tehran": ("Tehran", 35.69, 51.39), "isfahan": ("Isfahan", 32.65, 51.67),
 "natanz": ("Natanz", 33.72, 51.73), "fordow": ("Fordow", 34.88, 50.99),
 "bandar abbas": ("Bandar Abbas", 27.19, 56.28), "strait of hormuz": ("Strait of Hormuz", 26.57, 56.25),
 "riyadh": ("Riyadh", 24.71, 46.68), "jeddah": ("Jeddah", 21.49, 39.19),
 "doha": ("Doha", 25.29, 51.53), "al udeid": ("Al Udeid air base", 25.12, 51.32),
 "abu dhabi": ("Abu Dhabi", 24.45, 54.38), "dubai": ("Dubai", 25.20, 55.27),
 "manama": ("Manama", 26.23, 50.59), "kuwait city": ("Kuwait City", 29.38, 47.99),
 "muscat": ("Muscat", 23.59, 58.41),
 "sanaa": ("Sanaa", 15.37, 44.19), "sana'a": ("Sanaa", 15.37, 44.19),
 "aden": ("Aden", 12.79, 45.02), "hodeidah": ("Hodeidah", 14.80, 42.95),
 "marib": ("Marib", 15.46, 45.32), "bab el-mandeb": ("Bab el-Mandeb", 12.58, 43.33),
 "red sea": ("Red Sea", 20.00, 38.00),
 # --- Turkey, Caucasus, Central Asia, Afghanistan ---
 "ankara": ("Ankara", 39.93, 32.86), "istanbul": ("Istanbul", 41.01, 28.98),
 "incirlik": ("Incirlik air base", 37.00, 35.43), "diyarbakir": ("Diyarbakır", 37.91, 40.24),
 "yerevan": ("Yerevan", 40.18, 44.51), "baku": ("Baku", 40.41, 49.87),
 "nagorno-karabakh": ("Nagorno-Karabakh", 39.82, 46.75), "karabakh": ("Nagorno-Karabakh", 39.82, 46.75),
 "tbilisi": ("Tbilisi", 41.72, 44.78), "abkhazia": ("Abkhazia", 43.00, 41.00),
 "south ossetia": ("South Ossetia", 42.35, 43.97),
 "kabul": ("Kabul", 34.53, 69.17), "kandahar": ("Kandahar", 31.61, 65.71),
 "herat": ("Herat", 34.35, 62.20), "jalalabad": ("Jalalabad", 34.43, 70.45),
 "dushanbe": ("Dushanbe", 38.56, 68.79), "tashkent": ("Tashkent", 41.30, 69.24),
 "almaty": ("Almaty", 43.24, 76.89), "astana": ("Astana", 51.17, 71.45),
 # --- South Asia ---
 "islamabad": ("Islamabad", 33.68, 73.05), "rawalpindi": ("Rawalpindi", 33.60, 73.04),
 "karachi": ("Karachi", 24.86, 67.01), "peshawar": ("Peshawar", 34.01, 71.58),
 "quetta": ("Quetta", 30.18, 66.98), "balochistan": ("Balochistan", 28.50, 65.50),
 "kashmir": ("Kashmir", 34.08, 74.80), "srinagar": ("Srinagar", 34.08, 74.80),
 "line of control": ("Line of Control", 34.20, 74.20),
 "new delhi": ("New Delhi", 28.61, 77.21), "mumbai": ("Mumbai", 19.08, 72.88),
 "manipur": ("Manipur", 24.66, 93.91), "assam": ("Assam", 26.20, 92.94),
 "dhaka": ("Dhaka", 23.81, 90.41), "chittagong hill tracts": ("Chittagong Hill Tracts", 22.60, 92.20),
 "colombo": ("Colombo", 6.93, 79.86), "kathmandu": ("Kathmandu", 27.72, 85.32),
 # --- East & Southeast Asia ---
 "beijing": ("Beijing", 39.90, 116.41), "shanghai": ("Shanghai", 31.23, 121.47),
 "taiwan strait": ("Taiwan Strait", 24.50, 119.50), "taipei": ("Taipei", 25.03, 121.57),
 "kinmen": ("Kinmen", 24.44, 118.32), "south china sea": ("South China Sea", 13.00, 114.00),
 "spratly": ("Spratly Islands", 9.50, 114.00), "paracel": ("Paracel Islands", 16.50, 112.00),
 "scarborough shoal": ("Scarborough Shoal", 15.15, 117.76),
 "senkaku": ("Senkaku Islands", 25.75, 123.48), "diaoyu": ("Senkaku Islands", 25.75, 123.48),
 "xinjiang": ("Xinjiang", 41.00, 85.00), "tibet": ("Tibet", 31.00, 88.00),
 "pyongyang": ("Pyongyang", 39.04, 125.76), "yongbyon": ("Yongbyon", 39.80, 125.75),
 "panmunjom": ("Panmunjom", 37.96, 126.68), "seoul": ("Seoul", 37.57, 126.98),
 "tokyo": ("Tokyo", 35.68, 139.69), "okinawa": ("Okinawa", 26.34, 127.80),
 "guam": ("Guam", 13.44, 144.79), "manila": ("Manila", 14.60, 120.98),
 "mindanao": ("Mindanao", 7.50, 124.50), "jakarta": ("Jakarta", -6.21, 106.85),
 "west papua": ("West Papua", -4.00, 138.00), "papua": ("Papua", -4.00, 138.00),
 "naypyidaw": ("Naypyidaw", 19.75, 96.10), "yangon": ("Yangon", 16.87, 96.20),
 "rakhine": ("Rakhine State", 20.10, 93.50), "kachin": ("Kachin State", 25.80, 97.40),
 "karen state": ("Karen State", 17.30, 97.70), "shan state": ("Shan State", 21.50, 98.00),
 "bangkok": ("Bangkok", 13.76, 100.50), "hanoi": ("Hanoi", 21.03, 105.85),
 "phnom penh": ("Phnom Penh", 11.56, 104.92),
 # --- Africa ---
 "khartoum": ("Khartoum", 15.50, 32.56), "omdurman": ("Omdurman", 15.65, 32.48),
 "port sudan": ("Port Sudan", 19.62, 37.22), "darfur": ("Darfur", 13.00, 24.00),
 "el fasher": ("El Fasher", 13.63, 25.35), "nyala": ("Nyala", 12.05, 24.88),
 "juba": ("Juba", 4.85, 31.58), "addis ababa": ("Addis Ababa", 9.03, 38.74),
 "tigray": ("Tigray", 14.00, 38.50), "amhara": ("Amhara", 11.50, 38.00),
 "mekelle": ("Mekelle", 13.50, 39.47), "asmara": ("Asmara", 15.34, 38.93),
 "mogadishu": ("Mogadishu", 2.05, 45.32), "kismayo": ("Kismayo", -0.36, 42.55),
 "puntland": ("Puntland", 8.50, 49.00), "nairobi": ("Nairobi", -1.29, 36.82),
 "kinshasa": ("Kinshasa", -4.44, 15.27), "goma": ("Goma", -1.68, 29.22),
 "north kivu": ("North Kivu", -0.80, 29.00), "south kivu": ("South Kivu", -3.00, 28.30),
 "bukavu": ("Bukavu", -2.51, 28.86), "ituri": ("Ituri", 1.80, 29.90),
 "bangui": ("Bangui", 4.39, 18.56), "n'djamena": ("N'Djamena", 12.13, 15.06),
 "bamako": ("Bamako", 12.64, -8.00), "gao": ("Gao", 16.27, -0.04),
 "timbuktu": ("Timbuktu", 16.77, -3.01), "ouagadougou": ("Ouagadougou", 12.37, -1.52),
 "niamey": ("Niamey", 13.51, 2.13), "lake chad": ("Lake Chad basin", 13.00, 14.00),
 "abuja": ("Abuja", 9.06, 7.49), "borno": ("Borno State", 11.80, 13.10),
 "maiduguri": ("Maiduguri", 11.83, 13.15), "lagos": ("Lagos", 6.52, 3.38),
 "tripoli libya": ("Tripoli", 32.89, 13.19), "benghazi": ("Benghazi", 32.12, 20.07),
 "cairo": ("Cairo", 30.04, 31.24), "sinai": ("Sinai", 29.50, 33.80),
 "cabo delgado": ("Cabo Delgado", -12.50, 39.50), "maputo": ("Maputo", -25.97, 32.57),
 "harare": ("Harare", -17.83, 31.05), "pretoria": ("Pretoria", -25.75, 28.19),
 "johannesburg": ("Johannesburg", -26.20, 28.05), "cape town": ("Cape Town", -33.92, 18.42),
 # --- Europe ---
 "brussels": ("Brussels", 50.85, 4.35), "the hague": ("The Hague", 52.08, 4.31),
 "geneva": ("Geneva", 46.20, 6.14), "vienna": ("Vienna", 48.21, 16.37),
 "london": ("London", 51.51, -0.13), "paris": ("Paris", 48.86, 2.35),
 "berlin": ("Berlin", 52.52, 13.40), "ramstein": ("Ramstein air base", 49.44, 7.60),
 "rome": ("Rome", 41.90, 12.50), "madrid": ("Madrid", 40.42, -3.70),
 "warsaw": ("Warsaw", 52.23, 21.01), "rzeszow": ("Rzeszów", 50.04, 22.00),
 "kaliningrad": ("Kaliningrad", 54.71, 20.51), "suwalki": ("Suwałki gap", 54.10, 23.00),
 "minsk": ("Minsk", 53.90, 27.57), "chisinau": ("Chișinău", 47.01, 28.86),
 "transnistria": ("Transnistria", 47.20, 29.20), "vilnius": ("Vilnius", 54.69, 25.28),
 "riga": ("Riga", 56.95, 24.11), "tallinn": ("Tallinn", 59.44, 24.75),
 "helsinki": ("Helsinki", 60.17, 24.94), "stockholm": ("Stockholm", 59.33, 18.07),
 "oslo": ("Oslo", 59.91, 10.75), "gotland": ("Gotland", 57.50, 18.50),
 "belgrade": ("Belgrade", 44.79, 20.45), "pristina": ("Pristina", 42.66, 21.16),
 "sarajevo": ("Sarajevo", 43.86, 18.41), "black sea": ("Black Sea", 43.40, 34.30),
 "baltic sea": ("Baltic Sea", 57.00, 19.00), "arctic circle": ("Arctic", 70.00, 20.00),
 # --- Americas ---
 "washington": ("Washington DC", 38.91, -77.04), "pentagon": ("The Pentagon", 38.87, -77.06),
 "white house": ("White House", 38.90, -77.04), "new york": ("New York", 40.71, -74.01),
 "guantanamo": ("Guantánamo Bay", 19.90, -75.15), "diego garcia": ("Diego Garcia", -7.31, 72.41),
 "ottawa": ("Ottawa", 45.42, -75.70), "mexico city": ("Mexico City", 19.43, -99.13),
 "bogota": ("Bogotá", 4.71, -74.07), "bogotá": ("Bogotá", 4.71, -74.07),
 "caracas": ("Caracas", 10.49, -66.88), "essequibo": ("Essequibo", 6.00, -59.00),
 "port-au-prince": ("Port-au-Prince", 18.59, -72.31), "havana": ("Havana", 23.11, -82.37),
 "brasilia": ("Brasília", -15.79, -47.88), "brasília": ("Brasília", -15.79, -47.88),
 "buenos aires": ("Buenos Aires", -34.60, -58.38), "santiago": ("Santiago", -33.45, -70.67),
 "lima": ("Lima", -12.05, -77.04), "quito": ("Quito", -0.18, -78.47),
 "guayaquil": ("Guayaquil", -2.19, -79.89), "tegucigalpa": ("Tegucigalpa", 14.07, -87.19),
 "san salvador": ("San Salvador", 13.69, -89.19), "guatemala city": ("Guatemala City", 14.63, -90.51),
 # --- Oceania ---
 "canberra": ("Canberra", -35.28, 149.13), "darwin": ("Darwin", -12.46, 130.85),
 "wellington": ("Wellington", -41.29, 174.78), "noumea": ("Nouméa", -22.28, 166.46),
 "nouméa": ("Nouméa", -22.28, 166.46), "bougainville": ("Bougainville", -6.20, 155.20),
 "port moresby": ("Port Moresby", -9.44, 147.18), "honiara": ("Honiara", -9.43, 159.95),
}

_AREA_WORDS = ("state", "region", "sea", "strait", "basin", "islands", "gap", "arctic",
               "heights", "tracts", "province", "peninsula", "shoal", "circle")
_AREA_NAMES = {"Darfur", "Tigray", "Amhara", "Donbas", "Crimea", "Kashmir", "Balochistan",
               "Xinjiang", "Tibet", "Sinai", "Papua", "West Papua", "North-east Syria",
               "Puntland", "Nagorno-Karabakh", "Abkhazia", "South Ossetia", "West Bank",
               "Gaza", "Transnistria", "Ituri", "Cabo Delgado", "Mindanao",
               "North Kivu", "South Kivu", "Golan Heights", "Senkaku Islands",
               "South Lebanon", "Line of Control", "Manipur", "Assam", "Gotland", "Okinawa",
               "Guam", "Bougainville", "Kinmen", "Essequibo"}

_AREA_NAMES = {"Darfur", "Tigray", "Amhara", "Donbas", "Crimea", "Kashmir", "Balochistan",
               "Xinjiang", "Tibet", "Sinai", "Papua", "West Papua", "North-east Syria",
               "Puntland", "Nagorno-Karabakh", "Abkhazia", "South Ossetia", "West Bank",
               "Gaza", "Transnistria", "Ituri", "Cabo Delgado", "Mindanao",
               "North Kivu", "South Kivu", "Golan Heights", "Senkaku Islands",
               "South Lebanon", "Line of Control", "Manipur", "Assam", "Gotland", "Okinawa",
               "Guam", "Bougainville", "Kinmen", "Essequibo"}


def _rank(label):
    low = label.lower()
    if label in _AREA_NAMES or any(w in low for w in _AREA_WORDS):
        return 0          # an area
    return 1              # a point: city, base, facility

PRECISE_C = sorted(
    ((term, label, lat, lon, _compile(term), _rank(label))
     for term, (label, lat, lon) in PRECISE.items()),
    key=lambda row: (-row[5], -len(row[0])))   # points before areas, longest term first

LOCATIVE = [
 " in ", " im ", " en ", " au ", " aux ", " a ", " à ", " al ", " nel ", " nella ",
 " on ", " over ", " near ", " into ", " inside ", " across ", " throughout ",
 " sur ", " dans ", " van ", " naar ", " uit ", " w ", " na ", " do ", " em ", " no ",
 " v ", " в ", " на ", " у ", " до ", " στη", " στο", " την ", " στην ",
 "في ", "ب", "ל", "ב", "ที่", "ใน", "在", "で", "へ", "에서", "로",
]
_LOC_MAX = 12          # how far back to look for the marker


def _first_pos(text, compiled):
    """Where a place's terms first appear in the text, or None."""
    best = None
    for c in compiled:
        if isinstance(c, str):
            i = text.find(c)
        else:
            mo = c.search(text)
            i = mo.start() if mo else -1
        if i >= 0 and (best is None or i < best):
            best = i
    return best

def _is_scene(text, pos):
    """True when the name at this position is preceded by a locative marker."""
    if pos is None:
        return False
    window = text[max(0, pos - _LOC_MAX):pos]
    return any(mark in window for mark in LOCATIVE)

def precise_for(text):
    """A city, province, base or waterway named in the story. Checked before the
    country layer so a headline about Kharkiv is pinned on Kharkiv rather than
    the middle of Ukraine. Longest term wins."""
    for term, label, lat, lon, rx, _rk in PRECISE_C:
        if hit(text, [rx]):
            return label, [lat, lon]
    return None, None

def scene_first(text, places):
    """Reorder matched places so any marked as the scene of the story lead."""
    if len(places) < 2:
        return places
    terms = {}
    for _rid, _rl, sublist in GEO3_C:
        for _sid, _sl, plist in sublist:
            for pid, _pl, compiled in plist:
                if pid in places:
                    terms[pid] = compiled
    scene, rest = [], []
    for pid in places:
        (scene if _is_scene(text, _first_pos(text, terms.get(pid, []))) else rest).append(pid)
    return scene + rest


# --------------------------------------------------------------------------
# The gazetteer answers with a country; this wire's taxonomy is keyed on ids
# whose leading token is that country's ISO-2. Filing a placed story under its
# region is therefore a lookup, not a guess. Where a country is split across
# several places, only region and subregion are filled: which of the places a
# story belongs to is a question the country code cannot answer.
# --------------------------------------------------------------------------
ISO_REGION = {}
for _rid, _rlabel, _subs in GEO3:
    for _sid, _slabel, _places in _subs:
        for _pid, _plabel, _terms in _places:
            _iso = _pid.split("-")[0].lower()
            if len(_iso) == 2:
                ISO_REGION.setdefault(_iso, (_rid, _sid))


def file_by_country(row, cc):
    """Put a gazetteer-placed story in its region, if the wire has one."""
    if not cc:
        return
    hit = ISO_REGION.get(str(cc).lower())
    if not hit:
        return
    rid, sid = hit
    if not row.get("w") or row["w"] == ["unlocated"]:
        row["w"] = [rid]
    if not row.get("sr") or row["sr"] == ["unlocated"]:
        row["sr"] = [sid]



def country_for(raw, locale=None):
    """The ISO-2 the placement resolved to, or None."""
    if not _GAZETTEER:
        return None
    try:
        return galaxy_places.resolve_full(raw, locale)[4]
    except Exception:
        return None


def point_for(text, places, subs, regions, locale=None, raw=None):
    """The most specific point a story resolved to.

    The order is deliberate. This wire's own curated table goes first: it holds
    the places this subject actually turns up and the country list it was
    written against, and it beats a general gazetteer on its own ground. The
    shared gazetteer follows but only overrides at the settlement level, so a
    headline naming Kharkiv pins on Kharkiv rather than the middle of Ukraine,
    while a country reading from this wire's own table still wins over a
    country reading from the gazetteer. Then the bodies that stand for a
    jurisdiction without naming it — EFSA is a European story, ANVISA a
    Brazilian one. Last, and weakest, the country the source itself reports
    from.

    Returns (label_or_None, point_or_None, approx). approx is True only for
    that last case, where nothing in the story placed it and the point is the
    reporting locale rather than the scene. The page draws those hollow.
    """
    label, point = precise_for(text)
    if point:
        return label, point, False

    glabel, gpoint, grank = None, None, -1
    if _GAZETTEER:
        glabel, gpoint, grank, _approx = galaxy_places.resolve_ranked(raw or text)
        if grank == 3:
            return glabel, gpoint, False

    places = scene_first(text, places)
    for level in (places, subs, regions):
        for pid in level:
            if pid in COORDS:
                return None, COORDS[pid], False

    if gpoint:
        return glabel, gpoint, False

    if _GAZETTEER and locale:
        llabel, lpoint, _lrank, lapprox = galaxy_places.resolve_ranked("", locale)
        if lpoint:
            return llabel, lpoint, lapprox

    return None, None, False


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
                         "kind": "news", "url": build_gnews_url(loc), "gl": loc.get("gl")})
    return srcs, cfg


def run(dry_run=False, fixtures=None):
    global DEADLINE
    sources, cfg = load_sources()
    if not fixtures:
        DEADLINE = time.monotonic() + READ_BUDGET_MIN * 60
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
                regions, subs, places = places_for(text)
                total, reasons = pressure(text, src["standing"], regions != ["unlocated"])
                # No silent default. This used to read `or ["territory"]`,
                # which filed 112 of 175 stories under a subject none of them
                # had matched and made the wire's own subject counts a fiction —
                # territory read 149 where 37 had actually matched it. A story
                # that no subject will claim is refused, as on the other wires.
                subjects = topics_for(text)
                if not subjects:
                    stat["refused"] += 1
                    refused += 1
                    continue
                row["x"] = subjects
                row["w"] = regions
                row["sr"] = subs
                row["pl"] = places
                row["gl"] = src.get("gl")
                _raw = (row["t"] or "") + " " + (row.get("s") or "")
                row["pn"], row["ll"], row["pa"] = point_for(
                    text, places, subs, regions, src.get("gl"), _raw)
                if row["ll"]:
                    file_by_country(row, country_for(_raw, src.get("gl")))
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
        if "x" not in row:
            continue
        # A retained story is placed again rather than carried forward with the
        # answer it happened to get the day it was first read. RETAIN_DAYS is
        # 45, so without this a change to the placement layer takes a month and
        # a half to reach the map, and a story never re-fetched keeps its first
        # answer for good. Rows already holding a point resolved from their own
        # text are left alone; only the unplaced and the source-country
        # approximations are reconsidered.
        if not row.get("ll") or row.get("pa"):
            _raw = ((row.get("t") or "") + " " + (row.get("s") or ""))
            row["pn"], row["ll"], row["pa"] = point_for(
                _raw.lower(), row.get("pl") or [], row.get("sr") or [],
                row.get("w") or [], row.get("gl"), _raw)
            if row["ll"]:
                file_by_country(row, country_for(_raw, row.get("gl")))
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
        "coords": COORDS,
        "geo": ([{"id": rid, "label": rlabel,
                  "subs": [{"id": sid, "label": slabel,
                            "places": [{"id": pid, "label": plabel} for pid, plabel, _t in places]}
                           for sid, slabel, places in subs]}
                 for rid, rlabel, subs in GEO3] +
                [{"id": "unlocated", "label": "No single region", "subs": []}]),
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
