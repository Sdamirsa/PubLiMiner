"""
affiliation_parser.py — parse PubMed affiliation strings into structured components.

Returns (university, hospital, institution) for each raw affiliation string.
- university : degree-granting / educational institution
- hospital   : clinical care institution (incl. university hospitals)
- institution: research institute, centre, or foundation that is not a university or hospital

Design notes
────────────
• Input strings follow the breadcrumb pattern (most specific → most general):
      [sub-unit,]* [hospital / institute,]* [university,]* city[, state][, country]
• We split on ", " and classify each token left-to-right.
• University Hospital tokens (e.g. "Universitätsklinikum X") are classified as
  hospital rather than university, because the associated university is usually
  the next token.
• Encoding artefacts (√§ → ä etc.) are normalised before matching.
• Tokens that are departments/divisions are skipped; address tokens are skipped.
• The first accepted token wins for each category.
• No-comma strings (1.3 % of corpus) get a full-string keyword scan fallback.
• Returns None for any category not found — never guesses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ParsedAffiliation:
    university: Optional[str] = None
    hospital: Optional[str] = None
    institution: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Geography lookup tables
# ─────────────────────────────────────────────────────────────────────────────

_COUNTRIES: frozenset[str] = frozenset({
    "afghanistan", "albania", "algeria", "andorra", "angola", "argentina",
    "armenia", "australia", "austria", "azerbaijan", "bahrain", "bangladesh",
    "belarus", "belgium", "belize", "bhutan", "bolivia", "bosnia", "botswana",
    "brazil", "brunei", "bulgaria", "cambodia", "cameroon", "canada",
    "chile", "china", "colombia", "congo", "croatia", "cuba", "cyprus",
    "czech republic", "czechia", "denmark", "ecuador", "egypt", "eritrea",
    "estonia", "ethiopia", "finland", "france", "georgia", "germany",
    "ghana", "greece", "guatemala", "haiti", "honduras", "hungary", "iceland",
    "india", "indonesia", "iran", "iraq", "ireland", "israel", "italy",
    "jamaica", "japan", "jordan", "kazakhstan", "kenya", "kosovo", "kuwait",
    "laos", "latvia", "lebanon", "liechtenstein", "lithuania", "luxembourg",
    "malaysia", "mali", "malta", "mexico", "moldova", "monaco", "mongolia",
    "montenegro", "morocco", "mozambique", "myanmar", "namibia", "nepal",
    "netherlands", "new zealand", "nigeria", "north korea", "north macedonia",
    "norway", "oman", "pakistan", "panama", "paraguay", "peru", "philippines",
    "poland", "portugal", "qatar", "romania", "russia", "saudi arabia",
    "senegal", "serbia", "singapore", "slovakia", "slovenia", "south africa",
    "south korea", "spain", "sri lanka", "sweden", "switzerland", "taiwan",
    "tajikistan", "tanzania", "thailand", "tunisia", "turkey", "ukraine",
    "united arab emirates", "united kingdom", "united states", "uruguay",
    "uzbekistan", "venezuela", "vietnam", "yemen", "zimbabwe",
    # abbreviations & alternate forms
    "usa", "u.s.a.", "uk", "u.k.", "uae", "p.r.c.", "pr china",
    "people's republic of china", "republic of korea", "republic of china",
    # territories
    "england", "scotland", "wales", "northern ireland",
    "hong kong", "macau", "macao", "puerto rico", "guam",
    # common variants
    "turkiye", "türkiye", "the netherlands", "holland",
    # continent-as-country (sometimes appears)
    "europe", "asia",
})

_US_STATES: frozenset[str] = frozenset({
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
    # 2-letter
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi",
    "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi",
    "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc",
    "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut",
    "vt", "va", "wa", "wv", "wi", "wy", "dc", "d.c.",
})

# Short known city tokens that should be treated as addresses
# (kept small — only cities that commonly appear ALONE as a comma token AND
#  could be mistaken for institution names)
_KNOWN_CITIES: frozenset[str] = frozenset({
    "london", "paris", "berlin", "rome", "milan", "tokyo", "beijing",
    "shanghai", "guangzhou", "seoul", "singapore", "toronto", "montreal",
    "vancouver", "sydney", "melbourne", "amsterdam", "rotterdam", "leiden",
    "utrecht", "brussels", "stockholm", "oslo", "copenhagen", "helsinki",
    "vienna", "zurich", "geneva", "madrid", "barcelona", "lisbon",
    "athens", "budapest", "prague", "warsaw", "bucharest", "zagreb",
    "belgrade", "sofia", "bratislava", "ljubljana", "tallinn", "riga",
    "vilnius", "reykjavik", "nicosia", "munich", "hamburg", "cologne",
    "frankfurt", "stuttgart", "düsseldorf", "heidelberg", "tübingen",
    "erlangen", "freiburg", "göttingen", "mainz", "münchen",
    "boston", "new york", "chicago", "houston", "dallas", "phoenix",
    "philadelphia", "san antonio", "san diego", "san francisco",
    "seattle", "denver", "minneapolis", "atlanta", "detroit", "miami",
    "baltimore", "cleveland", "pittsburgh", "st. louis", "portland",
    "nashville", "kansas city", "columbus", "indianapolis",
    "osaka", "kyoto", "nagoya", "sapporo", "hiroshima", "fukuoka",
    "taipei", "hong kong", "bangkok", "kuala lumpur", "jakarta",
    "mumbai", "delhi", "kolkata", "chennai", "bangalore",
    "cairo", "johannesburg", "cape town", "nairobi",
    "melbourne", "brisbane", "perth", "auckland",
    "istanbul", "ankara", "tehran", "tel aviv", "jerusalem",
    "moscow", "st. petersburg", "kyiv", "kiev", "minsk",
    "rio de janeiro", "são paulo", "sao paulo", "buenos aires",
    "santiago", "bogotá", "bogota", "lima",
})

# ─────────────────────────────────────────────────────────────────────────────
# Encoding normalisation
# ─────────────────────────────────────────────────────────────────────────────

# Mojibake produced when UTF-8 is decoded as Latin-1 / Windows-1252
_MOJIBAKE: dict[str, str] = {
    "√§": "ä", "√©": "é", "√†": "â", "√≥": "ó", "√ü": "ü", "√∂": "ö",
    "√Å": "å", "√°": "à", "√è": "è", "√≠": "í", "√π": "ú", "√¶": "æ",
    "√∏": "ø", "√•": "å",
    # Also handle the literal escaped form sometimes present in JSON strings
    "\\u00e4": "ä", "\\u00f6": "ö", "\\u00fc": "ü",
    "\\u00e9": "é", "\\u00e8": "è", "\\u00e0": "à",
    "\\u00c4": "Ä", "\\u00d6": "Ö", "\\u00dc": "Ü",
    # German sharp-s variants
    "\\u00df": "ß", "√∞": "ß",
}

_MOJIBAKE_PAT = re.compile("|".join(re.escape(k) for k in _MOJIBAKE))


def _normalize(s: str) -> str:
    """Fix encoding artefacts, remove emails and boilerplate prefixes."""
    # Mojibake
    s = _MOJIBAKE_PAT.sub(lambda m: _MOJIBAKE[m.group(0)], s)
    # Null / replacement chars
    s = s.replace("�", "").replace("\x00", "")
    # Remove email addresses
    s = re.sub(r"\b[\w.+%-]+@[\w.-]+\.[A-Za-z]{2,}\b", "", s)
    # Remove URLs
    s = re.sub(r"https?://\S+", "", s)
    # "From the Department…" prefix
    s = re.sub(r"^From\s+the\s+", "", s, flags=re.IGNORECASE)
    # Leading number prefix:  "2 Department of…" → "Department of…"
    s = re.sub(r"^\d+\s+(?=[A-Z])", "", s)
    # Leading lowercase letter prefix: "aCentro…" → "Centro…"
    # (Only strip if it's a single lowercase letter followed by space + uppercase)
    s = re.sub(r"^[a-z]\s+(?=[A-Z])", "", s)
    # Strip trailing period, comma, semicolon, whitespace
    s = s.strip().rstrip(".,; ")
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Classification regex patterns
# ─────────────────────────────────────────────────────────────────────────────

# --- departments, divisions, sub-units (SKIP) ---
# Matches at the START of a token.  Use re.match (implicit ^).
_DEPT_RE = re.compile(
    r"""(?ix)
    ^(?:
        Department\s+of | Dept\.?\s+of |
        Division\s+of |
        Section\s+of |
        Unit\s+of |
        Laboratory\s+of | Laboratory\b | \bLab\b(?:\s+of|\s+for)? |
        Program(?:me)?\s+of | Program(?:me)\b |
        Service\s+of |
        Faculty\s+of(?!\s+(?:Medicine|Health|Medical)) |
        # German department prefixes
        Klinik\s+für |           # "Klinik für Innere Medizin" = dept
        Institut\s+für |         # "Institut für Radiologie" = dept
        Abteilung(?:\s+für)? |
        Dipartimento(?:\s+di)? |
        Département(?:\s+de)? |
        # French-style
        Unité\s+(?:de|d') |
        # Italian
        Unità\s+Operativa |
        Sezione\s+di |
        # Lone specialty as entire token (e.g. "Cardiology" alone between commas)
        (?:Cardiology|Radiology|Medicine|Surgery|Neurology|Oncology|
           Pediatrics|Paediatrics|Psychiatry|Pathology|Urology|Ophthalmology|
           Dermatology|Gastroenterology|Nephrology|Pulmonology|Hematology|
           Endocrinology|Anesthesiology|Anesthesia|Anaesthesiology|
           Orthopaedics|Orthopedics|Obstetrics|Gynecology|
           Rheumatology|Immunology|Radiology)\s*$
    )
    """,
)

# --- University Hospital / Academic Medical Centre (→ hospital, not university) ---
# These tokens contain BOTH a university-like word AND a hospital-like word.
# We resolve the ambiguity by classifying them as hospital.
_UNIV_HOSP_RE = re.compile(
    r"""(?ix)
    \b(?:
        University\s+(?:Hospital[s]?|Medical\s+(?:Center|Centre)[s]?) |
        Universitätsklinikum | Universitaetsklinikum |
        Universitätsmedizin  | Universitaetsmedizin  |
        # Dutch / Belgian
        Universitair(?:e)?\s+(?:Ziekenhuis|Medisch\s+Centrum|Medical\s+Centre) |
        # French
        Centre\s+Hospitalier\s+Universitaire | CHU\b |
        # Italian / Spanish / Portuguese
        Policlinico\s+Universitario | Hospital\s+Universitario |
        Centro\s+Hospitalar\s+Universitário | Centro\s+Hospitalar\s+Universitario |
        # Generic
        Academic\s+Medical\s+(?:Center|Centre) |
        Teaching\s+Hospital |
        # Charité is both a university and a hospital; classify as hospital
        Charité(?:-Universitätsmedizin)? | Charite\b
    )\b
    """,
)

# --- University (degree-granting, educational) ---
_UNIV_RE = re.compile(
    r"""(?ix)
    \b(?:
        Universit(?:y|ies|ät|aet|é|è|à|á|a\b|dad|ade|eit|eiten|as\b|eit) |
        Università | Université | Universidad | Universidade |
        Universitas | Universiteit |
        Medical\s+University | Medical\s+School | Medical\s+College |
        School\s+of\s+(?:Medicine|Public\s+Health|Nursing|Dentistry|Pharmacy|
                         Veterinary\s+Medicine) |
        College\s+of\s+(?:Medicine|Veterinary|Dentistry|Pharmacy) |
        Graduate\s+School\s+of\s+(?:Medicine|Medical) |
        Karolinska\s+Institutet |
        Institut\s+Pasteur |
        # Named US medical schools (common in this corpus)
        Harvard\s+Medical\s+School |
        Weill\s+(?:Cornell|Medical\s+College) |
        Icahn\s+School\s+of\s+Medicine |
        (?:Perelman|Pritzker|Feinberg|Keck|Jacobs|Morsani|Cumming|
           Geisel|Geffen|Miller)\s+(?:School\s+of\s+Medicine|College\s+of\s+Medicine) |
        Johns\s+Hopkins\s+(?:University|School\s+of\s+Medicine) |
        # UK colleges that ARE universities but use "College" not "University"
        Imperial\s+College |
        King'?s\s+College\s+London |
        University\s+College\s+London | UCL\b |
        London\s+School\s+of\s+(?:Hygiene|Medicine|Economics) |
        Queen\s+Mary\s+(?:University|College) |
        St\s+George['s]?\s+(?:University|Hospital\s+Medical\s+School) |
        # Other common standalone "College" universities
        Dartmouth\s+(?:College|Geisel) |
        Tufts\s+(?:University|School\s+of\s+Medicine) |
        # Named institutions that are universities without "University" in name
        Mayo\s+Clinic\s+Alix\s+School |
        Nuffield\s+(?:Department|College) |
        Sahlgrenska\s+Academy
    )\b
    """,
)

# --- Hospital (clinical care institution) ---
_HOSP_RE = re.compile(
    r"""(?ix)
    \b(?:
        Hospital[s]? | Hôpital | Hôpitaux | Hopital |
        Klinikum\b | Krankenhaus | Krankenhäuser |
        NHS\s+(?:Trust|Foundation\s+Trust|Foundation) |
        Medical\s+(?:Center|Centre)[s]? |
        Medical\s+System[s]? |
        Children['']?s?\s+(?:Hospital|Health|Medical) |
        General\s+Hospital |
        Memorial\s+(?:Hospital|Medical) |
        Regional\s+Hospital |
        National\s+(?:Heart\s+)?Hospital |
        Veterans?\s+(?:Administration|Affairs)\s+(?:Medical|Hospital) |
        \bVAMC\b |
        Ospedale[i]? |
        Azienda\s+Ospedaliera |
        Policlinico\b |
        Sjukhus(?:et)? |
        Ziekenhuis(?:en)? |
        Sjúkrahús |
        Rigshospitalet | Rikshospitalet | Haukeland |
        Karolinska\s+(?:Sjukhuset|University\s+Hospital) |
        # NHS named trusts (common pattern: "X Hospital Trust")
        (?:NHS\s+)?(?:Foundation\s+)?Trust\b |
        # Clinics
        \bClinic[s]?\b | Clinique[s]? | Clinica[s]? |
        # Danish / Norwegian
        Sygehus(?:et)? | Sykehus(?:et)? |
        # French hospital names
        Centre\s+Hospitalier(?!\s+Universitaire) |
        Hôpital\s+(?:Général|National|Universitaire|de) |
        # Portuguese
        Centro\s+Hospitalar(?!\s+Universitário) |
        # Health systems (USA / international)
        Health\s+(?:System[s]?|Network[s]?|Care\s+(?:System|Campus)) |
        (?:Beaumont|Langone|Northwell|Intermountain|Ascension|
           Providence|CommonSpirit|Atrium|Advocate|Geisinger|
           Sentara)\s+Health |
        NYU\s+Langone |
        # Polyclinic
        Polyclinic[s]? | Polyklinik[s]?
    )\b
    """,
)

# --- Research institution / centre / foundation (non-university, non-hospital) ---
_INST_RE = re.compile(
    r"""(?ix)
    \b(?:
        Research\s+(?:Institute|Foundation|Center|Centre) |
        Institute[s]?\s+(?:of|for) |
        # National institutes (NIH family)
        NIH\b | NHLBI\b | National\s+Institutes?\s+of\s+Health |
        # NHLBI with or without Oxford comma splitting:
        National\s+Heart,?\s+Lung,?\s+and\s+Blood\s+Institute |
        National\s+(?:Cancer|Heart|Lung|Blood|Neurological|Eye|Aging)\s+Institute |
        National\s+(?:Institute|Center)\s+(?:of|for) |
        # Italian
        IRCCS\b | Istituto\b |
        Centro\s+(?:Cardiologico|Nazionale|di\s+Ricerca) |
        # French
        INSERM\b | CNRS\b | INRIA\b |
        # German
        Helmholtz | Fraunhofer | Max\s+Planck | Leibniz |
        DZHK\b |                                # German Centre for Cardiovascular Research
        Deutsches\s+(?:Herzzentrum|Zentrum) |
        Herzzentrum\b |                         # "Herzzentrum Dresden"
        Zentrum\s+für |
        # UK
        Wellcome\s+(?:Trust|Sanger) | \bMRC\b |
        # Named US institutes
        Broad\s+Institute | Salk\s+Institute | Whitehead\s+Institute |
        Cold\s+Spring\s+Harbor |
        # Cardiovascular-specific centres
        Heart\s+(?:Center|Centre|Institute) |
        Cardiac\s+(?:Center|Centre|Institute) |
        Cardiovascular\s+(?:Research\s+)?(?:Center|Centre|Institute|Foundation) |
        Cancer\s+(?:Center|Centre) |
        Dalio\s+Institute |
        # Generic
        Center[s]?\s+for | Centre[s]?\s+for |
        Foundation[s]?\s+(?:for|of) |
        Baker\s+(?:Heart|Institute) |
        # Spanish / Portuguese
        \bInstituto\b |
        # Catch-all: standalone "Institute(s)" or "Institut" not already caught above.
        # Safe because DEPT_RE (checked first) already captures "Institut für X".
        \bInstitute[s]?\b | \bInstitut[s]?\b
    )\b
    """,
)


# ─────────────────────────────────────────────────────────────────────────────
# Address token detection
# ─────────────────────────────────────────────────────────────────────────────

# Tokens that look like postal codes
_POSTAL_RE = re.compile(
    r"""(?x)
    ^\d{4,6}(-\d{4})?$                  |  # US 5-digit or ZIP+4; generic 4-6 digit
    ^[A-Z]{1,2}\d[A-Z\d]\s*\d[A-Z]{2}$ |  # UK postcode  EC1A 1BB
    ^[A-Z]\d[A-Z]\s*\d[A-Z]\d$          |  # Canada  K1Y 4W7
    ^\d{4,5}\s[A-Z]{2,3}$                  # Dutch  1234 AB
    """,
)

# Tokens that start with a digit (street addresses)
_STARTS_DIGIT = re.compile(r"^\d")

# Institution keywords used to PROTECT a token from being classified as address
_ANY_INST_KW = re.compile(
    r"(?i)\b("
    r"universit|hospital|klinikum|klinik|medical|clinic|institute|institut|"
    r"center|centre|foundation|college|school|irccs|nhs|trust|sjukhus|"
    r"ziekenhuis|ospedale|policlinico|inserm|cnrs|dzhk|herzzentrum|"
    r"karolinska|charit|charité|rigshospitalet|rikshospitalet"
    r")\b"
)


def _is_address_token(tok: str) -> bool:
    """Return True if tok is clearly a geographic component (skip it)."""
    t = tok.strip()
    if not t:
        return True

    # Never skip tokens that contain institution keywords
    if _ANY_INST_KW.search(t):
        return False

    tl = t.lower().rstrip(".")

    if tl in _COUNTRIES:
        return True
    if tl in _US_STATES:
        return True
    if tl in _KNOWN_CITIES:
        return True

    if _POSTAL_RE.match(t):
        return True
    if _STARTS_DIGIT.match(t):
        return True

    # Short single-word token (≤ 15 chars) with no institution keyword → likely city/state
    words = t.split()
    if len(words) == 1 and len(t) <= 15:
        return True

    # Two-word token where the second word is a known country or US state → "Seoul Korea"
    if len(words) == 2:
        if words[1].lower() in _COUNTRIES or words[1].lower() in _US_STATES:
            return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Token classification
# ─────────────────────────────────────────────────────────────────────────────

# Pattern that matches the end of a "Department of <specialty>" prefix,
# i.e. "Department of" + anything up to (but not including) the next institution keyword.
# Used to skip the dept prefix and scan the remainder for embedded institution names.
_DEPT_PREFIX_END_RE = re.compile(
    r"""(?ix)
    ^(?:Department|Dept\.?|Division|Section|Unit|Laboratory|Lab|Program(?:me)?|Service)
    \s+(?:of|for|de|di|des|der|für)?\s*
    """,
)


def _embedded_institution(tok: str) -> tuple[str, str] | None:
    """
    If a DEPT token has institution keywords embedded after the dept prefix,
    extract (category, chunk).  E.g.:
      "Department of Cardiology Aarhus University Hospital"
      → ('university_hospital', 'Aarhus University Hospital')
    """
    m_prefix = _DEPT_PREFIX_END_RE.match(tok)
    if not m_prefix:
        return None
    # Scan the entire token (not just remainder) for institution keywords
    # so we can find the right window even with long specialty names
    for pat, cat in [
        (_UNIV_HOSP_RE, "university_hospital"),
        (_HOSP_RE, "hospital"),
        (_UNIV_RE, "university"),
        (_INST_RE, "institution"),
    ]:
        m = pat.search(tok)
        if m and m.start() >= m_prefix.end():
            # Expand backwards to include the pre-keyword word (e.g. "Aarhus" before "University")
            start = max(m_prefix.end(), m.start() - 25)
            end   = min(len(tok), m.end() + 25)
            chunk = tok[start:end].strip()
            return cat, chunk
    return None


def _classify(tok: str) -> str:
    """
    Classify a single comma-split token.
    Returns one of: dept | university_hospital | university | hospital | institution
                   | address | unknown
    """
    tok = tok.strip()
    if not tok:
        return "address"

    if _DEPT_RE.match(tok):
        # Before giving up, check for an embedded institution after the dept prefix
        # (e.g. "Department of Cardiology Aarhus University Hospital-Skejby")
        emb = _embedded_institution(tok)
        if emb:
            return emb[0]   # return the embedded category
        return "dept"

    if _is_address_token(tok):
        return "address"

    # university_hospital must be checked BEFORE university and hospital
    # so we don't misfile "University Hospital X" as university
    if _UNIV_HOSP_RE.search(tok):
        return "university_hospital"

    # Token contains BOTH a university keyword AND a hospital keyword even though
    # they are not adjacent (e.g. "Seoul National University Bundang Hospital").
    # Treat as university_hospital so it is assigned to the hospital field.
    if _UNIV_RE.search(tok) and _HOSP_RE.search(tok):
        return "university_hospital"

    if _UNIV_RE.search(tok):
        return "university"

    if _HOSP_RE.search(tok):
        return "hospital"

    if _INST_RE.search(tok):
        return "institution"

    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Token merging (Oxford-comma / split-name fix)
# ─────────────────────────────────────────────────────────────────────────────

def _merge_orphan_tokens(tokens: list[str]) -> list[str]:
    """
    Some institution names are split by enumeration commas:
      "National Heart, Lung and Blood Institute"  →  ["National Heart", "Lung and Blood Institute"]
      "National Heart, Lung, and Blood Institute" →  ["National Heart", "Lung", "and Blood Institute"]

    Rule: only merge when BOTH tokens are 'unknown' individually — this prevents
    incorrectly prepending sub-unit names (e.g. "Cardiac Imaging") onto a complete
    institution token that already classifies on its own ("University Hospital Zurich").

    We try up to 3-way merges for Oxford-comma cases.
    """
    result: list[str] = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if _classify(t) == "unknown":
            # 3-way merge for Oxford-comma splits like "National Heart, Lung, and Blood Institute".
            # The middle token must be short (≤ 3 words) and not already a valid institution;
            # the last token must carry an institution keyword.
            # We do NOT require tokens[i+2] to be 'unknown' — it may already weakly classify
            # (e.g. "and Blood Institute" matches the bare \bInstitute\b pattern) but the
            # merged string produces a BETTER / more specific classification.
            if (
                i + 2 < len(tokens)
                and _classify(tokens[i + 1]) in ("unknown", "address")
                and len(tokens[i + 1].split()) <= 3
                and _ANY_INST_KW.search(tokens[i + 2])
                and _classify(tokens[i + 1]) != "university_hospital"  # never extend a strong hospital
            ):
                merged3 = t + ", " + tokens[i + 1] + ", " + tokens[i + 2]
                cls3 = _classify(merged3)
                if cls3 != "unknown":
                    result.append(merged3)
                    i += 3
                    continue
            # 2-way merge: only when the next token is ALSO unknown on its own
            # (prevents prepending sub-unit names onto already-complete institution tokens)
            if (
                i + 1 < len(tokens)
                and _classify(tokens[i + 1]) == "unknown"
                and _ANY_INST_KW.search(tokens[i + 1])
            ):
                merged = t + ", " + tokens[i + 1]
                if _classify(merged) != "unknown":
                    result.append(merged)
                    i += 2
                    continue
        result.append(t)
        i += 1
    return result


def _strip_conjunction(s: str) -> str:
    """Strip leading conjunction artefacts like 'and X' or 'or X'."""
    return re.sub(r"^(?:and|or|the)\s+", "", s, flags=re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# No-comma fallback (1.3 % of corpus)
# ─────────────────────────────────────────────────────────────────────────────

def _no_comma_fallback(segment: str) -> ParsedAffiliation:
    """
    For strings with no commas (typically Asian or old-format affiliations),
    scan the full string for the first institution keyword and extract a
    reasonable window around it.
    """
    out = ParsedAffiliation()

    # Try university_hospital first
    m = _UNIV_HOSP_RE.search(segment)
    if m:
        start = max(0, m.start() - 30)
        end = min(len(segment), m.end() + 30)
        out.hospital = segment[start:end].strip(" ,;")
        return out

    # University
    m = _UNIV_RE.search(segment)
    if m:
        start = max(0, m.start() - 25)
        end = min(len(segment), m.end() + 25)
        out.university = segment[start:end].strip(" ,;")
        return out

    # Hospital
    m = _HOSP_RE.search(segment)
    if m:
        start = max(0, m.start() - 25)
        end = min(len(segment), m.end() + 35)
        out.hospital = segment[start:end].strip(" ,;")
        return out

    # Institution
    m = _INST_RE.search(segment)
    if m:
        start = max(0, m.start() - 25)
        end = min(len(segment), m.end() + 35)
        out.institution = segment[start:end].strip(" ,;")
        return out

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_affiliation(raw: str) -> ParsedAffiliation:
    """
    Parse a raw PubMed affiliation string into (university, hospital, institution).

    Example
    -------
    >>> p = parse_affiliation(
    ...     "Department of Radiology, Massachusetts General Hospital, "
    ...     "Harvard Medical School, Boston, MA, USA."
    ... )
    >>> p.hospital
    'Massachusetts General Hospital'
    >>> p.university
    'Harvard Medical School'
    """
    if not raw or not raw.strip():
        return ParsedAffiliation()

    s = _normalize(raw)
    # Take only the FIRST semicolon-delimited segment (the primary affiliation)
    segment = s.split(";")[0].strip()

    tokens = [t.strip() for t in segment.split(",")]
    tokens = _merge_orphan_tokens(tokens)

    out = ParsedAffiliation()
    for tok in tokens:
        if not tok:
            continue

        # Check for dept token with embedded institution name BEFORE calling _classify
        if _DEPT_RE.match(tok):
            emb = _embedded_institution(tok)
            if emb:
                cat, chunk = emb
                if cat == "university_hospital" and out.hospital is None:
                    out.hospital = chunk
                elif cat == "university" and out.university is None:
                    out.university = chunk
                elif cat == "hospital" and out.hospital is None:
                    out.hospital = chunk
                elif cat == "institution" and out.institution is None:
                    out.institution = chunk
            continue   # whether or not we found an embedded entity, skip the dept token itself

        cls = _classify(tok)
        if cls in ("dept", "address", "unknown"):
            continue
        val = _strip_conjunction(tok)
        if cls == "university_hospital":
            if out.hospital is None:
                out.hospital = val
        elif cls == "university":
            if out.university is None:
                out.university = val
        elif cls == "hospital":
            if out.hospital is None:
                out.hospital = val
        elif cls == "institution":
            if out.institution is None:
                out.institution = val

    # No-comma fallback
    if (
        out.university is None
        and out.hospital is None
        and out.institution is None
        and "," not in segment
    ):
        out = _no_comma_fallback(segment)

    return out
