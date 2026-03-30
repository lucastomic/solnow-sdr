#!/usr/bin/env python3
"""
SolNow — Web Enrichment with AI

Scrapes operator websites (including subpages) and uses Claude Haiku to extract
structured data (activity category, pricing, fleet size, etc.) for GMV estimation.
Falls back to GetYourGuide search when operator website lacks pricing.
"""

import asyncio
import json
import logging
import re
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ── Claude extraction prompt ─────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
Eres un extractor de datos para una empresa de software náutico español.

Analiza el siguiente contenido de una web de empresa de actividades acuáticas y extrae ÚNICAMENTE estos datos en formato JSON. Si no encuentras un dato, pon null.

{html_content}

Devuelve SOLO este JSON, sin texto adicional, sin markdown:
{{
  "categoria_principal": "jet_ski|kayak|paddle_surf|charter_privado|excursion_barco|buceo|mixto|otro",
  "actividades": ["lista de actividades que ofrece"],
  "num_activos": null,
  "descripcion_activos": null,
  "precio_minimo": null,
  "precio_maximo": null,
  "precio_medio": null,
  "tipo_precio": "por_persona|por_grupo|por_hora|por_dia|mixto|null",
  "capacidad_maxima_por_salida": null,
  "duracion_tipica_minutos": null,
  "menciona_contrato": false,
  "tiene_reserva_online": false,
  "menciona_numero_guias": null
}}
"""

# Prompt for inferring category from business name alone
CATEGORY_INFERENCE_PROMPT = """\
Dado el nombre de este negocio de actividades acuáticas en España, clasifícalo en UNA de estas categorías:
jet_ski | kayak | paddle_surf | charter_privado | excursion_barco | buceo | mixto | otro | no_acuatico

Nombre: "{nombre}"

Responde SOLO con la categoría, sin explicación.

Reglas:
- "motos de agua", "jet ski", "moto acuática" → jet_ski
- "kayak", "canoa", "piragua" → kayak
- "paddle", "SUP", "surf de remo" → paddle_surf
- "charter", "velero", "yate", "alquiler barco" → charter_privado
- "excursión", "paseo en barco", "catamaran", "catamarán" → excursion_barco
- "buceo", "diving", "snorkel" → buceo
- Múltiples actividades → mixto
- Gimnasio, tienda, restaurante, tour a pie → no_acuatico
"""


# Default result when extraction fails
_EMPTY_RESULT = {
    "categoria_principal": None,
    "actividades": [],
    "num_activos": None,
    "descripcion_activos": None,
    "precio_minimo": None,
    "precio_maximo": None,
    "precio_medio": None,
    "tipo_precio": None,
    "capacidad_maxima_por_salida": None,
    "duracion_tipica_minutos": None,
    "menciona_contrato": False,
    "tiene_reserva_online": False,
    "menciona_numero_guias": None,
    "emails": [],
}

# ── Email extraction (no AI needed) ──────────────────────────────────────────

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

_EMAIL_DOMAIN_BLACKLIST = {
    "example.com", "example.org", "sentry.io", "wixpress.com",
    "wix.com", "squarespace.com", "wordpress.com", "googleapis.com",
    "googleusercontent.com", "w3.org", "schema.org", "facebook.com",
    "twitter.com", "instagram.com", "youtube.com",
}

_EMAIL_EXTENSION_BLACKLIST = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js"}


def extract_emails(html: str) -> list[str]:
    """Extract email addresses from raw HTML using mailto: links and regex.

    Filters out common false positives (tracking pixels, CMS internals, etc.).
    Returns deduplicated, sorted list of emails.
    """
    emails = set()

    # 1. mailto: links (most reliable)
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            addr = href[7:].split("?")[0].strip().lower()
            if addr:
                emails.add(addr)

    # 2. Regex scan on raw HTML
    for match in _EMAIL_RE.findall(html):
        emails.add(match.lower())

    # 3. Filter out junk
    filtered = []
    for email in emails:
        domain = email.split("@", 1)[1] if "@" in email else ""
        # Skip blacklisted domains
        if domain in _EMAIL_DOMAIN_BLACKLIST:
            continue
        # Skip file-extension-looking addresses
        if any(email.endswith(ext) for ext in _EMAIL_EXTENSION_BLACKLIST):
            continue
        # Skip noreply / no-reply
        local = email.split("@")[0]
        if local in ("noreply", "no-reply", "mailer-daemon", "postmaster"):
            continue
        filtered.append(email)

    return sorted(set(filtered))


# ── Subpage discovery ────────────────────────────────────────────────────────

PRICE_PAGE_PATHS = [
    "/precios", "/tarifas", "/actividades", "/servicios",
    "/reservas", "/booking", "/rates", "/prices",
    "/alquiler", "/excursiones", "/tours",
]

_PRICE_SIGNALS = ["€", "precio", "tarifa", "desde", "hora", "minuto", "persona", "grupo"]


def contains_price_signals(html: str) -> bool:
    """Check if HTML contains enough pricing-related keywords."""
    html_lower = html.lower()
    return sum(1 for s in _PRICE_SIGNALS if s in html_lower) >= 3


# ── HTML fetching ────────────────────────────────────────────────────────────

async def fetch_static(url: str, client: httpx.AsyncClient, timeout: int = 15) -> str | None:
    """Fetch a web page with httpx. Returns HTML string or None on failure."""
    try:
        resp = await client.get(url, timeout=timeout, follow_redirects=True)
        if resp.status_code >= 400:
            log.debug("HTTP %d for %s", resp.status_code, url)
            return None
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type and "text" not in content_type:
            return None
        return resp.text
    except Exception as exc:
        log.debug("Fetch failed for %s: %s", url, exc)
        return None


async def find_best_page(base_url: str, client: httpx.AsyncClient) -> str | None:
    """Try homepage first, then common price/activity subpages."""
    # Try homepage
    homepage_html = await fetch_static(base_url, client)
    if homepage_html and contains_price_signals(homepage_html):
        return homepage_html

    # Try subpages in parallel
    base = base_url.rstrip("/")
    subpage_urls = [base + path for path in PRICE_PAGE_PATHS]

    tasks = [fetch_static(url, client) for url in subpage_urls]
    results = await asyncio.gather(*tasks)

    for html in results:
        if html and contains_price_signals(html):
            return html

    # Fallback to homepage even without price signals
    return homepage_html


# ── HTML cleaning ────────────────────────────────────────────────────────────

def clean_for_llm(html: str, max_chars: int = 15000) -> str:
    """Strip scripts/styles/nav/footer and extract text, limited to max_chars."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove noisy elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "header",
                              "iframe", "noscript", "svg", "form"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)

    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    if len(text) > max_chars:
        text = text[:max_chars] + "\n[...truncado]"

    return text


# ── GetYourGuide fallback ────────────────────────────────────────────────────

GYG_SEARCH_URL = "https://www.getyourguide.es/s/"

GYG_EXTRACTION_PROMPT = """\
Analiza estos resultados de búsqueda de GetYourGuide para el operador "{nombre}" en {ciudad}.
Extrae SOLO el precio y categoría del primer resultado relevante.

{html_content}

Devuelve SOLO este JSON:
{{
  "precio_medio": null,
  "tipo_precio": "por_persona|por_grupo|por_hora|null",
  "categoria_principal": "jet_ski|kayak|paddle_surf|charter_privado|excursion_barco|buceo|mixto|otro|null"
}}
"""


async def fetch_gyg_fallback(
    nombre: str,
    ciudad: str,
    anthropic_client,
    http_client: httpx.AsyncClient,
) -> dict | None:
    """Search GetYourGuide for operator pricing as fallback."""
    query = f"{nombre} {ciudad}"
    try:
        resp = await http_client.get(
            GYG_SEARCH_URL,
            params={"q": query},
            timeout=15,
        )
        if resp.status_code >= 400:
            return None

        html = resp.text
        if not html or len(html) < 500:
            return None

        clean_text = clean_for_llm(html, max_chars=10000)
        if len(clean_text.strip()) < 100:
            return None

        prompt = GYG_EXTRACTION_PROMPT.format(
            nombre=nombre, ciudad=ciudad, html_content=clean_text,
        )

        message = await asyncio.to_thread(
            anthropic_client.messages.create,
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        json_match = re.search(r"\{[\s\S]*\}", raw)
        if json_match:
            return json.loads(json_match.group())
    except Exception as exc:
        log.debug("GYG fallback failed for %s: %s", nombre, exc)

    return None


# ── Claude Haiku extraction ──────────────────────────────────────────────────

async def claude_extract(clean_text: str, anthropic_client) -> dict:
    """Call Claude Haiku to extract structured data from website text."""
    if not clean_text or len(clean_text.strip()) < 50:
        return dict(_EMPTY_RESULT)

    prompt = EXTRACTION_PROMPT.format(html_content=clean_text)

    try:
        message = await asyncio.to_thread(
            anthropic_client.messages.create,
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        # Try to extract JSON from the response
        json_match = re.search(r"\{[\s\S]*\}", raw)
        if json_match:
            result = json.loads(json_match.group())
            # Ensure all expected keys exist
            for key, default in _EMPTY_RESULT.items():
                result.setdefault(key, default)
            return result

        log.warning("No JSON found in Claude response")
        return dict(_EMPTY_RESULT)

    except json.JSONDecodeError as exc:
        log.warning("JSON parse error from Claude: %s", exc)
        return dict(_EMPTY_RESULT)
    except Exception as exc:
        log.warning("Claude extraction failed: %s", exc)
        return dict(_EMPTY_RESULT)


# ── Name-based category inference ────────────────────────────────────────────

async def infer_category_from_name(nombre: str, anthropic_client) -> str | None:
    """Use Claude to infer activity category from business name alone.

    Returns category string or None. Returns 'no_acuatico' for non-water businesses.
    """
    if not nombre or len(nombre.strip()) < 3:
        return None

    prompt = CATEGORY_INFERENCE_PROMPT.format(nombre=nombre)

    try:
        message = await asyncio.to_thread(
            anthropic_client.messages.create,
            model="claude-haiku-4-5-20251001",
            max_tokens=32,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip().lower()

        valid = {
            "jet_ski", "kayak", "paddle_surf", "charter_privado",
            "excursion_barco", "buceo", "mixto", "otro", "no_acuatico",
        }
        # Extract just the category token
        for cat in valid:
            if cat in raw:
                return cat
        return None
    except Exception as exc:
        log.debug("Category inference failed for '%s': %s", nombre, exc)
        return None


# ── Single operator enrichment ───────────────────────────────────────────────

async def enrich_operator(
    operator: dict,
    anthropic_client,
    http_client: httpx.AsyncClient,
    cache: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Enrich a single operator by scraping its website and extracting data."""
    web_url = operator.get("web", "")
    nombre = operator.get("nombre", "?")
    zona = operator.get("zona", "")

    if not web_url:
        return dict(_EMPTY_RESULT)

    # Cache by domain to avoid re-scraping the same site
    domain = urlparse(web_url).netloc.lower()
    if domain in cache:
        return cache[domain]

    async with semaphore:
        # Step 1: Find best page (homepage or subpage with prices)
        html = await find_best_page(web_url, http_client)

        if not html or len(html) < 500:
            result = dict(_EMPTY_RESULT)
            result["emails"] = []
            cache[domain] = result
            return result

        # Step 1b: Extract emails from raw HTML (before cleaning)
        emails = extract_emails(html)

        # Also try homepage for emails if we landed on a subpage
        homepage_html = await fetch_static(web_url, http_client)
        if homepage_html and homepage_html != html:
            emails = sorted(set(emails + extract_emails(homepage_html)))

        clean_text = clean_for_llm(html)
        result = await claude_extract(clean_text, anthropic_client)
        result["emails"] = emails

        # Step 2: If no price found, try GetYourGuide fallback
        if result.get("precio_medio") is None:
            gyg = await fetch_gyg_fallback(nombre, zona, anthropic_client, http_client)
            if gyg:
                if gyg.get("precio_medio") is not None:
                    result["precio_medio"] = gyg["precio_medio"]
                if gyg.get("tipo_precio"):
                    result["tipo_precio"] = result.get("tipo_precio") or gyg["tipo_precio"]
                if gyg.get("categoria_principal") and not result.get("categoria_principal"):
                    result["categoria_principal"] = gyg["categoria_principal"]

        cache[domain] = result
        return result


# ── Batch enrichment ─────────────────────────────────────────────────────────

async def enrich_all(
    operators: list[dict],
    anthropic_api_key: str,
    on_progress=None,
) -> list[dict]:
    """Enrich all operators that have a website.

    Args:
        operators: List of operator dicts (must have "web" key).
        anthropic_api_key: Anthropic API key for Claude Haiku.
        on_progress: Optional callback(current, total, operator_name) for progress.

    Returns:
        The same list with enrichment fields merged in.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=anthropic_api_key)
    cache: dict = {}
    semaphore = asyncio.Semaphore(10)  # max 10 concurrent requests

    # Identify operators with websites
    to_enrich = [(i, op) for i, op in enumerate(operators) if op.get("web")]
    total = len(to_enrich)

    if total == 0:
        return operators

    log.info("Enriching %d operators with websites...", total)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; SolNowBot/1.0)"},
        follow_redirects=True,
    ) as http_client:
        tasks = []
        for idx, (i, op) in enumerate(to_enrich):
            tasks.append(_enrich_one(
                i, op, client, http_client, cache, semaphore,
                on_progress, idx, total,
            ))

        await asyncio.gather(*tasks)

    # Phase 2: Infer category from name for operators still missing it
    log.info("Inferring categories from names for uncategorized operators...")
    missing_cat = [op for op in operators if not op.get("categoria_principal")]
    if missing_cat:
        infer_sem = asyncio.Semaphore(10)
        infer_tasks = [
            _infer_cat_one(op, client, infer_sem)
            for op in missing_cat
        ]
        await asyncio.gather(*infer_tasks)

    return operators


async def _infer_cat_one(operator: dict, anthropic_client, semaphore: asyncio.Semaphore):
    """Infer category from operator name if missing."""
    async with semaphore:
        cat = await infer_category_from_name(operator.get("nombre", ""), anthropic_client)
        if cat:
            operator["categoria_principal"] = cat


async def _enrich_one(
    index: int,
    operator: dict,
    anthropic_client,
    http_client: httpx.AsyncClient,
    cache: dict,
    semaphore: asyncio.Semaphore,
    on_progress,
    current: int,
    total: int,
):
    """Enrich a single operator and merge results back."""
    name = operator.get("nombre", "?")

    try:
        result = await enrich_operator(operator, anthropic_client, http_client, cache, semaphore)
        operator.update(result)
    except Exception as exc:
        log.warning("Enrichment failed for %s: %s", name, exc)

    if on_progress:
        try:
            on_progress(current + 1, total, name)
        except Exception:
            pass
