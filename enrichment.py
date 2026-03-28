#!/usr/bin/env python3
"""
SolNow — Web Enrichment with AI

Scrapes operator websites and uses Claude Haiku to extract structured data
(activity category, pricing, fleet size, etc.) for GMV estimation.
"""

import asyncio
import json
import logging
import re
from urllib.parse import urlparse

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
}


# ── HTML fetching ────────────────────────────────────────────────────────────

async def fetch_static(url: str, client: httpx.AsyncClient, timeout: int = 15) -> str | None:
    """Fetch a web page with httpx. Returns HTML string or None on failure."""
    try:
        resp = await client.get(url, timeout=timeout, follow_redirects=True)
        if resp.status_code >= 400:
            log.warning("HTTP %d for %s", resp.status_code, url)
            return None
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type and "text" not in content_type:
            return None
        return resp.text
    except Exception as exc:
        log.warning("Fetch failed for %s: %s", url, exc)
        return None


# ── HTML cleaning ────────────────────────────────────────────────────────────

def clean_for_llm(html: str, max_chars: int = 8000) -> str:
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
        # Sometimes the model wraps it in ```json ... ```
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


# ── Single operator enrichment ───────────────────────────────────────────────

async def enrich_operator(
    web_url: str,
    anthropic_client,
    http_client: httpx.AsyncClient,
    cache: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Enrich a single operator by scraping its website and extracting data."""
    # Cache by domain to avoid re-scraping the same site
    domain = urlparse(web_url).netloc.lower()
    if domain in cache:
        return cache[domain]

    async with semaphore:
        html = await fetch_static(web_url, http_client)

        if not html or len(html) < 500:
            result = dict(_EMPTY_RESULT)
            cache[domain] = result
            return result

        clean_text = clean_for_llm(html)
        result = await claude_extract(clean_text, anthropic_client)
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

    return operators


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
    web = operator.get("web", "")

    try:
        result = await enrich_operator(web, anthropic_client, http_client, cache, semaphore)
        operator.update(result)
    except Exception as exc:
        log.warning("Enrichment failed for %s: %s", name, exc)

    if on_progress:
        try:
            on_progress(current + 1, total, name)
        except Exception:
            pass
