#!/usr/bin/env python3
"""
SolNow — GMV Estimation Engine & ICP Scoring

Estimates annual GMV (Gross Merchandise Value) for water sports operators
and calculates an ICP (Ideal Customer Profile) score for SDR prioritization.
"""

import logging

log = logging.getLogger(__name__)

# ── Category defaults ────────────────────────────────────────────────────────

CATEGORIA_CONFIG = {
    "jet_ski": {
        "capacidad_default": 1.5,   # personas por salida
        "salidas_por_dia": 5,       # rotación alta
        "precio_default": 65,       # € si no se scrapeó
        "ocupacion": 0.70,
    },
    "kayak": {
        "capacidad_default": 1.5,
        "salidas_por_dia": 4,
        "precio_default": 40,
        "ocupacion": 0.65,
    },
    "paddle_surf": {
        "capacidad_default": 1,
        "salidas_por_dia": 6,
        "precio_default": 25,
        "ocupacion": 0.60,
    },
    "excursion_barco": {
        "capacidad_default": 20,
        "salidas_por_dia": 2,
        "precio_default": 55,       # por persona
        "ocupacion": 0.65,
    },
    "charter_privado": {
        "capacidad_default": 1,     # precio es por grupo
        "salidas_por_dia": 1.5,
        "precio_default": 500,      # por salida completa
        "ocupacion": 0.45,
    },
}

# ── Season days by coastal zone ──────────────────────────────────────────────

DIAS_TEMPORADA = {
    "canarias": 280,
    "costa_del_sol": 210,
    "baleares": 180,
    "levante": 170,
    "costa_brava": 140,
    "murcia": 160,
    "costa_de_la_luz": 150,
    "cantabrico": 90,
    "default": 160,
}

# ── City → coastal zone mapping ──────────────────────────────────────────────

_ZONA_MAP = {
    # Costa del Sol
    "marbella": "costa_del_sol",
    "málaga": "costa_del_sol",
    "malaga": "costa_del_sol",
    "torremolinos": "costa_del_sol",
    "nerja": "costa_del_sol",
    "estepona": "costa_del_sol",
    "fuengirola": "costa_del_sol",
    "benalmádena": "costa_del_sol",
    "benalmadena": "costa_del_sol",
    # Levante (Costa Blanca + Valencia)
    "alicante": "levante",
    "benidorm": "levante",
    "calpe": "levante",
    "denia": "levante",
    "jávea": "levante",
    "javea": "levante",
    "valencia": "levante",
    "gandía": "levante",
    "gandia": "levante",
    "cullera": "levante",
    # Baleares
    "ibiza": "baleares",
    "formentera": "baleares",
    "palma de mallorca": "baleares",
    "palma": "baleares",
    "mallorca": "baleares",
    "menorca": "baleares",
    # Costa Brava / Maresme
    "barcelona": "costa_brava",
    "sitges": "costa_brava",
    "lloret de mar": "costa_brava",
    "roses": "costa_brava",
    "tossa de mar": "costa_brava",
    # Canarias
    "tenerife": "canarias",
    "gran canaria": "canarias",
    "lanzarote": "canarias",
    "fuerteventura": "canarias",
    "las palmas": "canarias",
    # Costa de la Luz
    "tarifa": "costa_de_la_luz",
    "cádiz": "costa_de_la_luz",
    "cadiz": "costa_de_la_luz",
    "conil": "costa_de_la_luz",
    "huelva": "costa_de_la_luz",
    # Murcia
    "murcia": "murcia",
    "cartagena": "murcia",
    "la manga": "murcia",
    "águilas": "murcia",
    "aguilas": "murcia",
}


# ── GMV caps per category (prevent absurd estimates) ─────────────────────────

GMV_CAP_POR_CATEGORIA = {
    "jet_ski": 2_000_000,
    "kayak": 800_000,
    "paddle_surf": 500_000,
    "charter_privado": 3_000_000,
    "excursion_barco": 5_000_000,
    "buceo": 1_000_000,
    "mixto": 3_000_000,
    "otro": 2_000_000,
}

# ── Marketplace / intermediary detection ─────────────────────────────────────

MARKETPLACE_KEYWORDS = [
    "boatjump", "getmyboat", "samboat", "nautal", "click&boat",
    "clickandboat", "viator", "getyourguide", "civitatis", "airbnb",
    "marketplace", "plataforma", "broker", "intermediario",
    "comparador", "buscador",
]


def es_marketplace(nombre: str, web: str) -> bool:
    """Detect if an operator is actually a marketplace/intermediary."""
    text = (nombre + " " + web).lower()
    return any(kw in text for kw in MARKETPLACE_KEYWORDS)


def clasificar_zona_costera(direccion: str, zona_busqueda: str) -> str:
    """Map a search zone or address to a coastal zone key for DIAS_TEMPORADA."""
    # Try the search zone first (most reliable)
    key = zona_busqueda.lower().strip()
    if key in _ZONA_MAP:
        return _ZONA_MAP[key]

    # Try matching against the address
    addr = direccion.lower()
    for city, coastal in _ZONA_MAP.items():
        if city in addr:
            return coastal

    return "default"


def calcular_gmv(operador: dict) -> dict:
    """Estimate annual GMV using dual model (assets-based + reviews-based).

    Expected keys in operador:
        categoria_principal: str  (from enrichment or default "jet_ski")
        precio_medio: float|None (from enrichment)
        num_activos: int|None    (from enrichment)
        capacidad_maxima_por_salida: int|None (from enrichment)
        zona_costera: str        (from clasificar_zona_costera)
        google_reviews_total: int
        años_activo: float       (from first review date)
    """
    cat_key = operador.get("categoria_principal") or "jet_ski"
    # Fallback for unknown categories
    if cat_key not in CATEGORIA_CONFIG:
        cat_key = "jet_ski" if cat_key == "buceo" else "jet_ski"
        if operador.get("categoria_principal") == "mixto":
            cat_key = "jet_ski"  # mixto defaults to jet_ski economics

    config = CATEGORIA_CONFIG.get(cat_key, CATEGORIA_CONFIG["jet_ski"])

    precio = operador.get("precio_medio") or config["precio_default"]
    activos = operador.get("num_activos") or 1
    capacidad = operador.get("capacidad_maxima_por_salida") or config["capacidad_default"]
    zona = operador.get("zona_costera", "default")
    dias = DIAS_TEMPORADA.get(zona, DIAS_TEMPORADA["default"])

    # Model 1: assets-based
    gmv_activos = (
        activos
        * config["salidas_por_dia"]
        * capacidad
        * precio
        * dias
        * config["ocupacion"]
    )

    # Model 2: reviews-based (validation)
    reviews_total = operador.get("google_reviews_total") or 0
    años_activo = max(operador.get("años_activo", 1), 1)
    reviews_año = reviews_total / años_activo
    gmv_reviews = reviews_año * 10 * capacidad * precio  # ~10 clients per review

    # Weighted average if we have scraped asset data
    if operador.get("num_activos"):
        gmv_final = gmv_activos * 0.7 + gmv_reviews * 0.3
    else:
        gmv_final = gmv_reviews

    # Floor: at least €0
    gmv_final = max(gmv_final, 0)

    # Apply GMV cap per category to prevent absurd estimates
    cap = GMV_CAP_POR_CATEGORIA.get(cat_key, GMV_CAP_POR_CATEGORIA.get("otro", 3_000_000))
    if gmv_final > cap:
        log.debug("GMV capped for %s: €%d → €%d", operador.get("nombre", "?"), gmv_final, cap)
        gmv_final = cap

    return {
        "gmv_estimado_anual": round(gmv_final),
        "comision_solnow_anual": round(gmv_final * 0.015),
        "comision_solnow_mensual": round(gmv_final * 0.015 / 12),
    }


def calcular_score_icp(operador: dict) -> int:
    """Calculate ICP score (0-100) for SDR prioritization.

    When data is missing (None), applies benefit-of-the-doubt scoring
    to avoid penalizing operators that simply lack enrichment data.
    """
    score = 0

    # Reviews per year — proxy for operational volume (40 points)
    rpa = operador.get("reviews_por_año", 0) or 0
    if rpa >= 300:
        score += 40
    elif rpa >= 150:
        score += 30
    elif rpa >= 75:
        score += 20
    elif rpa >= 30:
        score += 10

    # Category — high volume by nature (25 points)
    # If null → 15 pts (benefit of the doubt)
    cat = operador.get("categoria_principal")
    if cat in ("jet_ski", "kayak", "paddle_surf"):
        score += 25
    elif cat == "excursion_barco":
        score += 20
    elif cat == "mixto":
        score += 15
    elif cat == "charter_privado":
        score += 5
    elif cat is None:
        score += 15  # benefit of the doubt

    # Price type (20 points)
    # If null → 10 pts (benefit of the doubt)
    tipo = operador.get("tipo_precio")
    if tipo == "por_persona":
        score += 20
    elif tipo == "por_hora":
        score += 15
    elif tipo == "por_grupo":
        score += 5
    elif tipo is None:
        score += 10  # benefit of the doubt

    # Low digitalization signals = more pain (15 points)
    reserva = operador.get("tiene_reserva_online")
    if reserva is False:
        score += 15
    elif reserva is None:
        score += 7  # unknown → partial credit
    elif operador.get("menciona_contrato") is False:
        score += 10

    return min(score, 100)


def score_and_estimate(operador: dict) -> dict:
    """Convenience: classify zone, calculate GMV, and score ICP.

    Mutates and returns the operador dict with added fields.
    """
    # Marketplace detection
    operador["es_marketplace"] = es_marketplace(
        operador.get("nombre", ""),
        operador.get("web", ""),
    )

    # Zone classification
    zona_costera = clasificar_zona_costera(
        operador.get("direccion", ""),
        operador.get("zona", ""),
    )
    operador["zona_costera"] = zona_costera
    operador["dias_temporada"] = DIAS_TEMPORADA.get(zona_costera, DIAS_TEMPORADA["default"])

    # GMV (skip for marketplaces — their reviews ≠ their own operations)
    if operador["es_marketplace"]:
        operador["gmv_estimado_anual"] = 0
        operador["comision_solnow_anual"] = 0
        operador["comision_solnow_mensual"] = 0
    else:
        gmv = calcular_gmv(operador)
        operador.update(gmv)

    # ICP score (marketplaces get 0 — they're not our customers)
    if operador["es_marketplace"]:
        operador["score_icp"] = 0
    else:
        operador["score_icp"] = calcular_score_icp(operador)

    operador["es_icp"] = operador["score_icp"] > 60

    return operador
