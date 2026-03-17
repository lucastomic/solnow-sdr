# SolNow Prospector

Herramienta de prospección que busca empresas en Google Places en tiempo real y exporta los resultados a Excel.

## Requisitos

- Python 3.10+
- Una [API Key de Google Places (New)](https://console.cloud.google.com/apis/library/places-backend.googleapis.com)

## Instalación

```bash
git clone <repo-url>
cd sdr-ai
pip install -r requirements.txt
```

## Uso

### Interfaz web (recomendado)

```bash
python app.py
```

Abre [http://localhost:5000](http://localhost:5000) en el navegador.

Desde la interfaz puedes:

1. Pegar tu **API Key** de Google Places (se guarda en localStorage del navegador).
2. Configurar las **zonas** de búsqueda (por defecto: Ibiza, Mallorca, Alicante, Valencia).
3. Editar las **palabras clave** de búsqueda — añadir, quitar o restaurar las por defecto.
4. Lanzar la búsqueda y ver resultados en tiempo real.
5. **Descargar Excel** con los resultados al finalizar.

### CLI

```bash
export GOOGLE_PLACES_API_KEY=tu_api_key
python prospect.py Ibiza Mallorca Alicante Valencia
```

Genera `prospects_solnow.xlsx` en el directorio actual.

## Estructura

```
sdr-ai/
├── app.py              # Servidor Flask + SSE streaming
├── prospect.py         # Motor de búsqueda (Google Places API)
├── requirements.txt    # Dependencias Python
├── templates/
│   └── index.html      # Interfaz web (Tailwind CSS)
└── output/             # Archivos Excel generados
```

## Configuración

| Variable de entorno       | Descripción                          | Requerida           |
|---------------------------|--------------------------------------|---------------------|
| `GOOGLE_PLACES_API_KEY`   | API Key de Google Places (New)       | Solo si no se pone en la UI |

## Palabras clave

Las búsquedas usan plantillas con `{zona}` como placeholder. Ejemplo:

```
alquiler motos de agua {zona}  →  alquiler motos de agua Ibiza
```

Si una palabra clave no incluye `{zona}`, la zona se añade automáticamente al final.

Se pueden editar desde la interfaz web sin tocar código.
# solnow-sdr
