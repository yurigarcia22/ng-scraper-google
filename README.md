# NG Scraper Google

Service de scraping do Google Maps usando Playwright, substituindo o Apify
no workflow de prospecção do Grupo NG.

## Endpoints

### `GET /health`
Healthcheck.

### `POST /scrape`
Scrape síncrono (aguarda e devolve resultados).

```json
{
  "nicho": "energia solar",
  "cidades": ["são paulo", "campinas"],
  "max_per_city": 500,
  "api_key": "SEU_KEY"
}
```

### `POST /scrape/async` + `GET /jobs/{id}`
Modo async para evitar timeout em N8N/HTTP clients.

## Deploy no Easypanel

1. App type: `App` (não é service)
2. Source: GitHub `yurigarcia22/ng-scraper-google`
3. Build method: `Dockerfile`
4. Environment:
   - `SCRAPER_API_KEY` (opcional)
5. Port: `8000`
6. Memory: `2048` MB (mínimo para Chromium)
7. Domain interno: `ng-scraper-google`

## Local

```bash
pip install -r requirements.txt
playwright install chromium
uvicorn main:app --reload
```
