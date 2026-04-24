"""
Google Maps scraper usando Playwright.
Abre o Maps, rola todos resultados, extrai dados de cada empresa.
Retorna formato compatível com o que o N8N já espera (campos do Apify).
"""
import asyncio
import logging
import re
from typing import Optional
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Seletor mais confiável: link âncora de cada card de resultado
CARD_SELECTOR = 'a.hfpxzc'


async def _scroll_feed_to_end(page: Page, max_results: int, idle_rounds: int = 8) -> int:
    """Rola o painel lateral de resultados até esgotar ou atingir max_results.

    Estratégia mais paciente: 8 rounds estáveis (vs 4 antes), 3s entre rounds,
    detecta o marker de "fim da lista" do Google Maps.
    """
    feed_selector = 'div[role="feed"]'
    try:
        await page.wait_for_selector(feed_selector, timeout=20000)
    except PlaywrightTimeout:
        logger.warning("Feed de resultados não apareceu")
        return 0

    stable = 0
    prev_count = 0
    rounds = 0
    max_total_rounds = 80

    while stable < idle_rounds and rounds < max_total_rounds:
        rounds += 1
        try:
            await page.evaluate(
                """() => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (feed) feed.scrollTo(0, feed.scrollHeight);
                }"""
            )
        except Exception:
            pass

        await page.wait_for_timeout(3000)

        # Verifica fim da lista
        end_marker = await page.locator(
            'div[role="feed"] >> text=/chegou ao fim|reached the end|não há mais|no more results|fim dos resultados/i'
        ).count()
        if end_marker > 0:
            logger.info(f"Fim da lista detectado após {rounds} rounds")
            break

        count = await page.locator(CARD_SELECTOR).count()
        if count == prev_count:
            stable += 1
        else:
            stable = 0
            prev_count = count
            logger.info(f"Round {rounds}: {count} cards carregados")

        if count >= max_results:
            break

    logger.info(f"Scroll finalizado: {prev_count} cards após {rounds} rounds")
    return prev_count


async def _extract_from_details(page: Page) -> dict:
    """Extrai dados do painel de detalhes que abre ao clicar num card."""
    data = {
        "title": None,
        "phone": None,
        "website": None,
        "totalScore": None,
        "reviewsCount": 0,
        "imagesCount": 0,
        "address": None,
    }

    # Nome
    try:
        h1 = page.locator('h1.DUwDvf, h1.lfPIob').first
        if await h1.count() > 0:
            data["title"] = (await h1.text_content() or "").strip()
    except Exception:
        pass

    # Rating + reviews — múltiplas estratégias
    try:
        # Estratégia 1: aria-label do container "div.F7nice"
        f7 = page.locator('div.F7nice').first
        if await f7.count() > 0:
            aria = await f7.get_attribute("aria-label") or ""
            # Formato típico: "4,5 estrelas 234 avaliações" ou "4.5 stars 234 reviews"
            m_rating = re.search(r"(\d+[,.]?\d*)\s*(?:estrela|star)", aria, re.IGNORECASE)
            if m_rating:
                try:
                    data["totalScore"] = float(m_rating.group(1).replace(",", "."))
                except ValueError:
                    pass
            m_reviews = re.search(r"(\d[\d\.]*)\s*(?:avalia|review)", aria, re.IGNORECASE)
            if m_reviews:
                try:
                    data["reviewsCount"] = int(m_reviews.group(1).replace(".", "").replace(",", ""))
                except ValueError:
                    pass

        # Fallback: parse texto visível "4,5  (234)"
        if data["totalScore"] is None or data["reviewsCount"] == 0:
            txt = (await f7.text_content() or "").strip() if await f7.count() > 0 else ""
            if data["totalScore"] is None:
                m = re.search(r"^\s*(\d+[,.]?\d*)", txt)
                if m:
                    try:
                        data["totalScore"] = float(m.group(1).replace(",", "."))
                    except ValueError:
                        pass
            if data["reviewsCount"] == 0:
                m = re.search(r"\(([\d\.]+)\)", txt)
                if m:
                    try:
                        data["reviewsCount"] = int(m.group(1).replace(".", "").replace(",", ""))
                    except ValueError:
                        pass
    except Exception:
        pass

    # Telefone
    try:
        phone_btn = page.locator('button[data-item-id^="phone:"]').first
        if await phone_btn.count() > 0:
            aria = await phone_btn.get_attribute("aria-label") or ""
            phone = re.sub(r"^(Telefone|Phone)\s*:\s*", "", aria).strip()
            data["phone"] = phone or None
    except Exception:
        pass

    # Website
    try:
        web_el = page.locator('a[data-item-id="authority"]').first
        if await web_el.count() > 0:
            href = await web_el.get_attribute("href")
            if href:
                data["website"] = href
    except Exception:
        pass

    # Endereço
    try:
        addr_btn = page.locator('button[data-item-id="address"]').first
        if await addr_btn.count() > 0:
            aria = await addr_btn.get_attribute("aria-label") or ""
            addr = re.sub(r"^(Endereço|Address)\s*:\s*", "", aria).strip()
            data["address"] = addr or None
    except Exception:
        pass

    # Fotos — botão "Ver fotos"
    try:
        photos_btn = page.locator('button[aria-label*="foto"], button[aria-label*="photo"]').first
        if await photos_btn.count() > 0:
            aria = await photos_btn.get_attribute("aria-label") or ""
            m = re.search(r"(\d+)", aria)
            if m:
                data["imagesCount"] = int(m.group(1))
    except Exception:
        pass

    return data


async def scrape_query(context: BrowserContext, query: str, max_results: int = 500) -> list[dict]:
    """Executa uma busca no Google Maps e retorna lista de empresas."""
    page = await context.new_page()
    results: list[dict] = []

    try:
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/?hl=pt-BR"
        logger.info(f"Abrindo: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)

        # Aceita cookies se aparecer
        try:
            btn = page.locator('button:has-text("Aceitar tudo"), button:has-text("Accept all")').first
            if await btn.count() > 0:
                await btn.click(timeout=3000)
        except Exception:
            pass

        # Se o Maps redirecionou direto para single result
        if "/maps/place/" in page.url:
            data = await _extract_from_details(page)
            if data.get("title"):
                results.append(data)
            return results

        # Rola os resultados
        total = await _scroll_feed_to_end(page, max_results=max_results)
        logger.info(f"Query '{query}' carregou {total} cards após scroll")

        # Pega os links de todos os cards usando seletor confiável
        cards = page.locator(CARD_SELECTOR)
        count = await cards.count()
        count = min(count, max_results)
        logger.info(f"Iterando {count} cards para extrair detalhes")

        for i in range(count):
            try:
                card = cards.nth(i)
                try:
                    await card.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                await card.click(timeout=5000)
                await page.wait_for_timeout(1400)
                await page.wait_for_selector('h1.DUwDvf, h1.lfPIob', timeout=6000)

                data = await _extract_from_details(page)
                if data.get("title"):
                    results.append(data)
            except Exception as e:
                logger.debug(f"Card #{i} falhou: {e}")
                continue

    except Exception as e:
        logger.exception(f"Erro scrapear '{query}': {e}")
    finally:
        await page.close()

    return results


async def scrape_multi(queries: list[str], max_per_query: int = 500) -> list[dict]:
    """Executa várias queries em sequência e agrega + deduplica por nome."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            viewport={"width": 1280, "height": 900},
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        all_results: list[dict] = []
        for q in queries:
            try:
                r = await scrape_query(context, q, max_per_query)
                all_results.extend(r)
            except Exception as e:
                logger.exception(f"Query '{q}' falhou: {e}")

        await context.close()
        await browser.close()

    # Deduplicar por nome
    seen = set()
    unique = []
    for r in all_results:
        name = (r.get("title") or "").lower().strip()
        if name and name not in seen:
            seen.add(name)
            unique.append(r)

    return unique
