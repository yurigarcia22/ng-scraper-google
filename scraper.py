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


async def _scroll_feed_to_end(page: Page, max_results: int, idle_rounds: int = 4) -> int:
    """Rola o painel lateral de resultados até esgotar ou atingir max_results."""
    feed_selector = 'div[role="feed"]'
    try:
        await page.wait_for_selector(feed_selector, timeout=20000)
    except PlaywrightTimeout:
        logger.warning("Feed de resultados não apareceu")
        return 0

    stable = 0
    prev_count = 0
    rounds = 0
    while stable < idle_rounds and rounds < 60:
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

        await page.wait_for_timeout(2200)

        # Verifica fim da lista
        end_marker = await page.locator('div[role="feed"] >> text=/chegou ao fim|reached the end|não há mais|no more/i').count()
        if end_marker > 0:
            break

        count = await page.locator('div[role="feed"] > div > div[jsaction*="mouseover"]').count()
        if count == prev_count:
            stable += 1
        else:
            stable = 0
            prev_count = count

        if count >= max_results:
            break

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

    # Rating + reviews — formato "4,5 (120)" ou "4.5 (120)"
    try:
        rating_el = page.locator('div.F7nice span[aria-hidden="true"]').first
        if await rating_el.count() > 0:
            txt = (await rating_el.text_content() or "").replace(",", ".").strip()
            if txt:
                try:
                    data["totalScore"] = float(txt)
                except ValueError:
                    pass
    except Exception:
        pass

    try:
        reviews_el = page.locator('div.F7nice span[aria-label*="avalia"], div.F7nice span[aria-label*="review"]').first
        if await reviews_el.count() > 0:
            aria = await reviews_el.get_attribute("aria-label") or ""
            m = re.search(r"([\d\.]+)", aria.replace(".", ""))
            if m:
                data["reviewsCount"] = int(m.group(1))
    except Exception:
        pass

    # Telefone
    try:
        phone_btn = page.locator('button[data-item-id^="phone:"]').first
        if await phone_btn.count() > 0:
            aria = await phone_btn.get_attribute("aria-label") or ""
            # "Telefone: +55 11 9999-9999" ou "Phone: ..."
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

    # Fotos — tenta contar pelo botão de "Ver fotos" ou similar
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

        # Se o Maps redirecionou direto para a página de um lugar (single result),
        # extrai e retorna
        if "/maps/place/" in page.url:
            data = await _extract_from_details(page)
            if data.get("title"):
                results.append(data)
            return results

        # Rola os resultados
        total = await _scroll_feed_to_end(page, max_results=max_results)
        logger.info(f"Query '{query}' carregou {total} cards")

        cards = page.locator('div[role="feed"] > div > div[jsaction*="mouseover"]')
        count = await cards.count()
        count = min(count, max_results)

        for i in range(count):
            try:
                card = cards.nth(i)
                # Traz o card para a viewport
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
        # Esconder "webdriver"
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

    # Deduplicar por nome (lowercase, strip)
    seen = set()
    unique = []
    for r in all_results:
        name = (r.get("title") or "").lower().strip()
        if name and name not in seen:
            seen.add(name)
            unique.append(r)

    return unique
