"""
Google Maps scraper usando Playwright.

Estratégia:
1. Abre o Maps com a busca
2. Rola o feed lateral até carregar todos os resultados
3. Coleta todas as URLs dos cards (href dos 'a.hfpxzc')
4. Visita cada URL uma por uma e extrai dados do painel de detalhes
5. Deduplica por nome e retorna
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

CARD_SELECTOR = 'a.hfpxzc'


async def _scroll_feed_to_end(page: Page, max_results: int, idle_rounds: int = 20) -> int:
    """Rola o feed lateral agressivamente até esgotar ou atingir max_results."""
    feed_selector = 'div[role="feed"]'
    try:
        await page.wait_for_selector(feed_selector, timeout=20000)
    except PlaywrightTimeout:
        logger.warning("Feed de resultados não apareceu")
        return 0

    try:
        await page.locator(feed_selector).first.hover()
    except Exception:
        pass

    stable = 0
    prev_count = 0
    rounds = 0
    max_total_rounds = 150

    while stable < idle_rounds and rounds < max_total_rounds:
        rounds += 1

        try:
            await page.evaluate(
                """() => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (!feed) return;
                    feed.scrollTo({ top: feed.scrollHeight, behavior: 'instant' });
                    feed.dispatchEvent(new WheelEvent('wheel', { deltaY: 5000, bubbles: true }));
                }"""
            )
        except Exception:
            pass

        try:
            await page.mouse.wheel(0, 4000)
        except Exception:
            pass

        try:
            await page.keyboard.press("End")
        except Exception:
            pass

        await page.wait_for_timeout(2200)

        try:
            end_marker = await page.locator(
                'div[role="feed"] >> text=/chegou ao fim|reached the end|não há mais|no more results|fim dos resultados|você chegou ao final/i'
            ).count()
            if end_marker > 0:
                logger.info(f"Fim da lista detectado após {rounds} rounds")
                break
        except Exception:
            pass

        count = await page.locator(CARD_SELECTOR).count()
        if count == prev_count:
            stable += 1
        else:
            stable = 0
            if count - prev_count >= 5 or count % 10 == 0:
                logger.info(f"Round {rounds}: {count} cards")
            prev_count = count

        if count >= max_results:
            logger.info(f"Atingiu max_results={max_results}")
            break

    logger.info(f"Scroll finalizado: {prev_count} cards após {rounds} rounds")
    return prev_count


async def _extract_from_details(page: Page) -> dict:
    """Extrai dados do painel de detalhes (com a URL em /maps/place/...)."""
    data = {
        "title": None,
        "phone": None,
        "website": None,
        "totalScore": None,
        "reviewsCount": 0,
        "imagesCount": 0,
        "address": None,
    }

    try:
        h1 = page.locator('h1.DUwDvf, h1.lfPIob').first
        if await h1.count() > 0:
            data["title"] = (await h1.text_content() or "").strip()
    except Exception:
        pass

    try:
        f7 = page.locator('div.F7nice').first
        if await f7.count() > 0:
            aria = await f7.get_attribute("aria-label") or ""
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

        if data["totalScore"] is None or data["reviewsCount"] == 0:
            txt = ""
            if await f7.count() > 0:
                txt = (await f7.text_content() or "").strip()
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

    try:
        phone_btn = page.locator('button[data-item-id^="phone:"]').first
        if await phone_btn.count() > 0:
            aria = await phone_btn.get_attribute("aria-label") or ""
            phone = re.sub(r"^(Telefone|Phone)\s*:\s*", "", aria).strip()
            data["phone"] = phone or None
    except Exception:
        pass

    try:
        web_el = page.locator('a[data-item-id="authority"]').first
        if await web_el.count() > 0:
            href = await web_el.get_attribute("href")
            if href:
                data["website"] = href
    except Exception:
        pass

    try:
        addr_btn = page.locator('button[data-item-id="address"]').first
        if await addr_btn.count() > 0:
            aria = await addr_btn.get_attribute("aria-label") or ""
            addr = re.sub(r"^(Endereço|Address)\s*:\s*", "", aria).strip()
            data["address"] = addr or None
    except Exception:
        pass

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
    """1) Abre busca, rola, coleta URLs. 2) Visita cada URL e extrai."""
    results: list[dict] = []

    # Fase 1: coletar URLs via scroll
    list_page = await context.new_page()
    urls: list[str] = []
    try:
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/?hl=pt-BR"
        logger.info(f"Abrindo busca: {search_url}")
        await list_page.goto(search_url, wait_until="domcontentloaded", timeout=45000)

        try:
            btn = list_page.locator('button:has-text("Aceitar tudo"), button:has-text("Accept all")').first
            if await btn.count() > 0:
                await btn.click(timeout=3000)
        except Exception:
            pass

        # Se redirecionou direto pra single place
        if "/maps/place/" in list_page.url:
            data = await _extract_from_details(list_page)
            if data.get("title"):
                results.append(data)
            await list_page.close()
            return results

        await _scroll_feed_to_end(list_page, max_results=max_results)

        # Coleta todas as URLs dos cards
        hrefs = await list_page.locator(CARD_SELECTOR).evaluate_all(
            "(els) => els.map(a => a.href).filter(Boolean)"
        )
        # Unique preservando ordem
        seen_urls = set()
        for h in hrefs:
            if h not in seen_urls:
                seen_urls.add(h)
                urls.append(h)
        logger.info(f"Query '{query}' coletou {len(urls)} URLs únicas")
    except Exception as e:
        logger.exception(f"Erro fase 1 de '{query}': {e}")
    finally:
        await list_page.close()

    if not urls:
        return results

    # Fase 2: visitar cada URL e extrair
    detail_page = await context.new_page()
    try:
        for idx, url in enumerate(urls[:max_results]):
            try:
                await detail_page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await detail_page.wait_for_selector('h1.DUwDvf, h1.lfPIob', timeout=8000)
                except PlaywrightTimeout:
                    logger.debug(f"Detail {idx+1}: h1 não apareceu")
                    continue

                # Espera extra para lazy load de botões
                await detail_page.wait_for_timeout(800)

                data = await _extract_from_details(detail_page)
                if data.get("title"):
                    results.append(data)
                    if (idx + 1) % 10 == 0:
                        logger.info(f"Detail {idx+1}/{len(urls)} extraído")
            except Exception as e:
                logger.debug(f"URL #{idx} falhou: {e}")
                continue
    finally:
        await detail_page.close()

    logger.info(f"Query '{query}' extraiu {len(results)} empresas de {len(urls)} URLs")
    return results


async def scrape_multi(queries: list[str], max_per_query: int = 500) -> list[dict]:
    """Executa várias queries, agrega e deduplica por nome."""
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
            viewport={"width": 1280, "height": 1500},
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

    seen = set()
    unique = []
    for r in all_results:
        name = (r.get("title") or "").lower().strip()
        if name and name not in seen:
            seen.add(name)
            unique.append(r)

    return unique
