"""
Google Maps scraper usando Playwright.

Estratégia:
1. Fase 1: abre busca, rola todo o feed lateral, coleta URLs dos cards
2. Fase 2: visita URLs em paralelo (3 abas) e extrai dados de cada uma
3. Detecta bloqueios do Google (captcha, bot detection) e aborta cedo
4. Delay aleatório entre requests + rotação de user-agent
"""
import asyncio
import logging
import random
import re
from typing import Optional
from playwright.async_api import (
    async_playwright, Page, BrowserContext,
    TimeoutError as PlaywrightTimeout,
)

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

CARD_SELECTOR = 'a.hfpxzc'

CONCURRENCY = 3  # Fase 2: quantas abas em paralelo


# ─── Detecção de bloqueio ───────────────────────────────────────────────────

async def _detect_block(page: Page) -> Optional[str]:
    """Retorna motivo do bloqueio ou None se está ok."""
    try:
        url = page.url or ""
        if "/sorry/" in url or "sorry.google" in url or "/recaptcha/" in url:
            return "captcha_redirect"
    except Exception:
        pass

    try:
        patterns = re.compile(
            r"tráfego incomum|unusual traffic|systems have detected|"
            r"verificar que você é humano|not a robot|sou um ser humano|"
            r"detectamos atividade|automated queries|computador está enviando",
            re.IGNORECASE,
        )
        body = await page.locator("body").text_content(timeout=2000) or ""
        if patterns.search(body[:5000]):
            return "bot_detection"
    except Exception:
        pass

    return None


# ─── Fase 1: scroll e coleta de URLs ────────────────────────────────────────

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


async def _collect_urls(context: BrowserContext, query: str, max_results: int) -> tuple[list[str], Optional[str]]:
    """Abre a busca, rola, coleta URLs. Retorna (urls, blocked_reason)."""
    page = await context.new_page()
    urls: list[str] = []
    blocked: Optional[str] = None

    try:
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/?hl=pt-BR"
        logger.info(f"Abrindo busca: {search_url}")
        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)

        blocked = await _detect_block(page)
        if blocked:
            logger.warning(f"Bloqueio detectado na fase 1: {blocked}")
            return urls, blocked

        try:
            btn = page.locator('button:has-text("Aceitar tudo"), button:has-text("Accept all")').first
            if await btn.count() > 0:
                await btn.click(timeout=3000)
        except Exception:
            pass

        # Single place result
        if "/maps/place/" in page.url:
            urls.append(page.url)
            return urls, None

        await _scroll_feed_to_end(page, max_results=max_results)

        hrefs = await page.locator(CARD_SELECTOR).evaluate_all(
            "(els) => els.map(a => a.href).filter(Boolean)"
        )
        seen = set()
        for h in hrefs:
            if h not in seen:
                seen.add(h)
                urls.append(h)
        logger.info(f"Query '{query}' coletou {len(urls)} URLs únicas")
    except Exception as e:
        logger.exception(f"Erro fase 1 de '{query}': {e}")
    finally:
        await page.close()

    return urls, blocked


# ─── Fase 2: extração por URL ────────────────────────────────────────────────

async def _extract_from_details(page: Page) -> dict:
    """Extrai dados do painel de detalhes (página /maps/place/...)."""
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


async def _scrape_one_url(
    context: BrowserContext,
    url: str,
    semaphore: asyncio.Semaphore,
    block_flag: dict,
) -> Optional[dict]:
    """Visita uma URL e extrai. Aborta se block_flag indica bloqueio."""
    async with semaphore:
        if block_flag.get("reason"):
            return None

        # Delay humano aleatório entre 0.8 e 2.5s
        await asyncio.sleep(random.uniform(0.8, 2.5))

        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            blocked = await _detect_block(page)
            if blocked:
                block_flag["reason"] = blocked
                logger.warning(f"Bloqueio detectado na fase 2: {blocked}")
                return None

            try:
                await page.wait_for_selector('h1.DUwDvf, h1.lfPIob', timeout=8000)
            except PlaywrightTimeout:
                return None

            await page.wait_for_timeout(700)
            data = await _extract_from_details(page)
            return data if data.get("title") else None
        except Exception as e:
            logger.debug(f"URL falhou: {e}")
            return None
        finally:
            await page.close()


# ─── Orquestração ───────────────────────────────────────────────────────────

async def scrape_query(
    context: BrowserContext,
    query: str,
    max_results: int = 500,
) -> tuple[list[dict], Optional[str]]:
    """Retorna (results, blocked_reason) para uma query."""
    urls, blocked = await _collect_urls(context, query, max_results)
    if blocked:
        return [], blocked
    if not urls:
        return [], None

    urls = urls[:max_results]
    semaphore = asyncio.Semaphore(CONCURRENCY)
    block_flag: dict = {}

    tasks = [_scrape_one_url(context, url, semaphore, block_flag) for url in urls]
    done = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    for d in done:
        if isinstance(d, dict):
            results.append(d)

    logger.info(
        f"Query '{query}': {len(results)} empresas de {len(urls)} URLs "
        f"(bloqueio: {block_flag.get('reason') or 'não'})"
    )
    return results, block_flag.get("reason")


async def scrape_multi(queries: list[str], max_per_query: int = 500) -> dict:
    """Executa todas as queries, agrega, deduplica. Retorna {companies, blocked, reason}."""
    blocked_reason: Optional[str] = None

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
            user_agent=random.choice(USER_AGENTS),
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            viewport={"width": 1280, "height": 1500},
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            },
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        all_results: list[dict] = []
        for q in queries:
            if blocked_reason:
                break
            try:
                results, block = await scrape_query(context, q, max_per_query)
                all_results.extend(results)
                if block:
                    blocked_reason = block
                    break
            except Exception as e:
                logger.exception(f"Query '{q}' falhou: {e}")

        await context.close()
        await browser.close()

    # Dedup por nome
    seen = set()
    unique = []
    for r in all_results:
        name = (r.get("title") or "").lower().strip()
        if name and name not in seen:
            seen.add(name)
            unique.append(r)

    return {
        "companies": unique,
        "blocked": blocked_reason is not None,
        "reason": blocked_reason,
    }
