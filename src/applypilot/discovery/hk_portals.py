"""Hong Kong job portal scrapers: JobsDB, eFinancialCareers, OfferToday.

Extracts job listings from top Hong Kong portals, normalizes the data into
the standard ApplyPilot schema, and stores them in the SQLite database.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus, urljoin

import httpx
from bs4 import BeautifulSoup

from applypilot import config
from applypilot.database import get_connection, init_db, store_jobs

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7",
}


# ---------------------------------------------------------------------------
# 1. JobsDB Hong Kong (SEEK Asia)
# ---------------------------------------------------------------------------

def scrape_jobsdb(query: str, location: str = "Hong Kong", limit: int = 50) -> list[dict[str, Any]]:
    """Scrape job postings from hk.jobsdb.com.
    
    Tries SEEK's GraphQL / REST search endpoint first, falling back to HTML scraping.
    """
    log.info("Scraping JobsDB HK for '%s'...", query)
    jobs: list[dict[str, Any]] = []
    
    # Try SEEK/JobsDB API endpoint
    api_url = "https://hk.jobsdb.com/api/chalice-search/v4/search"
    params = {
        "siteKey": "HK-Main",
        "sourcesystem": "houston",
        "where": location,
        "keywords": query,
        "pageSize": min(limit, 30),
        "page": 1,
        "sortmode": "ListedDate",
    }
    
    try:
        with httpx.Client(timeout=15, headers={**HEADERS, "Sec-Fetch-Site": "same-origin"}) as client:
            resp = client.get(api_url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("data", []):
                    job_id = item.get("id")
                    title = item.get("title")
                    company = (item.get("advertiser") or {}).get("description") or "Unknown"
                    teaser = item.get("teaser", "")
                    job_loc = (item.get("locationHierarchy") or {}).get("city") or location
                    salary = item.get("salary") or None
                    job_url = f"https://hk.jobsdb.com/job/{job_id}" if job_id else None
                    
                    if job_url and title:
                        jobs.append({
                            "url": job_url,
                            "title": title,
                            "salary": salary,
                            "description": teaser,
                            "location": job_loc,
                            "site": f"JobsDB ({company})",
                            "strategy": "jobsdb_api",
                        })
                if jobs:
                    log.info("JobsDB API returned %d jobs for '%s'", len(jobs), query)
                    return jobs[:limit]
    except Exception as e:
        log.debug("JobsDB API request failed (%s), falling back to HTML parsing", e)

    # Fallback: HTML Search Parsing
    search_url = f"https://hk.jobsdb.com/jobs?keywords={quote_plus(query)}&where={quote_plus(location)}"
    try:
        with httpx.Client(timeout=15, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(search_url)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # SEEK job card containers
                articles = soup.find_all("article")
                for art in articles:
                    title_elem = art.find("a", attrs={"data-automation": re.compile(r"jobTitle|job-title", re.I)}) or art.find("a", href=re.compile(r"/job/"))
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    rel_url = title_elem.get("href", "")
                    full_url = urljoin("https://hk.jobsdb.com", rel_url)
                    
                    company_elem = art.find("a", attrs={"data-automation": re.compile(r"jobCompany", re.I)})
                    company = company_elem.get_text(strip=True) if company_elem else "Unknown"
                    
                    salary_elem = art.find("span", attrs={"data-automation": re.compile(r"jobSalary", re.I)})
                    salary = salary_elem.get_text(strip=True) if salary_elem else None
                    
                    loc_elem = art.find("a", attrs={"data-automation": re.compile(r"jobLocation", re.I)})
                    loc = loc_elem.get_text(strip=True) if loc_elem else location

                    teaser_elem = art.find("span", attrs={"data-automation": re.compile(r"jobShortDescription", re.I)})
                    teaser = teaser_elem.get_text(strip=True) if teaser_elem else ""

                    jobs.append({
                        "url": full_url,
                        "title": title,
                        "salary": salary,
                        "description": teaser,
                        "location": loc,
                        "site": f"JobsDB ({company})",
                        "strategy": "jobsdb_html",
                    })
                    if len(jobs) >= limit:
                        break
        log.info("JobsDB HTML fallback extracted %d jobs for '%s'", len(jobs), query)
    except Exception as e:
        log.warning("JobsDB HTML scrape failed: %s", e)

    return jobs


# ---------------------------------------------------------------------------
# 2. eFinancialCareers (eFC Hong Kong)
# ---------------------------------------------------------------------------

def scrape_efinancialcareers(query: str, limit: int = 50) -> list[dict[str, Any]]:
    """Scrape banking, fintech, and finance jobs from efinancialcareers.hk."""
    log.info("Scraping eFinancialCareers HK for '%s'...", query)
    jobs: list[dict[str, Any]] = []
    
    # Try public search page
    search_url = f"https://www.efinancialcareers.hk/jobs?q={quote_plus(query)}&countryCode=HK&pageSize={min(limit, 50)}"
    try:
        with httpx.Client(timeout=15, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(search_url)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Check for Next.js embedded JSON (__NEXT_DATA__)
                next_data = soup.find("script", id="__NEXT_DATA__")
                if next_data and next_data.string:
                    try:
                        data = json.loads(next_data.string)
                        raw_jobs = (
                            data.get("props", {})
                            .get("pageProps", {})
                            .get("dehydratedState", {})
                            .get("queries", [{}])[0]
                            .get("state", {})
                            .get("data", {})
                            .get("jobs", [])
                        )
                        for rj in raw_jobs:
                            jid = rj.get("id")
                            title = rj.get("title")
                            company = (rj.get("company") or {}).get("name") or "Unknown"
                            sal = (rj.get("salary") or {}).get("text") or None
                            loc = (rj.get("location") or {}).get("text") or "Hong Kong"
                            j_url = rj.get("url") or (f"https://www.efinancialcareers.hk/jobs/{jid}" if jid else None)
                            desc = rj.get("summary") or ""
                            
                            if j_url and title:
                                jobs.append({
                                    "url": j_url,
                                    "title": title,
                                    "salary": sal,
                                    "description": desc,
                                    "location": loc,
                                    "site": f"eFinancialCareers ({company})",
                                    "strategy": "efinancialcareers_json",
                                })
                        if jobs:
                            log.info("eFinancialCareers extracted %d jobs from Next.js payload", len(jobs))
                            return jobs[:limit]
                    except Exception as e:
                        log.debug("Failed parsing eFC Next.js data: %s", e)

                # HTML fallback: find job cards
                cards = soup.find_all("div", attrs={"data-test": re.compile(r"job-card", re.I)}) or soup.find_all("article")
                for card in cards:
                    a_tag = card.find("a", href=re.compile(r"/jobs/"))
                    if not a_tag:
                        continue
                    title = a_tag.get_text(strip=True)
                    rel_url = a_tag.get("href", "")
                    full_url = urljoin("https://www.efinancialcareers.hk", rel_url)
                    
                    comp_elem = card.find("span", attrs={"data-test": re.compile(r"company", re.I)})
                    company = comp_elem.get_text(strip=True) if comp_elem else "Unknown"

                    sal_elem = card.find("span", attrs={"data-test": re.compile(r"salary", re.I)})
                    salary = sal_elem.get_text(strip=True) if sal_elem else None

                    desc_elem = card.find("p", attrs={"data-test": re.compile(r"summary|description", re.I)})
                    desc = desc_elem.get_text(strip=True) if desc_elem else ""

                    jobs.append({
                        "url": full_url,
                        "title": title,
                        "salary": salary,
                        "description": desc,
                        "location": "Hong Kong",
                        "site": f"eFinancialCareers ({company})",
                        "strategy": "efinancialcareers_html",
                    })
                    if len(jobs) >= limit:
                        break
        log.info("eFinancialCareers HTML fallback extracted %d jobs for '%s'", len(jobs), query)
    except Exception as e:
        log.warning("eFinancialCareers scrape failed: %s", e)

    return jobs


# ---------------------------------------------------------------------------
# 3. OfferToday (智聘香港 - offertoday.com)
# ---------------------------------------------------------------------------

def scrape_offertoday(query: str, limit: int = 50) -> list[dict[str, Any]]:
    """Scrape AI, Fintech, and graduate/experienced jobs from offertoday.com."""
    log.info("Scraping OfferToday HK for '%s'...", query)
    jobs: list[dict[str, Any]] = []

    search_url = f"https://www.offertoday.com/jobs?keyword={quote_plus(query)}"
    try:
        with httpx.Client(timeout=15, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(search_url)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")

                # Try parsing JSON state or standard job item cards
                cards = soup.find_all("div", class_=re.compile(r"job-item|job-card|JobItem", re.I)) or soup.find_all("a", href=re.compile(r"/job/|/jobs/"))
                for card in cards:
                    if card.name == "a":
                        a_tag = card
                    else:
                        a_tag = card.find("a", href=re.compile(r"/job/|/jobs/")) or card.find("a")
                    
                    if not a_tag:
                        continue

                    title = a_tag.get_text(strip=True)
                    if not title or len(title) < 2 or title in ("Apply", "View Detail", "Apply Now"):
                        continue

                    rel_url = a_tag.get("href", "")
                    full_url = urljoin("https://www.offertoday.com", rel_url)

                    jobs.append({
                        "url": full_url,
                        "title": title,
                        "salary": None,
                        "description": "",
                        "location": "Hong Kong",
                        "site": "OfferToday",
                        "strategy": "offertoday_html",
                    })
                    if len(jobs) >= limit:
                        break
        log.info("OfferToday extracted %d jobs for '%s'", len(jobs), query)
    except Exception as e:
        log.warning("OfferToday scrape failed: %s", e)

    return jobs


# ---------------------------------------------------------------------------
# Unified Discovery Entrypoint
# ---------------------------------------------------------------------------

def discover_hk_jobs(
    queries: list[str] | None = None,
    platforms: list[str] | None = None,
    results_per_platform: int = 30,
) -> dict[str, int]:
    """Execute HK multi-platform discovery and store results in ApplyPilot DB.

    Returns:
        Dict with total, new, and existing counts.
    """
    if queries is None:
        search_cfg = config.load_search_config()
        query_items = search_cfg.get("queries", [{"query": "AI Architect"}])
        queries = [q["query"] if isinstance(q, dict) else q for q in query_items]

    if platforms is None:
        platforms = ["jobsdb", "efinancialcareers", "offertoday"]

    all_jobs: list[dict[str, Any]] = []

    for q in queries:
        log.info("=== Running HK Discovery for query: '%s' ===", q)
        if "jobsdb" in platforms:
            all_jobs.extend(scrape_jobsdb(q, limit=results_per_platform))
        if "efinancialcareers" in platforms:
            all_jobs.extend(scrape_efinancialcareers(q, limit=results_per_platform))
        if "offertoday" in platforms:
            all_jobs.extend(scrape_offertoday(q, limit=results_per_platform))

    if not all_jobs:
        return {"total": 0, "new": 0, "existing": 0}

    conn = init_db()
    new, existing = store_jobs(conn, all_jobs)
    log.info("HK Discovery completed: %d total found, %d new, %d existing in DB", len(all_jobs), new, existing)

    return {"total": len(all_jobs), "new": new, "existing": existing}
