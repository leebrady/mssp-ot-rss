#!/usr/bin/env python3
"""
Extract podcast audio URLs from Matt and Shane's Secret Podcast episode pages.
Designed for GitHub Codespaces—uses requests + BeautifulSoup (pip install -r requirements.txt).
Outputs extracted URLs and metadata to CSV for RSS feed generation.
"""

import os
import csv
import json
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# ===== CONFIG =====
BASE_URL = "https://msspoldt.com"  # Replace with actual base URL
EPISODE_INDEX_URL = f"{BASE_URL}/episodes"  # URL to episode listing page
OUTPUT_DIR = Path("./podcast_data")
OUTPUT_CSV = OUTPUT_DIR / "episodes.csv"
OUTPUT_JSON = OUTPUT_DIR / "episodes.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

REQUEST_TIMEOUT = 10
RETRY_ATTEMPTS = 3
BACKOFF_FACTOR = 0.5


# ===== SESSION SETUP =====
def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=RETRY_ATTEMPTS,
        connect=RETRY_ATTEMPTS,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(500, 502, 503, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ===== URL EXTRACTION =====
def extract_audio_urls(html_content: str, page_url: str) -> Optional[Dict[str, str]]:
    """
    Extract audio URL and metadata from episode HTML.
    
    Args:
        html_content: HTML page content
        page_url: URL of the page (for resolving relative URLs)
    
    Returns:
        Dict with keys: audio_url, title, date (or None if no audio found)
    """
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Find audio tag
    audio_tag = soup.find("audio", {"controls": True})
    if not audio_tag or not audio_tag.get("src"):
        return None
    
    audio_url = audio_tag["src"]
    
    # Resolve relative URLs to absolute
    audio_url = urljoin(page_url, audio_url)
    
    # Extract episode title (try multiple selectors)
    title = None
    for selector in ["h1", "title", ".episode-title"]:
        tag = soup.find(selector)
        if tag:
            title = tag.get_text(strip=True)
            break
    
    # Extract date (try multiple selectors)
    date = None
    for selector in [".episode-date", "time", ".date"]:
        tag = soup.find(selector)
        if tag:
            date = tag.get_text(strip=True) or tag.get("datetime")
            break
    
    return {
        "audio_url": audio_url,
        "title": title or "Unknown",
        "date": date or "Unknown",
        "page_url": page_url,
    }


def get_episode_links(index_html: str, base_url: str) -> List[str]:
    """
    Extract episode page URLs from index page.
    Customize based on your site structure.
    """
    soup = BeautifulSoup(index_html, "html.parser")
    links = []
    
    # Option 1: Find all links in a table with id 'artTable' (used on msspoldt.com)
    table = soup.find("table", id="artTable")
    if table:
        # Each episode is in a table row, second column (index 1)
        for row in table.find_all("tr")[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) >= 2:
                link = cells[1].find("a", href=True)
                if link:
                    href = link["href"]
                    links.append(urljoin(base_url, href))
    
    # Option 2: Find all links in a table/list with class 'episode'
    if not links:
        for link in soup.find_all("a", class_="episode"):
            href = link.get("href")
            if href:
                links.append(urljoin(base_url, href))
    
    # Option 3: If no 'episode' class, try all <a> tags in a specific container
    if not links:
        container = soup.find("div", class_="episodes") or soup.find("main")
        if container:
            for link in container.find_all("a", href=True):
                href = link["href"]
                # Filter out anchors and external links
                if href.startswith(("#", "http")) and not href.startswith(base_url):
                    continue
                links.append(urljoin(base_url, href))
    
    return links


# ===== CRAWLING =====
def crawl_episodes(
    session: requests.Session,
    start_url: str,
    visited: Optional[set] = None,
) -> List[Dict[str, str]]:
    """
    Recursively crawl episode pages and extract audio URLs.
    
    Args:
        session: Requests session
        start_url: URL to start crawling from
        visited: Set of already-visited URLs (for cycle detection)
    
    Returns:
        List of dicts with extracted audio data
    """
    if visited is None:
        visited = set()
    
    episodes = []
    
    print(f"[*] Fetching index: {start_url}")
    
    try:
        response = session.get(start_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[!] Error fetching {start_url}: {e}")
        return episodes
    
    # Get all episode links from index
    episode_links = get_episode_links(response.text, BASE_URL)
    print(f"[+] Found {len(episode_links)} episode links")
    
    # Crawl each episode
    for idx, episode_url in enumerate(episode_links, 1):
        if episode_url in visited:
            print(f"[~] Skipping (already visited): {episode_url}")
            continue
        
        visited.add(episode_url)
        print(f"[{idx}/{len(episode_links)}] Fetching: {episode_url}")
        
        try:
            response = session.get(episode_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            time.sleep(0.5)  # Be respectful—don't hammer the server
        except requests.RequestException as e:
            print(f"    [!] Error: {e}")
            continue
        
        # Extract audio URL and metadata
        episode_data = extract_audio_urls(response.text, episode_url)
        if episode_data:
            episodes.append(episode_data)
            print(f"    [+] Audio URL: {episode_data['audio_url']}")
        else:
            print(f"    [~] No audio found on this page")
    
    return episodes


# ===== FILE OUTPUT =====
def save_to_csv(episodes: List[Dict[str, str]], output_path: Path) -> None:
    """Save episodes to CSV for RSS generation."""
    if not episodes:
        print("[!] No episodes to save")
        return
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "date", "audio_url", "page_url"])
        writer.writeheader()
        writer.writerows(episodes)
    
    print(f"[+] Saved {len(episodes)} episodes to {output_path}")


def save_to_json(episodes: List[Dict[str, str]], output_path: Path) -> None:
    """Save episodes to JSON (also useful for RSS generation)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(episodes, f, indent=2, ensure_ascii=False)
    
    print(f"[+] Saved {len(episodes)} episodes to {output_path}")


# ===== MAIN =====
def main():
    print("=" * 60)
    print("Matt & Shane's Secret Podcast - Audio URL Extractor")
    print("=" * 60)
    
    session = create_session()
    
    # Check for local HTML file for debugging
    local_html_file = None
    if os.path.exists("The Old Testament.html"):
        print("[*] Found local 'The Old Testament.html' - using for testing")
        local_html_file = "The Old Testament.html"
    
    try:
        # Load episodes from local HTML file or fetch from web
        if local_html_file:
            print(f"[*] Loading index from local file: {local_html_file}")
            with open(local_html_file, "r", encoding="utf-8") as f:
                index_html = f.read()
            episode_links = get_episode_links(index_html, BASE_URL)
            print(f"[+] Found {len(episode_links)} episode links")
            
            episodes = []
            for idx, episode_url in enumerate(episode_links, 1):
                print(f"[{idx}/{len(episode_links)}] Fetching: {episode_url}")
                try:
                    response = session.get(episode_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    time.sleep(0.5)
                except requests.RequestException as e:
                    print(f"    [!] Error: {e}")
                    continue
                
                episode_data = extract_audio_urls(response.text, episode_url)
                if episode_data:
                    episodes.append(episode_data)
                    print(f"    [+] Audio URL: {episode_data['audio_url']}")
                else:
                    print(f"    [~] No audio found on this page")
        else:
            # Crawl episodes from web
            episodes = crawl_episodes(session, EPISODE_INDEX_URL)
        
        if episodes:
            print("\n" + "=" * 60)
            print(f"SUMMARY: Extracted {len(episodes)} episodes")
            print("=" * 60)
            
            # Save outputs
            save_to_csv(episodes, OUTPUT_CSV)
            save_to_json(episodes, OUTPUT_JSON)
            
            print(f"\n[✓] Next step: Use {OUTPUT_CSV} or {OUTPUT_JSON} to generate RSS feed")
        else:
            print("[!] No episodes extracted")
    
    finally:
        session.close()


if __name__ == "__main__":
    main()
