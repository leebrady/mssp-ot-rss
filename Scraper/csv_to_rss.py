#!/usr/bin/env python3
"""
Convert podcast episode CSV to RSS feed compatible with Overcast.
"""

import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import requests


def parse_date(date_str: str) -> str:
    """
    Parse various date formats and return RFC 2822 format for RSS.
    """
    if not date_str or date_str == "Unknown":
        return datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0000")
    
    # Try common date formats
    formats = [
        "%b. %d, %Y",  # Nov. 22, 2016
        "%B %d, %Y",   # November 22, 2016
        "%b %d, %Y",   # Nov 22, 2016
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.strftime("%a, %d %b %Y %H:%M:%S +0000")
        except ValueError:
            continue
    
    # If parsing fails, use current date
    return datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0000")


def get_audio_duration(audio_url: str) -> int:
    """
    Attempt to get audio file duration using HEAD request.
    Returns duration in seconds (0 if unable to determine).
    """
    try:
        response = requests.head(audio_url, timeout=5, allow_redirects=True)
        if "content-length" in response.headers:
            # This is a rough estimate; without downloading the file,
            # we can't determine exact duration. Overcast can handle duration=0.
            pass
    except Exception:
        pass
    
    return 0


def csv_to_rss(csv_path: Path, rss_path: Path, feed_url: str = None) -> None:
    """
    Convert CSV episodes to RSS feed.
    
    Args:
        csv_path: Path to episodes.csv
        rss_path: Path to output RSS file
        feed_url: URL where the RSS feed will be hosted (used in guid)
    """
    
    # Read CSV
    episodes = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            episodes.append(row)
    
    # Reverse to have newest first (typical for podcasts)
    episodes.reverse()
    
    # Create RSS structure
    rss = ET.Element("rss")
    rss.set("version", "2.0")
    rss.set("xmlns:content", "http://purl.org/rss/1.0/modules/content/")
    rss.set("xmlns:itunes", "http://www.itunes.com/dtds/podcast-1.0.dtd")
    
    channel = ET.SubElement(rss, "channel")
    
    # Channel metadata
    ET.SubElement(channel, "title").text = "Matt and Shane's Secret Podcast - Old Testament"
    ET.SubElement(channel, "link").text = "https://msspoldt.com/"
    ET.SubElement(channel, "description").text = "Matt and Shane's Secret Podcast - The Old Testament Era"
    ET.SubElement(channel, "language").text = "en-us"
    
    # iTunes metadata
    itunes_ns = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"
    ET.SubElement(channel, itunes_ns + "author").text = "Matt and Shane"
    ET.SubElement(channel, itunes_ns + "owner")
    owner = channel.find(itunes_ns + "owner")
    ET.SubElement(owner, itunes_ns + "name").text = "Matt and Shane"
    ET.SubElement(owner, itunes_ns + "email").text = "mssptold@gmail.com"
    
    # iTunes category
    category = ET.SubElement(channel, itunes_ns + "category")
    category.set("text", "Comedy")
    
    ET.SubElement(channel, itunes_ns + "explicit").text = "yes"
    ET.SubElement(channel, itunes_ns + "image").set("href", "https://msspoldt.com/image.jpg")
    
    # Add episodes as items
    for idx, episode in enumerate(episodes, 1):
        item = ET.SubElement(channel, "item")
        
        title = episode.get("title", f"Episode {idx}").strip(' ">')
        ET.SubElement(item, "title").text = title
        
        description = episode.get("description", title)
        ET.SubElement(item, "description").text = description or title
        
        audio_url = episode.get("audio_url", "").strip(' ">')
        if audio_url:
            ET.SubElement(item, "link").text = episode.get("page_url", audio_url).strip(' ">')
            
            # Enclosure (audio file)
            enclosure = ET.SubElement(item, "enclosure")
            enclosure.set("url", audio_url)
            enclosure.set("type", "audio/mpeg")
            enclosure.set("length", "0")  # Length in bytes - set to 0 if unknown
            
            # GUID
            guid = ET.SubElement(item, "guid")
            guid.set("isPermaLink", "false")
            guid.text = audio_url
        
        # Publish date
        pub_date = parse_date(episode.get("date", "Unknown"))
        ET.SubElement(item, "pubDate").text = pub_date
        
        # iTunes metadata for item
        ET.SubElement(item, itunes_ns + "explicit").text = "yes"
        ET.SubElement(item, itunes_ns + "duration").text = "0"  # Duration in seconds
    
    # Pretty print
    xml_str = minidom.parseString(ET.tostring(rss)).toprettyxml(indent="  ")
    # Remove XML declaration and extra blank lines
    xml_str = "\n".join([line for line in xml_str.split("\n") if line.strip()])
    xml_str = xml_str.replace('<?xml version="1.0" ?>', '<?xml version="1.0" encoding="UTF-8"?>')
    
    # Write to file
    rss_path.parent.mkdir(parents=True, exist_ok=True)
    with open(rss_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    
    print(f"[+] Generated RSS feed with {len(episodes)} episodes")
    print(f"[+] Saved to {rss_path}")
    print(f"\n[*] RSS Feed URL for Overcast:")
    print(f"    Raw GitHub URL: https://raw.githubusercontent.com/leebrady/mssp-ot-rss/main/feed.xml")


if __name__ == "__main__":
    csv_path = Path("./podcast_data/episodes.csv")
    rss_path = Path("../feed.xml")
    
    if not csv_path.exists():
        print(f"[!] CSV file not found: {csv_path}")
        exit(1)
    
    csv_to_rss(csv_path, rss_path)
