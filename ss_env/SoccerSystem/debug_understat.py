"""
One-off diagnostic: inspect what understat.com actually returns right now,
so we can see why 'datesData' wasn't found.
"""
import re
import requests
from bs4 import BeautifulSoup

url = "https://understat.com/league/EPL/2025"
headers = {"User-Agent": "Mozilla/5.0 (compatible; soccer-research-bot/1.0)"}

resp = requests.get(url, headers=headers, timeout=20)

print("Status code:", resp.status_code)
print("Final URL (after redirects):", resp.url)
print("Content length:", len(resp.text))
print()
print("First 500 characters of response:")
print(resp.text[:500])
print()

soup = BeautifulSoup(resp.text, "lxml")
scripts = soup.find_all("script")
print(f"Found {len(scripts)} <script> tags total")
print()

# Look for any JS variable assignments that look like Understat's data blobs
var_pattern = re.compile(r"var\s+(\w+)\s*=\s*JSON\.parse")
found_vars = []
for s in scripts:
    text = s.string or ""
    matches = var_pattern.findall(text)
    found_vars.extend(matches)

print("JS variables found matching 'var X = JSON.parse(...)':", found_vars or "NONE FOUND")