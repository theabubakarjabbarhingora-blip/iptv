#!/usr/bin/env python3
"""
IPTV playlist tester.

Reads channels.json (every channel with its main link and backup links),
tests each link, and writes:
  123.m3u     - only channels that have a working link (first working link wins)
  report.txt  - what worked, what was removed, and why

Uses only the Python standard library, so it runs on GitHub Actions as-is.
"""
import json
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

SOURCE = sys.argv[1] if len(sys.argv) > 1 else "channels.json"
OUT_M3U = sys.argv[2] if len(sys.argv) > 2 else "123.m3u"
OUT_REPORT = sys.argv[3] if len(sys.argv) > 3 else "report.txt"

TIMEOUT = 12          # seconds per network step
DEADLINE = 20         # seconds max for one download
WORKERS = 48          # channels tested at the same time
PER_HOST = 4          # max simultaneous requests to one server (avoids false failures)
MIN_WORKING_SHARE = 0.25  # if fewer than this work, assume the test itself broke and keep the old playlist

DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_host_locks = defaultdict(lambda: threading.Semaphore(PER_HOST))
_host_guard = threading.Lock()


def _sem(url):
    host = urllib.parse.urlsplit(url).netloc.lower()
    with _host_guard:
        return _host_locks[host]


def fetch(url, headers, limit):
    """GET url, read at most `limit` bytes. Returns (status, final_url, bytes, error_text)."""
    req = urllib.request.Request(url, headers=headers)
    start = time.time()
    with _sem(url):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                data = b""
                while len(data) < limit and time.time() - start < DEADLINE:
                    chunk = r.read(min(16384, limit - len(data)))
                    if not chunk:
                        break
                    data += chunk
                return r.status, r.geturl(), data, ""
        except urllib.error.HTTPError as e:
            return e.code, url, b"", "HTTP %s" % e.code
        except Exception as e:  # timeouts, DNS, TLS, connection reset...
            return 0, url, b"", type(e).__name__
    return 0, url, b"", "unknown"


def headers_for(link):
    h = {"User-Agent": DEFAULT_UA, "Accept": "*/*"}
    for line in link.get("o", []):
        m = re.match(r"#EXTVLCOPT:http-user-agent=(.+)", line)
        if m:
            h["User-Agent"] = m.group(1).strip()
        m = re.match(r"#EXTVLCOPT:http-referrer=(.+)", line)
        if m:
            h["Referer"] = m.group(1).strip()
    return h


def is_m3u(data):
    return data.lstrip(b"\xef\xbb\xbf \r\n\t")[:7] == b"#EXTM3U"


def playlist_lines(data, limit):
    text = data.decode("utf-8", "replace")
    lines = text.splitlines()
    if len(data) >= limit and lines:   # cut off mid-line: drop the last, incomplete line
        lines = lines[:-1]
    return [l.strip() for l in lines if l.strip()]


def check(link):
    """Returns (ok, reason, height). height = real picture height if the stream says so."""
    url = link["u"]
    hdr = headers_for(link)
    limit = 262144
    status, final, data, err = fetch(url, hdr, limit)
    if status not in (200, 206):
        return False, err or "HTTP %s" % status, 0
    if not data:
        return False, "empty reply", 0

    if not is_m3u(data):
        head = data[:600].lower()
        if b"<html" in head or b"<!doctype" in head or b"<?xml" in head:
            return False, "web page instead of video", 0
        if len(data) >= 8192:          # a direct video stream (not HLS) that is sending data
            return True, "direct stream", 0
        return False, "not a video stream", 0

    lines = playlist_lines(data, limit)
    height = 0
    base = final

    if any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
        variants = []
        for i, l in enumerate(lines):
            if not l.startswith("#EXT-X-STREAM-INF"):
                continue
            uri = next((x for x in lines[i + 1:i + 4] if not x.startswith("#")), None)
            if not uri:
                continue
            bw = re.search(r"[:,]BANDWIDTH=(\d+)", l)
            rs = re.search(r"RESOLUTION=\d+x(\d+)", l)
            variants.append((int(bw.group(1)) if bw else 0, int(rs.group(1)) if rs else 0, uri))
        if not variants:
            return False, "no qualities listed", 0
        height = max(v[1] for v in variants)
        variants.sort()
        vurl = urllib.parse.urljoin(final, variants[0][2])   # test the lightest quality
        status, base, data, err = fetch(vurl, hdr, limit)
        if status not in (200, 206) or not is_m3u(data):
            return False, "quality list broken (%s)" % (err or status), height
        lines = playlist_lines(data, limit)

    segments = [l for l in lines if not l.startswith("#")]
    if not segments:
        return False, "no video segments", height
    seg_url = urllib.parse.urljoin(base, segments[-1])
    status, _, data, err = fetch(seg_url, hdr, 4096)
    if status not in (200, 206) or not data:
        return False, "video segment missing (%s)" % (err or status), height
    return True, "ok", height


def test_channel(ch):
    reasons = []
    for attempt in (1, 2):                      # second pass = one retry for flaky servers
        links = ch["links"] if attempt == 1 else ch["links"][:2]
        for idx, link in enumerate(links):
            ok, reason, height = check(link)
            if ok:
                return {"ch": ch, "link": link, "index": idx, "height": height or link.get("q", 0)}
            if attempt == 1:
                reasons.append(reason)
        time.sleep(2)
    return {"ch": ch, "link": None, "reasons": reasons}


def main():
    src = json.load(open(SOURCE, encoding="utf-8"))
    channels, groups = src["channels"], src["groups"]
    t0 = time.time()
    with ThreadPoolExecutor(WORKERS) as pool:
        results = list(pool.map(test_channel, channels))
    working = [r for r in results if r["link"]]
    dead = [r for r in results if not r["link"]]
    share = len(working) / max(1, len(channels))

    rep = []
    rep.append("IPTV playlist test - %s UTC" % time.strftime("%Y-%m-%d %H:%M", time.gmtime()))
    rep.append("Tested from a GitHub server (USA). Took %d seconds." % (time.time() - t0))
    rep.append("")
    rep.append("Channels tested : %d" % len(channels))
    rep.append("Working         : %d" % len(working))
    rep.append("Removed (dead)  : %d" % len(dead))
    rep.append("Saved by a backup link : %d" % sum(1 for r in working if r["index"] > 0))
    rep.append("")
    rep.append("Per category (working / tested):")
    tot = Counter(c["group"] for c in channels)
    okc = Counter(r["ch"]["group"] for r in working)
    for g in groups:
        if tot[g]:
            rep.append("  %4d / %-4d  %s" % (okc[g], tot[g], g))
    rep.append("")
    rep.append("Why links failed (first link of each removed channel):")
    why = Counter(re.sub(r"\(.*\)", "", r["reasons"][0]).strip() if r.get("reasons") else "?" for r in dead)
    for k, v in why.most_common():
        rep.append("  %4d  %s" % (v, k))
    rep.append("")
    rep.append("Removed channels:")
    by = defaultdict(list)
    for r in dead:
        by[r["ch"]["group"]].append(r["ch"]["name"])
    for g in groups:
        if by[g]:
            rep.append("  [%s] %s" % (g, "; ".join(sorted(by[g]))))

    if share < MIN_WORKING_SHARE:
        rep.insert(0, "!!! Only %d%% of channels passed - the test run itself looks broken. "
                      "Playlist NOT changed.\n" % round(share * 100))
        open(OUT_REPORT, "w", encoding="utf-8").write("\n".join(rep) + "\n")
        print("\n".join(rep[:12]))
        sys.exit(1)

    with open(OUT_M3U, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for r in working:                       # same order as channels.json
            ch, link = r["ch"], r["link"]
            label = " (%dp)" % r["height"] if r["height"] else ""
            flag = " [Not 24/7]" if link.get("n") else ""
            f.write('#EXTINF:-1 tvg-id="%s" tvg-logo="%s" group-title="%s",%s%s%s\n'
                    % (ch.get("id", ""), ch.get("logo", ""), ch["group"], ch["name"], label, flag))
            for line in link.get("o", []):
                f.write(line + "\n")
            f.write(link["u"] + "\n")
    open(OUT_REPORT, "w", encoding="utf-8").write("\n".join(rep) + "\n")
    print("\n".join(rep[:60]))


if __name__ == "__main__":
    main()
