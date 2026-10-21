# scripts/harvest.py
# python 3.12 — pip install aiohttp aiofiles pysocks
# fetches all sources, dedupes, validates elite anonymity via login.live.com,
# separates by protocol, writes to proxies/{http,https,socks4,socks5}.txt

import asyncio
import aiohttp
import aiofiles
import os
import re
import socket
import time
from pathlib import Path
from typing import NamedTuple

# ─── sources ────────────────────────────────────────────────────────────────

SOURCES_HTTP = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&anonymity=elite&timeout=5000",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/relayglass/free-proxy-list/main/protocol/http/http.txt",
    "https://raw.githubusercontent.com/proxio-io/proxy-list/main/http.txt",
    "https://databay.com/free-proxy-list/http.txt",
    "https://hproxy.com/api/proxy-list?format=txt&protocol=http",
    "https://proxio.io/download?type=http&limit=500",
    "https://spys.me/proxy.txt",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http&anonymityLevel=elite",
    # HQ extras
    "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
    "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt",
    "https://raw.githubusercontent.com/hyperbeam/free-proxy-list/main/proxy-list.txt",
    "https://openproxy.space/list/http",
    "https://www.proxy-list.download/api/v1/get?type=http&anon=elite",
    "https://www.proxy-list.download/api/v1/get?type=https&anon=elite",
    "https://api.openproxylist.xyz/http.txt",
    "https://api.openproxylist.xyz/https.txt",
]

SOURCES_SOCKS4 = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks4&timeout=5000",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
    "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/socks4/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
    "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks4_proxies.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks4.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks4.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
    "https://raw.githubusercontent.com/relayglass/free-proxy-list/main/protocol/socks4/socks4.txt",
    "https://raw.githubusercontent.com/proxio-io/proxy-list/main/socks4.txt",
    "https://databay.com/free-proxy-list/socks4.txt",
    "https://hproxy.com/api/proxy-list?format=txt&protocol=socks4",
    "https://proxio.io/download?type=socks4&limit=500",
    "https://spys.me/socks.txt",
    "https://api.openproxylist.xyz/socks4.txt",
    "https://www.proxy-list.download/api/v1/get?type=socks4",
]

SOURCES_SOCKS5 = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks5&timeout=5000",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/socks5/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
    "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks5.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks5.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
    "https://raw.githubusercontent.com/relayglass/free-proxy-list/main/protocol/socks5/socks5.txt",
    "https://raw.githubusercontent.com/proxio-io/proxy-list/main/socks5.txt",
    "https://databay.com/free-proxy-list/socks5.txt",
    "https://hproxy.com/api/proxy-list?format=txt&protocol=socks5",
    "https://proxio.io/download?type=socks5&limit=500",
    "https://api.openproxylist.xyz/socks5.txt",
    "https://www.proxy-list.download/api/v1/get?type=socks5",
    "https://openproxy.space/list/socks5",
]

# ─── config ─────────────────────────────────────────────────────────────────

TARGET_URL   = "https://login.live.com"
TIMEOUT_SEC  = 8
CONCURRENCY  = 300          # simultaneous checks
OUT_DIR      = Path("proxies")

# ─── parse ──────────────────────────────────────────────────────────────────

PROXY_RE = re.compile(
    r"(?:(?:https?|socks[45])://)?"
    r"((?:\d{1,3}\.){3}\d{1,3})"
    r":(\d{2,5})"
)

def parse_proxies(text: str) -> set[tuple[str, int]]:
    out = set()
    for m in PROXY_RE.finditer(text):
        ip, port = m.group(1), int(m.group(2))
        if 1 <= port <= 65535:
            out.add((ip, port))
    return out

# ─── fetch sources ──────────────────────────────────────────────────────────

async def fetch_source(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.text(errors="ignore")
    except Exception:
        pass
    return ""

async def harvest(sources: list[str]) -> set[tuple[str, int]]:
    conn = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=conn) as session:
        texts = await asyncio.gather(*[fetch_source(session, u) for u in sources])
    proxies: set[tuple[str, int]] = set()
    for t in texts:
        proxies |= parse_proxies(t)
    return proxies

# ─── validate ───────────────────────────────────────────────────────────────
# elite check: proxy must not forward X-Forwarded-For / Via headers
# we fetch login.live.com headers and confirm 200/30x + no leak headers

async def check_http(session: aiohttp.ClientSession, ip: str, port: int) -> bool:
    proxy = f"http://{ip}:{port}"
    try:
        async with session.get(
            TARGET_URL,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC),
            allow_redirects=False,
        ) as r:
            if r.status not in (200, 301, 302, 303, 307, 308):
                return False
            # elite filter: must not leak real IP via these headers
            leak_headers = ("x-forwarded-for", "via", "x-real-ip", "forwarded", "x-proxy-id")
            for h in leak_headers:
                if h in r.headers:
                    return False
            return True
    except Exception:
        return False

async def check_socks(ip: str, port: int, version: int) -> bool:
    # basic TCP reachability for socks — full SOCKS validation needs pysocks
    # for elite we just confirm connection + login.live.com TCP handshake
    import socks
    import ssl
    loop = asyncio.get_event_loop()
    def _check():
        try:
            s = socks.socksocket()
            s.set_proxy(
                socks.SOCKS5 if version == 5 else socks.SOCKS4,
                ip, port
            )
            s.settimeout(TIMEOUT_SEC)
            ctx = ssl.create_default_context()
            # wrap for HTTPS target
            wrapped = ctx.wrap_socket(s, server_hostname="login.live.com")
            wrapped.connect(("login.live.com", 443))
            wrapped.sendall(b"HEAD / HTTP/1.1\r\nHost: login.live.com\r\nConnection: close\r\n\r\n")
            resp = wrapped.recv(512).decode(errors="ignore")
            wrapped.close()
            return resp.startswith("HTTP/")
        except Exception:
            return False
    return await loop.run_in_executor(None, _check)

sem = asyncio.Semaphore(CONCURRENCY)

async def validate_http(session: aiohttp.ClientSession, ip: str, port: int) -> tuple[str,int] | None:
    async with sem:
        ok = await check_http(session, ip, port)
        return (ip, port) if ok else None

async def validate_socks(ip: str, port: int, version: int) -> tuple[str,int] | None:
    async with sem:
        ok = await check_socks(ip, port, version)
        return (ip, port) if ok else None

async def validate_all_http(proxies: set[tuple[str,int]]) -> list[tuple[str,int]]:
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        tasks = [validate_http(session, ip, port) for ip, port in proxies]
        results = await asyncio.gather(*tasks)
    return [r for r in results if r]

async def validate_all_socks(proxies: set[tuple[str,int]], version: int) -> list[tuple[str,int]]:
    tasks = [validate_socks(ip, port, version) for ip, port in proxies]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r]

# ─── write ──────────────────────────────────────────────────────────────────

async def write_list(path: Path, proxies: list[tuple[str,int]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = sorted(f"{ip}:{port}" for ip, port in proxies)
    async with aiofiles.open(path, "w") as f:
        await f.write("\n".join(lines) + "\n")
    print(f"  wrote {len(lines):>5} → {path}")

# ─── main ────────────────────────────────────────────────────────────────────

async def main():
    t0 = time.time()
    print(f"[*] harvesting sources...")

    http_raw, socks4_raw, socks5_raw = await asyncio.gather(
        harvest(SOURCES_HTTP),
        harvest(SOURCES_SOCKS4),
        harvest(SOURCES_SOCKS5),
    )

    print(f"[*] raw counts — http:{len(http_raw)}  socks4:{len(socks4_raw)}  socks5:{len(socks5_raw)}")
    print(f"[*] validating against {TARGET_URL} (elite only, timeout {TIMEOUT_SEC}s)...")

    elite_http, elite_socks4, elite_socks5 = await asyncio.gather(
        validate_all_http(http_raw),
        validate_all_socks(socks4_raw, 4),
        validate_all_socks(socks5_raw, 5),
    )

    print(f"[*] elite counts — http:{len(elite_http)}  socks4:{len(elite_socks4)}  socks5:{len(elite_socks5)}")

    await asyncio.gather(
        write_list(OUT_DIR / "http.txt",   elite_http),
        write_list(OUT_DIR / "socks4.txt", elite_socks4),
        write_list(OUT_DIR / "socks5.txt", elite_socks5),
    )

    # combined elite list
    combined = list(set(elite_http) | set(elite_socks4) | set(elite_socks5))
    await write_list(OUT_DIR / "all.txt", combined)

    elapsed = time.time() - t0
    print(f"[*] done in {elapsed:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())
