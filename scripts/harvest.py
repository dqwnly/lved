# scripts/harvest.py — full rewrite with hard time budget + tuned concurrency
# pip install aiohttp aiofiles pysocks

import asyncio
import aiohttp
import aiofiles
import time
import re
import socks
import ssl
from pathlib import Path

# ─── tuning ──────────────────────────────────────────────────────────────────

TIMEOUT_SEC    = 5        # per-proxy timeout (down from 8)
CONCURRENCY    = 600      # simultaneous checks (up from 300)
HARD_BUDGET    = 18 * 60  # 18 min wall-clock max for validation phase
TARGET_URL     = "https://login.live.com"
OUT_DIR        = Path("proxies")

# ─── sources ─────────────────────────────────────────────────────────────────

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
    "https://www.proxy-list.download/api/v1/get?type=http&anon=elite",
    "https://www.proxy-list.download/api/v1/get?type=https&anon=elite",
    "https://api.openproxylist.xyz/http.txt",
    "https://openproxy.space/list/http",
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
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks4.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
    "https://raw.githubusercontent.com/proxio-io/proxy-list/main/socks4.txt",
    "https://databay.com/free-proxy-list/socks4.txt",
    "https://hproxy.com/api/proxy-list?format=txt&protocol=socks4",
    "https://proxio.io/download?type=socks4&limit=500",
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
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks5.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
    "https://raw.githubusercontent.com/proxio-io/proxy-list/main/socks5.txt",
    "https://databay.com/free-proxy-list/socks5.txt",
    "https://hproxy.com/api/proxy-list?format=txt&protocol=socks5",
    "https://proxio.io/download?type=socks5&limit=500",
    "https://api.openproxylist.xyz/socks5.txt",
    "https://www.proxy-list.download/api/v1/get?type=socks5",
    "https://openproxy.space/list/socks5",
]

# ─── parse ───────────────────────────────────────────────────────────────────

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

# ─── fetch sources ───────────────────────────────────────────────────────────

async def fetch_source(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.text(errors="ignore")
    except Exception:
        pass
    return ""

async def harvest(sources: list[str]) -> set[tuple[str, int]]:
    conn = aiohttp.TCPConnector(limit=60)
    async with aiohttp.ClientSession(connector=conn) as session:
        texts = await asyncio.gather(*[fetch_source(session, u) for u in sources])
    out: set[tuple[str, int]] = set()
    for t in texts:
        out |= parse_proxies(t)
    return out

# ─── validate ────────────────────────────────────────────────────────────────

LEAK_HEADERS = {"x-forwarded-for", "via", "x-real-ip", "forwarded", "x-proxy-id"}

sem = asyncio.Semaphore(CONCURRENCY)

async def check_http(session: aiohttp.ClientSession, ip: str, port: int) -> bool:
    async with sem:
        try:
            async with session.get(
                TARGET_URL,
                proxy=f"http://{ip}:{port}",
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC),
                allow_redirects=False,
            ) as r:
                if r.status != 200:
                    return False
                return not any(h in r.headers for h in LEAK_HEADERS)
        except Exception:
            return False

def _socks_check_sync(ip: str, port: int, version: int) -> bool:
    try:
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS5 if version == 5 else socks.SOCKS4, ip, port)
        s.settimeout(TIMEOUT_SEC)
        ctx = ssl.create_default_context()
        wrapped = ctx.wrap_socket(s, server_hostname="login.live.com")
        wrapped.connect(("login.live.com", 443))
        wrapped.sendall(b"HEAD / HTTP/1.1\r\nHost: login.live.com\r\nConnection: close\r\n\r\n")
        resp = wrapped.recv(256).decode(errors="ignore")
        wrapped.close()
        return resp.startswith("HTTP/") and " 200 " in resp
    except Exception:
        return False

async def check_socks(ip: str, port: int, version: int) -> bool:
    async with sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _socks_check_sync, ip, port, version)

# ─── batched validation with hard time budget ────────────────────────────────

async def validate_http_list(proxies: set[tuple[str,int]], deadline: float) -> list[tuple[str,int]]:
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0"}
    good = []
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        tasks = {asyncio.ensure_future(check_http(session, ip, port)): (ip, port)
                 for ip, port in proxies}
        for coro in asyncio.as_completed(tasks):
            if time.monotonic() > deadline:
                print("  [!] http budget hit — stopping early")
                for t in tasks:
                    t.cancel()
                break
            proxy = tasks[coro]
            try:
                if await coro:
                    good.append(proxy)
            except Exception:
                pass
    return good

async def validate_socks_list(proxies: set[tuple[str,int]], version: int, deadline: float) -> list[tuple[str,int]]:
    good = []
    tasks = {asyncio.ensure_future(check_socks(ip, port, version)): (ip, port)
             for ip, port in proxies}
    for coro in asyncio.as_completed(tasks):
        if time.monotonic() > deadline:
            print(f"  [!] socks{version} budget hit — stopping early")
            for t in tasks:
                t.cancel()
            break
        proxy = tasks[coro]
        try:
            if await coro:
                good.append(proxy)
        except Exception:
            pass
    return good

# ─── write ───────────────────────────────────────────────────────────────────

async def write_list(path: Path, proxies: list[tuple[str,int]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = sorted(f"{ip}:{port}" for ip, port in proxies)
    async with aiofiles.open(path, "w") as f:
        await f.write("\n".join(lines) + "\n")
    print(f"  wrote {len(lines):>5} → {path}")

# ─── main ────────────────────────────────────────────────────────────────────

async def main():
    t0 = time.monotonic()
    print("[*] harvesting all sources...")

    http_raw, s4_raw, s5_raw = await asyncio.gather(
        harvest(SOURCES_HTTP),
        harvest(SOURCES_SOCKS4),
        harvest(SOURCES_SOCKS5),
    )
    print(f"[*] raw — http:{len(http_raw)}  socks4:{len(s4_raw)}  socks5:{len(s5_raw)}")

    # split budget: http gets half, socks4+socks5 share the other half
    validate_start = time.monotonic()
    http_deadline   = validate_start + HARD_BUDGET * 0.45
    s4_deadline     = validate_start + HARD_BUDGET * 0.72
    s5_deadline     = validate_start + HARD_BUDGET * 0.99

    print("[*] validating http...")
    elite_http  = await validate_http_list(http_raw, http_deadline)

    print("[*] validating socks4...")
    elite_s4    = await validate_socks_list(s4_raw,  4, s4_deadline)

    print("[*] validating socks5...")
    elite_s5    = await validate_socks_list(s5_raw,  5, s5_deadline)

    print(f"[*] elite — http:{len(elite_http)}  socks4:{len(elite_s4)}  socks5:{len(elite_s5)}")

    await asyncio.gather(
        write_list(OUT_DIR / "http.txt",   elite_http),
        write_list(OUT_DIR / "socks4.txt", elite_s4),
        write_list(OUT_DIR / "socks5.txt", elite_s5),
        write_list(OUT_DIR / "all.txt",    list(set(elite_http)|set(elite_s4)|set(elite_s5))),
    )

    print(f"[*] done in {time.monotonic()-t0:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())
