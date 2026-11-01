"""
Mesin migrasi Roblox DataStore (Open Cloud v2) - versi web.
Dipakai oleh server.py. Tidak ada GUI. Folder hasil per-pekerjaan lewat cfg["out_dir"].
"""
import concurrent.futures as cf
import glob
import html
import json
import os
import queue
import re
import struct
import threading
import time
import urllib.parse

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

APP_NAME = "Migration DS By WP"
APP_VER = "3.5"
BASE = os.environ.get("RBX_API_BASE", "https://apis.roblox.com/cloud/v2")


def q(s):
    return urllib.parse.quote(str(s), safe="")


# ==========================================================================
#  SCAN FILE GAME  (.rbxl/.rbxlx/.lua) -> cari GetOrderedDataStore("nama","scope")
# ==========================================================================
ORD_STATIC = re.compile(
    r"""GetOrderedDataStore\s*\(\s*(["'])([^"']+)\1\s*(?:,\s*(["'])([^"']*)\3\s*)?\)""")
ORD_NAME_DYNSCOPE = re.compile(
    r"""GetOrderedDataStore\s*\(\s*(["'])([^"']+)\1\s*,\s*[^"'\s)]""")
ORD_PREFIX = re.compile(r"""GetOrderedDataStore\s*\(\s*(["'])([^"']+)\1\s*\.\.""")
ORD_ANY = re.compile(r"GetOrderedDataStore\s*\(")
SCAN_EXT = (".rbxl", ".rbxm", ".rbxlx", ".rbxmx", ".lua", ".luau", ".txt")

# Nama Ordered DataStore yang paling sering dipakai game Roblox (untuk auto-probe)
COMMON_ORDERED = """Points Point Cash Coins Coin Gems Gem Money Gold Diamonds Tokens Credits Robux Souls Orbs
Kills Kill Deaths Wins Win Losses Level Levels XP Exp Experience Score Scores HighScore Highscore BestScore
Rank Ranks Rebirths Rebirth Prestige Playtime PlayTime TimePlayed Time Donations Donated Donation Raised
Streak Streaks Likes Votes Eliminations Trophies Stage Stages Distance Steps Fish Clicks Taps Power Strength
Leaderboard Leaderboards Global GlobalLeaderboard TopPlayers Top Stats Crates Eggs Pets Bounty Wave Waves
Damage Hatches Hatched Items Collected Points_Total Wins_Total Kills_Total AllTime Weekly Daily Monthly Season""".split()


def _lz4_block(src, size):
    out = bytearray(); i = 0; n = len(src)
    while i < n:
        tok = src[i]; i += 1
        lit = tok >> 4
        if lit == 15:
            while True:
                b = src[i]; i += 1; lit += b
                if b != 255:
                    break
        out += src[i:i + lit]; i += lit
        if i >= n:
            break
        off = src[i] | (src[i + 1] << 8); i += 2
        ml = tok & 15
        if ml == 15:
            while True:
                b = src[i]; i += 1; ml += b
                if b != 255:
                    break
        ml += 4
        st = len(out) - off
        if off >= ml:
            out += out[st:st + ml]
        else:
            for k in range(ml):
                out.append(out[st + k])
    return bytes(out)


def read_rbx_binary(data):
    """Gabungkan isi semua chunk file biner .rbxl/.rbxm (teks script ikut di dalamnya)."""
    pos, parts = 32, []
    while pos + 16 <= len(data):
        name = data[pos:pos + 4]
        clen, ulen = struct.unpack_from("<II", data, pos + 4)
        pos += 16
        if name == b"END\x00":
            break
        if clen == 0:
            chunk = data[pos:pos + ulen]; pos += ulen
        else:
            raw = data[pos:pos + clen]; pos += clen
            if raw[:4] == b"\x28\xb5\x2f\xfd":  # zstd
                import zstandard  # opsional: pip install zstandard
                chunk = zstandard.ZstdDecompressor().decompress(raw, max_output_size=ulen or 1 << 28)
            else:
                chunk = _lz4_block(raw, ulen)
        parts.append(chunk)
    return b"\n".join(parts)


def parse_ordered_targets(text, default_scopes):
    """'A, B:PROD26, C' -> [(A,scope)..., (B,PROD26), ...]. 'nama:scope' = scope khusus."""
    out = []
    for tok in [x.strip() for x in re.split(r"[,\n;]", text) if x.strip()]:
        if ":" in tok:
            n, sc = tok.split(":", 1)
            pairs = [(n.strip(), sc.strip() or "global")]
        else:
            pairs = [(tok, sc) for sc in default_scopes]
        for pr in pairs:
            if pr[0] and pr not in out:
                out.append(pr)
    return out


def expand_patterns(text):
    """'Top_{1-12}, Season{01-05}' -> ['Top_1', ..., 'Season01', ...]. Maksimal 500 per pola."""
    out = []
    for tok in [x.strip() for x in re.split(r"[,\n;]", text or "") if x.strip()]:
        m = re.search(r"\{(\d+)-(\d+)\}", tok)
        if not m:
            out.append(tok); continue
        a, b = m.group(1), m.group(2)
        lo, hi, width = int(a), int(b), (len(a) if a.startswith("0") else 0)
        for i in range(lo, min(hi, lo + 500) + 1):
            out.append(tok[:m.start()] + str(i).zfill(width) + tok[m.end():])
    return list(dict.fromkeys(out))


def scan_sources(paths):
    """Return (targets, prefixes, dinamis, dilewati, jml_file)."""
    found, prefixes, dynamic, skipped, files = set(), set(), 0, [], 0
    for p in paths:
        try:
            with open(p, "rb") as f:
                data = f.read()
        except OSError:
            skipped.append(os.path.basename(p)); continue
        if data.startswith(b"<roblox!"):
            try:
                data = read_rbx_binary(data)
            except Exception:
                skipped.append(os.path.basename(p)); continue
        text = html.unescape(data.decode("utf-8", "ignore"))
        files += 1
        st = ORD_STATIC.findall(text)
        for _, name, _, sc in st:
            found.add((name, sc or "global"))
        for _, name in ORD_NAME_DYNSCOPE.findall(text):
            found.add((name, "global"))
        for _, pre in ORD_PREFIX.findall(text):
            prefixes.add(pre)
        dynamic += max(0, len(ORD_ANY.findall(text)) - len(st))
    return sorted(found), sorted(prefixes), dynamic, skipped, files


# ==========================================================================
#  ENGINE
# ==========================================================================
class Stopped(Exception):
    pass


class Fatal(Exception):
    pass


class RetryExhausted(RuntimeError):
    pass


class ApiError(RuntimeError):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


def explain(code):
    return {
        401: "API Key salah / kedaluwarsa.",
        403: "Izin API Key kurang: experience ini belum dipilih di key, operasi belum dicentang, atau IP ditolak.",
        404: "Universe ID / nama DataStore tidak ditemukan, atau key bukan milik experience itu.",
        400: "Request tidak valid (cek nama DataStore / isi data).",
    }.get(code, "")


def is_exists(r):
    """Entry sudah ada di tujuan: Roblox membalas 409 atau 400 'Entry already exists'."""
    if r.status_code == 409:
        return True
    if r.status_code == 400:
        try:
            return "already exist" in r.text.lower()
        except Exception:
            return False
    return False


def split_entry(e):
    """Ambil (scope, key) dari objek entry v2. Prioritas: path -> id 'scope/key' -> global."""
    path = e.get("path", "") or ""
    m = re.search(r"/scopes/([^/]+)/entries/(.+)$", path)
    if m:
        return urllib.parse.unquote(m.group(1)), urllib.parse.unquote(m.group(2))
    eid = str(e.get("id") or "")
    if not eid and "/entries/" in path:
        eid = urllib.parse.unquote(path.split("/entries/", 1)[1])
    if "/" in eid:
        sc, k = eid.split("/", 1)
        return sc, k
    return "global", eid


class RateLimiter:
    """Pembatas request/detik bersama untuk semua worker, turun otomatis saat kena 429."""

    def __init__(self, rate):
        self.max = self.rate = max(0.5, float(rate))
        self.lock = threading.Lock()
        self.next = 0.0
        self.streak = 0

    def acquire(self, check):
        with self.lock:
            now = time.monotonic()
            t = max(self.next, now)
            self.next = t + 1.0 / self.rate
        while True:
            d = t - time.monotonic()
            if d <= 0:
                return
            check()
            time.sleep(min(d, 0.1))

    def throttle(self):
        with self.lock:
            self.rate = max(1.0, self.max * 0.15, self.rate * 0.85); self.streak = 0

    def success(self):
        with self.lock:
            self.streak += 1
            if self.streak >= 15 and self.rate < self.max:
                self.rate = min(self.max, self.rate * 1.1); self.streak = 0


class Tracker:
    """Statistik + progres; kirim snapshot ke GUI (dibatasi ~5x/detik)."""
    KINDS = ("migrated", "dry", "exists", "resumed", "missing", "failed", "verify_fail")

    def __init__(self, qu):
        self.q = qu
        self.lock = threading.Lock()
        self.stats = {k: 0 for k in self.KINDS}
        self.msg = ""
        self.t_run = time.time()
        self.begin(0)

    def begin(self, total):
        with self.lock:
            self.total, self.done, self.work = total, 0, 0
            self.t0, self.last = time.time(), 0.0
        self.q.put(("state", self.snapshot()))

    def item(self, kind=None):
        with self.lock:
            if kind:
                self.stats[kind] += 1
            self.done += 1
            if kind != "resumed":
                self.work += 1
        self.emit()

    def snapshot(self):
        el = time.time() - self.t0
        speed = self.work / el if el > 0.5 and self.work else 0.0
        left = self.total - self.done
        eta = (left / speed) if speed else None
        return dict(done=self.done, total=self.total, stats=dict(self.stats), msg=self.msg,
                    elapsed=time.time() - self.t_run, speed=speed, eta=eta)

    def emit(self, force=False):
        now = time.time()
        if force or now - self.last >= 0.2:
            self.last = now
            self.q.put(("state", self.snapshot()))


class DsCtx:
    """File backup + progress (resume) untuk satu DataStore / satu scope Ordered."""

    def __init__(self, tag, fresh, out_dir):
        self.lock = threading.Lock()
        self.backup_path = os.path.join(out_dir, f"backup_{tag}.jsonl")
        self.prog_path = os.path.join(out_dir, f"progress_{tag}.txt")
        self.done = set()
        if os.path.exists(self.prog_path) and not fresh:
            with open(self.prog_path, encoding="utf-8") as f:
                self.done = {l.rstrip("\n") for l in f if l.strip()}
        self.bk = open(self.backup_path, "a", encoding="utf-8")
        self.pg = open(self.prog_path, "w" if fresh else "a", encoding="utf-8")

    def backup(self, obj):
        with self.lock:
            self.bk.write(json.dumps(obj, ensure_ascii=False) + "\n"); self.bk.flush()

    def mark(self, eid):
        with self.lock:
            self.pg.write(eid + "\n"); self.pg.flush()

    def close(self):
        for f in (self.bk, self.pg):
            try:
                f.close()
            except Exception:
                pass


def safe_tag(*parts):
    return re.sub(r"[^\w.-]", "_", "_".join(str(p) for p in parts))


class Migrator:
    def __init__(self, cfg, qu, stop_event):
        self.c, self.q, self.stop = cfg, qu, stop_event
        self.tr = Tracker(qu)
        self.limiter = RateLimiter(cfg.get("rate", 6))
        self.tls = threading.local()
        self.fatal = None
        self.n403 = {"r": 0, "w": 0}
        self.w_ok = False
        self.failed = []
        self._flock = threading.Lock()
        self._lim = {}

    # ---- util
    def log(self, msg, lvl="info"):
        self.q.put(("log", msg, lvl))

    def status(self, msg):
        self.tr.msg = msg
        self.tr.emit(force=True)

    def log_limited(self, bucket, msg, lvl="info", cap=5):
        n = self._lim.get(bucket, 0) + 1
        self._lim[bucket] = n
        if n <= cap:
            self.log(msg, lvl)
        elif n == cap + 1:
            self.log(f"({bucket}: pesan serupa berikutnya tidak ditampilkan satu per satu)", lvl)

    def phase(self, name, i, n):
        self.log(f"=== Fase {i}/{n}: {name} ===", "ok")
        self.q.put(("phase", name, i, n))

    def check_stop(self):
        if self.stop.is_set() or self.fatal:
            raise Stopped()

    def sleep(self, sec):
        end = time.time() + sec
        while time.time() < end:
            self.check_stop(); time.sleep(0.1)

    def die(self, msg):
        self.fatal = msg
        raise Fatal(msg)

    def guard(self, r, side):
        """side: 'r' (key sumber) atau 'w' (key tujuan). 401 / 403 -> hentikan dengan pesan jelas.
        Penulisan pertama ke tujuan yang kena 403 langsung menghentikan migrasi (tidak menunggu 8x)."""
        s = r.status_code
        who = "sumber" if side == "r" else "tujuan"
        if s == 401:
            self.die(f"API Key {who} ditolak (401): key salah atau kedaluwarsa.")
        if s == 403:
            self.n403[side] += 1
            limit = 1 if (side == "w" and not self.w_ok) else 8
            if self.n403[side] >= limit:
                if side == "w":
                    self.die("Key TUJUAN tidak punya izin menulis ke Universe ID tujuan (403). Tidak ada data yang tertulis.\n"
                             "Perbaiki di create.roblox.com/dashboard/credentials -> buka key tujuan:\n"
                             "  1) Access Permissions -> tambah 'Data Stores' (dan 'Ordered Data Stores' bila perlu)\n"
                             "  2) Pada bagian Experiences pilih experience TUJUAN (bukan hanya yang lama)\n"
                             "  3) Centang operasi: Read Entry, Create Entry, Update Entry\n"
                             "  4) Accepted IP Addresses = 0.0.0.0/0, lalu Save. Pastikan key tujuan ditempel di kolom 'API Key tujuan'.\n"
                             f"Respons server: {r.text[:160]}")
                self.die(f"Akses ditolak (403) berulang pada API Key {who}. Cek izin key (operasi & experience) "
                         f"dan 'Accepted IP Addresses' = 0.0.0.0/0.")
        elif 200 <= s < 300 or (side == "w" and is_exists(r)):
            self.n403[side] = 0
            if side == "w":
                self.w_ok = True

    def fail(self, ctx_name, ident, msg):
        with self._flock:
            self.failed.append(f"{ctx_name}\t{ident}")
        self.log_limited("gagal", msg, "err", cap=30)
        self.tr.item("failed")

    def flush_failed(self):
        if self.failed:
            with open(os.path.join(self.c["out_dir"], "failed_ids.txt"), "w", encoding="utf-8") as f:
                f.write("\n".join(self.failed))
            self.log(f"{len(self.failed)} item gagal -> failed_ids.txt (ada di zip hasil) "
                     f"(jalankan ulang: yang gagal otomatis dicoba lagi).", "warn")

    # ---- HTTP (retry + rate limit + auto-throttle)
    def sess(self):
        s = getattr(self.tls, "s", None)
        if s is None:
            s = self.tls.s = requests.Session()
        return s

    def http(self, method, url, key, **kw):
        delay = 1.0
        for attempt in range(1, 8):
            self.check_stop()
            self.limiter.acquire(self.check_stop)
            try:
                r = self.sess().request(method, url, headers={"x-api-key": key}, timeout=30, **kw)
            except requests.RequestException as e:
                self.log_limited("jaringan", f"Jaringan error ({type(e).__name__}), coba lagi {attempt}/7...", "warn", 10)
                self.sleep(delay); delay = min(delay * 2, 30)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if r.status_code == 429:
                    self.limiter.throttle()
                try:
                    wait = float(r.headers.get("retry-after", delay))
                except ValueError:
                    wait = delay
                wait = min(max(wait, 0.5), 60)
                self.log_limited(f"http{r.status_code}",
                                 f"Server {r.status_code}, menunggu {wait:.0f}s (coba {attempt}/7; "
                                 f"kecepatan otomatis {self.limiter.rate:.1f} req/s)...", "warn", 5)
                self.sleep(wait); delay = min(delay * 2, 30)
                continue
            self.limiter.success()
            return r
        raise RetryExhausted(f"Gagal setelah 7x retry: {method} {url}")

    # ---- URL builder
    def eu(self, uni, ds, scope=None, key=None):
        u = f"{BASE}/universes/{uni}/data-stores/{q(ds)}"
        if scope is not None:
            u += f"/scopes/{q(scope)}"
        u += "/entries"
        if key is not None:
            u += f"/{q(key)}"
        return u

    def ou(self, uni, ds, scope, key=None):
        u = f"{BASE}/universes/{uni}/ordered-data-stores/{q(ds)}/scopes/{q(scope)}/entries"
        return u + (f"/{q(key)}" if key is not None else "")

    # ---- Standard API
    def list_datastores(self, uni, key):
        url = f"{BASE}/universes/{uni}/data-stores"
        token, names = None, []
        while True:
            params = {"maxPageSize": 100}
            if token:
                params["pageToken"] = token
            r = self.http("GET", url, key, params=params)
            if r.status_code != 200:
                raise ApiError(r.status_code, f"List DataStore gagal ({r.status_code}). {explain(r.status_code)}\n{r.text[:200]}")
            d = r.json()
            for ds in d.get("dataStores", []):
                n = ds.get("id") or ds.get("path", "").split("/")[-1]
                if n:
                    names.append(n)
            token = d.get("nextPageToken")
            if not token:
                return names

    def list_standard(self, uni, ds, key, limit, on_page):
        url = self.eu(uni, ds)
        token, out = None, []
        while True:
            params = {"maxPageSize": 100, "showDeleted": "false"}
            if token:
                params["pageToken"] = token
            r = self.http("GET", url, key, params=params)
            if r.status_code != 200:
                raise ApiError(r.status_code, f"List entry '{ds}' gagal ({r.status_code}). {explain(r.status_code)}\n{r.text[:200]}")
            d = r.json()
            out += [split_entry(e) for e in d.get("dataStoreEntries", [])]
            on_page(len(out))
            if limit and len(out) >= limit:
                return out[:limit]
            token = d.get("nextPageToken")
            if not token:
                return out

    def read_entry(self, ds, sc, k):
        c = self.c
        r = self.http("GET", self.eu(c["src_uni"], ds, sc, k), c["src_key"])
        self.guard(r, "r")
        if r.status_code == 404:  # fallback alamat tanpa scope
            eid = k if sc == "global" else f"{sc}/{k}"
            r2 = self.http("GET", self.eu(c["src_uni"], ds, None, eid), c["src_key"])
            if r2.status_code == 200:
                return r2
        return r

    def write_entry(self, ds, sc, k, body):
        """Return ('ok'|'exists'|'fail', response). Scope dipertahankan."""
        c = self.c
        uni, key = c["dst_uni"], c["dst_key"]
        scoped = True
        w = self.http("POST", self.eu(uni, ds, sc), key, params={"id": k}, json=body)
        if w.status_code == 404:
            scoped = False
            eid = k if sc == "global" else f"{sc}/{k}"
            w = self.http("POST", self.eu(uni, ds), key, params={"id": eid}, json=body)
        self.guard(w, "w")
        if is_exists(w):
            if not c["overwrite"]:
                return "exists", w
            url = (self.eu(uni, ds, sc, k) if scoped
                   else self.eu(uni, ds, None, k if sc == "global" else f"{sc}/{k}"))
            w = self.http("PATCH", url, key, params={"allowMissing": "true"}, json=body)
            self.guard(w, "w")
        return ("ok" if w.status_code in (200, 201) else "fail"), w

    # ---- Ordered API
    def list_ordered(self, uni, ds, scope, key, limit, on_page, page=100):
        url = self.ou(uni, ds, scope)
        token, rows = None, []
        while True:
            params = {"maxPageSize": page}
            if token:
                params["pageToken"] = token
            r = self.http("GET", url, key, params=params)
            if r.status_code != 200:
                raise ApiError(r.status_code, f"List Ordered '{ds}/{scope}' gagal ({r.status_code}). "
                                              f"{explain(r.status_code)}\n{r.text[:200]}")
            d = r.json()
            items = d.get("orderedDataStoreEntries") or d.get("entries") or []
            for e in items:
                eid = e.get("id")
                if eid is None:
                    eid = urllib.parse.unquote((e.get("path") or "").rsplit("/", 1)[-1])
                rows.append((str(eid), e.get("value")))
            on_page(len(rows))
            if limit and len(rows) >= limit:
                return rows[:limit]
            token = d.get("nextPageToken")
            if not token:
                return rows

    # ---- pool
    def run_pool(self, items, fn, workers):
        workers = max(1, int(workers))
        if workers == 1:
            for it in items:
                self.check_stop(); fn(it)
            return
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            it = iter(items); pending = set()
            try:
                while True:
                    while len(pending) < workers * 4:
                        try:
                            x = next(it)
                        except StopIteration:
                            break
                        pending.add(ex.submit(fn, x))
                    if not pending:
                        break
                    done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
                    for f in done:
                        f.result()
            except BaseException:
                for f in pending:
                    f.cancel()
                raise

    # ======================================================================
    #  Tes koneksi
    # ======================================================================
    def test(self):
        c = self.c
        try:
            if c["mode"] in ("auto", "standard"):
                self.status("Mengetes API Key sumber (Standard)...")
                try:
                    names = self.list_datastores(c["src_uni"], c["src_key"])
                    self.log(f"[OK] Standard: key sumber valid. {len(names)} DataStore: {', '.join(names[:25]) or '-'}"
                             + (" ..." if len(names) > 25 else ""), "ok")
                except ApiError as e:
                    self.log(f"[X] Standard: {e}", "err")
                self.status("Mengetes API Key tujuan...")
                r = self.http("GET", f"{BASE}/universes/{c['dst_uni']}/data-stores", c["dst_key"], params={"maxPageSize": 1})
                if r.status_code == 200:
                    self.log("[OK] Key tujuan valid.", "ok")
                elif r.status_code == 403:
                    self.log("[?] Key tujuan 403 pada List DataStore. Tidak masalah bila izin hanya Create/Update.", "warn")
                else:
                    self.log(f"[X] Key tujuan bermasalah ({r.status_code}). {explain(r.status_code)}", "err")
                self.status("Mengetes izin tulis key tujuan...")
                w = self.http("PATCH", self.eu(c["dst_uni"], "__MigratorTest__", "global", "probe"), c["dst_key"],
                              json={"value": 0})
                if w.status_code == 403:
                    self.log("[X] Key tujuan TIDAK punya izin tulis ke Universe tujuan (403 'not authorized'). "
                             "Buka key di dashboard -> pilih experience TUJUAN + centang Create Entry & Update Entry "
                             "+ IP 0.0.0.0/0.", "err")
                elif w.status_code == 401:
                    self.log("[X] Key tujuan salah / kedaluwarsa (401).", "err")
                elif w.status_code == 404 and "universe" in w.text.lower():
                    self.log("[X] Universe ID tujuan tidak ditemukan (404). Pastikan Universe ID, bukan Place ID.", "err")
                else:
                    self.log(f"[OK] Izin tulis key tujuan tampak benar (HTTP {w.status_code}, tidak ada data dibuat).", "ok")
            if c["mode"] in ("auto", "ordered"):
                self.status("Mengetes izin Ordered DataStore...")
                for label, uni, key in (("sumber", c["src_uni"], c["src_key"]),):
                    r = self.http("GET", self.ou(uni, "Points", "global"), key, params={"maxPageSize": 1})
                    if r.status_code in (200, 404):
                        self.log(f"[OK] Izin Ordered ({label}) tampak benar (HTTP {r.status_code}).", "ok")
                    else:
                        self.log(f"[X] Ordered ({label}) HTTP {r.status_code}. {explain(r.status_code)} "
                                 f"Pastikan key punya API System 'Ordered Data Stores' (entry read).", "err")
                for ds, sc in c.get("targets", [])[:20]:
                    try:
                        rows = self.list_ordered(c["src_uni"], ds, sc, c["src_key"], 1, lambda n: None, page=1)
                        self.log(f"[OK] Ordered {ds}/{sc}: " + ("ada data." if rows else "akses OK tapi kosong."),
                                 "ok" if rows else "warn")
                    except ApiError as e:
                        self.log(f"[X] Ordered {ds}/{sc}: {e}", "err")
        except (Stopped, Fatal):
            pass
        except Exception as e:
            self.log(f"[X] {e}", "err")
        self.q.put(("test_done",))

    # ======================================================================
    #  Cek tujuan: baca balik dari experience tujuan & bandingkan dengan backup
    # ======================================================================
    def check_dest(self):
        import glob
        c = self.c
        try:
            uni, key = c["dst_uni"], c["dst_key"]
            self.log(f"=== Cek tujuan: Universe ID {uni} ===", "ok")
            self.status("Membaca daftar DataStore di tujuan...")
            try:
                names = self.list_datastores(uni, key)
            except ApiError as e:
                self.log(f"[X] Tidak bisa melihat DataStore tujuan: {e}", "err")
                names = []
            if names:
                self.log(f"Universe {uni} berisi {len(names)} DataStore: {', '.join(names[:40])}"
                         + (" ..." if len(names) > 40 else ""), "ok")
            else:
                self.log("Universe tujuan ini TIDAK berisi DataStore apa pun (atau tidak bisa dilihat). "
                         "Kemungkinan Universe ID tujuan di aplikasi berbeda dengan experience yang Anda buka.", "warn")
            pref = os.path.join(c["out_dir"], f"backup_{safe_tag(c['src_uni'], c['dst_uni'])}_*.jsonl")
            files = sorted(glob.glob(pref))
            if not files:
                self.log("Belum ada file backup untuk pasangan Universe ini, tidak ada yang bisa dibandingkan.", "warn")
            tot = dict(ok=0, diff=0, miss=0, err=0)
            for fp in files:
                rows = []
                try:
                    with open(fp, encoding="utf-8") as f:
                        rows = [json.loads(l) for l in f if l.strip()]
                except Exception:
                    continue
                if not rows:
                    continue
                n = min(8, len(rows))
                step = max(1, len(rows) // n)
                sample = rows[::step][:n]
                ds = sample[0]["datastore"]
                res = dict(ok=0, diff=0, miss=0, err=0)
                for r in sample:
                    self.check_stop()
                    sc, k = r.get("scope", "global"), r["id"]
                    kk = k.split("/", 1)[1] if (sc != "global" and k.startswith(sc + "/")) else k
                    rr = self.http("GET", self.eu(uni, ds, sc, kk), key)
                    if rr.status_code == 200:
                        res["ok" if rr.json().get("value") == r.get("value") else "diff"] += 1
                    elif rr.status_code == 404:
                        res["miss"] += 1
                    else:
                        res["err"] += 1
                for kx in tot:
                    tot[kx] += res[kx]
                lvl = "ok" if res["miss"] == 0 and res["err"] == 0 else "err"
                self.log(f"  {ds}: sampel {len(sample)} -> cocok {res['ok']}, nilai beda {res['diff']}, "
                         f"TIDAK ADA {res['miss']}, error {res['err']}", lvl)
            if files:
                if tot["miss"] == 0 and tot["err"] == 0:
                    self.log("[OK] Data yang disampel ADA di experience tujuan. Jika tidak terlihat di Studio / dashboard, "
                             "cek: (1) Anda membuka experience dengan Universe ID yang sama, (2) di Studio aktifkan "
                             "Game Settings > Security > 'Enable Studio Access to API Services', (3) di Data Stores "
                             "Manager gunakan scope & nama yang sama dengan skrip game, lalu refresh.", "ok")
                else:
                    self.log("[X] Sebagian data TIDAK ditemukan di tujuan. Jalankan Mulai lagi dengan Dry run mati "
                             "dan 'Salin ulang semua' aktif.", "err")
        except (Stopped, Fatal):
            pass
        except Exception as e:
            self.log(f"[X] {e}", "err")
        self.q.put(("check_done",))

    # ======================================================================
    #  Discovery Ordered
    # ======================================================================
    def discover(self, std_names=(), std_scopes=(), known=()):
        """Cari Ordered DataStore tanpa API list: probe nama umum x scope. Return [(ds, scope)]."""
        c = self.c
        names = []

        def add(n):
            if n and n not in names:
                names.append(n)
        for n in c.get("probe_names", []):
            add(n)
        for n in std_names:
            add(n)
        for pre in c.get("prefixes", []):
            add(pre.rstrip("_-. "))
            for i in range(0, 41):
                add(f"{pre}{i}")
        for n in COMMON_ORDERED:
            add(n)
        user_sc = [s for s in c.get("scope_list", []) if s]
        seen = sorted(set(std_scopes))
        scopes_a = ["global"] + [s for s in user_sc if s != "global"]
        if len(seen) <= 5:
            scopes_a += [s for s in seen if s not in scopes_a]
        known = list(known)
        jobs = [(n, s) for n in names for s in scopes_a]
        cap = 3000
        if len(jobs) > cap:
            self.log(f"Probe dibatasi {cap} dari {len(jobs)} kombinasi.", "warn"); jobs = jobs[:cap]
        found, flock, probed = [], threading.Lock(), set()
        bad = [0]

        def probe(job):
            self.check_stop()
            ds, sc = job
            try:
                rows = self.list_ordered(c["src_uni"], ds, sc, c["src_key"], 1, lambda n: None, page=1)
            except ApiError as e:
                if e.code in (401, 403):
                    bad[0] += 1
                    if bad[0] == 4:
                        self.log("Ordered: akses ditolak berulang saat probe. Key sumber perlu API System "
                                 "'Ordered Data Stores' (entry read).", "err")
                return
            except RetryExhausted:
                return
            finally:
                self.tr.item()
                self.tr.msg = f"Mencari Ordered DataStore... ({ds} / {sc}) ditemukan: {len(found)}"
            if rows:
                with flock:
                    found.append((ds, sc))
                self.log(f"[+] Ordered ditemukan: {ds} / scope {sc}", "ok")
                self.q.put(("found", ds, sc))

        self.log(f"Auto-probe: {len(jobs)} kombinasi (nama x scope)...", "info")
        self.tr.begin(len(jobs))
        self.run_pool(jobs, probe, c["workers"])
        probed.update(jobs)

        # tahap B: nama yang sudah dikenal dicoba di scope tambahan (mis. scope musiman dari Standard)
        base = {n for n, _ in found} | {n for n, _ in known}
        extra = [s for s in seen if s not in scopes_a] + [s for _, s in known if s not in scopes_a]
        extra = list(dict.fromkeys(extra))[:30]
        jobs_b = [(n, s) for n in base for s in extra if (n, s) not in probed]
        if jobs_b:
            self.log(f"Auto-probe tahap 2: {len(jobs_b)} kombinasi (nama dikenal x scope tambahan)...", "info")
            self.tr.begin(len(jobs_b))
            self.run_pool(jobs_b, probe, c["workers"])
        return found

    def discover_only(self):
        found = []
        try:
            names, scopes = [], set()
            try:
                names = self.list_datastores(self.c["src_uni"], self.c["src_key"])
                self.log(f"Seed dari {len(names)} nama DataStore Standard.", "info")
            except Exception:
                pass
            found = self.discover(names, scopes, known=self.c.get("targets", []))
            self.log(f"Discovery selesai: {len(found)} Ordered DataStore berisi data ditemukan.",
                     "ok" if found else "warn")
        except (Stopped, Fatal):
            self.log("Discovery dihentikan.", "warn")
        except Exception as e:
            self.log(f"[X] Discovery: {e}", "err")
        self.tr.emit(force=True)
        self.q.put(("discover_done", len(found)))

    # ======================================================================
    #  Fase Standard
    # ======================================================================
    def phase_standard(self, i, n):
        c = self.c
        self.phase("Standard DataStore", i, n)
        if c["only"]:
            names = list(c["only"])
        else:
            self.status("Mengambil daftar semua DataStore...")
            names = self.list_datastores(c["src_uni"], c["src_key"])
        if not names:
            raise RuntimeError("Tidak ada DataStore ditemukan (cek izin 'List Data Stores').")
        if c["skip"]:
            sk = [d for d in names if any(x in d.lower() for x in c["skip"])]
            names = [d for d in names if d not in sk]
            if sk:
                self.log(f"DataStore dilewati (cocok filter): {', '.join(sk)}", "warn")
        self.log(f"{len(names)} DataStore akan diproses: {', '.join(names)}", "ok")

        plan, seen_scopes = {}, set()
        for k, ds in enumerate(names, 1):
            self.status(f"Menghitung key di '{ds}' ({k}/{len(names)})...")
            items = self.list_standard(c["src_uni"], ds, c["src_key"], c["limit"],
                                       lambda n, ds=ds: self.status(f"Menghitung key di '{ds}'... {n} key"))
            plan[ds] = items
            seen_scopes.update(sc for sc, _ in items if sc != "global")
            self.log(f"  {ds}: {len(items)} key" + (f" (scope: {len({s for s,_ in items})})" if items else ""), "info")
        total = sum(len(v) for v in plan.values())
        self.log(f"Total {total} key Standard.", "ok")
        self.tr.begin(total)
        for di, (ds, items) in enumerate(plan.items(), 1):
            self.q.put(("sub", di, len(plan), ds))
            dst_ds = c["dst_ds"] if (len(plan) == 1 and c["dst_ds"]) else ds
            ctx = DsCtx(safe_tag(c["src_uni"], c["dst_uni"], ds), c["fresh"], c["out_dir"])
            if ctx.done:
                self.log(f"Resume '{ds}': {len(ctx.done)} key sudah selesai sebelumnya.", "info")
            try:
                self.run_pool(items, lambda it, ds=ds, dst=dst_ds, ctx=ctx: self._std_item(ctx, ds, dst, it), c["workers"])
            finally:
                ctx.close()
            self.log(f"Selesai DataStore '{ds}'.", "ok")
        self.tr.emit(force=True)
        return names, seen_scopes

    def _std_item(self, ctx, ds, dst_ds, item):
        c = self.c
        sc, k = item
        eid = k if sc == "global" else f"{sc}/{k}"
        self.check_stop()
        if eid in ctx.done:
            self.tr.item("resumed"); return
        self.tr.msg = f"[{ds}] {eid}"
        try:
            r = self.read_entry(ds, sc, k)
            if r.status_code == 404:
                self.log_limited("missing", f"dilewati (entry sudah tidak ada / kedaluwarsa) {ds}/{eid}", "warn", 3)
                self.tr.item("missing"); return
            if r.status_code != 200:
                self.fail(ds, eid, f"GAGAL baca {ds}/{eid}: {r.status_code} {explain(r.status_code)} {r.text[:120]}")
                return
            j = r.json()
            body = {"value": j.get("value")}
            if j.get("users"):
                body["users"] = j["users"]
            if j.get("attributes"):
                body["attributes"] = j["attributes"]
            ctx.backup({"datastore": ds, "scope": sc, "id": eid, **body})
            if c["dry"]:
                self.log_limited("dry", f"(dry-run) {ds}/{eid}", "info", 15)
                self.tr.item("dry"); return
            res, w = self.write_entry(dst_ds, sc, k, body)
            if res == "exists":
                ctx.mark(eid)
                self.log_limited("exists", f"dilewati, sudah ada di tujuan (aktifkan Overwrite untuk menimpa): {ds}/{eid}", "warn", 5)
                self.tr.item("exists")
            elif res == "ok":
                ctx.mark(eid)
                if c["verify"]:
                    try:
                        if w.json().get("value") != body["value"]:
                            self.tr.stats["verify_fail"] += 1
                            self.log_limited("verify", f"VERIFIKASI beda: {ds}/{eid}", "err", 10)
                    except Exception:
                        pass
                self.tr.item("migrated")
            else:
                self.fail(ds, eid, f"GAGAL tulis {ds}/{eid}: {w.status_code} {explain(w.status_code)} {w.text[:150]}")
        except (Stopped, Fatal):
            raise
        except Exception as e:
            self.fail(ds, eid, f"GAGAL {ds}/{eid}: {type(e).__name__}: {e}")

    # ======================================================================
    #  Fase Ordered
    # ======================================================================
    def phase_ordered(self, i, n, std_names=(), std_scopes=()):
        c = self.c
        self.phase("Ordered DataStore", i, n)
        targets = list(dict.fromkeys(c.get("targets", [])))
        if c.get("probe"):
            if not std_names and not c.get("only"):
                try:
                    std_names = self.list_datastores(c["src_uni"], c["src_key"])
                except Exception:
                    std_names = []
            for t in self.discover(std_names, std_scopes, known=targets):
                if t not in targets:
                    targets.append(t)
        if not targets:
            self.log("Ordered: tidak ada Ordered DataStore yang ditemukan/diisi. Scan file game, isi nama manual, "
                     "atau pakai pola. (Roblox tidak menyediakan API untuk melihat daftar Ordered.)", "warn")
            return
        self.log(f"{len(targets)} Ordered DataStore/scope akan dimigrasi.", "ok")

        plan = {}
        for k, (ds, sc) in enumerate(targets, 1):
            self.status(f"Membaca Ordered '{ds}' scope '{sc}' ({k}/{len(targets)})...")
            try:
                rows = self.list_ordered(c["src_uni"], ds, sc, c["src_key"], c["limit"],
                                         lambda n, ds=ds, sc=sc: self.status(f"Membaca Ordered '{ds}/{sc}'... {n} entry"))
            except ApiError as e:
                self.log(f"[X] {ds}/{sc} dilewati: {e}", "err"); continue
            plan[(ds, sc)] = rows
            self.log(f"  {ds}/{sc}: {len(rows)} entry", "info")
        total = sum(len(v) for v in plan.values())
        self.log(f"Total {total} entry Ordered dari {len(plan)} DataStore/scope.", "ok")
        self.tr.begin(total)
        single = len(plan) == 1
        for si, ((ds, sc), rows) in enumerate(plan.items(), 1):
            self.q.put(("sub", si, len(plan), f"{ds} / scope {sc}"))
            dst_ds = c["dst_ds"] if (single and c.get("dst_ds") and c["mode"] == "ordered") else ds
            ctx = DsCtx(safe_tag("ordered", c["src_uni"], c["dst_uni"], ds, sc), c["fresh"], c["out_dir"])
            if ctx.done:
                self.log(f"Resume '{ds}/{sc}': {len(ctx.done)} entry sudah selesai.", "info")
            try:
                self.run_pool(rows, lambda it, ds=ds, sc=sc, dst=dst_ds, ctx=ctx: self._ord_item(ctx, ds, sc, dst, it),
                              c["workers"])
            finally:
                ctx.close()
            self.log(f"Selesai Ordered '{ds}/{sc}'.", "ok")
        self.tr.emit(force=True)

    def _ord_item(self, ctx, ds, sc, dst_ds, row):
        c = self.c
        eid, val = row
        self.check_stop()
        if eid in ctx.done:
            self.tr.item("resumed"); return
        self.tr.msg = f"[{ds}/{sc}] {eid} = {val}"
        name = f"{ds}/{sc}"
        try:
            ctx.backup({"datastore": ds, "scope": sc, "id": eid, "value": val})
            if c["dry"]:
                self.log_limited("dry-ord", f"(dry-run) {name}/{eid} = {val}", "info", 15)
                self.tr.item("dry"); return
            body = {"value": val}
            w = self.http("POST", self.ou(c["dst_uni"], dst_ds, sc), c["dst_key"], params={"id": eid}, json=body)
            self.guard(w, "w")
            if is_exists(w):
                if c["overwrite"]:
                    w = self.http("PATCH", self.ou(c["dst_uni"], dst_ds, sc, eid), c["dst_key"],
                                  params={"allowMissing": "true"}, json=body)
                    self.guard(w, "w")
                else:
                    ctx.mark(eid)
                    self.log_limited("exists-ord", f"lewati (sudah ada) {name}/{eid}", "warn", 5)
                    self.tr.item("exists"); return
            if w.status_code in (200, 201):
                ctx.mark(eid)
                if c["verify"]:
                    try:
                        if w.json().get("value") != val:
                            self.tr.stats["verify_fail"] += 1
                            self.log_limited("verify", f"VERIFIKASI beda: {name}/{eid}", "err", 10)
                    except Exception:
                        pass
                self.tr.item("migrated")
            else:
                self.fail(name, eid, f"GAGAL tulis {name}/{eid}: {w.status_code} {explain(w.status_code)} {w.text[:150]}")
        except (Stopped, Fatal):
            raise
        except Exception as e:
            self.fail(name, eid, f"GAGAL {name}/{eid}: {type(e).__name__}: {e}")

    # ======================================================================
    #  Entry point migrasi
    # ======================================================================
    def run(self):
        c = self.c
        t0 = time.time()
        ok = False
        try:
            n = 2 if c["mode"] == "auto" else 1
            names, scopes, idx = [], set(), 1
            if c["mode"] in ("auto", "standard"):
                names, scopes = self.phase_standard(idx, n); idx += 1
            if c["mode"] in ("auto", "ordered"):
                self.phase_ordered(idx, n, names, scopes)
            st = self.tr.stats
            if c["dry"]:
                msg = "SIMULASI selesai. Tidak ada data ditulis ke tujuan. Matikan Dry run lalu klik Mulai untuk migrasi sungguhan."
                ok = st["failed"] == 0
            elif st["migrated"] == 0:
                msg = ("Selesai, tetapi 0 data tertulis ke tujuan"
                       + (" (semua sudah ada di tujuan; aktifkan Overwrite untuk menimpa)." if st["exists"] else "."))
                ok = False
            else:
                msg = f"Migrasi selesai: {st['migrated']} data tertulis ke tujuan."
                ok = st["failed"] == 0
        except Stopped:
            msg = self.fatal or "Dihentikan. Jalankan lagi untuk melanjutkan (resume otomatis)."
            if self.fatal:
                self.log(f"[BERHENTI] {self.fatal}", "err")
        except Fatal as e:
            msg = str(e); self.log(f"[BERHENTI] {msg}", "err")
        except Exception as e:
            msg = f"Berhenti karena error: {e}"; self.log(f"[ERROR] {e}", "err")
        self.flush_failed()
        self.tr.emit(force=True)
        self.q.put(("done", dict(self.tr.stats), time.time() - t0, msg, ok))


