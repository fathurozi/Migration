"""
Server web untuk Migration DS By WP.

- Frontend statis ada di ./static (di-serve di "/").
- API memakai polling (tanpa WebSocket) supaya aman di hosting gratis / di belakang proxy.
- API Key HANYA dipakai di memori selama pekerjaan berjalan: tidak ditulis ke disk dan
  tidak pernah masuk log (semua teks yang dikirim ke browser disaring dulu).
"""
import os
import re
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from collections import defaultdict, deque
from typing import List, Literal, Optional

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

import engine

APP_NAME = "Migration DS By WP"
APP_VER = "web-1.0"

# ----------------------------------------------------------------- konfigurasi (env)
DATA_DIR = os.environ.get("DATA_DIR") or os.path.join(tempfile.gettempdir(), "rbx_migrator")
RETENTION_H = float(os.environ.get("RETENTION_HOURS", "3"))        # hapus data hasil setelah N jam
MAX_RUNNING = int(os.environ.get("MAX_RUNNING_JOBS", "6"))         # pekerjaan berjalan bersamaan
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))              # batas worker per pekerjaan
MAX_RATE = float(os.environ.get("MAX_RATE", "15"))                 # batas request/detik per pekerjaan
STARTS_PER_HOUR = int(os.environ.get("STARTS_PER_HOUR", "40"))     # batas mulai-pekerjaan per IP/jam
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "50"))         # per file scan
MAX_UPLOAD_FILES = int(os.environ.get("MAX_UPLOAD_FILES", "6"))
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]
os.makedirs(DATA_DIR, exist_ok=True)

WS_RE = re.compile(r"^[a-f0-9]{16,64}$")
JOB_RE = re.compile(r"^[a-f0-9]{32}$")

app = FastAPI(title=APP_NAME, docs_url=None, redoc_url=None, openapi_url=None)
if CORS_ORIGINS:
    app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS, allow_methods=["GET", "POST", "DELETE"],
                       allow_headers=["Content-Type", "X-Workspace"], max_age=600)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Cache-Control"] = "no-store" if request.url.path.startswith("/api/") else "public, max-age=300"
    return resp


# ----------------------------------------------------------------- pekerjaan
class Sink:
    """Pengganti queue.Queue: engine memanggil put(); kita ubah jadi event JSON yang sudah disaring."""

    def __init__(self, job):
        self.job = job

    def put(self, ev):
        self.job.add(ev)


class Job:
    def __init__(self, ws, action, out_dir, secrets):
        self.id = uuid.uuid4().hex
        self.ws, self.action, self.out_dir = ws, action, out_dir
        self.secrets = [s for s in secrets if s and len(s) >= 8]
        self.events, self.base = [], 0          # events[0] punya nomor urut self.base
        self.state = None
        self.finished = False
        self.result = None
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.created = self.touched = time.time()
        self.finished_at = None

    def scrub(self, text):
        if not isinstance(text, str):
            return text
        for s in self.secrets:
            text = text.replace(s, "***")
        return text

    def push(self, d):
        with self.lock:
            self.events.append(d)
            if len(self.events) > 4000:             # buang log lama agar memori aman
                drop = len(self.events) - 3000
                del self.events[:drop]
                self.base += drop

    def add(self, ev):
        k = ev[0]
        if k == "state":
            s = dict(ev[1])
            s["msg"] = self.scrub(s.get("msg", ""))
            self.state = s
        elif k == "log":
            self.push(dict(t="log", msg=self.scrub(ev[1]), lvl=ev[2]))
        elif k == "phase":
            self.push(dict(t="phase", name=ev[1], i=ev[2], n=ev[3]))
        elif k == "sub":
            self.push(dict(t="sub", i=ev[1], n=ev[2], name=self.scrub(ev[3])))
        elif k == "found":
            self.push(dict(t="found", ds=self.scrub(ev[1]), scope=self.scrub(ev[2])))
        elif k in ("test_done", "check_done"):
            self.push(dict(t=k))
            self.finish(None)
        elif k == "discover_done":
            self.push(dict(t=k, n=ev[1]))
            self.finish(None)
        elif k == "done":
            self.result = dict(stats=ev[1], elapsed=ev[2], msg=self.scrub(ev[3]), ok=bool(ev[4]))
            self.push(dict(t="done", **self.result))
            self.finish(self.result)

    def finish(self, result):
        self.finished = True
        self.finished_at = time.time()
        self.secrets = []


JOBS = {}
JOBS_LOCK = threading.Lock()
STARTS = defaultdict(deque)


def client_ip(request: Request):
    xf = request.headers.get("x-forwarded-for")
    return (xf.split(",")[0].strip() if xf else (request.client.host if request.client else "?"))


def get_ws(x_workspace: Optional[str]):
    if not x_workspace or not WS_RE.match(x_workspace):
        raise HTTPException(400, "Workspace tidak valid.")
    return x_workspace


def ws_dir(ws):
    d = os.path.join(DATA_DIR, ws)
    os.makedirs(d, exist_ok=True)
    os.utime(d, None)
    return d


def get_job(job_id, ws):
    if not JOB_RE.match(job_id or ""):
        raise HTTPException(404, "Pekerjaan tidak ditemukan.")
    j = JOBS.get(job_id)
    if not j or j.ws != ws:
        raise HTTPException(404, "Pekerjaan tidak ditemukan (mungkin server sempat restart).")
    j.touched = time.time()
    return j


# ----------------------------------------------------------------- model request
class StartReq(BaseModel):
    action: Literal["test", "check", "run", "discover"]
    src_key: str = Field("", max_length=600)
    dst_key: str = Field("", max_length=600)
    src_uni: str = Field("", max_length=20)
    dst_uni: str = Field("", max_length=20)
    mode: Literal["auto", "standard", "ordered"] = "auto"
    dst_ds: str = Field("", max_length=120)
    only: str = Field("", max_length=2000)
    skip: str = Field("", max_length=2000)
    dry: bool = True
    overwrite: bool = False
    fresh: bool = False
    verify: bool = True
    limit: int = Field(0, ge=0, le=10_000_000)
    workers: int = Field(4, ge=1, le=64)
    rate: float = Field(6, ge=0.5, le=1000)
    scopes: str = Field("global", max_length=400)
    manual: str = Field("", max_length=20000)
    patterns: str = Field("", max_length=2000)
    targets: List[List[str]] = []
    prefixes: List[str] = []
    probe: bool = True


def build_cfg(r: StartReq, out_dir):
    need = ["src_key", "src_uni"] + (["dst_key", "dst_uni"] if r.action != "discover" else [])
    for k in need:
        if not getattr(r, k).strip():
            raise HTTPException(422, "Isi API Key dan Universe ID " + ("sumber dan tujuan." if r.action != "discover" else "sumber."))
    for k in ("src_uni", "dst_uni"):
        v = getattr(r, k).strip()
        if v and not v.isdigit():
            raise HTTPException(422, "Universe ID harus angka saja.")
    scopes = [s.strip() for s in r.scopes.split(",") if s.strip()][:20] or ["global"]
    targets = []
    for t in r.targets[:2000]:
        if len(t) == 2 and t[0].strip() and len(t[0]) <= 200 and len(t[1]) <= 200:
            targets.append((t[0], t[1] or "global"))
    for pr in engine.parse_ordered_targets(r.manual, scopes):
        if pr not in targets:
            targets.append(pr)
    split = lambda s: [x.strip() for x in s.split(",") if x.strip()]
    return dict(
        src_key=r.src_key.strip(), dst_key=r.dst_key.strip(), src_uni=r.src_uni.strip(), dst_uni=r.dst_uni.strip(),
        mode=r.mode, dst_ds=r.dst_ds.strip(), only=split(r.only), skip=[x.lower() for x in split(r.skip)],
        dry=r.dry, overwrite=r.overwrite, fresh=r.fresh, verify=r.verify, limit=r.limit,
        workers=min(r.workers, MAX_WORKERS), rate=min(r.rate, MAX_RATE), scope_list=scopes,
        targets=targets, probe_names=engine.expand_patterns(r.patterns)[:5000],
        prefixes=[p for p in r.prefixes[:200] if len(p) <= 120], probe=r.probe, out_dir=out_dir)


def run_job(job, cfg):
    m = engine.Migrator(cfg, Sink(job), job.stop)
    fn = dict(test=m.test, check=m.check_dest, run=m.run, discover=m.discover_only)[job.action]
    try:
        fn()
    except Exception as e:  # jaring pengaman terakhir
        job.push(dict(t="log", msg=job.scrub(f"[ERROR] {e}"), lvl="err"))
        job.push(dict(t="done", stats={}, elapsed=0, msg=job.scrub(f"Berhenti karena error: {e}"), ok=False))
        job.finish(None)
    finally:
        cfg.clear()                       # buang API Key dari memori secepatnya
        if not job.finished:
            job.finish(None)


# ----------------------------------------------------------------- API
@app.get("/api/config")
def api_config():
    return dict(name=APP_NAME, version=APP_VER, max_workers=MAX_WORKERS, max_rate=MAX_RATE,
                max_upload_mb=MAX_UPLOAD_MB, max_upload_files=MAX_UPLOAD_FILES, retention_hours=RETENTION_H,
                tutorial=[dict(kind=k, text=t) for k, t in TUTORIAL])


@app.get("/api/health")
def health():
    return dict(ok=True)


@app.post("/api/jobs")
def start_job(req: StartReq, request: Request, x_workspace: Optional[str] = Header(None)):
    ws = get_ws(x_workspace)
    ip = client_ip(request)
    now = time.time()
    dq = STARTS[ip]
    while dq and now - dq[0] > 3600:
        dq.popleft()
    if len(dq) >= STARTS_PER_HOUR:
        raise HTTPException(429, "Terlalu banyak permintaan dari alamat ini. Coba lagi nanti.")
    with JOBS_LOCK:
        running = [j for j in JOBS.values() if not j.finished]
        if any(j.ws == ws for j in running):
            raise HTTPException(409, "Masih ada pekerjaan yang berjalan. Tunggu selesai atau tekan Stop.")
        if len(running) >= MAX_RUNNING:
            raise HTTPException(503, "Server sedang penuh. Coba lagi beberapa menit lagi.")
        out_dir = ws_dir(ws)
        cfg = build_cfg(req, out_dir)
        job = Job(ws, req.action, out_dir, [cfg["src_key"], cfg["dst_key"]])
        JOBS[job.id] = job
    dq.append(now)
    threading.Thread(target=run_job, args=(job, cfg), daemon=True).start()
    return dict(job=job.id)


@app.get("/api/jobs/{job_id}")
def poll_job(job_id: str, after: int = 0, x_workspace: Optional[str] = Header(None)):
    j = get_job(job_id, get_ws(x_workspace))
    fin = j.finished  # dibaca SEBELUM snapshot: kalau True, semua event sudah masuk daftar
    with j.lock:
        start = max(after - j.base, 0)
        evs = j.events[start:]
        nxt = j.base + len(j.events)
    return dict(events=evs, next=nxt, state=j.state, finished=fin, result=j.result)


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str, x_workspace: Optional[str] = Header(None)):
    j = get_job(job_id, get_ws(x_workspace))
    j.stop.set()
    return dict(ok=True)


def _zip_dir(d):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp.close()
    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as z:
        for name in sorted(os.listdir(d)):
            p = os.path.join(d, name)
            if os.path.isfile(p):
                z.write(p, name)
    return tmp.name


@app.get("/api/results")
def download_results(x_workspace: Optional[str] = Header(None)):
    d = ws_dir(get_ws(x_workspace))
    if not [f for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]:
        raise HTTPException(404, "Belum ada file hasil.")
    path = _zip_dir(d)
    return FileResponse(path, media_type="application/zip", filename="migration_output.zip",
                        background=BackgroundTask(lambda: os.path.exists(path) and os.remove(path)))


@app.delete("/api/results")
def wipe_results(x_workspace: Optional[str] = Header(None)):
    ws = get_ws(x_workspace)
    if any(j.ws == ws and not j.finished for j in JOBS.values()):
        raise HTTPException(409, "Hentikan pekerjaan yang berjalan dulu.")
    shutil.rmtree(os.path.join(DATA_DIR, ws), ignore_errors=True)
    return dict(ok=True)


@app.post("/api/scan")
async def scan(files: List[UploadFile] = File(...), x_workspace: Optional[str] = Header(None)):
    get_ws(x_workspace)
    if len(files) > MAX_UPLOAD_FILES:
        raise HTTPException(413, f"Maksimal {MAX_UPLOAD_FILES} file sekali scan.")
    tmpdir = tempfile.mkdtemp(prefix="scan_")
    paths = []
    try:
        for n, f in enumerate(files):
            name = re.sub(r"[^\w.\-]", "_", os.path.basename(f.filename or f"file{n}"))
            if not name.lower().endswith(engine.SCAN_EXT):
                continue
            p = os.path.join(tmpdir, f"{n}_{name}")
            size = 0
            with open(p, "wb") as out:
                while True:
                    chunk = await f.read(1 << 20)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_UPLOAD_MB << 20:
                        raise HTTPException(413, f"File {name} lebih dari {MAX_UPLOAD_MB} MB.")
                    out.write(chunk)
            paths.append(p)
        targets, prefixes, dyn, skipped, nfiles = engine.scan_sources(paths)
        skipped = [re.sub(r"^\d+_", "", s) for s in skipped]
        return dict(targets=[list(t) for t in targets], prefixes=prefixes, dynamic=dyn, skipped=skipped, files=nfiles)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ----------------------------------------------------------------- pembersihan berkala
def janitor():
    while True:
        time.sleep(300)
        now = time.time()
        with JOBS_LOCK:
            for jid in [k for k, j in JOBS.items() if j.finished and now - (j.finished_at or now) > 3600]:
                JOBS.pop(jid, None)
        try:
            busy = {j.ws for j in JOBS.values() if not j.finished}
            for name in os.listdir(DATA_DIR):
                p = os.path.join(DATA_DIR, name)
                if name not in busy and os.path.isdir(p) and now - os.path.getmtime(p) > RETENTION_H * 3600:
                    shutil.rmtree(p, ignore_errors=True)
        except OSError:
            pass


threading.Thread(target=janitor, daemon=True).start()

# ----------------------------------------------------------------- tutorial (teks dari aplikasi desktop, disesuaikan web)
TUTORIAL = [
    ("h", "Mulai cepat (mode Otomatis Penuh)"),
    ("p", "1. Isi API Key + Universe ID sumber dan tujuan di tab Migrasi.\n"
          "2. Klik Tes Koneksi -> harus muncul [OK] hijau.\n"
          "3. Biarkan Dry Run aktif, klik Mulai. Cek hasilnya di log.\n"
          "4. Matikan Dry Run, klik Mulai lagi. Selesai: semua Standard DataStore (semua scope) dan "
          "Ordered DataStore yang terdeteksi ikut pindah."),
    ("h", "1) Buat API Key (Open Cloud)"),
    ("p", "Buka https://create.roblox.com/dashboard/credentials -> Create API Key.\n\n"
          "KEY SUMBER (experience lama):\n"
          "  - API System 'Data Stores' + experience sumber: List Data Stores, List Entries, Read Entry\n"
          "  - API System 'Ordered Data Stores' + experience sumber: Read Entry (untuk Ordered)\n"
          "KEY TUJUAN (experience baru):\n"
          "  - 'Data Stores' + pilih experience TUJUAN: Read Entry, Create Entry, Update Entry\n"
          "  - 'Ordered Data Stores' + experience TUJUAN: Read Entry, Create Entry, Update Entry\n"
          "  - Error 403 'Resources not authorized' = experience tujuan belum dipilih di key tersebut.\n"
          "Kalau akun/grup sama, boleh 1 key untuk kedua experience.\n"
          "Accepted IP Addresses: 0.0.0.0/0 (server ini tidak punya IP tetap). Salin key (hanya tampil sekali)."),
    ("h", "2) Universe ID"),
    ("p", "create.roblox.com/dashboard/creations -> pilih experience -> Universe ID ada di URL / Overview.\n"
          "Universe ID BUKAN Place ID."),
    ("h", "3) Bagaimana Ordered bisa 'otomatis'?"),
    ("p", "Roblox TIDAK menyediakan API untuk melihat daftar Ordered DataStore. Karena itu aplikasi ini mencari "
          "nama-nama tersebut sendiri, dengan 4 cara yang digabung:\n\n"
          "  a) Scan file game (.rbxl/.rbxlx/.lua): membaca semua GetOrderedDataStore(\"nama\",\"scope\"). "
          "Ini yang paling akurat. Game yang sudah banned: pakai file lama / backup / autosave Studio.\n"
          "  b) Auto-probe: mencoba ~130 nama umum (Points, Cash, Kills, Wins, Level, ...) + semua nama "
          "Standard DataStore + scope yang terlihat di Standard. Yang berisi data otomatis ikut dimigrasi.\n"
          "  c) Pola nama dinamis: tulis mis. Top_{1-30} atau Season{01-12}.\n"
          "  d) Manual: nama atau nama:scope (pisah koma/baris), mis. Points, Donor:PROD26.\n\n"
          "Batas yang jujur: nama yang sepenuhnya acak/dinamis dan tidak ada di file game tidak mungkin "
          "ditemukan oleh siapa pun. Untuk itu tambahkan manual."),
    ("h", "4) Opsi penting"),
    ("p", "Dry Run: hanya baca + backup, tidak menulis.\n"
          "Overwrite: timpa key yang sudah ada di tujuan (mati = aman, key yang ada dilewati).\n"
          "Salin ulang semua: abaikan progress lama (dengan Overwrite untuk memperbarui data).\n"
          "Verifikasi: bandingkan nilai yang diterima tujuan dengan sumber.\n"
          "Worker & Kecepatan: paralel + batas request/detik; otomatis turun kalau Roblox membalas 429.\n"
          "Limit: 0 = semua; isi 5 untuk uji coba kecil."),
    ("h", "5) Privasi & file hasil"),
    ("p", "- API Key hanya dipakai di memori server selama proses berjalan; tidak disimpan dan tidak masuk log.\n"
          "- Backup data + progress (untuk resume) disimpan sementara di server, lalu terhapus otomatis "
          "beberapa jam kemudian. Klik 'Unduh hasil' untuk menyimpan, atau 'Hapus data saya' untuk menghapus sekarang.\n"
          "- Tetap buat API Key khusus migrasi dan hapus setelah selesai. Jangan bagikan API Key."),
    ("h", "6) Catatan"),
    ("p", "- Biarkan tab ini terbuka selama proses berjalan.\n"
          "- Scope Standard DataStore dipertahankan.\n"
          "- Data versi lama (history) tidak ikut, hanya nilai terbaru; users & attributes ikut.\n"
          "- 401/403 berulang -> migrasi berhenti otomatis dengan penjelasan.\n"
          "- Kalau berhenti di tengah jalan, jalankan lagi: yang sudah selesai dilewati (resume)."),
]

# ----------------------------------------------------------------- frontend statis (harus paling akhir)
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
