# api_call.py (필수 최소 동작 버전)
import os, re, time
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import requests
import psycopg2
from psycopg2 import sql, extras as pgx

KST = ZoneInfo("Asia/Seoul")
MARKET_START = dtime(8, 0, 0)
MARKET_END   = dtime(15, 30, 0)

# ---------- 상태(health) ----------
_STATE = {"running": True, "last_ts": None, "count": 0, "last_error": None}
def get_state(): return _STATE

# ---------- 유틸 ----------
def is_trading(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(KST)
    return (now.weekday() < 5) and (MARKET_START <= now.time() <= MARKET_END)

def _norm_ident(name: str, prefix="col") -> str:
    s = re.sub(r'[^0-9a-zA-Z_]', '_', name.strip())
    if not s or s[0].isdigit(): s = f"{prefix}_{s or 'x'}"
    return s.lower()

def _pg_type_of(v: Any) -> str:
    if v is None: return "text"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, int): return "bigint"
    if isinstance(v, float): return "double precision"
    if isinstance(v, (dict, list)): return "jsonb"
    if isinstance(v, str):
        if re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', v): return "timestamptz"
        if re.match(r'^\d{4}-\d{2}-\d{2}$', v): return "date"
        return "text"
    return "text"

def _adapt_value_for_pg(v: Any):
    return pgx.Json(v) if isinstance(v, (dict, list)) else v

def get_by_path(d: Any, path: str) -> Any:
    if not isinstance(d, (dict, list)): return None
    if not path or path == "$": return d
    if path.startswith("$."): path = path[2:]
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try: cur = cur[int(part)]
            except Exception: return None
        else:
            return None
    return cur

# ---------- DB ----------
class DB:
    def __init__(self):
        self.conn = None
    def _conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=os.environ["PGHOST"],
                port=int(os.environ.get("PGPORT","5432")),
                user=os.environ.get("PGUSER","root"),
                password=os.environ["PGPASSWORD"],
                dbname=os.environ.get("PGDATABASE","postgres"),
                connect_timeout=5
            )
            self.conn.autocommit = True
        return self.conn

    def _ensure_table(self, table: str):
        tbl = _norm_ident(table, "t")
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=%s",(tbl,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE TABLE {} (ts timestamptz DEFAULT NOW())").format(sql.Identifier(tbl)))

    def _existing_cols(self, table: str) -> set:
        tbl = _norm_ident(table, "t")
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",(tbl,))
            return {r[0] for r in cur.fetchall()}

    def _add_column(self, table: str, col: str, typ: str):
        tbl = _norm_ident(table, "t"); coln = _norm_ident(col, "c")
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}")
                        .format(sql.Identifier(tbl), sql.Identifier(coln), sql.SQL(typ)))

    def ensure_columns_for_row(self, table: str, row: Dict[str, Any]):
        self._ensure_table(table)
        existing = self._existing_cols(table)
        for k, v in row.items():
            col = _norm_ident(k, "c")
            if col not in existing:
                self._add_column(table, col, _pg_type_of(v))
        if "ts" not in existing and "ts" not in [_norm_ident(k) for k in row.keys()]:
            self._add_column(table, "ts", "timestamptz")

    def insert_auto(self, table: str, row: Dict[str, Any]):
        tbl = _norm_ident(table, "t")
        if "ts" not in row: row["ts"] = datetime.now(KST)
        self.ensure_columns_for_row(tbl, row)
        cols = []; vals = []
        for k, v in row.items():
            cols.append(_norm_ident(k, "c"))
            vals.append(_adapt_value_for_pg(v))
        ph = ", ".join(["%s"]*len(vals))
        with self._conn() as c, c.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO {} ({}) VALUES (" + ph + ")")
                .format(sql.Identifier(tbl), sql.SQL(", ").join(map(sql.Identifier, cols))),
                vals
            )

# ---------- Token ----------
class Token:
    def __init__(self, conf: Dict[str, Any]):
        auth = conf["auth"]
        self.host = auth["host"]; self.path = auth["token_path"]
        self.token_field = auth.get("token_field","access_token")
        self.expires_in_field = auth.get("expires_in_field","expires_in")
        self.appkey = os.environ[auth["appkey_env"]]
        self.secret = os.environ[auth["secret_env"]]
        self.value: Optional[str] = None
        self.expire_at: datetime = datetime.now(KST)

    def ensure(self) -> str:
        if self.value and datetime.now(KST) < self.expire_at - timedelta(seconds=60):
            return self.value
        r = requests.post(self.host + self.path,
                          headers={"Content-Type":"application/json;charset=UTF-8"},
                          json={"grant_type":"client_credentials","appkey":self.appkey,"secretkey":self.secret},
                          timeout=8)
        r.raise_for_status()
        data = r.json()
        self.value = data.get(self.token_field) or data.get("token")
        ttl = int(data.get(self.expires_in_field, 1800))
        self.expire_at = datetime.now(KST) + timedelta(seconds=ttl)
        return self.value

# ---------- 한 Job 실행 ----------
def run_job(conf: Dict[str, Any], job: Dict[str, Any], token: Token, db: DB):
    url = conf["auth"]["host"] + job["path"]
    headers = {"Content-Type":"application/json;charset=UTF-8","authorization":"Bearer "+token.ensure()}
    headers.update(job.get("headers",{}))
    payload = job.get("body",{})

    r = requests.request(job["method"], url, headers=headers, json=payload, timeout=8)
    r.raise_for_status()
    data = r.json()
    items: List[Any] = data if isinstance(data, list) else [data]

    table = job["parse"].get("table", job["id"])
    mapping = job["parse"]["mapping"]

    for it in items:
        row = {k: get_by_path(it, path) for k, path in mapping.items()}
        db.insert_auto(table, row)

# ---------- 루프(백그라운드) ----------
def loop(conf: Dict[str, Any]):
    db = DB(); tok = Token(conf)
    last_at = {j["id"]: datetime.fromtimestamp(0, tz=KST) for j in conf["jobs"]}
    while True:
        try:
            if is_trading():
                now = datetime.now(KST)
                for j in conf["jobs"]:
                    gap = int(j.get("interval_sec", 1))
                    if (now - last_at[j["id"]]).total_seconds() >= gap:
                        run_job(conf, j, tok, db)
                        last_at[j["id"]] = now
                        _STATE["count"] += 1
                        _STATE["last_ts"] = now.isoformat()
                time.sleep(0.2)
            else:
                time.sleep(30)
        except Exception as e:
            _STATE["last_error"] = str(e)
            print("loop error:", e)
            time.sleep(2)
