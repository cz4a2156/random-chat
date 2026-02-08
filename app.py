import os
import json
import uuid
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import geoip2.database
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

# =========================
# Config
# =========================
APP_TITLE = "ランダムテキストチャット（超シンプル）"
DB_PATH = os.getenv("DB_PATH", "app.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")  # RenderのEnvironmentで設定推奨
GEOIP_DB_PATH = os.getenv("GEOIP_DB_PATH", "GeoLite2-City.mmdb")  # mmdbを同階層に置く or Path指定

_geo_reader = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# Fake online (display only)
# 2分ごとに 3→4→5 を循環
# =========================
def fake_online_offset() -> int:
    cycle = (int(time.time()) // 120) % 3  # 0,1,2
    return 3 + cycle  # 3,4,5


# =========================
# DB
# =========================
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _ensure_column(cur: sqlite3.Cursor, table: str, col: str, col_type: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def init_db():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS ws_connections (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        event TEXT NOT NULL,           -- connect / disconnect
        client_id TEXT,
        session_id TEXT,
        ip TEXT,
        country TEXT,
        region TEXT,
        city TEXT,
        subdivision TEXT,
        user_agent TEXT
    )
    """
    )

    _ensure_column(cur, "ws_connections", "region", "TEXT")
    _ensure_column(cur, "ws_connections", "city", "TEXT")
    _ensure_column(cur, "ws_connections", "subdivision", "TEXT")

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS ws_sessions (
        session_id TEXT PRIMARY KEY,
        ts_start TEXT NOT NULL,
        ts_end TEXT,
        client_a TEXT NOT NULL,
        client_b TEXT NOT NULL,
        ip_a TEXT,
        ip_b TEXT,
        country_a TEXT,
        country_b TEXT,
        region_a TEXT,
        region_b TEXT,
        city_a TEXT,
        city_b TEXT,
        subdivision_a TEXT,
        subdivision_b TEXT
    )
    """
    )

    _ensure_column(cur, "ws_sessions", "region_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "region_b", "TEXT")
    _ensure_column(cur, "ws_sessions", "city_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "city_b", "TEXT")
    _ensure_column(cur, "ws_sessions", "subdivision_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "subdivision_b", "TEXT")

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS ws_messages (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        session_id TEXT NOT NULL,
        sender_client_id TEXT NOT NULL,
        text TEXT NOT NULL
    )
    """
    )

    conn.commit()
    conn.close()


def db_insert_connection(
    event: str,
    client_id: str,
    session_id: Optional[str],
    ip: str,
    country: str,
    region: str,
    city: str,
    subdivision: str,
    ua: str,
):
    conn = db_conn()
    conn.execute(
        """INSERT INTO ws_connections
        (id, ts, event, client_id, session_id, ip, country, region, city, subdivision, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(uuid.uuid4()),
            utc_now_iso(),
            event,
            client_id,
            session_id,
            ip,
            country,
            region,
            city,
            subdivision,
            (ua or "unknown")[:500],
        ),
    )
    conn.commit()
    conn.close()


def db_start_session(
    session_id: str,
    a: str,
    b: str,
    ip_a: str,
    ip_b: str,
    country_a: str,
    country_b: str,
    region_a: str,
    region_b: str,
    city_a: str,
    city_b: str,
    subdivision_a: str,
    subdivision_b: str,
):
    conn = db_conn()
    conn.execute(
        """INSERT INTO ws_sessions
        (session_id, ts_start, client_a, client_b, ip_a, ip_b, country_a, country_b, region_a, region_b, city_a, city_b, subdivision_a, subdivision_b)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            utc_now_iso(),
            a,
            b,
            ip_a,
            ip_b,
            country_a,
            country_b,
            region_a,
            region_b,
            city_a,
            city_b,
            subdivision_a,
            subdivision_b,
        ),
    )
    conn.commit()
    conn.close()


def db_end_session(session_id: str):
    conn = db_conn()
    conn.execute(
        "UPDATE ws_sessions SET ts_end=? WHERE session_id=? AND ts_end IS NULL",
        (utc_now_iso(), session_id),
    )
    conn.commit()
    conn.close()


def db_insert_message(session_id: str, sender: str, text: str):
    conn = db_conn()
    conn.execute(
        "INSERT INTO ws_messages (id, ts, session_id, sender_client_id, text) VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), utc_now_iso(), session_id, sender, (text or "")[:2000]),
    )
    conn.commit()
    conn.close()


# =========================
# IP / Geo
# =========================
def get_client_ip(ws: WebSocket) -> str:
    # Render等のプロキシ越しで x-forwarded-for が来る
    xff = ws.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if ws.client:
        return ws.client.host
    return "unknown"


def _get_geo_reader():
    global _geo_reader
    if _geo_reader is None:
        _geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)
    return _geo_reader


def get_geo(ip: str) -> Tuple[str, str, str, str]:
    """
    return: (country_code, region_name, city_name, subdivision_name)
    region: 都道府県/州っぽい
    subdivision: 区っぽい（DB次第）
    """
    if ip in ("unknown", "127.0.0.1"):
        return ("unknown", "unknown", "unknown", "unknown")
    try:
        reader = _get_geo_reader()
        r = reader.city(ip)

        country = (r.country.iso_code or "unknown").upper()

        region = "unknown"
        subdivision = "unknown"
        if r.subdivisions:
            most = r.subdivisions.most_specific
            if most and most.name:
                region = most.name
            if len(r.subdivisions) >= 2 and r.subdivisions[1].name:
                subdivision = r.subdivisions[1].name

        city = r.city.name or "unknown"
        return (country, region, city, subdivision)
    except Exception:
        return ("unknown", "unknown", "unknown", "unknown")


def require_admin(request: Request):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not set")
    token = request.headers.get("x-admin-token") or request.query_params.get("token")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


# =========================
# Matchmaking
# =========================
@dataclass
class ClientInfo:
    ws: WebSocket
    client_id: str
    ip: str
    country: str
    region: str
    city: str
    subdivision: str
    ua: str
    partner_id: Optional[str] = None
    session_id: Optional[str] = None


class Matchmaker:
    def __init__(self):
        self.clients: Dict[str, ClientInfo] = {}
        self.waiting: Optional[str] = None  # 待機中の1人だけ

    def online_count(self) -> int:
        return len(self.clients)

    def waiting_count(self) -> int:
        return 1 if (self.waiting and self.waiting in self.clients and not self.clients[self.waiting].partner_id) else 0

    def add_client(self, info: ClientInfo):
        self.clients[info.client_id] = info

    def remove_client(self, client_id: str):
        if self.waiting == client_id:
            self.waiting = None

        info = self.clients.get(client_id)

        # 相手がいるなら相手側を解放
        if info and info.partner_id:
            partner = self.clients.get(info.partner_id)
            if partner:
                partner.partner_id = None
                if partner.session_id:
                    db_end_session(partner.session_id)
                partner.session_id = None

        if info and info.session_id:
            db_end_session(info.session_id)

        self.clients.pop(client_id, None)

    def force_end_my_session(self, client_id: str) -> Optional[str]:
        """client_id のセッションを強制終了して、相手に ended を送るため相手IDを返す"""
        me = self.clients.get(client_id)
        if not me:
            return None
        partner_id = me.partner_id
        sid = me.session_id

        me.partner_id = None
        me.session_id = None
        if sid:
            db_end_session(sid)

        if partner_id and partner_id in self.clients:
            partner = self.clients[partner_id]
            partner.partner_id = None
            partner.session_id = None
        return partner_id

    def match(self, client_id: str) -> Tuple[bool, Optional[str]]:
        me = self.clients.get(client_id)
        if not me or me.partner_id:
            return False, None

        # waiting が無効なら自分が waiting に入る
        if (
            not self.waiting
            or self.waiting not in self.clients
            or self.clients[self.waiting].partner_id
        ):
            self.waiting = client_id
            return False, None

        other_id = self.waiting
        if other_id == client_id:
            return False, None

        other = self.clients.get(other_id)
        if not other or other.partner_id:
            self.waiting = client_id
            return False, None

        # マッチ成立
        session_id = str(uuid.uuid4())
        me.partner_id = other_id
        other.partner_id = client_id
        me.session_id = session_id
        other.session_id = session_id
        self.waiting = None

        db_start_session(
            session_id,
            me.client_id,
            other.client_id,
            me.ip,
            other.ip,
            me.country,
            other.country,
            me.region,
            other.region,
            me.city,
            other.city,
            me.subdivision,
            other.subdivision,
        )
        return True, other_id


mm = Matchmaker()

# =========================
# FastAPI
# =========================
app = FastAPI(title="random-chat-logs")
init_db()

INDEX_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{APP_TITLE}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 920px; margin: 24px auto; padding: 0 12px; }}
    .row {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
    button {{ padding: 10px 14px; }}
    #log {{ border:1px solid #ddd; padding:10px; height:420px; overflow:auto; margin-top:12px; border-radius:10px; background:#fff; }}
    .badge {{ padding:6px 10px; border-radius:999px; background:#f2f2f2; }}
    input {{ padding:10px; border-radius:10px; border:1px solid #ddd; }}
  </style>
</head>
<body>
  <h2>{APP_TITLE}</h2>

  <div class="row">
    <button id="btnStart">開始</button>
    <button id="btnNext" disabled>次の人</button>
    <button id="btnDisconnect" disabled>切断</button>
    <span class="badge">オンライン表示: <b id="onlineDisplay">-</b></span>
    <span class="badge">待機中: <b id="waitingNow">-</b></span>
  </div>

  <div id="log"></div>

  <div class="row" style="margin-top:12px;">
    <input id="msg" placeholder="メッセージ..." style="width:min(520px, 72vw);">
    <button id="btnSend" disabled>送信</button>
  </div>

<script>
let ws = null;
let matched = false;

let clientId = localStorage.getItem("client_id");
if(!clientId) {{
  clientId = crypto.randomUUID();
  localStorage.setItem("client_id", clientId);
}}

const elLog = document.getElementById("log");
const log = (s) => {{
  elLog.innerHTML += s + "<br>";
  elLog.scrollTop = elLog.scrollHeight;
}};

function setUIConnected(isConnected) {{
  document.getElementById("btnDisconnect").disabled = !isConnected;
  if(!isConnected) {{
    document.getElementById("btnSend").disabled = true;
    document.getElementById("btnNext").disabled = true;
    matched = false;
  }}
}}

function setUIMatched(isMatched) {{
  matched = isMatched;
  document.getElementById("btnSend").disabled = !isMatched;
  document.getElementById("btnNext").disabled = !isMatched;
}}

function beepSequence(steps) {{
  // WebAudio: steps = [{freq, dur, gap, type}]
  try {{
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    let t = ctx.currentTime;
    steps.forEach(st => {{
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.type = st.type || "sine";
      o.frequency.value = st.freq;

      g.gain.setValueAtTime(0.0001, t);
      g.gain.exponentialRampToValueAtTime(0.15, t + 0.01);
      g.gain.exponentialRampToValueAtTime(0.0001, t + st.dur);

      o.connect(g);
      g.connect(ctx.destination);

      o.start(t);
      o.stop(t + st.dur);

      t += st.dur + (st.gap || 0.02);
    }});
  }} catch(e) {{
    // 音が出せない環境でも無視
  }}
}}

function playMatchSound() {{
  // テンション上がる：上昇音
  beepSequence([
    {{freq: 660, dur: 0.08, gap: 0.02, type:"triangle"}},
    {{freq: 880, dur: 0.08, gap: 0.02, type:"triangle"}},
    {{freq: 1100, dur: 0.10, gap: 0.00, type:"triangle"}},
  ]);
}}

function playEndSound() {{
  // テンション下がる：下降音
  beepSequence([
    {{freq: 440, dur: 0.10, gap: 0.02, type:"sine"}},
    {{freq: 330, dur: 0.12, gap: 0.00, type:"sine"}},
  ]);
}}

async function refreshOnline() {{
  try {{
    const r = await fetch("/api/online");
    const j = await r.json();
    document.getElementById("onlineDisplay").textContent = j.online_display;
    document.getElementById("waitingNow").textContent = j.waiting_now;
  }} catch(e) {{}}
}}
setInterval(refreshOnline, 2000);
refreshOnline();

function connect() {{
  ws = new WebSocket((location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/ws?client_id="+encodeURIComponent(clientId));

  ws.onopen = ()=> {{
    setUIConnected(true);
    log("✅ 接続しました。マッチング中...");
    ws.send(JSON.stringify({{type:"start"}}));
  }};

  ws.onmessage = (ev)=> {{
    const data = JSON.parse(ev.data);

    if(data.type==="matched") {{
      log("🎉 マッチしました！");
      setUIMatched(true);
      playMatchSound();
    }} else if(data.type==="system") {{
      log("（システム）"+data.text);
    }} else if(data.type==="msg") {{
      log("相手: "+data.text);
    }} else if(data.type==="ended") {{
      log("🚪 相手が退出しました。終了します。");
      setUIMatched(false);
      playEndSound();
    }} else if(data.type==="disconnect_ack") {{
      log("🧹 切断しました。");
      setUIMatched(false);
      playEndSound();
      try {{ ws.close(); }} catch(e) {{}}
    }}
  }};

  ws.onclose = ()=> {{
    log("🗡 切断しました。");
    setUIConnected(false);
  }};
}}

document.getElementById("btnStart").onclick = async ()=> {{
  // iOS等で音を鳴らすにはユーザー操作の直後が安全なので、ここでAudioContextを起こす意図もある
  if(ws && ws.readyState===1) return;
  connect();
}};

document.getElementById("btnNext").onclick = ()=> {{
  if(ws && ws.readyState===1) {{
    ws.send(JSON.stringify({{type:"next"}}));
    setUIMatched(false);
    log("（システム）待機中...相手を探しています");
  }}
}};

document.getElementById("btnDisconnect").onclick = ()=> {{
  if(ws && ws.readyState===1) {{
    ws.send(JSON.stringify({{type:"disconnect"}}));
  }}
}};

document.getElementById("btnSend").onclick = ()=> {{
  const t = document.getElementById("msg").value;
  if(!t) return;
  document.getElementById("msg").value="";
  log("あなた: "+t);
  ws.send(JSON.stringify({{type:"msg", text:t}}));
}};

document.getElementById("msg").addEventListener("keydown", (e)=> {{
  if(e.key === "Enter") {{
    document.getElementById("btnSend").click();
  }}
}});
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


# =========================
# Public tiny API (UI polling)
# =========================
@app.get("/api/online")
def api_online():
    real = mm.online_count()
    fake = fake_online_offset()
    return JSONResponse(
        {
            "online_real": real,
            "online_display": real + fake,
            "waiting_now": mm.waiting_count(),
        }
    )


# =========================
# Admin APIs
# =========================
@app.get("/admin/stats")
def admin_stats(request: Request):
    require_admin(request)

    real = mm.online_count()
    fake = fake_online_offset()

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ws_connections WHERE event='connect'")
    total_connects = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ws_connections WHERE event='disconnect'")
    total_disconnects = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ws_messages")
    total_msgs = cur.fetchone()[0]
    conn.close()

    return JSONResponse(
        {
            "online_real": real,
            "online_display": real + fake,
            "waiting_now": mm.waiting_count(),
            "total_connects": total_connects,
            "total_disconnects": total_disconnects,
            "total_messages": total_msgs,
        }
    )


@app.get("/admin/recent")
def admin_recent(request: Request, limit: int = 50):
    require_admin(request)
    limit = max(1, min(limit, 200))

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts, event, client_id, session_id, ip, country, region, city, subdivision
        FROM ws_connections
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    return JSONResponse(
        {
            "connections": [
                {
                    "ts": r[0],
                    "event": r[1],
                    "client_id": r[2],
                    "session_id": r[3],
                    "ip": r[4],
                    "country": r[5],
                    "region": r[6],
                    "city": r[7],
                    "subdivision": r[8],
                }
                for r in rows
            ]
        }
    )


@app.get("/admin/messages")
def admin_messages(request: Request, limit: int = 100):
    require_admin(request)
    limit = max(1, min(limit, 500))

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts, session_id, sender_client_id, text
        FROM ws_messages
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    return JSONResponse(
        {
            "messages": [
                {
                    "ts": r[0],
                    "session_id": r[1],
                    "sender": r[2],
                    "text": r[3],
                }
                for r in rows
            ]
        }
    )


@app.get("/admin/geo_summary")
def admin_geo_summary(request: Request, region: Optional[str] = None, limit: int = 50):
    """
    地域（region/city/subdivision）別の connect 集計
    region を指定すると、その地域だけに絞る
    """
    require_admin(request)
    limit = max(1, min(limit, 200))

    conn = db_conn()
    cur = conn.cursor()

    if region:
        cur.execute(
            """
            SELECT region, city, subdivision, COUNT(*) as c
            FROM ws_connections
            WHERE event='connect' AND region = ?
            GROUP BY region, city, subdivision
            ORDER BY c DESC
            LIMIT ?
            """,
            (region, limit),
        )
    else:
        cur.execute(
            """
            SELECT region, city, subdivision, COUNT(*) as c
            FROM ws_connections
            WHERE event='connect'
            GROUP BY region, city, subdivision
            ORDER BY c DESC
            LIMIT ?
            """,
            (limit,),
        )

    rows = cur.fetchall()
    conn.close()

    return JSONResponse(
        {
            "rows": [
                {
                    "region": r[0] or "unknown",
                    "city": r[1] or "unknown",
                    "subdivision": r[2] or "unknown",
                    "connects": r[3],
                }
                for r in rows
            ]
        }
    )


# （大阪固定が気になるなら、これは削除してOK。残したい場合だけ使ってね）
@app.get("/admin/osaka_top")
def admin_osaka_top(request: Request, limit: int = 50):
    """
    大阪府内だけの city/subdivision 上位（オマケ）
    """
    require_admin(request)
    limit = max(1, min(limit, 200))

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT city, subdivision, COUNT(*) as c
        FROM ws_connections
        WHERE event='connect' AND region = 'Osaka'
        GROUP BY city, subdivision
        ORDER BY c DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    return JSONResponse(
        {
            "rows": [
                {"city": r[0] or "unknown", "subdivision": r[1] or "unknown", "connects": r[2]}
                for r in rows
            ]
        }
    )


# =========================
# WebSocket
# =========================
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, client_id: str):
    await ws.accept()

    ip = get_client_ip(ws)
    country, region, city, subdivision = get_geo(ip)
    ua = ws.headers.get("user-agent", "unknown")

    info = ClientInfo(
        ws=ws,
        client_id=client_id,
        ip=ip,
        country=country,
        region=region,
        city=city,
        subdivision=subdivision,
        ua=ua,
    )
    mm.add_client(info)

    db_insert_connection("connect", client_id, None, ip, country, region, city, subdivision, ua)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            typ = data.get("type")
            me = mm.clients.get(client_id)

            if typ == "start":
                await ws.send_text(json.dumps({"type": "system", "text": "待機中...相手を探しています"}))
                matched, other_id = mm.match(client_id)
                if matched and other_id:
                    other = mm.clients.get(other_id)
                    if other and me:
                        await me.ws.send_text(json.dumps({"type": "matched"}))
                        await other.ws.send_text(json.dumps({"type": "matched"}))

            elif typ == "next":
                # 今の相手を切って次へ
                if me and me.partner_id:
                    partner = mm.clients.get(me.partner_id)
                    sid = me.session_id
                    me.partner_id = None
                    me.session_id = None
                    if sid:
                        db_end_session(sid)
                    if partner:
                        partner.partner_id = None
                        partner.session_id = None
                        await partner.ws.send_text(json.dumps({"type": "ended"}))

                await ws.send_text(json.dumps({"type": "system", "text": "待機中...相手を探しています"}))
                matched, other_id = mm.match(client_id)
                if matched and other_id:
                    other = mm.clients.get(other_id)
                    if other and me:
                        await me.ws.send_text(json.dumps({"type": "matched"}))
                        await other.ws.send_text(json.dumps({"type": "matched"}))

            elif typ == "disconnect":
                # 自分から切断
                partner_id = mm.force_end_my_session(client_id)
                if partner_id and partner_id in mm.clients:
                    try:
                        await mm.clients[partner_id].ws.send_text(json.dumps({"type": "ended"}))
                    except Exception:
                        pass
                await ws.send_text(json.dumps({"type": "disconnect_ack"}))
                # ここで break して finally へ（disconnectログ書く）
                break

            elif typ == "msg":
                text = (data.get("text") or "").strip()
                if not text:
                    continue

                if me and me.session_id:
                    db_insert_message(me.session_id, client_id, text)

                if me and me.partner_id and me.partner_id in mm.clients:
                    partner = mm.clients[me.partner_id]
                    await partner.ws.send_text(json.dumps({"type": "msg", "text": text}))
                else:
                    await ws.send_text(json.dumps({"type": "system", "text": "まだマッチしていません"}))

            else:
                await ws.send_text(json.dumps({"type": "system", "text": "unknown command"}))

    except WebSocketDisconnect:
        pass
    finally:
        session_id = None
        info2 = mm.clients.get(client_id)
        if info2:
            session_id = info2.session_id

        db_insert_connection("disconnect", client_id, session_id, ip, country, region, city, subdivision, ua)
        mm.remove_client(client_id)
