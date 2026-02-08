import os
import json
import uuid
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import geoip2.database
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

APP_TITLE = "ランダムテキストチャット（超シンプル）"
DB_PATH = os.getenv("DB_PATH", "app.db")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")  # RenderのEnvironmentで設定推奨

# GeoIP DB
GEOIP_DB_PATH = os.getenv("GEOIP_DB_PATH", "GeoLite2-City.mmdb")
_geo_reader = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

import time

def fake_online_offset() -> int:
    """
    2分ごとに 3 → 4 → 5 を循環
    """
    cycle = (int(time.time()) // 120) % 3  # 120秒ごと
    return 3 + cycle


# ---------------------------
# DB helpers
# ---------------------------
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ws_connections (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        event TEXT NOT NULL,                 -- connect / disconnect
        client_id TEXT,
        session_id TEXT,
        ip TEXT,
        country TEXT,
        region TEXT,
        city TEXT,
        subdivision TEXT,
        user_agent TEXT
    )
    """)

    # 後から列追加（既存DBを壊さない）
    _ensure_column(cur, "ws_connections", "region", "TEXT")
    _ensure_column(cur, "ws_connections", "city", "TEXT")
    _ensure_column(cur, "ws_connections", "subdivision", "TEXT")

    cur.execute("""
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
    """)

    _ensure_column(cur, "ws_sessions", "region_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "region_b", "TEXT")
    _ensure_column(cur, "ws_sessions", "city_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "city_b", "TEXT")
    _ensure_column(cur, "ws_sessions", "subdivision_a", "TEXT")
    _ensure_column(cur, "ws_sessions", "subdivision_b", "TEXT")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ws_messages (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        session_id TEXT NOT NULL,
        sender_client_id TEXT NOT NULL,
        text TEXT NOT NULL
    )
    """)

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
    ua: str
):
    conn = db_conn()
    conn.execute(
        """INSERT INTO ws_connections
        (id, ts, event, client_id, session_id, ip, country, region, city, subdivision, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (str(uuid.uuid4()), utc_now_iso(), event, client_id, session_id, ip, country, region, city, subdivision, ua[:500]),
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
        (session_id, ts_start, client_a, client_b, ip_a, ip_b, country_a, country_b,
         region_a, region_b, city_a, city_b, subdivision_a, subdivision_b)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, utc_now_iso(), a, b, ip_a, ip_b, country_a, country_b,
         region_a, region_b, city_a, city_b, subdivision_a, subdivision_b),
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
        (str(uuid.uuid4()), utc_now_iso(), session_id, sender, text[:2000]),
    )
    conn.commit()
    conn.close()


# ---------------------------
# IP / Geo
# ---------------------------
def get_client_ip(ws: WebSocket) -> str:
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
    region: 都道府県/州レベル
    subdivision: “区”っぽい情報が出ることがある（DB次第）
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
            # subdivisionsが複数あると “区” っぽいのが入ることがある（国・DBによる）
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


# ---------------------------
# Matchmaking + counts
# ---------------------------
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
        self.waiting: Optional[str] = None

    def online_count(self) -> int:
        return len(self.clients)

    def idle_count(self) -> int:
        # 相手がいない（待機中 or 未マッチ）の人数
        return sum(1 for c in self.clients.values() if c.partner_id is None)

    def add_client(self, info: ClientInfo):
        self.clients[info.client_id] = info

    def remove_client(self, client_id: str):
        if self.waiting == client_id:
            self.waiting = None

        info = self.clients.get(client_id)

        # 相手がいたら相手も解除＆セッション終了
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

        # waitingが死んでたら解除
        if self.waiting and self.waiting not in self.clients:
            self.waiting = None

    def match(self, client_id: str) -> Tuple[bool, Optional[str]]:
        me = self.clients.get(client_id)
        if not me or me.partner_id:
            return False, None

        # waitingが空 or 無効なら自分をwaitingに
        if not self.waiting or self.waiting not in self.clients or self.clients[self.waiting].partner_id:
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
            me.client_id, other.client_id,
            me.ip, other.ip,
            me.country, other.country,
            me.region, other.region,
            me.city, other.city,
            me.subdivision, other.subdivision,
        )
        return True, other_id


mm = Matchmaker()


async def broadcast_counts():
    """
    全クライアントに接続数・待機数を配信
    """
    payload = json.dumps({
        "type": "counts",
        "online": mm.online_count(),
        "idle": mm.idle_count(),
    })

    dead: List[str] = []
    for cid, info in mm.clients.items():
        try:
            await info.ws.send_text(payload)
        except Exception:
            dead.append(cid)

    for cid in dead:
        mm.remove_client(cid)


# ---------------------------
# FastAPI
# ---------------------------
app = FastAPI(title="random-chat-logs")
init_db()

INDEX_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{APP_TITLE}</title>
</head>
<body style="font-family: sans-serif; max-width: 900px; margin: 30px auto;">
  <h2>{APP_TITLE}</h2>

  <div style="display:flex; align-items:center; gap:8px;">
    <button id="btnStart">開始</button>
    <button id="btnNext" disabled>次の人</button>
    <button id="btnDisconnect" disabled>切断</button>
    <span id="status" style="margin-left:12px;color:#666;"></span>
  </div>

  <div style="border:1px solid #ddd; padding:10px; height:420px; overflow:auto; margin-top:12px;" id="log"></div>

  <div style="margin-top:12px; display:flex; gap:8px;">
    <input id="msg" placeholder="メッセージ..." style="flex:1;">
    <button id="btnSend" disabled>送信</button>
  </div>

<script>
let ws=null;

// 同一ブラウザ内では同一ユーザー扱い（自分とマッチしない）
let clientId = localStorage.getItem("client_id");
if(!clientId) {{
  clientId = crypto.randomUUID();
  localStorage.setItem("client_id", clientId);
}}

const log = (s) => {{
  const el=document.getElementById("log");
  el.innerHTML += s + "<br>";
  el.scrollTop = el.scrollHeight;
}};

// ---- SFX (WebAudio) ----
let audioCtx = null;
function beep(freq, durationMs, type="sine", gainValue=0.06) {{
  try {{
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.type = type;
    osc.frequency.value = freq;
    gain.gain.value = gainValue;
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.start();
    setTimeout(() => osc.stop(), durationMs);
  }} catch(e) {{}}
}}

function sfxMatched() {{
  beep(880, 120, "square", 0.05);
  setTimeout(()=>beep(1175, 160, "square", 0.05), 140);
}}

function sfxEnded() {{
  beep(440, 160, "sine", 0.05);
  setTimeout(()=>beep(330, 220, "sine", 0.05), 180);
}}

function setStatus(text) {{
  document.getElementById("status").textContent = text || "";
}}

function connect() {{
  ws = new WebSocket((location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/ws?client_id="+encodeURIComponent(clientId));

  ws.onopen = ()=> {{
    log("✅ 接続しました。マッチング中...");
    document.getElementById("btnDisconnect").disabled=false;
    ws.send(JSON.stringify({{type:"start"}}));
  }};

  ws.onmessage = (ev)=> {{
    const data = JSON.parse(ev.data);

    if(data.type==="matched") {{
      log("🎉 マッチしました！");
      sfxMatched();
      document.getElementById("btnSend").disabled=false;
      document.getElementById("btnNext").disabled=false;
      document.getElementById("btnDisconnect").disabled=false;

    }} else if(data.type==="system") {{
      log("（システム）"+data.text);

    }} else if(data.type==="msg") {{
      log("相手: "+data.text);

    }} else if(data.type==="ended") {{
      log("🚪 相手が退出しました。終了します。");
      sfxEnded();
      document.getElementById("btnSend").disabled=true;
      document.getElementById("btnNext").disabled=true;

    }} else if(data.type==="counts") {{
      setStatus(`接続中: ${{data.online}} / 待機: ${{data.idle}}`);
    }}
  }};

  ws.onclose = ()=> {{
    log("🗡 切断しました。");
    sfxEnded();
    document.getElementById("btnSend").disabled=true;
    document.getElementById("btnNext").disabled=true;
    document.getElementById("btnDisconnect").disabled=true;
  }};
}}

document.getElementById("btnStart").onclick=()=> {{
  if(ws && ws.readyState===1) return;
  connect();
}};

document.getElementById("btnNext").onclick=()=> {{
  if(ws && ws.readyState===1) {{
    ws.send(JSON.stringify({{type:"next"}}));
    document.getElementById("btnSend").disabled=true;
    document.getElementById("btnNext").disabled=true;
    log("（システム）待機中...相手を探しています");
  }}
}};

document.getElementById("btnDisconnect").onclick=()=> {{
  if(ws && ws.readyState===1) {{
    try {{ ws.send(JSON.stringify({{type:"disconnect"}})); }} catch(e) {{}}
    ws.close();
  }}
}};

document.getElementById("btnSend").onclick=()=> {{
  const t=document.getElementById("msg").value;
  if(!t) return;
  document.getElementById("msg").value="";
  log("あなた: "+t);
  ws.send(JSON.stringify({{type:"msg", text:t}}));
}};
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


# ---------------------------
# Admin APIs
# ---------------------------
@app.get("/admin/stats")
def admin_stats(request: Request):
    require_admin(request)

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ws_connections WHERE event='connect'")
    total_connects = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ws_connections WHERE event='disconnect'")
    total_disconnects = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ws_messages")
    total_msgs = cur.fetchone()[0]
    conn.close()

    return JSONResponse({
        "online_real": real,
        "online_display": real + fake,
        "idle_now": mm.idle_count(),
        "total_connects": total_connects,
        "total_disconnects": total_disconnects,
        "total_messages": total_msgs,
    })


@app.get("/admin/recent")
def admin_recent(request: Request, limit: int = 50):
    require_admin(request)
    limit = max(1, min(limit, 200))

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT ts, event, client_id, session_id, ip, country, region, city, subdivision "
        "FROM ws_connections ORDER BY ts DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    return JSONResponse({
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
    })


@app.get("/admin/messages")
def admin_messages(request: Request, limit: int = 100):
    """
    直近メッセージ（会話ログ）を見る
    """
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

    return JSONResponse({
        "messages": [
            {"ts": r[0], "session_id": r[1], "sender": r[2], "text": r[3]}
            for r in rows
        ]
    })


@app.get("/admin/geo_summary")
def admin_geo_summary(request: Request, region: Optional[str] = None, limit: int = 50):
    """
    地域別（region/city/subdivision）集計
    region=（実際に出てくる表記）で絞れる
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

    return JSONResponse({
        "rows": [
            {"region": r[0] or "unknown", "city": r[1] or "unknown", "subdivision": r[2] or "unknown", "connects": r[3]}
            for r in rows
        ]
    })


# ---------------------------
# WebSocket endpoint
# ---------------------------
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

    # connectログ
    db_insert_connection("connect", client_id, None, ip, country, region, city, subdivision, ua)

    # counts配信
    await broadcast_counts()

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
                await broadcast_counts()

            elif typ == "next":
                # 現在の相手を切って再待機
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

                matched, other_id = mm.match(client_id)
                if matched and other_id:
                    other = mm.clients.get(other_id)
                    if other and me:
                        await me.ws.send_text(json.dumps({"type": "matched"}))
                        await other.ws.send_text(json.dumps({"type": "matched"}))

                await broadcast_counts()

            elif typ == "disconnect":
                # 自分から切断
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

                await broadcast_counts()
                await ws.close()
                break

            elif typ == "msg":
                text = (data.get("text") or "").strip()
                if not text:
                    continue

                # セッションがある場合だけログ（マッチ中のみ）
                if me and me.session_id:
                    db_insert_message(me.session_id, client_id, text)

                # 相手に転送
                if me and me.partner_id and me.partner_id in mm.clients:
                    partner = mm.clients[me.partner_id]
                    await partner.ws.send_text(json.dumps({"type": "msg", "text": text}))

            else:
                await ws.send_text(json.dumps({"type": "system", "text": "unknown command"}))

    except WebSocketDisconnect:
        pass
    finally:
        session_id = None
        current = mm.clients.get(client_id)
        if current:
            session_id = current.session_id

        # disconnectログ
        db_insert_connection("disconnect", client_id, session_id, ip, country, region, city, subdivision, ua)

        # 片付け
        mm.remove_client(client_id)

        # counts配信（残った人へ）
        await broadcast_counts()
