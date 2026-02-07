import asyncio
import secrets
from typing import Dict, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# -----------------------------
# Matching / Room state
# -----------------------------
waiting: Optional[WebSocket] = None
waiting_lock = asyncio.Lock()

# room_id -> set[WebSocket]
rooms: Dict[str, Set[WebSocket]] = {}
rooms_lock = asyncio.Lock()

# ws -> room_id (高速に所属部屋を引ける)
ws_room: Dict[WebSocket, str] = {}
ws_room_lock = asyncio.Lock()


def new_room_id() -> str:
    return secrets.token_urlsafe(8)


async def safe_send(ws: WebSocket, text: str) -> bool:
    try:
        await ws.send_text(text)
        return True
    except Exception:
        return False


async def get_room_id(ws: WebSocket) -> Optional[str]:
    async with ws_room_lock:
        return ws_room.get(ws)


async def set_room_id(ws: WebSocket, room_id: str):
    async with ws_room_lock:
        ws_room[ws] = room_id


async def clear_room_id(ws: WebSocket):
    async with ws_room_lock:
        ws_room.pop(ws, None)


async def create_room(ws1: WebSocket, ws2: WebSocket) -> str:
    room_id = new_room_id()
    async with rooms_lock:
        rooms[room_id] = {ws1, ws2}
    await set_room_id(ws1, room_id)
    await set_room_id(ws2, room_id)
    return room_id


async def remove_from_room(ws: WebSocket):
    room_id = await get_room_id(ws)
    if not room_id:
        return

    async with rooms_lock:
        members = rooms.get(room_id)
        if members:
            members.discard(ws)
            if len(members) == 0:
                rooms.pop(room_id, None)

    await clear_room_id(ws)


async def notify_partner_left(ws: WebSocket):
    room_id = await get_room_id(ws)
    if not room_id:
        return

    async with rooms_lock:
        members = rooms.get(room_id, set()).copy()

    for m in members:
        if m is not ws:
            await safe_send(m, "__PARTNER_LEFT__")


async def leave_and_requeue(ws: WebSocket):
    # 部屋を抜けて相手に通知
    await notify_partner_left(ws)
    await remove_from_room(ws)

    # 待機に戻る（マッチング）
    global waiting
    async with waiting_lock:
        if waiting is None:
            waiting = ws
            await safe_send(ws, "（システム）待機中…相手を探しています")
            return

        other = waiting
        waiting = None

    # ルーム作成（lock外で）
    await create_room(other, ws)
    await safe_send(other, "__MATCHED__")
    await safe_send(ws, "__MATCHED__")


# -----------------------------
# Frontend (single file)
# -----------------------------
INDEX_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Random Text Chat</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 24px auto; padding: 0 12px; }
    #log { border: 1px solid #ddd; border-radius: 8px; padding: 12px; height: 50vh; overflow: auto; white-space: pre-wrap; }
    #row { display: flex; gap: 8px; margin-top: 12px; }
    input { flex: 1; padding: 10px; border-radius: 8px; border: 1px solid #ccc; }
    button { padding: 10px 14px; border-radius: 8px; border: 1px solid #ccc; background: #fff; }
    button:disabled { opacity: .5; }
    .muted { color: #666; }
  </style>
</head>
<body>
  <h2>ランダムテキストチャット（超シンプル）</h2>
  <div class="muted">「開始」→マッチしたら送信できます。相手が切断したら自動で終了します。</div>

  <div style="margin:12px 0;">
    <button id="start">開始</button>
    <button id="next" disabled>次の人</button>
  </div>

  <div id="log"></div>

  <div id="row">
    <input id="msg" placeholder="メッセージ..." disabled />
    <button id="send" disabled>送信</button>
  </div>

<script>
let ws = null;
let connecting = false;

const log = (t) => {
  const el = document.getElementById("log");
  el.textContent += t + "\n";
  el.scrollTop = el.scrollHeight;
};

const setChatEnabled = (on) => {
  document.getElementById("msg").disabled = !on;
  document.getElementById("send").disabled = !on;
  document.getElementById("next").disabled = !on;
};

const setStartEnabled = (on) => {
  document.getElementById("start").disabled = !on;
};

const connect = () => {
  const proto = (location.protocol === "https:") ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    connecting = false;
    setStartEnabled(false);
    log("✅ 接続しました。マッチング中...");
    setChatEnabled(false);
  };

  ws.onmessage = (ev) => {
    const data = ev.data;

    if (data === "__MATCHED__") {
      log("🎉 マッチしました！");
      setChatEnabled(true);
      return;
    }

    if (data === "__PARTNER_LEFT__") {
      log("⚠️ 相手が退出しました。終了します。");
      setChatEnabled(false);
      try { ws.close(); } catch(e) {}
      return;
    }

    log(data);
  };

  ws.onclose = () => {
    connecting = false;
    setStartEnabled(true);
    log("🔌 切断しました。");
    setChatEnabled(false);
  };

  ws.onerror = () => {
    connecting = false;
    setStartEnabled(true);
  };
};

document.getElementById("start").onclick = () => {
  if (connecting) return;

  // 既存接続があるなら閉じて作り直す（多重接続防止）
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    try { ws.close(); } catch(_) {}
  }

  document.getElementById("log").textContent = "";
  connecting = true;
  setStartEnabled(false);
  connect();
};

document.getElementById("next").onclick = () => {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send("__NEXT__");
};

document.getElementById("send").onclick = () => {
  const inp = document.getElementById("msg");
  const v = inp.value.trim();
  if (!v || !ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(v);
  inp.value = "";
};

document.getElementById("msg").addEventListener("keydown", (e) => {
  if (e.key === "Enter") document.getElementById("send").click();
});
</script>
</body>
</html>
"""


@app.get("/")
def index():
    return HTMLResponse(INDEX_HTML)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    global waiting

    # --- マッチング ---
    async with waiting_lock:
        if waiting is None:
            waiting = ws
            await safe_send(ws, "（システム）待機中…相手を探しています")
        else:
            other = waiting
            waiting = None

            # ルーム作成（lock外でやりたいが、ここは直後に作るだけなのでOK）
            await create_room(other, ws)
            await safe_send(other, "__MATCHED__")
            await safe_send(ws, "__MATCHED__")

    try:
        while True:
            msg = await ws.receive_text()

            if msg == "__NEXT__":
                await leave_and_requeue(ws)
                continue

            room_id = await get_room_id(ws)
            if not room_id:
                await safe_send(ws, "（システム）まだマッチしていません")
                continue

            async with rooms_lock:
                members = rooms.get(room_id, set()).copy()

            # 相手に中継
            for m in members:
                if m is ws:
                    await safe_send(m, f"あなた: {msg}")
                else:
                    await safe_send(m, f"相手: {msg}")

    except WebSocketDisconnect:
        # 待機中だったら waiting を解除
        async with waiting_lock:
            if waiting is ws:
                waiting = None

        # 部屋にいたら相手へ通知して抜ける
        await notify_partner_left(ws)
        await remove_from_room(ws)

