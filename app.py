import asyncio
import secrets
from typing import Dict, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# 待機キュー（単純に先着1名を待たせる）
waiting: Optional[WebSocket] = None
waiting_lock = asyncio.Lock()

# room_id -> {ws1, ws2}
rooms: Dict[str, Set[WebSocket]] = {}
rooms_lock = asyncio.Lock()


def new_room_id() -> str:
    return secrets.token_urlsafe(8)


INDEX_HTML = """
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

const log = (t) => {
  const el = document.getElementById("log");
  el.textContent += t + "\\n";
  el.scrollTop = el.scrollHeight;
};

const setChatEnabled = (on) => {
  document.getElementById("msg").disabled = !on;
  document.getElementById("send").disabled = !on;
  document.getElementById("next").disabled = !on;
};

const connect = () => {
  const proto = (location.protocol === "https:") ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
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
      ws.close();
      return;
    }
    log(data);
  };

  ws.onclose = () => {
    log("🔌 切断しました。");
    setChatEnabled(false);
  };
};

document.getElementById("start").onclick = () => {
  if (ws && ws.readyState === WebSocket.OPEN) return;
  document.getElementById("log").textContent = "";
  connect();
};

document.getElementById("next").onclick = () => {
  if (!ws) return;
  ws.send("__NEXT__");
};

document.getElementById("send").onclick = () => {
  const inp = document.getElementById("msg");
  const v = inp.value.trim();
  if (!v || !ws) return;
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


async def safe_send(ws: WebSocket, text: str) -> bool:
    try:
        await ws.send_text(text)
        return True
    except Exception:
        return False


async def remove_from_room(room_id: str, ws: WebSocket):
    async with rooms_lock:
        s = rooms.get(room_id)
        if not s:
            return
        s.discard(ws)
        if len(s) == 0:
            rooms.pop(room_id, None)


async def find_room_of(ws: WebSocket) -> Optional[str]:
    async with rooms_lock:
        for rid, members in rooms.items():
            if ws in members:
                return rid
    return None


async def notify_partner_left(room_id: str, leaver: WebSocket):
    async with rooms_lock:
        members = rooms.get(room_id, set()).copy()
    for m in members:
        if m is not leaver:
            await safe_send(m, "__PARTNER_LEFT__")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    global waiting

    # --- マッチング処理 ---
    async with waiting_lock:
        if waiting is None:
            waiting = ws
            await safe_send(ws, "（システム）待機中…相手を探しています")
            my_room = None
        else:
            other = waiting
            waiting = None
            room_id = new_room_id()
            async with rooms_lock:
                rooms[room_id] = {other, ws}
            await safe_send(other, "__MATCHED__")
            await safe_send(ws, "__MATCHED__")
            my_room = room_id

    try:
        while True:
            msg = await ws.receive_text()

            # 次の人（退出→再マッチ）
            if msg == "__NEXT__":
                room_id = await find_room_of(ws)
                if room_id:
                    await notify_partner_left(room_id, ws)
                    await remove_from_room(room_id, ws)

                # 自分を待機に戻す
                async with waiting_lock:
                    if waiting is None:
                        waiting = ws
                        await safe_send(ws, "（システム）待機中…相手を探しています")
                        continue
                    else:
                        other = waiting
                        waiting = None
                        room_id = new_room_id()
                        async with rooms_lock:
                            rooms[room_id] = {other, ws}
                        await safe_send(other, "__MATCHED__")
                        await safe_send(ws, "__MATCHED__")
                        continue

            # 通常メッセージ：同じ部屋の相手へ中継
            room_id = await find_room_of(ws)
            if not room_id:
                await safe_send(ws, "（システム）まだマッチしていません")
                continue

            async with rooms_lock:
                members = rooms.get(room_id, set()).copy()

            for m in members:
                if m is ws:
                    await safe_send(m, f"あなた: {msg}")
                else:
                    await safe_send(m, f"相手: {msg}")

    except WebSocketDisconnect:
        # 切断時：待機中なら解除、部屋中なら相手に通知
        async with waiting_lock:
            if waiting is ws:
                waiting = None

        room_id = await find_room_of(ws)
        if room_id:
            await notify_partner_left(room_id, ws)
            await remove_from_room(room_id, ws)
