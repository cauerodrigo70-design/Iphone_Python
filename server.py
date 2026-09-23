"""
Discord clone - backend
Dependencias: pip install websockets
"""
import asyncio, json, os, sqlite3, time
from http import HTTPStatus
import websockets
from websockets.server import WebSocketServerProtocol, serve

PORT    = int(os.environ.get("PORT", 8000))
DB_PATH = os.environ.get("DB_PATH", "discord.db")
HERE    = os.path.dirname(os.path.abspath(__file__))

# ── banco ────────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id       TEXT PRIMARY KEY,
            username TEXT,
            avatar   TEXT DEFAULT '',
            status   TEXT DEFAULT 'online'
        );
        CREATE TABLE IF NOT EXISTS servers (
            id      TEXT PRIMARY KEY,
            name    TEXT,
            icon    TEXT DEFAULT '',
            owner   TEXT
        );
        CREATE TABLE IF NOT EXISTS members (
            server_id TEXT,
            user_id   TEXT,
            PRIMARY KEY(server_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS channels (
            id        TEXT PRIMARY KEY,
            server_id TEXT,
            name      TEXT,
            type      TEXT DEFAULT 'text'
        );
        CREATE TABLE IF NOT EXISTS messages (
            id         TEXT PRIMARY KEY,
            channel_id TEXT,
            user_id    TEXT,
            content    TEXT,
            ts         INTEGER,
            edited     INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS reactions (
            msg_id  TEXT,
            emoji   TEXT,
            user_id TEXT,
            PRIMARY KEY(msg_id, emoji, user_id)
        );
        CREATE TABLE IF NOT EXISTS dms (
            id      TEXT PRIMARY KEY,
            user_a  TEXT,
            user_b  TEXT
        );
    """)
    # servidor padrão
    row = con.execute("SELECT id FROM servers WHERE id='general'").fetchone()
    if not row:
        con.execute("INSERT INTO servers VALUES('general','Geral','🌐','system')")
        con.execute("INSERT INTO channels VALUES('geral-geral','general','geral','text')")
        con.execute("INSERT INTO channels VALUES('geral-off','general','off-topic','text')")
        con.execute("INSERT INTO channels VALUES('geral-anuncios','general','anúncios','text')")
    con.commit(); con.close()

def db(): return sqlite3.connect(DB_PATH)

def uid(): return os.urandom(6).hex()

# ── helpers ──────────────────────────────────────────────────────────────────
def get_user(user_id):
    con = db()
    r = con.execute("SELECT id,username,avatar,status FROM users WHERE id=?", (user_id,)).fetchone()
    con.close()
    return {"id":r[0],"username":r[1],"avatar":r[2],"status":r[3]} if r else None

def get_servers(user_id):
    con = db()
    rows = con.execute("""
        SELECT s.id,s.name,s.icon FROM servers s
        JOIN members m ON m.server_id=s.id WHERE m.user_id=?
    """, (user_id,)).fetchall()
    con.close()
    return [{"id":r[0],"name":r[1],"icon":r[2]} for r in rows]

def get_channels(server_id):
    con = db()
    rows = con.execute("SELECT id,name,type FROM channels WHERE server_id=? ORDER BY rowid", (server_id,)).fetchall()
    con.close()
    return [{"id":r[0],"name":r[1],"type":r[2]} for r in rows]

def get_members(server_id):
    con = db()
    rows = con.execute("""
        SELECT u.id,u.username,u.avatar,u.status FROM users u
        JOIN members m ON m.user_id=u.id WHERE m.server_id=?
    """, (server_id,)).fetchall()
    con.close()
    return [{"id":r[0],"username":r[1],"avatar":r[2],"status":r[3]} for r in rows]

def get_messages(channel_id, limit=50):
    con = db()
    rows = con.execute("""
        SELECT m.id,m.user_id,u.username,u.avatar,m.content,m.ts,m.edited
        FROM messages m JOIN users u ON u.id=m.user_id
        WHERE m.channel_id=? ORDER BY m.ts DESC LIMIT ?
    """, (channel_id, limit)).fetchall()
    con.close()
    msgs = [{"id":r[0],"user_id":r[1],"username":r[2],"avatar":r[3],
             "content":r[4],"ts":r[5],"edited":bool(r[6])} for r in reversed(rows)]
    return msgs

def get_reactions(msg_id):
    con = db()
    rows = con.execute("SELECT emoji,user_id FROM reactions WHERE msg_id=?", (msg_id,)).fetchall()
    con.close()
    agg = {}
    for emoji, uid_ in rows:
        agg.setdefault(emoji, []).append(uid_)
    return [{"emoji":e,"users":u,"count":len(u)} for e,u in agg.items()]

def get_dms(user_id):
    con = db()
    rows = con.execute("""
        SELECT d.id, CASE WHEN d.user_a=? THEN d.user_b ELSE d.user_a END as other_id,
               u.username, u.avatar, u.status
        FROM dms d JOIN users u ON u.id=(CASE WHEN d.user_a=? THEN d.user_b ELSE d.user_a END)
        WHERE d.user_a=? OR d.user_b=?
    """, (user_id,user_id,user_id,user_id)).fetchall()
    con.close()
    return [{"id":r[0],"user_id":r[1],"username":r[2],"avatar":r[3],"status":r[4]} for r in rows]

# ── estado online ─────────────────────────────────────────────────────────────
clients = {}  # user_id -> ws

async def broadcast_server(server_id, payload, exclude=None):
    con = db()
    members = con.execute("SELECT user_id FROM members WHERE server_id=?", (server_id,)).fetchall()
    con.close()
    for (uid_,) in members:
        if uid_ != exclude and uid_ in clients:
            try: await clients[uid_].send(payload)
            except: pass

async def broadcast_channel(channel_id, payload, exclude=None):
    con = db()
    row = con.execute("SELECT server_id FROM channels WHERE id=?", (channel_id,)).fetchone()
    con.close()
    if row: await broadcast_server(row[0], payload, exclude)

async def send_dm(user_a, user_b, payload):
    for uid_ in (user_a, user_b):
        if uid_ in clients:
            try: await clients[uid_].send(payload)
            except: pass

# ── handler ───────────────────────────────────────────────────────────────────
async def ws_handler(ws: WebSocketServerProtocol):
    user_id = None
    try:
        async for raw in ws:
            m = json.loads(raw)
            t = m.get("tipo")

            # ── auth ──
            if t == "auth":
                user_id = m.get("user_id")
                username = m.get("username","").strip() or "Usuário"
                avatar = m.get("avatar","")
                con = db()
                con.execute("INSERT OR IGNORE INTO users(id,username,avatar) VALUES(?,?,?)",
                            (user_id, username, avatar))
                con.execute("UPDATE users SET username=?,avatar=?,status='online' WHERE id=?",
                            (username, avatar, user_id))
                # entra no servidor geral automaticamente
                con.execute("INSERT OR IGNORE INTO members VALUES('general',?)", (user_id,))
                con.commit(); con.close()
                clients[user_id] = ws
                user = get_user(user_id)
                servers = get_servers(user_id)
                dms = get_dms(user_id)
                await ws.send(json.dumps({"tipo":"auth_ok","user":user,"servers":servers,"dms":dms}))
                # avisa membros que ficou online
                await broadcast_server("general", json.dumps({"tipo":"presence","user_id":user_id,"status":"online"}), exclude=user_id)

            # ── pegar canais ──
            elif t == "get_channels":
                channels = get_channels(m["server_id"])
                members_ = get_members(m["server_id"])
                await ws.send(json.dumps({"tipo":"channels","server_id":m["server_id"],"channels":channels,"members":members_}))

            # ── pegar mensagens ──
            elif t == "get_messages":
                msgs = get_messages(m["channel_id"])
                for msg in msgs:
                    msg["reactions"] = get_reactions(msg["id"])
                await ws.send(json.dumps({"tipo":"messages","channel_id":m["channel_id"],"msgs":msgs}))

            # ── enviar mensagem ──
            elif t == "send_message":
                msg_id = uid()
                ts = int(time.time()*1000)
                channel_id = m["channel_id"]
                content = m["content"].strip()
                if not content: continue
                con = db()
                con.execute("INSERT INTO messages VALUES(?,?,?,?,?,0)",
                            (msg_id, channel_id, user_id, content, ts))
                con.commit(); con.close()
                user = get_user(user_id)
                payload = json.dumps({"tipo":"new_message","msg":{
                    "id":msg_id,"channel_id":channel_id,"user_id":user_id,
                    "username":user["username"],"avatar":user["avatar"],
                    "content":content,"ts":ts,"edited":False,"reactions":[]
                }})
                # verifica se é DM ou canal de servidor
                con = db()
                is_dm = con.execute("SELECT id FROM dms WHERE id=?", (channel_id,)).fetchone()
                con.close()
                if is_dm:
                    con = db()
                    dm = con.execute("SELECT user_a,user_b FROM dms WHERE id=?", (channel_id,)).fetchone()
                    con.close()
                    await send_dm(dm[0], dm[1], payload)
                else:
                    await broadcast_channel(channel_id, payload)

            # ── editar mensagem ──
            elif t == "edit_message":
                con = db()
                con.execute("UPDATE messages SET content=?,edited=1 WHERE id=? AND user_id=?",
                            (m["content"], m["msg_id"], user_id))
                con.commit(); con.close()
                payload = json.dumps({"tipo":"message_edited","msg_id":m["msg_id"],"content":m["content"]})
                await broadcast_channel(m["channel_id"], payload)

            # ── apagar mensagem ──
            elif t == "delete_message":
                con = db()
                con.execute("DELETE FROM messages WHERE id=? AND user_id=?", (m["msg_id"], user_id))
                con.commit(); con.close()
                payload = json.dumps({"tipo":"message_deleted","msg_id":m["msg_id"],"channel_id":m["channel_id"]})
                await broadcast_channel(m["channel_id"], payload)

            # ── reação ──
            elif t == "react":
                con = db()
                exists = con.execute("SELECT 1 FROM reactions WHERE msg_id=? AND emoji=? AND user_id=?",
                                     (m["msg_id"], m["emoji"], user_id)).fetchone()
                if exists:
                    con.execute("DELETE FROM reactions WHERE msg_id=? AND emoji=? AND user_id=?",
                                (m["msg_id"], m["emoji"], user_id))
                else:
                    con.execute("INSERT INTO reactions VALUES(?,?,?)", (m["msg_id"], m["emoji"], user_id))
                con.commit(); con.close()
                reactions = get_reactions(m["msg_id"])
                payload = json.dumps({"tipo":"reactions_update","msg_id":m["msg_id"],"reactions":reactions})
                await broadcast_channel(m["channel_id"], payload)

            # ── digitando ──
            elif t == "typing":
                user = get_user(user_id)
                payload = json.dumps({"tipo":"typing","channel_id":m["channel_id"],"user_id":user_id,"username":user["username"]})
                await broadcast_channel(m["channel_id"], payload, exclude=user_id)

            # ── criar servidor ──
            elif t == "create_server":
                srv_id = uid()
                name = m.get("name","Meu Servidor").strip()
                icon = m.get("icon","🎮")
                con = db()
                con.execute("INSERT INTO servers VALUES(?,?,?,?)", (srv_id, name, icon, user_id))
                con.execute("INSERT INTO members VALUES(?,?)", (srv_id, user_id))
                con.execute("INSERT INTO channels VALUES(?,?,?,?)", (uid(), srv_id, "geral", "text"))
                con.execute("INSERT INTO channels VALUES(?,?,?,?)", (uid(), srv_id, "off-topic", "text"))
                con.commit(); con.close()
                servers = get_servers(user_id)
                await ws.send(json.dumps({"tipo":"servers_update","servers":servers}))

            # ── entrar em servidor por código ──
            elif t == "join_server":
                srv_id = m.get("server_id","").strip()
                con = db()
                exists = con.execute("SELECT id,name,icon FROM servers WHERE id=?", (srv_id,)).fetchone()
                if exists:
                    con.execute("INSERT OR IGNORE INTO members VALUES(?,?)", (srv_id, user_id))
                    con.commit(); con.close()
                    servers = get_servers(user_id)
                    await ws.send(json.dumps({"tipo":"servers_update","servers":servers}))
                    await broadcast_server(srv_id, json.dumps({"tipo":"member_joined","server_id":srv_id,"user":get_user(user_id)}))
                else:
                    con.close()
                    await ws.send(json.dumps({"tipo":"error","msg":"Servidor não encontrado"}))

            # ── DM ──
            elif t == "open_dm":
                target_id = m["target_id"]
                con = db()
                existing = con.execute("""
                    SELECT id FROM dms WHERE (user_a=? AND user_b=?) OR (user_a=? AND user_b=?)
                """, (user_id, target_id, target_id, user_id)).fetchone()
                if existing:
                    dm_id = existing[0]
                else:
                    dm_id = uid()
                    con.execute("INSERT INTO dms VALUES(?,?,?)", (dm_id, user_id, target_id))
                con.commit(); con.close()
                target = get_user(target_id)
                msgs = get_messages(dm_id)
                await ws.send(json.dumps({"tipo":"dm_open","dm_id":dm_id,"target":target,"msgs":msgs}))

            # ── buscar usuário ──
            elif t == "find_user":
                code = m.get("code","").strip()
                con = db()
                row = con.execute("SELECT id,username,avatar,status FROM users WHERE id=?", (code,)).fetchone()
                con.close()
                if row:
                    await ws.send(json.dumps({"tipo":"user_found","user":{"id":row[0],"username":row[1],"avatar":row[2],"status":row[3]}}))
                else:
                    await ws.send(json.dumps({"tipo":"error","msg":"Usuário não encontrado"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if user_id:
            clients.pop(user_id, None)
            con = db()
            con.execute("UPDATE users SET status='offline' WHERE id=?", (user_id,))
            con.commit(); con.close()
            await broadcast_server("general", json.dumps({"tipo":"presence","user_id":user_id,"status":"offline"}))

# ── HTTP ──────────────────────────────────────────────────────────────────────
async def process_request(path, headers):
    if headers.get("Upgrade","").lower() == "websocket": return None
    if path not in ("/", "/index.html"):
        return HTTPStatus.NOT_FOUND, {}, b"Not found"
    body = open(os.path.join(HERE,"index.html"), encoding="utf-8").read().encode("utf-8")
    return HTTPStatus.OK, {"Content-Type":"text/html; charset=utf-8"}, body

async def main():
    init_db()
    async with serve(ws_handler, "0.0.0.0", PORT, process_request=process_request):
        print(f"Rodando em http://0.0.0.0:{PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
