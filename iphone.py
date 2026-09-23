"""
iPhone com app de Mensagens em tempo real.
Dependencias: pip install websockets
Uso local:  python iphone.py
"""

import asyncio
import json
import os
import random
import sqlite3
import string
from http import HTTPStatus

import websockets
from websockets.server import WebSocketServerProtocol, serve

PORT    = int(os.environ.get("PORT", 8000))
DB_PATH = os.environ.get("DB_PATH", "mensagens.db")
HERE    = os.path.dirname(os.path.abspath(__file__))

# ── banco de dados ──────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS usuarios (
            codigo TEXT PRIMARY KEY,
            nome   TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS mensagens (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            de    TEXT,
            para  TEXT,
            texto TEXT,
            ts    INTEGER
        );
    """)
    con.commit(); con.close()

def db(): return sqlite3.connect(DB_PATH)

def salvar_usuario(codigo):
    con = db()
    con.execute("INSERT OR IGNORE INTO usuarios(codigo) VALUES(?)", (codigo,))
    con.commit(); con.close()

def salvar_msg(de, para, texto, ts):
    con = db()
    con.execute("INSERT INTO mensagens(de,para,texto,ts) VALUES(?,?,?,?)", (de, para, texto, ts))
    con.commit(); con.close()

def historico(a, b):
    con = db()
    rows = con.execute("""
        SELECT de, para, texto, ts FROM mensagens
        WHERE (de=? AND para=?) OR (de=? AND para=?)
        ORDER BY ts ASC
    """, (a, b, b, a)).fetchall()
    con.close()
    return [{"de": r[0], "para": r[1], "texto": r[2], "ts": r[3]} for r in rows]

# ── websocket ───────────────────────────────────────────────────────────────
clientes = {}

async def ws_handler(ws: WebSocketServerProtocol):
    codigo = None
    try:
        async for raw in ws:
            m = json.loads(raw)
            t = m.get("tipo")

            if t == "entrar":
                codigo = m["codigo"]
                salvar_usuario(codigo)
                clientes[codigo] = ws
                await ws.send(json.dumps({"tipo": "ok", "codigo": codigo}))

            elif t == "historico":
                h = historico(m["eu"], m["outro"])
                await ws.send(json.dumps({"tipo": "historico", "msgs": h}))

            elif t == "digitando":
                dest = m.get("para")
                if dest and dest in clientes:
                    await clientes[dest].send(json.dumps({"tipo": "digitando", "de": m["de"], "ativo": m["ativo"]}))

            elif t == "mensagem":
                de, para, texto, ts = m["de"], m["para"], m["texto"], m["ts"]
                salvar_msg(de, para, texto, ts)
                # avisar que parou de digitar
                if para in clientes:
                    await clientes[para].send(json.dumps({"tipo": "digitando", "de": de, "ativo": False}))
                payload = json.dumps({"tipo": "mensagem", "de": de, "para": para, "texto": texto, "ts": ts})
                for dest in (para, de):
                    if dest in clientes:
                        await clientes[dest].send(payload)

            elif t == "verificar":
                con = db()
                existe = con.execute("SELECT 1 FROM usuarios WHERE codigo=?", (m["codigo"],)).fetchone()
                con.close()
                await ws.send(json.dumps({"tipo": "verificar", "existe": bool(existe), "codigo": m["codigo"]}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if codigo and clientes.get(codigo) is ws:
            del clientes[codigo]

# ── HTTP embutido no mesmo servidor WebSocket ───────────────────────────────
async def process_request(path, headers):
    """Responde requisições HTTP normais; deixa WS passar."""
    if headers.get("Upgrade", "").lower() == "websocket":
        return None  # deixa o websockets tratar normalmente

    if path not in ("/", "/index.html"):
        return HTTPStatus.NOT_FOUND, {}, b"Not found"

    body = open(os.path.join(HERE, "index.html"), encoding="utf-8").read().encode("utf-8")
    return HTTPStatus.OK, {"Content-Type": "text/html; charset=utf-8"}, body

# ── main ────────────────────────────────────────────────────────────────────
async def main():
    init_db()
    async with serve(ws_handler, "0.0.0.0", PORT, process_request=process_request):
        print(f"Rodando em http://0.0.0.0:{PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
