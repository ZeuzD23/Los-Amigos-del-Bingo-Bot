import os, io, re, csv, json, asyncio, logging, time, uuid, mimetypes
from datetime import datetime, UTC
from typing import Optional, Dict, List, Tuple

import pandas as pd
from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------- Google Drive (Service Account)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# =========================
# Config & Logging
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("bot")

# ENV obligatorias en Render
BOT_TOKEN = os.environ["BOT_TOKEN"]
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
GSA_JSON = os.environ["GSA_JSON"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# URL pública para webhook (Render la expone en RENDER_EXTERNAL_URL)
PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
PUBLIC_URL = PUBLIC_URL.rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

# CSV en Drive
USUARIOS_CSV = "usuarios.csv"
LOTES_CSV = "lotes.csv"
DEVOL_CSV = "devoluciones.csv"
REGISTRO_CSV = "registro.csv"
RANGO_TXT = "rango.txt"  # contenido: "ini-fin" (ej. "1-1000")

# Cabeceras base
USUARIOS_HEADERS = ["usuario_id", "nombre_usuario", "nombre_completo"]
LOTES_HEADERS    = ["nombre_usuario", "carton"]
DEVOL_HEADERS    = ["timestamp", "usuario_id", "nombre_usuario", "imagen", "motivo"]
REGISTRO_HEADERS = ["timestamp", "usuario_id", "nombre_usuario", "imagen"]

# Concurrencia
CSV_LOCK = asyncio.Lock()
MEM_LOCK = asyncio.Lock()

# Estado en memoria
usuarios_pendientes: set[int] = set()
kicked_users: set[int] = set()
maintenance_until_ts: float = 0.0  # /off: modo mantenimiento

# Utilidades
IMAGE_EXTS = [".jpg", ".jpeg", ".png", ".webp"]
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")

def canon(x: str) -> str:
    return (x or "").strip().casefold()

# =========================
# Google Drive helpers
# =========================
def drive_client(readwrite: bool = True):
    scopes = ["https://www.googleapis.com/auth/drive" if readwrite else "https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_info(json.loads(GSA_JSON), scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_find_file(service, name_exact: str) -> Optional[Dict]:
    # sanitizar comillas simples para la query
    name_sanitized = (name_exact or "").replace("'", "\\'")
    q = f"'{DRIVE_FOLDER_ID}' in parents and name = '{name_sanitized}' and trashed = false"
    r = service.files().list(q=q, fields="files(id,name,mimeType)").execute()
    files = r.get("files", [])
    return files[0] if files else None

def drive_search_contains(service, substr: str, mime_contains: Optional[str] = None) -> Optional[Dict]:
    q = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    if mime_contains:
        q += f" and mimeType contains '{mime_contains}'"
    page_token = None
    low = (substr or "").lower()
    while True:
        resp = service.files().list(q=q, fields="nextPageToken,files(id,name,mimeType)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            if low in f["name"].lower():
                return f
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return None

def drive_download_bytes(service, file_id: str) -> bytes:
    req = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def drive_upload_bytes(service, name: str, data: bytes, mime_type: str = "text/csv") -> str:
    meta = drive_find_file(service, name)
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=False)
    if meta:
        up = service.files().update(fileId=meta["id"], media_body=media, fields="id").execute()
        return up["id"]
    else:
        file_metadata = {"name": name, "parents": [DRIVE_FOLDER_ID]}
        created = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return created["id"]

# =========================
# CSV I/O (Drive)
# =========================
async def ensure_csv_exists(name: str, headers: List[str]) -> str:
    service = drive_client(True)
    meta = drive_find_file(service, name)
    if meta:
        return meta["id"]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(headers)
    return drive_upload_bytes(service, name, buf.getvalue().encode("utf-8"))

async def csv_read_all(name: str, headers: List[str]) -> List[Dict[str, str]]:
    service = drive_client(False)
    meta = drive_find_file(service, name)
    if not meta:
        await ensure_csv_exists(name, headers)
        meta = drive_find_file(service, name)
        if not meta:
            return []
    data = drive_download_bytes(service, meta["id"]).decode("utf-8", errors="replace")
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(io.StringIO(data))
    if not reader.fieldnames:
        return rows
    for r in reader:
        rows.append({h: (r.get(h, "") or "") for h in reader.fieldnames})
    return rows

async def csv_write_all(name: str, headers: List[str], rows: List[Dict[str, str]]):
    async with CSV_LOCK:
        service = drive_client(True)
        await ensure_csv_exists(name, headers)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=headers, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})
        drive_upload_bytes(service, name, buf.getvalue().encode("utf-8"))

async def csv_append_row(name: str, headers: List[str], row: Dict[str, str]):
    async with CSV_LOCK:
        rows = await csv_read_all(name, headers)
        rows.append({h: row.get(h, "") for h in headers})
        await csv_write_all(name, headers, rows)

# =========================
# Rango global (Drive)
# =========================
async def read_rango() -> Optional[Tuple[int,int]]:
    service = drive_client(False)
    meta = drive_find_file(service, RANGO_TXT)
    if not meta:
        return None
    txt = drive_download_bytes(service, meta["id"]).decode("utf-8", "replace").strip()
    m = RANGE_RE.match(txt)
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    return (min(a,b), max(a,b))

async def write_rango(a: int, b: int):
    s = f"{min(a,b)}-{max(a,b)}\n"
    service = drive_client(True)
    drive_upload_bytes(service, RANGO_TXT, s.encode("utf-8"), mime_type="text/plain")

# =========================
# Imágenes desde Drive (enviar SIEMPRE 1x1)
# =========================
def normalize_query_to_candidates(text: str) -> List[str]:
    text = (text or "").strip()
    cands = [text]
    low = text.lower()
    if not any(low.endswith(ext) for ext in IMAGE_EXTS):
        for ext in IMAGE_EXTS:
            cands.append(text + ext)
    return cands

def drive_find_image(service, query_text: str) -> Optional[Dict]:
    for cand in normalize_query_to_candidates(query_text):
        meta = drive_find_file(service, cand)
        if meta and meta.get("mimeType","").startswith("image/"):
            return meta
    return drive_search_contains(service, query_text, mime_contains="image/")

async def get_image_inputfile(query_text: str) -> Optional[Tuple[InputFile, str]]:
    service = drive_client(False)
    meta = drive_find_image(service, query_text)
    if not meta:
        return None

    data = drive_download_bytes(service, meta["id"])
    bio = io.BytesIO(data); bio.seek(0)

    base_name = meta.get("name") or f"{query_text}.jpg"
    mime = meta.get("mimeType", "")
    ext = None
    if mime and mime.startswith("image/"):
        guess = mimetypes.guess_extension(mime)
        if guess:
            ext = guess
    if not ext:
        for e in IMAGE_EXTS:
            if base_name.lower().endswith(e):
                ext = e
                break
    if not ext:
        ext = ".jpg"

    # Nombre limpio y único para evitar problemas de parsing en Telegram
    clean_name = f"img_{uuid.uuid4().hex}{ext}"
    return InputFile(bio, filename=clean_name), base_name

# =========================
# Utilidades de negocio
# =========================
def parse_numeros(parts: List[str]) -> List[int]:
    nums: set[int] = set()
    for p in parts:
        p = p.strip().strip(",")
        if not p: 
            continue
        m = RANGE_RE.match(p)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b:
                for n in range(a, b+1): nums.add(n)
            else:
                for n in range(b, a+1): nums.add(n)
        elif p.isdigit():
            nums.add(int(p))
        else:
            base = re.sub(r"\D", "", p)
            if base.isdigit():
                nums.add(int(base))
    return sorted(nums)

async def ensure_ready():
    await ensure_csv_exists(USUARIOS_CSV, USUARIOS_HEADERS)
    await ensure_csv_exists(LOTES_CSV, LOTES_HEADERS)
    await ensure_csv_exists(DEVOL_CSV, DEVOL_HEADERS)
    await ensure_csv_exists(REGISTRO_CSV, REGISTRO_HEADERS)

async def get_users_df() -> pd.DataFrame:
    rows = await csv_read_all(USUARIOS_CSV, USUARIOS_HEADERS)
    return pd.DataFrame(rows, columns=USUARIOS_HEADERS)

async def get_lotes_df() -> pd.DataFrame:
    rows = await csv_read_all(LOTES_CSV, LOTES_HEADERS)
    df = pd.DataFrame(rows, columns=LOTES_HEADERS)
    if not df.empty:
        df["carton"] = pd.to_numeric(df["carton"], errors="coerce").fillna(0).astype(int)
    return df

async def get_reg_df() -> pd.DataFrame:
    rows = await csv_read_all(REGISTRO_CSV, REGISTRO_HEADERS)
    return pd.DataFrame(rows, columns=REGISTRO_HEADERS)

async def get_devs_df() -> pd.DataFrame:
    rows = await csv_read_all(DEVOL_CSV, DEVOL_HEADERS)
    return pd.DataFrame(rows, columns=DEVOL_HEADERS)

async def is_admin(uid: int) -> bool:
    return ADMIN_ID and int(uid) == int(ADMIN_ID)

async def is_kicked(uid: int) -> bool:
    async with MEM_LOCK:
        return uid in kicked_users or (time.time() < maintenance_until_ts)

# =========================
# Handlers (mensajes y textos iguales a tu bot de PC)
# =========================
async def info_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Comando solo para el administrador.")
        return
    texto = (
        "<b>🛠️ Comandos del bot</b>\n\n"
        "<b>👥 Usuarios</b>\n"
        "• Envía números o rangos (ej. <code>1 3 5-8</code>) para pedir cartones.\n"
        "• <code>/help</code> — Ayuda.\n"
        "• <code>/v</code> — Tus vendidos (o global si eres admin).\n"
        "• <code>/r &lt;números/rangos&gt;</code> — Devolver cartones propios.\n"
        "• <code>/disp</code> — Ver <u>tus</u> cartones disponibles.\n\n"
        "<b>👑 Admin</b>\n"
        "• <code>/rango &lt;ini&gt; &lt;fin&gt;</code>, <code>/lista</code>, <code>/usuarios</code>, <code>/kick &lt;usuario&gt;</code>\n"
        "• <code>/vendido &lt;usuario&gt; &lt;nums/rangos&gt;</code>, <code>/c</code>\n"
        "• <code>/lote</code>, <code>/ver_lote</code>, <code>/quitar_lote</code>\n"
        "• <code>/reset</code>, <code>/id</code>\n"
    )
    await update.message.reply_text(texto, parse_mode=ParseMode.HTML)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_admin = (update.effective_user.id == ADMIN_ID)
    texto = (
        "<b>ℹ️ Ayuda</b>\n\n"
        "<b>Para todos</b>\n"
        "• Envía números o rangos para pedir cartones (ej: <code>1 3 5-8</code>).\n"
        "• <code>/v</code> — Ver tus vendidos.\n"
        "• <code>/r &lt;números/rangos&gt;</code> — Devolver cartones propios.\n"
        "• <code>/disp</code> — Ver <u>tus</u> cartones disponibles.\n"
    )
    if is_admin:
        texto += (
            "\n<b>Admin</b>\n"
            "• <code>/rango</code>, <code>/lista</code>, <code>/usuarios</code>, <code>/kick</code>\n"
            "• <code>/vendido</code>, <code>/c</code>, <code>/lote</code>, <code>/ver_lote</code>, <code>/quitar_lote</code>\n"
            "• <code>/reset</code>, <code>/id</code>, <code>/info</code>\n"
        )
    await update.message.reply_text(texto, parse_mode=ParseMode.HTML)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    uid = update.effective_user.id
    dfu = await get_users_df()
    registrados = set(map(int, dfu["usuario_id"].tolist())) if not dfu.empty else set()
    if uid not in registrados:
        usuarios_pendientes.add(uid)
        await update.message.reply_text(
            "Por favor, envía el *nombre de usuario* que quieres registrar.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    await update.message.reply_text(
        "¡Hola! Envía números o rangos (ej. `1 3 5-8`) para recibir cartones.\nComandos: /help",
        parse_mode=ParseMode.MARKDOWN
    )

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Tu ID: `{update.effective_user.id}`", parse_mode=ParseMode.MARKDOWN)

async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Mantenemos este texto corto; /info (admin) tiene la guía larga
    await update.message.reply_text("Bot en Render (webhook) + Google Drive (CSV + imágenes).", parse_mode=ParseMode.HTML)

async def usuarios_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    dfu = await get_users_df()
    if dfu.empty:
        await update.message.reply_text("📇 No hay usuarios registrados.")
        return
    dfu = dfu.copy()
    dfu["usuario_id"] = dfu["usuario_id"].astype(str)
    lineas = [f"👤 {r['nombre_usuario']} — ID: {r['usuario_id']}" for _, r in dfu.sort_values("nombre_usuario").iterrows()]
    await update.message.reply_text("🧑‍💻 <b>Usuarios registrados</b>\n" + "\n".join(lineas), parse_mode=ParseMode.HTML)

async def lista_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    dfr = await get_reg_df()
    if dfr.empty:
        ventas_txt = "📄 No se ha vendido ningún cartón."
    else:
        lineas = []
        # agrupar por nombre_usuario
        grp = dfr.groupby("nombre_usuario")["imagen"].apply(list).reset_index()
        for _, row in grp.iterrows():
            nums = sorted(map(int, row["imagen"]))
            lineas.append(f"👤 {row['nombre_usuario']} (total {len(nums)}): " + ", ".join(map(str, nums)))
        ventas_txt = "\n".join(lineas)

    devs = await get_devs_df()
    if devs.empty:
        devs_txt = "—"
    else:
        dgrp = devs.groupby("nombre_usuario")["imagen"].apply(list).reset_index()
        dlines = [f"♻️ {row['nombre_usuario']}: " + ", ".join(map(str, sorted(map(int, row['imagen'])))) for _, row in dgrp.iterrows()]
        devs_txt = "\n".join(dlines)

    mensaje = "🧾 <b>Ventas actuales</b>\n" + ventas_txt + "\n\n" + "♻️ <b>Devoluciones</b>\n" + devs_txt
    await update.message.reply_text(mensaje, parse_mode=ParseMode.HTML)

async def rango_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        rg = await read_rango()
        if rg:
            await update.message.reply_text(f"Rango actual: {rg[0]}-{rg[1]}")
        else:
            await update.message.reply_text("No hay rango definido.")
        return
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("Solo admin puede cambiar el rango.")
        return
    m = RANGE_RE.match(" ".join(context.args))
    if not m:
        await update.message.reply_text("Uso correcto: /rango a-b")
        return
    a, b = int(m.group(1)), int(m.group(2))
    await write_rango(a, b)
    await update.message.reply_text(f"✅ Rango definido: desde {min(a,b)} hasta {max(a,b)}.")

async def kick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Uso: /kick <user_id>")
        return
    uid_s = context.args[0]
    if not uid_s.isdigit():
        await update.message.reply_text("Uso: /kick <user_id>")
        return
    uid = int(uid_s)
    async with MEM_LOCK:
        kicked_users.add(uid)
    await update.message.reply_text(f"⛔ Usuario {uid} bloqueado hasta reinicio.")

async def off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    mins = 2
    ts = time.time() + mins*60
    async with MEM_LOCK:
        global maintenance_until_ts
        maintenance_until_ts = ts
    await update.message.reply_text(f"⚙️ Entrando en mantenimiento {mins} minutos. Durante ese tiempo el bot no responderá.")

async def ver_lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfu = await get_users_df()
    dfl = await get_lotes_df()
    if dfu.empty:
        await update.message.reply_text("No hay usuarios.")
        return
    target = None
    if context.args:
        target = " ".join(context.args).strip()
    else:
        me = dfu[dfu["usuario_id"] == str(update.effective_user.id)]
        if not me.empty:
            target = me.iloc[0]["nombre_usuario"]
    if not target:
        await update.message.reply_text("Uso: /ver_lote [usuario]")
        return
    mask = dfl["nombre_usuario"].astype(str).str.casefold() == canon(target)
    nums = sorted(dfl.loc[mask, "carton"].astype(int).tolist()) if not dfl.empty else []
    display = target
    if not nums:
        await update.message.reply_text(f"'{display}' no tiene cartones asignados.")
        return
    await update.message.reply_text(f"Cartones asignados a '{display}': " + ", ".join(map(str, nums)))

async def lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /lote <nombre_usuario> <números/rangos> (ej: /lote Diego 1-5 10 12)")
        return
    raw_name = context.args[0]
    target_canon = canon(raw_name)
    nums = set(parse_numeros(context.args[1:]))
    if not nums:
        await update.message.reply_text("⚠️ No se detectaron números válidos para asignar.")
        return
    dfl = await get_lotes_df()
    owner_by_num = {int(r["carton"]): str(r["nombre_usuario"]) for _, r in dfl.iterrows()} if not dfl.empty else {}
    nuevos, ya_mios, conflictos = [], [], []
    for n in sorted(nums):
        owner = owner_by_num.get(n)
        if owner is None:
            nuevos.append(n)
        else:
            if canon(owner) == target_canon:
                ya_mios.append(n)
            else:
                conflictos.append((n, owner))
    rows = dfl.to_dict("records") if not dfl.empty else []
    rows.extend([{"nombre_usuario": raw_name, "carton": str(n)} for n in nuevos])
    await csv_write_all(LOTES_CSV, LOTES_HEADERS, rows)

    partes = []
    if nuevos:
        partes.append(f"✅ Asignados a '{raw_name}': {', '.join(map(str, nuevos))}")
    if ya_mios:
        partes.append(f"ℹ️ Ya estaban asignados a ese usuario: {', '.join(map(str, ya_mios))}")
    if conflictos:
        partes.append("⛔ En conflicto (ya asignados a otra persona): " + ", ".join(f"{n}→{o}" for n, o in conflictos))
        partes.append("Para reasignar, usa /quitar_lote al usuario actual y luego /lote al nuevo.")
    if not partes:
        partes.append("ℹ️ No hubo nada que asignar.")
    await update.message.reply_text("\n".join(partes))

async def quitar_lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /quitar_lote <nombre_usuario> <números/rangos>")
        return
    target_canon = canon(context.args[0])
    nums = set(parse_numeros(context.args[1:]))
    dfl = await get_lotes_df()
    if dfl.empty:
        await update.message.reply_text("No hay lotes.")
        return
    keep, removed = [], []
    for _, r in dfl.iterrows():
        if canon(r["nombre_usuario"]) == target_canon and int(r["carton"]) in nums:
            removed.append(int(r["carton"]))
        else:
            keep.append({"nombre_usuario": r["nombre_usuario"], "carton": str(int(r["carton"]))})
    await csv_write_all(LOTES_CSV, LOTES_HEADERS, keep)
    if removed:
        await update.message.reply_text(f"✅ Quitados {len(removed)} cartones del lote de '{context.args[0]}'.")
    else:
        await update.message.reply_text("ℹ️ No se encontró ninguno de esos cartones en el lote.")

async def vendido_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    if not context.args:
        await update.message.reply_text("Uso: /vendido <numeros/rangos>")
        return
    nums = parse_numeros(context.args)
    dfu = await get_users_df()
    me = dfu[dfu["usuario_id"] == str(update.effective_user.id)]
    if me.empty:
        await update.message.reply_text("No estás registrado. /start")
        return
    nombre = me.iloc[0]["nombre_usuario"]
    ts = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
    for n in nums:
        row = {"timestamp": ts, "usuario_id": str(update.effective_user.id), "nombre_usuario": nombre, "imagen": str(n)}
        await csv_append_row(REGISTRO_CSV, REGISTRO_HEADERS, row)
    await update.message.reply_text("Marcado como vendido: " + ", ".join(map(str, nums)))

async def devol_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    if not context.args:
        await update.message.reply_text("Uso: /r <numeros/rangos>")
        return
    nums = parse_numeros(context.args)
    dfu = await get_users_df()
    me = dfu[dfu["usuario_id"] == str(update.effective_user.id)]
    if me.empty:
        await update.message.reply_text("No estás registrado. /start")
        return
    nombre = me.iloc[0]["nombre_usuario"]
    dfr = await get_reg_df()
    left, removed = [], 0
    for _, r in dfr.iterrows():
        if r["usuario_id"] == str(update.effective_user.id) and r["imagen"] in set(map(str, nums)):
            removed += 1
            continue
        left.append({h: r.get(h, "") for h in REGISTRO_HEADERS})
    if removed:
        await csv_write_all(REGISTRO_CSV, REGISTRO_HEADERS, left)
    ts = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
    for n in nums:
        row = {"timestamp": ts, "usuario_id": str(update.effective_user.id), "nombre_usuario": nombre, "imagen": str(n), "motivo": "devolucion"}
        await csv_append_row(DEVOL_CSV, DEVOL_HEADERS, row)
    await update.message.reply_text(f"Devoluciones registradas: {', '.join(map(str, nums))} (quitados {removed} de registro)")

async def mostrar_vendidos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: global. Usuario: propios."""
    uid = update.effective_user.id
    dfr = await get_reg_df()
    if dfr.empty:
        await update.message.reply_text("📦 Aún no se ha vendido ningún cartón.")
        return
    if await is_admin(uid):
        vendidos = sorted(set(int(x) for x in dfr["imagen"].tolist()))
        await update.message.reply_text(f"🧾 Total vendidos (global): {len(vendidos)}\n🔢 Números: {', '.join(map(str, vendidos))}")
        return
    propios = sorted(set(int(x) for x in dfr[dfr["usuario_id"] == str(uid)]["imagen"].tolist()))
    if propios:
        await update.message.reply_text(f"🧾 Tus vendidos: {len(propios)}\n🔢 Números: {', '.join(map(str, propios))}")
    else:
        await update.message.reply_text("ℹ️ Aún no has vendido ningún cartón.")

async def disp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfu = await get_users_df()
    dfl = await get_lotes_df()
    dfr = await get_reg_df()
    uid = update.effective_user.id
    me = dfu[dfu["usuario_id"] == str(uid)]
    if me.empty:
        await update.message.reply_text("Debes registrarte primero. Envía tu <b>nombre de usuario</b>.", parse_mode=ParseMode.HTML)
        return
    nombre = me.iloc[0]["nombre_usuario"]
    mine = dfl[dfl["nombre_usuario"].astype(str).str.casefold() == canon(nombre)]
    vendidos = set(dfr["imagen"].tolist())
    disponibles = sorted([int(x) for x in mine["carton"].tolist() if str(int(x)) not in vendidos])
    if not disponibles:
        await update.message.reply_text("ℹ️ No tienes cartones disponibles para pedir ahora mismo.")
        return
    # agrupar como en tu bot
    disp = []
    start = prev = None
    for n in disponibles:
        if start is None:
            start = prev = n
        elif n == prev + 1:
            prev = n
        else:
            disp.append(str(start) if start == prev else f"{start}-{prev}")
            start = prev = n
    if start is not None:
        disp.append(str(start) if start == prev else f"{start}-{prev}")
    await update.message.reply_text("🎟️ <b>Tus</b> cartones disponibles:\n" + ", ".join(disp), parse_mode=ParseMode.HTML)

async def v_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mostrar_vendidos(update, context)

async def reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    await update.message.reply_text("CSV recargados desde Drive (on-demand).")

async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Versión nube: no rotamos archivos locales. Solo “limpieza” lógica si eres admin.
    if not await is_admin(update.effective_user.id):
        return
    # Vaciar CSVs en Drive
    await csv_write_all(USUARIOS_CSV, USUARIOS_HEADERS, [])
    await csv_write_all(LOTES_CSV, LOTES_HEADERS, [])
    await csv_write_all(REGISTRO_CSV, REGISTRO_HEADERS, [])
    await csv_write_all(DEVOL_CSV, DEVOL_HEADERS, [])
    await update.message.reply_text("✅ Reseteados usuarios, lotes, registro y devoluciones en Drive.")

# Mensajes de números → envío de imágenes (SIEMPRE 1x1) + registro
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    if await is_kicked(update.effective_user.id):
        return

    uid = update.effective_user.id
    msg = (update.message.text or "").strip()

    # Registro de usuario (si está pendiente)
    if uid in usuarios_pendientes:
        nombre_usuario = msg.strip()
        if not nombre_usuario:
            await update.message.reply_text("El nombre no puede estar vacío.")
            return
        dfu = await get_users_df()
        existing = set(dfu["nombre_usuario"].astype(str).str.casefold().tolist()) if not dfu.empty else set()
        if canon(nombre_usuario) in existing:
            await update.message.reply_text("Ese nombre ya existe. Elige otro.")
            return
        row = {"usuario_id": str(uid), "nombre_usuario": nombre_usuario, "nombre_completo": update.effective_user.full_name or ""}
        await csv_append_row(USUARIOS_CSV, USUARIOS_HEADERS, row)
        usuarios_pendientes.discard(uid)
        await update.message.reply_text("¡Registrado! Envía números o /help.")
        return

    # Parseo de números/rangos
    nums = parse_numeros(re.split(r"[\s,]+", msg))
    if not nums:
        await update.message.reply_text("No detecté números válidos. Ej: 1 3 5-8.")
        return

    # Validación de asignaciones
    dfu = await get_users_df()
    me = dfu[dfu["usuario_id"] == str(uid)]
    if me.empty:
        usuarios_pendientes.add(uid)
        await update.message.reply_text("No estás registrado. Envía el *nombre de usuario* para registrarte.", parse_mode=ParseMode.MARKDOWN)
        return
    mi_nombre = me.iloc[0]["nombre_usuario"]

    dfl = await get_lotes_df()
    asign_map = {int(r["carton"]): r["nombre_usuario"] for _, r in dfl.iterrows()} if not dfl.empty else {}
    mine = {int(r["carton"]) for _, r in dfl.iterrows() if canon(r["nombre_usuario"]) == canon(mi_nombre)}
    tengo_lote = len(mine) > 0

    permitidos = set()
    bloqueados_otro = []
    fuera_de_mi_lote = []
    for n in nums:
        duenio = asign_map.get(n)
        if duenio is not None:
            if canon(duenio) == canon(mi_nombre):
                permitidos.add(n)
            else:
                bloqueados_otro.append((n, duenio))
        else:
            if tengo_lote:
                fuera_de_mi_lote.append(n)
            else:
                permitidos.add(n)

    if bloqueados_otro:
        await update.message.reply_text("⛔ Asignados a otra persona: " + ", ".join(f"{n}({d})" for n,d in bloqueados_otro))
    if fuera_de_mi_lote:
        await update.message.reply_text("⚠️ Fuera de tu lote: " + ", ".join(map(str, fuera_de_mi_lote)))
    if not permitidos:
        await update.message.reply_text("No hay cartones válidos según tus asignaciones.")
        return

    # Descarta ya vendidos por cualquiera
    dfr = await get_reg_df()
    vendidos_global = set(dfr["imagen"].tolist()) if not dfr.empty else set()
    a_enviar = [n for n in sorted(permitidos) if str(n) not in vendidos_global]
    if not a_enviar:
        await update.message.reply_text("Todos esos cartones ya están vendidos/enviados.")
        return

    await update.message.reply_text(f"📨 Enviando N°: {', '.join(map(str, a_enviar))}\n⏳ Espere...")

    # Envío SIEMPRE 1x1 + registro inmediato
    enviados_ok = []
    for n in a_enviar:
        res = await get_image_inputfile(str(n))
        if not res:
            await update.message.reply_text(f"❌ No encontré {n} en Drive.")
            continue
        input_file, fname = res
        try:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=input_file,
                caption=fname
            )
            enviados_ok.append(str(n))
            row = {
                "timestamp": datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds"),
                "usuario_id": str(uid),
                "nombre_usuario": mi_nombre,
                "imagen": str(n),
            }
            await csv_append_row(REGISTRO_CSV, REGISTRO_HEADERS, row)
        except Exception as e:
            log.exception("Error enviando foto %s: %s", n, e)

# =========================
# Comandos extra como en tu bot de PC
# =========================
async def comando_c(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /c <números/rangos> — enviar directo (no registra venta). Admin.
    if not await is_admin(update.effective_user.id):
        return
    numeros = set(parse_numeros(context.args))
    if not numeros:
        await update.message.reply_text("Uso: /c <número(s)> o rangos (ej: 1 2 5-10)")
        return
    await update.message.reply_text(f"📨 Enviando cartones: {', '.join(map(str, sorted(numeros)))}\n⏳ Espere...")
    for n in sorted(numeros):
        res = await get_image_inputfile(str(n))
        if not res:
            await update.message.reply_text(f"❌ No se encontró el cartón N° {n}.")
            continue
        input_file, _ = res
        try:
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=input_file)
        except Exception:
            pass

# =========================
# Webhook startup
# =========================
async def on_startup(app: Application):
    await ensure_ready()
    if not PUBLIC_URL:
        log.warning("PUBLIC_URL/RENDER_EXTERNAL_URL no definido; PTB usará webhook_url de run_webhook.")
    else:
        log.info(f"PUBLIC_URL detectada: {PUBLIC_URL}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # comandos (alineados con tu bot de PC)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("info", info_admin))      # /info como guía admin (igual que tu bot)
    app.add_handler(CommandHandler("usuarios", usuarios_cmd))
    app.add_handler(CommandHandler("ver_lote", ver_lote_cmd))
    app.add_handler(CommandHandler("disp", disp_cmd))
    app.add_handler(CommandHandler("v", v_cmd))
    app.add_handler(CommandHandler("lista", lista_cmd))
    app.add_handler(CommandHandler("rango", rango_cmd))
    app.add_handler(CommandHandler("reload", reload_cmd))
    app.add_handler(CommandHandler("kick", kick_cmd))
    app.add_handler(CommandHandler("off", off_cmd))
    app.add_handler(CommandHandler("lote", lote_cmd))
    app.add_handler(CommandHandler("quitar_lote", quitar_lote_cmd))
    app.add_handler(CommandHandler("vendido", vendido_cmd))
    app.add_handler(CommandHandler("r", devol_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("c", comando_c))

    # textos
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    app.post_init = on_startup

    webhook_path = f"/webhook/{BOT_TOKEN}"
    base_url = (PUBLIC_URL or "").rstrip("/")
    if not base_url.startswith("http"):
        base_url = f"https://{base_url.lstrip('/')}"
    webhook_url = f"{base_url}{webhook_path}"

    log.info(f"Iniciando servidor en 0.0.0.0:{PORT}{webhook_path}")
    log.info(f"Webhook URL usado por PTB: {webhook_url}")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()

