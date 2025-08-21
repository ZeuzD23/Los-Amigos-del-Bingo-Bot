import os, io, re, csv, json, asyncio, logging, time, uuid
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import pandas as pd
from telegram import Update, InputFile, InputMediaPhoto
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

BOT_TOKEN = os.environ["BOT_TOKEN"]
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
GSA_JSON = os.environ["GSA_JSON"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
PUBLIC_URL = PUBLIC_URL.rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

# CSV en tu Drive
USUARIOS_CSV = "usuarios.csv"
LOTES_CSV = "lotes.csv"
DEVOL_CSV = "devoluciones.csv"
REGISTRO_CSV = "registro.csv"

# Cabeceras base
USUARIOS_HEADERS = ["usuario_id", "nombre_usuario", "nombre_completo"]
LOTES_HEADERS    = ["nombre_usuario", "carton"]
DEVOL_HEADERS    = ["timestamp", "usuario_id", "nombre_usuario", "imagen", "motivo"]
REGISTRO_HEADERS = ["timestamp", "usuario_id", "nombre_usuario", "imagen"]

# Rango global (texto)
RANGO_TXT = "rango.txt"  # ej. "1-1000"

# Concurrencia
CSV_LOCK = asyncio.Lock()
MEM_LOCK = asyncio.Lock()

# En memoria
usuarios_pendientes: set[int] = set()
kicked_users: set[int] = set()
maintenance_until_ts: float = 0.0  # /off: modo mantenimiento

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
# Rango global en Drive
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
# Imágenes desde Drive
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
    bio = io.BytesIO(data)
    bio.seek(0)
    base_name = meta.get("name") or f"{query_text}.jpg"
    # filename único para evitar errores en media groups
    unique_name = f"{base_name}__{uuid.uuid4().hex}.bin"
    return InputFile(bio, filename=unique_name), base_name

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
# Handlers
# =========================
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
    await update.message.reply_text("¡Hola! Envía números o rangos (ej. `1 3 5-8`) para recibir cartones.\nComandos: /help", parse_mode=ParseMode.MARKDOWN)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "🧾 *Comandos disponibles*\n"
        "• /start — Registro / bienvenida\n"
        "• /help — Esta ayuda\n"
        "• /id — Ver tu ID\n"
        "• /info — Info del bot\n"
        "• /usuarios — Lista de usuarios\n"
        "• /lote <usuario> <rangos> — Asignar cartones\n"
        "• /quitar_lote <usuario> <rangos> — Quitar asignación\n"
        "• /ver_lote [usuario] — Ver lote\n"
        "• /disp — Ver *tus* disponibles (asignados no vendidos)\n"
        "• /v — Ver *tus* vendidos\n"
        "• /vendido <rangos> — Marcar vendidos (manual)\n"
        "• /r <rangos> — Registrar devoluciones\n"
        "• /lista — Resumen general\n"
        "• /rango [a-b] — Ver/actualizar rango global\n"
        "• /reload — Recargar CSV desde Drive\n"
        "• /kick <user_id> — Bloquear (admin)\n"
        "• /off — Modo mantenimiento 2 min (admin)\n"
        "_También puedes enviar números/rangos como mensaje (envío de imágenes)_"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Tu ID: `{update.effective_user.id}`", parse_mode=ParseMode.MARKDOWN)

async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot en Render (webhook) + Google Drive (CSV + imágenes).", parse_mode=ParseMode.HTML)

async def usuarios_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfu = await get_users_df()
    if dfu.empty:
        await update.message.reply_text("No hay usuarios registrados.")
        return
    lines = [f"• {r['nombre_usuario']} ({r['usuario_id']})" for _, r in dfu.iterrows()]
    await update.message.reply_text("👥 *Usuarios*\n" + "\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def ver_lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfu = await get_users_df()
    dfL = await get_lotes_df()
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
    mask = dfL["nombre_usuario"].astype(str).str.casefold() == canon(target)
    nums = sorted(dfL.loc[mask, "carton"].astype(int).tolist()) if not dfL.empty else []
    await update.message.reply_text(f"📦 Lote de {target}: " + (", ".join(map(str, nums)) if nums else "vacío"))

async def disp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfu = await get_users_df()
    dfl = await get_lotes_df()
    dfr = await get_reg_df()
    uid = update.effective_user.id
    me = dfu[dfu["usuario_id"] == str(uid)]
    if me.empty:
        await update.message.reply_text("No estás registrado. Usa /start.")
        return
    nombre = me.iloc[0]["nombre_usuario"]
    mine = dfl[dfl["nombre_usuario"].astype(str).str.casefold() == canon(nombre)]
    vendidos = set(dfr["imagen"].tolist())
    disponibles = sorted([int(x) for x in mine["carton"].tolist() if str(int(x)) not in vendidos])
    await update.message.reply_text("✅ *Tus disponibles*: " + (", ".join(map(str, disponibles)) if disponibles else "ninguno"), parse_mode=ParseMode.MARKDOWN)

async def v_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfr = await get_reg_df()
    mine = dfr[dfr["usuario_id"] == str(update.effective_user.id)]
    imgs = [r["imagen"] for _, r in mine.iterrows()]
    await update.message.reply_text("🧾 *Tus vendidos*: " + (", ".join(imgs) if imgs else "ninguno"), parse_mode=ParseMode.MARKDOWN)

async def lista_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    dfr = await get_reg_df()
    total = len(dfr.index) if not dfr.empty else 0
    por_user = {}
    for _, r in dfr.iterrows():
        por_user[r["nombre_usuario"]] = por_user.get(r["nombre_usuario"], 0) + 1
    lines = [f"• {k}: {v}" for k, v in sorted(por_user.items())] if por_user else ["(sin ventas)"]
    await update.message.reply_text(f"📊 *Registro total:* {total}\n" + "\n".join(lines), parse_mode=ParseMode.MARKDOWN)

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
        await update.message.reply_text("Uso: /rango a-b")
        return
    a, b = int(m.group(1)), int(m.group(2))
    await write_rango(a, b)
    await update.message.reply_text(f"Rango actualizado a: {min(a,b)}-{max(a,b)}")

async def reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_ready()
    await update.message.reply_text("CSV recargados desde Drive (on-demand).")

async def kick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("Solo admin.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Uso: /kick <user_id>")
        return
    uid = int(context.args[0])
    async with MEM_LOCK:
        kicked_users.add(uid)
    await update.message.reply_text(f"Usuario {uid} bloqueado (memoria).")

async def off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("Solo admin.")
        return
    mins = 2
    ts = time.time() + mins*60
    async with MEM_LOCK:
        global maintenance_until_ts
        maintenance_until_ts = ts
    await update.message.reply_text(f"⚙️ Entrando en mantenimiento {mins} minutos. Durante ese tiempo el bot no responderá.")

async def lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("Solo admin.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /lote <usuario> <numeros/rangos>\nEj: /lote juan 1 3 5-8")
        return
    target = context.args[0]
    nums = parse_numeros(context.args[1:])
    if not nums:
        await update.message.reply_text("No hay números válidos.")
        return
    dfl = await get_lotes_df()
    existentes = set(dfl["carton"].astype(int).tolist()) if not dfl.empty else set()
    nuevos = [n for n in nums if n not in existentes]
    add_rows = [{"nombre_usuario": target, "carton": str(n)} for n in nuevos]
    rows = dfl.to_dict("records") if not dfl.empty else []
    rows.extend(add_rows)
    await csv_write_all(LOTES_CSV, LOTES_HEADERS, rows)
    await update.message.reply_text(f"Asignados a {target}: " + (", ".join(map(str, nuevos)) if nuevos else "(ninguno; ya ocupados)"))

async def quitar_lote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("Solo admin.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /quitar_lote <usuario> <numeros/rangos>")
        return
    target = context.args[0]
    nums = set(parse_numeros(context.args[1:]))
    dfl = await get_lotes_df()
    if dfl.empty:
        await update.message.reply_text("No hay lotes.")
        return
    keep = []
    removed = []
    for _, r in dfl.iterrows():
        if canon(r["nombre_usuario"]) == canon(target) and int(r["carton"]) in nums:
            removed.append(int(r["carton"]))
        else:
            keep.append({"nombre_usuario": r["nombre_usuario"], "carton": str(int(r["carton"]))})
    await csv_write_all(LOTES_CSV, LOTES_HEADERS, keep)
    await update.message.reply_text(f"Quitados de {target}: " + (", ".join(map(str, sorted(removed))) if removed else "(ninguno)"))

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
    for n in nums:
        row = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "usuario_id": str(update.effective_user.id),
            "nombre_usuario": nombre,
            "imagen": str(n),
        }
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
    left = []
    removed = 0
    for _, r in dfr.iterrows():
        if r["usuario_id"] == str(update.effective_user.id) and r["imagen"] in set(map(str, nums)):
            removed += 1
            continue
        left.append({h: r.get(h, "") for h in REGISTRO_HEADERS})
    if removed:
        await csv_write_all(REGISTRO_CSV, REGISTRO_HEADERS, left)
    for n in nums:
        row = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "usuario_id": str(update.effective_user.id),
            "nombre_usuario": nombre,
            "imagen": str(n),
            "motivo": "devolucion",
        }
        await csv_append_row(DEVOL_CSV, DEVOL_HEADERS, row)
    await update.message.reply_text(f"Devoluciones registradas: {', '.join(map(str, nums))} (quitados {removed} de registro)")

# Mensajes de números → envío de imágenes + registro
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

    # Media group en bloques de 10
    batch: List[InputMediaPhoto] = []
    names: List[str] = []

    async def flush():
        nonlocal batch, names
        if not batch:
            return

        if len(batch) == 1:
            # 1 imagen → send_photo
            item = batch[0]
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=item.media,
                caption=item.caption or None,
            )
        else:
            # 2..n → enviar en grupos de 10
            for i in range(0, len(batch), 10):
                chunk = batch[i:i+10]
                await context.bot.send_media_group(
                    chat_id=update.effective_chat.id,
                    media=chunk
                )

        # Registro
        for fname in names:
            row = {
                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                "usuario_id": str(uid),
                "nombre_usuario": mi_nombre,
                "imagen": fname,
            }
            await csv_append_row(REGISTRO_CSV, REGISTRO_HEADERS, row)

        batch, names = [], []

    # Construcción del lote
    for n in a_enviar:
        res = await get_image_inputfile(str(n))
        if not res:
            await update.message.reply_text(f"❌ No encontré {n} en Drive.")
            continue
        input_file, fname = res
        batch.append(InputMediaPhoto(media=input_file, caption=fname))
        names.append(str(n))
        if len(batch) == 10:
            await flush()
    await flush()

# =========================
# Webhook startup
# =========================
async def on_startup(app: Application):
    # Solo preparamos CSV y logueamos; NO registramos webhook aquí
    await ensure_ready()
    if not PUBLIC_URL:
        log.warning("PUBLIC_URL/RENDER_EXTERNAL_URL no definido; PTB usará webhook_url de run_webhook.")
    else:
        log.info(f"PUBLIC_URL detectada: {PUBLIC_URL}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("info", info_cmd))
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
        webhook_url=webhook_url,        # PTB hace setWebhook con esta URL
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
