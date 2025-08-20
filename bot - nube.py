import os
import io
import re
import csv
import json
import time
import logging
import asyncio
from datetime import datetime
from typing import Optional, Tuple, List, Dict

from telegram import Update, InputFile, InputMediaPhoto
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]  # Token de BotFather
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]  # ID carpeta Drive con imágenes + CSV
GSA_JSON = os.environ["GSA_JSON"]  # Contenido JSON (texto) de la Service Account

# Webhook/public URL (Render expone RENDER_EXTERNAL_URL). Puedes definir PUBLIC_URL manualmente si quieres.
PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

# Nombres de CSV en Drive (puedes cambiarlos por ENV si ya usas otros nombres)
USERS_CSV = os.environ.get("USERS_CSV", "users.csv")
LOTES_CSV = os.environ.get("LOTES_CSV", "lotes.csv")
VENTAS_CSV = os.environ.get("VENTAS_CSV", "ventas.csv")

# Cabeceras por defecto (ajústalas si ya usas otras columnas)
USERS_HEADERS = ["usuario_id", "nombre_usuario", "nombre_completo"]
LOTES_HEADERS = ["nombre_usuario", "carton"]
VENTAS_HEADERS = ["timestamp", "usuario_id", "nombre_usuario", "imagen"]

# Concurrency locks para escrituras CSV
CSV_LOCK = asyncio.Lock()

# =========================
# Utilidades Google Drive
# =========================
def drive_client():
    info = json.loads(GSA_JSON)
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_find_file(service, name_exact: str) -> Optional[Dict]:
    """Busca archivo por nombre exacto en la carpeta (no basura)."""
    q = (
        f"'{DRIVE_FOLDER_ID}' in parents and name = '{name_exact.replace(\"'\", \"\\'\")}' "
        "and trashed = false"
    )
    res = service.files().list(q=q, fields="files(id,name,mimeType)").execute()
    files = res.get("files", [])
    return files[0] if files else None

def drive_search_contains(service, substr: str, mime_contains: Optional[str] = None) -> Optional[Dict]:
    """Busca el primer archivo cuyo nombre contenga substr (case-insensitive)."""
    q = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    if mime_contains:
        q += f" and mimeType contains '{mime_contains}'"
    page_token = None
    low = substr.lower()
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken,files(id,name,mimeType)",
            pageToken=page_token
        ).execute()
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
    """
    Crea o actualiza archivo por nombre exacto dentro de la carpeta.
    Retorna file_id.
    """
    meta = drive_find_file(service, name)
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=False)
    if meta:
        file_id = meta["id"]
        updated = service.files().update(fileId=file_id, media_body=media).execute()
        return updated["id"]
    else:
        file_metadata = {"name": name, "parents": [DRIVE_FOLDER_ID]}
        created = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return created["id"]

# =========================
# CSV en Drive (lectura/escritura)
# =========================
async def ensure_csv_exists(name: str, headers: List[str]) -> str:
    """Garantiza que el CSV exista en Drive con las cabeceras. Retorna file_id."""
    service = drive_client()
    meta = drive_find_file(service, name)
    if meta:
        return meta["id"]
    # Crear CSV vacío con cabeceras
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(headers)
    file_id = drive_upload_bytes(service, name, buf.getvalue().encode("utf-8"))
    return file_id

async def csv_read_all(name: str, headers: List[str]) -> List[Dict[str, str]]:
    """Lee todo el CSV desde Drive y retorna lista de dicts."""
    service = drive_client()
    meta = drive_find_file(service, name)
    if not meta:
        await ensure_csv_exists(name, headers)
        meta = drive_find_file(service, name)
        if not meta:
            return []
    data = drive_download_bytes(service, meta["id"]).decode("utf-8", errors="replace")
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(io.StringIO(data))
    for r in reader:
        rows.append({k: (r.get(k, "") or "") for k in reader.fieldnames})
    return rows

async def csv_write_all(name: str, headers: List[str], rows: List[Dict[str, str]]) -> None:
    """Sobrescribe completo el CSV en Drive con filas dadas (dicts)."""
    service = drive_client()
    await ensure_csv_exists(name, headers)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({h: r.get(h, "") for h in headers})
    drive_upload_bytes(service, name, buf.getvalue().encode("utf-8"))

async def csv_append_row(name: str, headers: List[str], row: Dict[str, str]) -> None:
    """Agrega una fila al CSV en Drive (descarga + agrega + sube)."""
    async with CSV_LOCK:
        rows = await csv_read_all(name, headers)
        rows.append({h: row.get(h, "") for h in headers})
        await csv_write_all(name, headers, rows)

# =========================
# Imágenes desde Drive
# =========================
IMAGE_EXTS = [".jpg", ".jpeg", ".png", ".webp"]

def normalize_query_to_candidates(text: str) -> List[str]:
    """Convierte '1750' en ['1750', '1750.jpg', ...]."""
    text = text.strip()
    cands = [text]
    lower = text.lower()
    if not any(lower.endswith(ext) for ext in IMAGE_EXTS):
        for ext in IMAGE_EXTS:
            cands.append(text + ext)
    return cands

def drive_find_image(service, query_text: str) -> Optional[Dict]:
    """
    Busca imagen por nombre exacto (con variantes) y si no, por 'contains'.
    Retorna {id,name,mimeType} o None.
    """
    # Intentos exactos
    for cand in normalize_query_to_candidates(query_text):
        meta = drive_find_file(service, cand)
        if meta and meta.get("mimeType", "").startswith("image/"):
            return meta
    # Fallback: contains
    return drive_search_contains(service, query_text, mime_contains="image/")

async def get_image_inputfile(query_text: str) -> Optional[Tuple[InputFile, str]]:
    """Devuelve (InputFile, filename) o None si no encuentra."""
    service = drive_client()
    meta = drive_find_image(service, query_text)
    if not meta:
        return None
    data = drive_download_bytes(service, meta["id"])
    bio = io.BytesIO(data)
    bio.name = meta["name"]
    return InputFile(bio), meta["name"]

# =========================
# Helpers del bot
# =========================
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")

def parse_numbers(text: str) -> List[str]:
    """
    Acepta entradas como:
      - "1750"
      - "1 3,5-8 10-12"
    Retorna lista expandida de strings (números) SIN duplicados, ordenados.
    """
    parts = re.split(r"[,\s]+", text.strip())
    nums: set[int] = set()
    for p in parts:
        if not p:
            continue
        m = RANGE_RE.match(p)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b:
                for n in range(a, b + 1):
                    nums.add(n)
            else:
                for n in range(b, a + 1):
                    nums.add(n)
        elif p.isdigit():
            nums.add(int(p))
        else:
            # si el usuario pone '1750.jpg' lo tratamos como número base '1750'
            base = re.sub(r"\D", "", p)
            if base.isdigit():
                nums.add(int(base))
    return [str(n) for n in sorted(nums)]

async def ensure_csvs_ready():
    await ensure_csv_exists(USERS_CSV, USERS_HEADERS)
    await ensure_csv_exists(LOTES_CSV, LOTES_HEADERS)
    await ensure_csv_exists(VENTAS_CSV, VENTAS_HEADERS)

# =========================
# Handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_csvs_ready()
    await update.message.reply_text(
        "Hola 👋\n"
        "Envíame un número o rango (ej. `1750` o `1 3 5-8`) y te envío los cartones desde Drive.\n"
        "También puedes usar /img 1750.",
        parse_mode="Markdown"
    )

async def img(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_csvs_ready()
    if not context.args:
        await update.message.reply_text("Uso: /img <número|nombre|id>\nEj: /img 1750")
        return

    query = " ".join(context.args).strip()
    res = await get_image_inputfile(query)
    if not res:
        await update.message.reply_text("No encontré esa imagen en la carpeta de Drive.")
        return

    input_file, fname = res
    msg = await update.message.reply_photo(photo=input_file, caption=fname)

    # Log a ventas.csv
    user = update.effective_user
    row = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "usuario_id": str(user.id),
        "nombre_usuario": user.username or user.full_name or "",
        "imagen": fname,
    }
    await csv_append_row(VENTAS_CSV, VENTAS_HEADERS, row)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_csvs_ready()
    text = (update.message.text or "").strip()
    nums = parse_numbers(text)
    if not nums:
        await update.message.reply_text("No detecté números válidos. Ejemplo: 1 3 5-8")
        return

    # Enviar en grupos de hasta 10 (Telegram limita los media groups)
    sent_any = False
    batch: List[InputMediaPhoto] = []
    batch_names: List[str] = []
    uploaded_files_to_close: List[io.BufferedReader] = []

    async def flush_batch():
        nonlocal sent_any, batch, batch_names, uploaded_files_to_close
        if not batch:
            return
        # Enviar como media group
        for i in range(0, len(batch), 10):
            chunk = batch[i:i+10]
            await context.bot.send_media_group(chat_id=update.effective_chat.id, media=chunk)
        # Log
        user = update.effective_user
        for fname in batch_names:
            row = {
                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                "usuario_id": str(user.id),
                "nombre_usuario": user.username or user.full_name or "",
                "imagen": fname,
            }
            await csv_append_row(VENTAS_CSV, VENTAS_HEADERS, row)
        # Limpieza
        for f in uploaded_files_to_close:
            try: f.close()
            except Exception: pass
        batch, batch_names, uploaded_files_to_close = [], [], []
        sent_any = True

    # Construir lote
    for n in nums:
        res = await get_image_inputfile(n)
        if not res:
            await update.message.reply_text(f"❌ No encontré '{n}' en Drive.")
            continue
        input_file, fname = res
        media = InputMediaPhoto(media=input_file, caption=fname)
        batch.append(media)
        batch_names.append(fname)
        # El InputFile ya usa BytesIO, no hace falta cerrar streams extra aquí

        if len(batch) == 10:
            await flush_batch()

    await flush_batch()
    if not sent_any:
        await update.message.reply_text("No se envió ninguna imagen.")

# =========================
# Arranque como Web Service (Webhook)
# =========================
async def on_startup(app: Application):
    await ensure_csvs_ready()
    if not PUBLIC_URL:
        log.warning("PUBLIC_URL/RENDER_EXTERNAL_URL no definido; webhook no se registrará.")
        return
    webhook_path = f"/webhook/{BOT_TOKEN}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"
    await app.bot.delete_webhook(drop_pending_updates=True)
    await app.bot.set_webhook(url=webhook_url)
    log.info(f"Webhook registrado en: {webhook_url}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("img", img))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Startup hook
    app.post_init = on_startup

    # Ejecutar servidor webhook (aiohttp interno de PTB)
    webhook_path = f"/webhook/{BOT_TOKEN}"
    log.info(f"Iniciando servidor en 0.0.0.0:{PORT}{webhook_path}")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        # Importante: PUBLIC_URL usado en on_startup para set_webhook
    )

if __name__ == "__main__":
    main()

