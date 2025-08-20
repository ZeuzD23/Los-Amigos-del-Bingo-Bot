import os
import re
import io
import asyncio
import logging
from pathlib import Path
import tempfile
import time
from datetime import datetime
import pandas as pd

from telegram import Update, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut, RetryAfter

# =====================
# CONFIG
# =====================
TOKEN = "7566059380:AAHU6w0i3oc2CkMtmabbTQpG7OuR23DZB28"
IMAGENES_FOLDER = r"E:\\Telgram bot\\Imagenes cartones 1"
CSV_FILE = "registro.csv"
USUARIOS_FILE = "usuarios.csv"
LOTES_FILE = "lotes.csv"
RANGO_FILE = "rango.txt"
DEVOLUCIONES_FILE = "devoluciones.csv"
ADMIN_ID = 1644932856

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# Append-only logs (crash safety)
VENTAS_LOG = "ventas.log"
DEVOL_LOG = "devoluciones.log"

# Users waiting to send their username
usuarios_pendientes: set[int] = set()

# Kicked users (until next restart)
kicked_users: set[int] = set()

# =====================
# ATOMIC CSV + LOG HELPERS (Windows-safe)
# =====================
def atomic_write_csv(df: pd.DataFrame, path: str, retries: int = 6, base_sleep: float = 0.25):
    """Write CSV atomically in the same directory. Retries on PermissionError (Excel/AV)."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    base = os.path.basename(path)
    last_exc = None
    for attempt in range(retries):
        fd, tmp = tempfile.mkstemp(prefix=f"~{base}.", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
                df.to_csv(f, index=False)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.replace(tmp, path)  # atomic on same filesystem
                return
            except PermissionError as e:
                last_exc = e
                time.sleep(base_sleep * (attempt + 1))
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
    if last_exc:
        raise PermissionError(f"No se pudo escribir {path}. ¿Está abierto en Excel?") from last_exc

def append_log_line(path: str, line: str, retries: int = 5, base_sleep: float = 0.15):
    """Append a single line and fsync, with retries for Windows."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    last_exc = None
    for attempt in range(retries):
        try:
            with open(path, "a", encoding="utf-8", newline="") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())
            return
        except PermissionError as e:
            last_exc = e
            time.sleep(base_sleep * (attempt + 1))
    if last_exc:
        raise last_exc

def canon(s: str) -> str:
    return (s or "").strip().casefold()

# =====================
# DATA STORE (in-memory cache with locks)
# =====================
class Store:
    def __init__(self):
        self.users_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "nombre_completo"])
        self.reg_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"])
        self.lotes_df = pd.DataFrame(columns=["nombre_usuario", "carton"])
        self.devs_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por", "fecha"])

        self.users_lock = asyncio.Lock()
        self.reg_lock = asyncio.Lock()
        self.lotes_lock = asyncio.Lock()
        self.devs_lock = asyncio.Lock()

    def load_all_sync(self):
        if os.path.exists(USUARIOS_FILE):
            self.users_df = pd.read_csv(USUARIOS_FILE)
        if os.path.exists(CSV_FILE):
            self.reg_df = pd.read_csv(CSV_FILE)
            if "devuelto_por" not in self.reg_df.columns:
                self.reg_df["devuelto_por"] = ""
        if os.path.exists(LOTES_FILE):
            self.lotes_df = pd.read_csv(LOTES_FILE)
        if os.path.exists(DEVOLUCIONES_FILE):
            self.devs_df = pd.read_csv(DEVOLUCIONES_FILE)

    async def save_users(self):
        async with self.users_lock:
            df = self.users_df.copy()
        await asyncio.to_thread(atomic_write_csv, df, USUARIOS_FILE)

    async def save_reg(self):
        async with self.reg_lock:
            df = self.reg_df.copy()
        await asyncio.to_thread(atomic_write_csv, df, CSV_FILE)

    async def save_lotes(self):
        async with self.lotes_lock:
            df = self.lotes_df.copy()
        await asyncio.to_thread(atomic_write_csv, df, LOTES_FILE)

    async def save_devs(self):
        async with self.devs_lock:
            df = self.devs_df.copy()
        await asyncio.to_thread(atomic_write_csv, df, DEVOLUCIONES_FILE)

store = Store()

# =====================
# RANGE (cache)
# =====================
RANGO: tuple[int, int] | None = None

def load_rango_from_disk_sync():
    global RANGO
    p = Path(RANGO_FILE)
    if not p.exists():
        RANGO = None
        return
    try:
        ini, fin = map(int, p.read_text(encoding="utf-8").strip().split(","))
        RANGO = (ini, fin)
    except Exception as e:
        logger.error(f"Error leyendo rango: {e}")
        RANGO = None

# =====================
# STARTUP RECONCILIATION (recover from crash using logs)
# =====================
def parse_log_line(line: str):
    # tipo;user_id;nombre;imagen;timestamp;extra?
    parts = [p.strip() for p in line.strip().split(";")]
    if len(parts) < 5:
        return None
    tipo, uid_s, nombre, imagen_s, ts = parts[:5]
    if tipo not in ("venta", "devol"):
        return None
    try:
        uid = int(uid_s)
        imagen = int(imagen_s)
    except:
        return None
    extra = parts[5] if len(parts) > 5 else ""
    return {"tipo": tipo, "usuario_id": uid, "nombre_usuario": nombre, "imagen": imagen, "ts": ts, "extra": extra}

def reconcile_from_logs_sync():
    """Recover missing rows from ventas/devol logs into CSVs (idempotent)."""
    ventas = []
    if os.path.exists(VENTAS_LOG):
        with open(VENTAS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                v = parse_log_line(line)
                if v and v["tipo"] == "venta":
                    ventas.append(v)
    devols = []
    if os.path.exists(DEVOL_LOG):
        with open(DEVOL_LOG, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                d = parse_log_line(line)
                if d and d["tipo"] == "devol":
                    devols.append(d)

    # Merge ventas to reg_df
    if ventas:
        df = store.reg_df.copy()
        existing = set((int(r["usuario_id"]), str(r["nombre_usuario"]), int(r["imagen"])) for r in df.to_dict("records"))
        new_rows = []
        for v in ventas:
            key = (v["usuario_id"], v["nombre_usuario"], v["imagen"])
            if key not in existing:
                new_rows.append([v["usuario_id"], v["nombre_usuario"], v["imagen"], ""])
                existing.add(key)
        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows, columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"])], ignore_index=True)
            store.reg_df = df
            try:
                atomic_write_csv(df, CSV_FILE)
            except Exception as e:
                logger.warning(f"No se pudo guardar registro.csv en reconciliación: {e}")

    # Apply devoluciones: remove from reg and log to devs
    if devols:
        df = store.reg_df.copy()
        to_remove_mask = pd.Series(False, index=df.index)
        dev_rows = []
        for d in devols:
            mask = (
                (df["usuario_id"].astype(int) == d["usuario_id"]) &
                (df["nombre_usuario"].astype(str) == d["nombre_usuario"]) &
                (df["imagen"].astype(int) == d["imagen"])
            )
            if mask.any():
                to_remove_mask = to_remove_mask | mask
                dev_rows.append([d["usuario_id"], d["nombre_usuario"], d["imagen"], d.get("extra", ""), d["ts"]])
        if to_remove_mask.any():
            df2 = df[~to_remove_mask]
            store.reg_df = df2
            try:
                atomic_write_csv(df2, CSV_FILE)
            except Exception as e:
                logger.warning(f"No se pudo guardar registro.csv (post-devol): {e}")
        if dev_rows:
            dev_df = store.devs_df.copy()
            dev_df = pd.concat([dev_df, pd.DataFrame(dev_rows, columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por", "fecha"])], ignore_index=True)
            store.devs_df = dev_df
            try:
                atomic_write_csv(dev_df, DEVOLUCIONES_FILE)
            except Exception as e:
                logger.warning(f"No se pudo guardar devoluciones.csv: {e}")

# =====================
# HELPERS
# =====================
def agrupar_en_rangos(numeros: list[int]) -> list[str]:
    if not numeros:
        return []
    numeros = sorted(numeros)
    rangos = []
    start = prev = numeros[0]
    for n in numeros[1:]:
        if n == prev + 1:
            prev = n
        else:
            rangos.append(str(start) if start == prev else f"{start}-{prev}")
            start = prev = n
    rangos.append(str(start) if start == prev else f"{start}-{prev}")
    return rangos

def parse_numeros(tokens: list[str]) -> set[int] | None:
    numeros: set[int] = set()
    for t in tokens:
        if re.match(r"^\d+-\d+$", t):
            a, b = map(int, t.split("-"))
            if a <= b:
                numeros.update(range(a, b + 1))
        elif t.isdigit():
            numeros.add(int(t))
        else:
            return None
    return numeros

def is_kicked(uid: int) -> bool:
    return uid in kicked_users

# Network retries
async def reply_text_retry(message, text, **kwargs):
    for delay in (0, 0.5, 1):
        try:
            return await message.reply_text(text, **kwargs)
        except (NetworkError, TimedOut):
            await asyncio.sleep(delay)
    return await message.reply_text(text, **kwargs)

async def send_media_group_retry(bot, chat_id, media, **kwargs):
    for delay in (0, 0.5, 1):
        try:
            return await bot.send_media_group(chat_id=chat_id, media=media, **kwargs)
        except (NetworkError, TimedOut):
            await asyncio.sleep(delay)
    return await bot.send_media_group(chat_id=chat_id, media=media, **kwargs)

# Global error handler
async def error_handler(update, context):
    try:
        raise context.error
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
    except (NetworkError, TimedOut) as e:
        logger.warning(f"Error de red: {e}")
        await asyncio.sleep(0.5)
    except Exception:
        logger.exception("Excepción no controlada")

# =====================
# COMMANDS (ADMIN + ALL) – single definitions to avoid duplicates
# =====================
async def info_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await reply_text_retry(update.message, "⛔ Comando solo para el administrador.")
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
    await reply_text_retry(update.message, texto, parse_mode=ParseMode.HTML)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_kicked(update.effective_user.id):
        await reply_text_retry(update.message, "⛔ Has sido bloqueado temporalmente por el administrador.")
        return
    is_admin = update.effective_user.id == ADMIN_ID
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
    await reply_text_retry(update.message, texto, parse_mode=ParseMode.HTML)

async def usuarios_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    async with store.users_lock:
        df = store.users_df.copy()
    if df.empty:
        await reply_text_retry(update.message, "📇 No hay usuarios registrados.")
        return
    df = df.astype({"usuario_id": "int64"})
    df = df.sort_values("nombre_usuario", key=lambda s: s.str.lower())
    lineas = [f"👤 {row['nombre_usuario']} — ID: {row['usuario_id']}" for _, row in df.iterrows()]
    await reply_text_retry(update.message, "🧑‍💻 <b>Usuarios registrados</b>\n" + "\n".join(lineas), parse_mode=ParseMode.HTML)

async def kick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) != 1:
        await reply_text_retry(update.message, "Uso: /kick <nombre_usuario>")
        return
    target = canon(context.args[0])
    async with store.users_lock:
        df = store.users_df.copy()
        mask = df["nombre_usuario"].astype(str).str.casefold() == target
        matches = df[mask]
        if matches.empty:
            await reply_text_retry(update.message, f"No encontré al usuario '{context.args[0]}'.")
            return
        ids = set(matches["usuario_id"].astype(int).tolist())
        for uid in ids:
            kicked_users.add(uid)
        store.users_df = df[~mask]
    await store.save_users()
    listado = ", ".join(f"{u}({i})" for u, i in zip(matches["nombre_usuario"], matches["usuario_id"]))
    await reply_text_retry(update.message, f"⛔ Usuario(s) bloqueado(s) hasta reinicio: {listado}")

async def lista(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    async with store.reg_lock:
        df = store.reg_df.copy()
    if df.empty:
        ventas_txt = "📄 No se ha vendido ningún cartón."
    else:
        lineas = []
        grp = df.groupby("nombre_usuario")["imagen"].apply(lambda s: sorted(map(int, s.tolist()))).reset_index()
        for _, row in grp.iterrows():
            nums = row["imagen"]
            lineas.append(f"👤 {row['nombre_usuario']} (total {len(nums)}): " + ", ".join(map(str, nums)))
        ventas_txt = "\n".join(lineas)

    async with store.devs_lock:
        devs = store.devs_df.copy()
    if devs.empty:
        devs_txt = "—"
    else:
        dlines = []
        dgrp = devs.groupby("nombre_usuario")["imagen"].apply(lambda s: sorted(map(int, s.tolist()))).reset_index()
        for _, row in dgrp.iterrows():
            dlines.append(f"♻️ {row['nombre_usuario']}: " + ", ".join(map(str, row["imagen"])))
        devs_txt = "\n".join(dlines)

    mensaje = "🧾 <b>Ventas actuales</b>\n" + ventas_txt + "\n\n" + "♻️ <b>Devoluciones</b>\n" + devs_txt
    await reply_text_retry(update.message, mensaje, parse_mode=ParseMode.HTML)

async def definir_rango(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) != 2:
        await reply_text_retry(update.message, "Uso correcto: /rango <inicio> <fin>")
        return
    try:
        inicio = int(context.args[0]); fin = int(context.args[1])
    except ValueError:
        await reply_text_retry(update.message, "⚠️ Debes enviar dos números enteros válidos.")
        return
    if inicio > fin:
        await reply_text_retry(update.message, "⚠️ El número de inicio debe ser menor o igual que el de fin.")
        return
    global RANGO
    RANGO = (inicio, fin)
    try:
        Path(RANGO_FILE).write_text(f"{inicio},{fin}", encoding="utf-8")
    except Exception as e:
        await reply_text_retry(update.message, f"⚠️ Error guardando rango: {e}")
        return
    await reply_text_retry(update.message, f"✅ Rango definido: desde {inicio} hasta {fin}.")

async def enviar_directo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    numeros = parse_numeros(context.args)
    if numeros is None or not numeros:
        await reply_text_retry(update.message, "Uso: /c <número(s)> o rangos (ej: 1 2 5-10)")
        return
    await reply_text_retry(update.message, f"📨 Enviando cartones: {', '.join(map(str, sorted(numeros)))}\n⏳ Espere...")
    for n in sorted(numeros):
        ruta = os.path.join(IMAGENES_FOLDER, f"{n}.jpg")
        if os.path.exists(ruta):
            with open(ruta, "rb") as f:
                # send one by one (doesn't register sale)
                try:
                    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=f)
                except Exception:
                    pass
        else:
            await reply_text_retry(update.message, f"❌ No se encontró el cartón N° {n}.")

async def ver_lote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) != 1:
        await reply_text_retry(update.message, "Uso: /ver_lote <nombre_usuario>")
        return
    target_canon = canon(context.args[0])
    async with store.lotes_lock:
        dfl = store.lotes_df.copy()
    mask = dfl["nombre_usuario"].astype(str).str.casefold() == target_canon
    nums = sorted(dfl[mask]["carton"].astype(int).tolist())
    display = context.args[0]
    if not nums:
        await reply_text_retry(update.message, f"'{display}' no tiene cartones asignados.")
        return
    await reply_text_retry(update.message, f"Cartones asignados a '{display}': " + ", ".join(map(str, nums)))

async def quitar_lote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await reply_text_retry(update.message, "Uso: /quitar_lote <nombre_usuario> <números/rangos>")
        return
    target_canon = canon(context.args[0])
    nums = parse_numeros(context.args[1:])
    if nums is None or not nums:
        await reply_text_retry(update.message, "⚠️ No se detectaron números válidos para quitar.")
        return
    async with store.lotes_lock:
        dfl = store.lotes_df.copy()
        keep_mask = ~((dfl["nombre_usuario"].astype(str).str.casefold() == target_canon) & (dfl["carton"].astype(int).isin(nums)))
        quitados = len(dfl) - int(keep_mask.sum())
        store.lotes_df = dfl[keep_mask]
    await store.save_lotes()
    if quitados > 0:
        await reply_text_retry(update.message, f"✅ Quitados {quitados} cartones del lote de '{context.args[0]}'.")
    else:
        await reply_text_retry(update.message, "ℹ️ No se encontró ninguno de esos cartones en el lote.")

async def comando_lote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Assign numbers to a user (case-insensitive), skipping conflicts with others."""
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await reply_text_retry(update.message, "Uso: /lote <nombre_usuario> <números/rangos> (ej: /lote Diego 1-5 10 12)")
        return
    raw_name = context.args[0]
    target_canon = canon(raw_name)
    numeros = parse_numeros(context.args[1:])
    if numeros is None or not numeros:
        await reply_text_retry(update.message, "⚠️ No se detectaron números válidos para asignar.")
        return

    async with store.lotes_lock:
        dfl = store.lotes_df.copy()
        owner_by_num = {int(r.carton): str(r.nombre_usuario) for r in dfl.itertuples(index=False)}
        nuevos = []
        ya_mios = []
        conflictos = []
        for n in sorted(numeros):
            owner = owner_by_num.get(n)
            if owner is None:
                nuevos.append(n)
            else:
                if canon(owner) == target_canon:
                    ya_mios.append(n)
                else:
                    conflictos.append((n, owner))
        if nuevos:
            add = pd.DataFrame([[raw_name, n] for n in nuevos], columns=["nombre_usuario", "carton"])
            store.lotes_df = pd.concat([dfl, add], ignore_index=True)
        else:
            store.lotes_df = dfl
    await store.save_lotes()

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
    await reply_text_retry(update.message, "\n".join(partes))

    # Notify user if registered
    async with store.users_lock:
        dfu = store.users_df.copy()
    mask = dfu["nombre_usuario"].astype(str).str.casefold() == target_canon
    if nuevos and mask.any():
        uid = int(dfu[mask].iloc[0]["usuario_id"])
        try:
            await context.bot.send_message(chat_id=uid, text=f"📦 Se te han asignado nuevos cartones: {', '.join(map(str, nuevos))}")
        except Exception:
            pass

async def mostrar_vendidos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: global totals. User: own totals."""
    uid = update.effective_user.id
    async with store.reg_lock:
        df = store.reg_df.copy()
    if df.empty or "imagen" not in df.columns:
        await reply_text_retry(update.message, "📦 Aún no se ha vendido ningún cartón.")
        return
    if uid == ADMIN_ID:
        vendidos = sorted(set(int(x) for x in df["imagen"].tolist()))
        await reply_text_retry(update.message, f"🧾 Total vendidos (global): {len(vendidos)}\n🔢 Números: {', '.join(map(str, vendidos))}")
        return
    # user-specific
    propios = sorted(set(int(x) for x in df[df["usuario_id"] == uid]["imagen"].tolist()))
    if propios:
        await reply_text_retry(update.message, f"🧾 Tus vendidos: {len(propios)}\n🔢 Números: {', '.join(map(str, propios))}")
    else:
        await reply_text_retry(update.message, "ℹ️ Aún no has vendido ningún cartón.")

async def mostrar_disponibles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show ONLY the current user's available numbers."""
    global RANGO
    if RANGO is None:
        load_rango_from_disk_sync()
    if RANGO is None:
        await reply_text_retry(update.message, "❌ No hay rango definido aún. Usa /rango para definir uno.")
        return
    inicio, fin = RANGO

    uid = update.effective_user.id
    # Ensure registered
    async with store.users_lock:
        if uid not in set(store.users_df["usuario_id"].values):
            await reply_text_retry(update.message, "Debes registrarte primero. Envía tu <b>nombre de usuario</b>.", parse_mode=ParseMode.HTML)
            return
        nombre_usuario = store.users_df.loc[store.users_df["usuario_id"] == uid, "nombre_usuario"].values[0]

    async with store.reg_lock:
        vendidos = set(map(int, store.reg_df["imagen"].tolist())) if "imagen" in store.reg_df.columns else set()
    async with store.lotes_lock:
        mis_asignados = set(map(int, store.lotes_df[store.lotes_df["nombre_usuario"].astype(str).str.casefold() == canon(nombre_usuario)]["carton"].tolist()))
        asignados_por_num = {int(r.carton): r.nombre_usuario for r in store.lotes_df.itertuples(index=False)}

    disponibles = []
    if mis_asignados:
        for n in sorted(mis_asignados):
            if inicio <= n <= fin and n not in vendidos:
                disponibles.append(n)
    else:
        for n in range(inicio, fin + 1):
            if n in vendidos: 
                continue
            if n in asignados_por_num:
                continue
            disponibles.append(n)

    if not disponibles:
        await reply_text_retry(update.message, "ℹ️ No tienes cartones disponibles para pedir ahora mismo.")
        return
    await reply_text_retry(update.message, "🎟️ <b>Tus</b> cartones disponibles:\n" + ", ".join(agrupar_en_rangos(disponibles)), parse_mode=ParseMode.HTML)

async def remover_carton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    numeros = parse_numeros(context.args)
    if numeros is None or not numeros:
        await reply_text_retry(update.message, "Uso: /r <número(s)/rangos> (ej: /r 2 4 6-9)")
        return
    uid = update.effective_user.id
    user_name = update.effective_user.full_name

    async with store.reg_lock:
        df = store.reg_df
        mask = (df["usuario_id"] == uid) & (df["imagen"].astype(int).isin(numeros))
        cartones_usuario = df[mask]
        if cartones_usuario.empty:
            await reply_text_retry(update.message, "❌ No tienes ninguno de los cartones indicados para devolver.")
            return
        if "devuelto_por" not in df.columns:
            df["devuelto_por"] = ""
        # log lines first (crash safety)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for _, fila in cartones_usuario.iterrows():
            append_log_line(DEVOL_LOG, f"devol;{int(fila['usuario_id'])};{fila['nombre_usuario']};{int(fila['imagen'])};{ts};{user_name}")
        # apply removal
        df.loc[mask, "devuelto_por"] = user_name
        store.reg_df = df[~mask]
        dev_rows = pd.DataFrame([
            [int(fila["usuario_id"]), str(fila["nombre_usuario"]), int(fila["imagen"]), user_name, ts]
            for _, fila in cartones_usuario.iterrows()
        ], columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por", "fecha"])
    async with store.devs_lock:
        store.devs_df = pd.concat([store.devs_df, dev_rows], ignore_index=True)
    await store.save_reg()
    await store.save_devs()

    cartones_devueltos = sorted(cartones_usuario["imagen"].astype(int).tolist())
    await reply_text_retry(update.message, f"♻️ Cartones devueltos: {', '.join(map(str, cartones_devueltos))}")

async def vendido_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /vendido <usuario> <nums/rangos> — register sales for that user with checks."""
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await reply_text_retry(update.message, "Uso: /vendido <nombre_usuario> <números/rangos>\nEj: /vendido diego 1-5 10 12")
        return
    target_input = context.args[0]
    numeros = parse_numeros(context.args[1:])
    if numeros is None or not numeros:
        await reply_text_retry(update.message, "⚠️ No se detectaron números válidos para registrar.")
        return

    # Find user by name (case-insensitive)
    async with store.users_lock:
        mask = store.users_df["nombre_usuario"].astype(str).str.casefold() == canon(target_input)
        if not mask.any():
            await reply_text_retry(update.message, f"⚠️ El usuario '{target_input}' no está registrado.")
            return
        row = store.users_df[mask].iloc[0]
        target_uid = int(row["usuario_id"])
        target_name = str(row["nombre_usuario"])

    # Check assignments and sold
    async with store.lotes_lock:
        owner_by_num = {int(r.carton): str(r.nombre_usuario) for r in store.lotes_df.itertuples(index=False)}
        my_lote = set(int(x) for x in store.lotes_df[store.lotes_df["nombre_usuario"].astype(str).str.casefold() == canon(target_name)]["carton"].tolist())
    async with store.reg_lock:
        sold_global = set(int(x) for x in store.reg_df["imagen"].tolist())

    global RANGO
    if RANGO is None:
        load_rango_from_disk_sync()
    check_range = RANGO is not None
    if check_range:
        inicio, fin = RANGO

    permitidos, ya_vendidos, de_otro, fuera_lote, fuera_rango = [], [], [], [], []
    tiene_lote = len(my_lote) > 0
    for n in sorted(numeros):
        if check_range and not (inicio <= n <= fin):
            fuera_rango.append(n); continue
        if n in sold_global:
            ya_vendidos.append(n); continue
        owner = owner_by_num.get(n)
        if owner is not None and canon(owner) != canon(target_name):
            de_otro.append((n, owner)); continue
        if tiene_lote and (owner is None):
            fuera_lote.append(n); continue
        permitidos.append(n)

    # Append ventas to log first (durable), then to CSV
    if permitidos:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for n in permitidos:
            append_log_line(VENTAS_LOG, f"venta;{target_uid};{target_name};{n};{ts}")
        async with store.reg_lock:
            nuevos = pd.DataFrame([[target_uid, target_name, n, ""] for n in permitidos], columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"])
            store.reg_df = pd.concat([store.reg_df, nuevos], ignore_index=True)
        await store.save_reg()

    partes = []
    if permitidos:
        partes.append("✅ Registrados como vendidos: " + ", ".join(map(str, permitidos)))
    if ya_vendidos:
        partes.append("ℹ️ Ya estaban vendidos: " + ", ".join(map(str, ya_vendidos)))
    if de_otro:
        partes.append("⛔ Asignados a otra persona: " + ", ".join(f"{n}→{d}" for n, d in de_otro))
    if fuera_lote:
        partes.append("⚠️ Fuera del lote del usuario: " + ", ".join(map(str, fuera_lote)))
    if fuera_rango:
        partes.append("⚠️ Fuera del rango definido: " + ", ".join(map(str, fuera_rango)))
    if not partes:
        partes.append("No se registró ningún número.")
    await reply_text_retry(update.message, "\n".join(partes))

async def reset_registro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # rotate files (ignore if missing)
    def rotate(pth, prefix):
        if os.path.exists(pth):
            try:
                os.replace(pth, f"{prefix}_{ts}.csv")
            except Exception:
                # fallback: copy content and truncate
                try:
                    with open(pth, "r", encoding="utf-8") as f:
                        data = f.read()
                    with open(f"{prefix}_{ts}.csv", "w", encoding="utf-8") as f2:
                        f2.write(data)
                except Exception:
                    pass
                try:
                    open(pth, "w").close()
                except Exception:
                    pass

    rotate(CSV_FILE, "Registro")
    rotate(USUARIOS_FILE, "Usuarios")
    rotate(LOTES_FILE, "Lotes")
    rotate(DEVOLUCIONES_FILE, "Devoluciones")
    # rotate logs too
    if os.path.exists(VENTAS_LOG):
        os.replace(VENTAS_LOG, f"ventas_{ts}.log")
    if os.path.exists(DEVOL_LOG):
        os.replace(DEVOL_LOG, f"devoluciones_{ts}.log")

    async with store.users_lock, store.reg_lock, store.lotes_lock, store.devs_lock:
        store.users_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "nombre_completo"])
        store.reg_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"])
        store.lotes_df = pd.DataFrame(columns=["nombre_usuario", "carton"])
        store.devs_df = pd.DataFrame(columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por", "fecha"])

    # persist empties atomically
    await store.save_users()
    await store.save_reg()
    await store.save_lotes()
    await store.save_devs()
    await reply_text_retry(update.message, "🔄 Registro, usuarios, lotes y devoluciones reiniciados.")

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply_text_retry(update.message, f"Tu ID: {update.effective_user.id}\nNombre: {update.effective_user.full_name}")

# =====================
# MESSAGE HANDLER (single; avoids duplicate replies)
# =====================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_kicked(update.effective_user.id):
        await reply_text_retry(update.message, "⛔ Has sido bloqueado temporalmente por el administrador.")
        return

    uid = update.effective_user.id
    nombre_completo = update.effective_user.full_name
    mensaje = (update.message.text or "").strip()

    # Registration flow
    async with store.users_lock:
        dfu = store.users_df.copy()
        registrados = set(dfu["usuario_id"].values)
    if uid not in registrados:
        if uid not in usuarios_pendientes:
            usuarios_pendientes.add(uid)
            await reply_text_retry(update.message, "Por favor, envía el <b>nombre de usuario</b> que quieres registrar.", parse_mode=ParseMode.HTML)
            return
        else:
            nombre_usuario = mensaje.strip()
            if not nombre_usuario:
                await reply_text_retry(update.message, "El nombre de usuario no puede estar vacío. Envía un nombre válido.")
                return
            nombres_norm = set(dfu["nombre_usuario"].astype(str).str.casefold().tolist())
            if canon(nombre_usuario) in nombres_norm:
                await reply_text_retry(update.message, "⚠️ Ese nombre de usuario ya está en uso (no distingue mayúsculas). Elige otro.")
                return
            async with store.users_lock:
                store.users_df = pd.concat([store.users_df, pd.DataFrame([[uid, nombre_usuario, nombre_completo]], columns=["usuario_id", "nombre_usuario", "nombre_completo"])], ignore_index=True)
            await store.save_users()
            usuarios_pendientes.discard(uid)

            comandos_info = (
                f"¡Hola {nombre_usuario}! Ya estás registrado.\n\n"
                "📋 <b>Comandos</b>:\n"
                "• Envía números o rangos para pedir cartones. Ej: <code>1 3 5-8</code>\n"
                "• <code>/help</code> — Ver ayuda.\n"
                "• <code>/v</code> — Ver tus cartones vendidos.\n"
                "• <code>/r &lt;números/rangos&gt;</code> — Devolver cartones. Ej: <code>/r 2 4 6-9</code>\n"
                "• <code>/disp</code> — Ver <u>tus</u> disponibles.\n"
            )
            await reply_text_retry(update.message, comandos_info, parse_mode=ParseMode.HTML)

            # Inform assigned numbers (case-insensitive)
            async with store.lotes_lock:
                dfl = store.lotes_df.copy()
            mask = dfl["nombre_usuario"].astype(str).str.casefold() == canon(nombre_usuario)
            lotes_usuario = sorted(dfl[mask]["carton"].astype(int).tolist())
            if lotes_usuario:
                await reply_text_retry(update.message, f"Tienes asignados estos cartones (solo podrás pedir estos): {', '.join(map(str, lotes_usuario))}")
            return

    # Parse request numbers
    numeros = parse_numeros(mensaje.split())
    if numeros is None or not numeros:
        await reply_text_retry(update.message, "⚠️ No se detectaron números válidos.")
        return

    # User's name and assignments
    async with store.users_lock:
        mi_nombre = store.users_df.loc[store.users_df["usuario_id"] == uid, "nombre_usuario"].values[0]
    async with store.lotes_lock:
        dfl = store.lotes_df.copy()
    asignados_por_num = {int(r.carton): canon(str(r.nombre_usuario)) for r in dfl.itertuples(index=False)}
    mis_asignados = set(int(r.carton) for r in dfl.itertuples(index=False) if canon(str(r.nombre_usuario)) == canon(mi_nombre))
    tiene_lote = len(mis_asignados) > 0

    permitidos: set[int] = set()
    bloqueados_otro = []
    fuera_de_mi_lote = []
    for n in sorted(numeros):
        duenio = asignados_por_num.get(n)
        if duenio is not None:
            if duenio == canon(mi_nombre):
                permitidos.add(n)
            else:
                # find display owner
                du_rows = dfl[dfl["carton"].astype(int) == n]["nombre_usuario"].astype(str).tolist()
                bloqueados_otro.append((n, du_rows[0] if du_rows else "otro"))
        else:
            if tiene_lote:
                fuera_de_mi_lote.append(n)
            else:
                permitidos.add(n)

    if bloqueados_otro:
        await reply_text_retry(update.message, "⛔ No puedes pedir cartones asignados a otra persona: " + ", ".join(f"{n}({d})" for n, d in bloqueados_otro))
    if fuera_de_mi_lote:
        await reply_text_retry(update.message, "⚠️ Estos cartones no están en tu lote: " + ", ".join(map(str, fuera_de_mi_lote)))
    if not permitidos:
        await reply_text_retry(update.message, "ℹ️ No hay cartones válidos para enviar según tus asignaciones.")
        return

    # Not sold yet
    async with store.reg_lock:
        enviados_usuario = set(int(x) for x in store.reg_df[store.reg_df["usuario_id"] == uid]["imagen"].tolist())
        vendidos_global = set(int(x) for x in store.reg_df["imagen"].tolist())
    a_enviar = [n for n in sorted(permitidos) if n not in enviados_usuario and n not in vendidos_global]
    if not a_enviar:
        await reply_text_retry(update.message, "✅ Ya se enviaron todos los cartones solicitados o no están disponibles.")
        return

    await reply_text_retry(update.message, f"📨 Enviando cartones N°: {', '.join(map(str, a_enviar))}\n⏳ Por favor, espere...")

    # Build media group and register sales (log first)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    media_group, ok_nums, open_files = [], [], []
    for n in a_enviar:
        ruta = os.path.join(IMAGENES_FOLDER, f"{n}.jpg")
        if os.path.exists(ruta):
            f = open(ruta, "rb")
            open_files.append(f)
            media_group.append(InputMediaPhoto(media=f, filename=f"{n}.jpg"))
            ok_nums.append(n)
        else:
            await reply_text_retry(update.message, f"❌ No se encontró el cartón N° {n}.")

    if media_group:
        # Persist intent in ventas.log first (so we can recover after crash)
        for n in ok_nums:
            append_log_line(VENTAS_LOG, f"venta;{uid};{mi_nombre};{n};{ts}")
        try:
            for i in range(0, len(media_group), 10):
                await send_media_group_retry(context.bot, chat_id=update.effective_chat.id, media=media_group[i:i+10])
        finally:
            for f in open_files:
                try: f.close()
                except Exception: pass

        # Save to CSV
        async with store.reg_lock:
            nuevos = pd.DataFrame([[uid, mi_nombre, n, ""] for n in ok_nums], columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"])
            store.reg_df = pd.concat([store.reg_df, nuevos], ignore_index=True)
        await store.save_reg()
# ========= /reload (solo admin) =========
async def reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recarga desde los CSV a la memoria (usuarios, registro, lotes, devoluciones) y el rango."""
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        users_df = pd.read_csv(USUARIOS_FILE) if os.path.exists(USUARIOS_FILE) else pd.DataFrame(
            columns=["usuario_id", "nombre_usuario", "nombre_completo"]
        )
        reg_df = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else pd.DataFrame(
            columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por"]
        )
        if "devuelto_por" not in reg_df.columns:
            reg_df["devuelto_por"] = ""

        lotes_df = pd.read_csv(LOTES_FILE) if os.path.exists(LOTES_FILE) else pd.DataFrame(
            columns=["nombre_usuario", "carton"]
        )
        devs_df = pd.read_csv(DEVOLUCIONES_FILE) if os.path.exists(DEVOLUCIONES_FILE) else pd.DataFrame(
            columns=["usuario_id", "nombre_usuario", "imagen", "devuelto_por", "fecha"]
        )

        async with store.users_lock, store.reg_lock, store.lotes_lock, store.devs_lock:
            store.users_df = users_df
            store.reg_df = reg_df
            store.lotes_df = lotes_df
            store.devs_df = devs_df

        load_rango_from_disk_sync()
        reconcile_from_logs_sync()

        await reply_text_retry(
            update.message,
            (
                "🔄 Recarga completada.\n"
                f"👥 usuarios={len(users_df)} | 🧾 vendidos={len(reg_df)} | 📦 lotes={len(lotes_df)} | ♻️ devoluciones={len(devs_df)}\n"
                f"📐 rango={'sin definir' if RANGO is None else f'{RANGO[0]}–{RANGO[1]}'}"
            )
        )
    except Exception as e:
        await reply_text_retry(update.message, f"⚠️ Error al recargar: {e}")


# ========= Avisos de inicio/detención =========
async def _broadcast_to_all(app, text: str):
    """Envía un mensaje a todos los usuarios registrados (ignora errores por usuario)."""
    async with store.users_lock:
        ids = [int(x) for x in store.users_df["usuario_id"].tolist()]
    # Throttle suave para no gatillar flood control
    for uid in ids:
        try:
            await app.bot.send_message(chat_id=uid, text=text)
            await asyncio.sleep(0.05)
        except Exception:
            # Usuario bloqueó el bot o no tiene chat abierto; ignorar
            pass

async def _on_startup(app):
    # Aviso de que el bot está encendido
    try:
        await _broadcast_to_all(app, "✅ El bot está encendido nuevamente.")
    except Exception:
        pass

async def _on_shutdown(app):
    # Aviso de que el bot se detuvo
    try:
        await _broadcast_to_all(app, "⚠️ El bot se ha detenido. Volverá pronto.")
    except Exception:
        pass
# ========= PARCHE: /off (aviso y apagado en 2 minutos) =========
OFF_TASK = None
OFF_DEADLINE = None

async def _broadcast_to_all__off(app, text: str):
    """Envía un mensaje a todos los usuarios registrados (+ admin). Ignora errores individuales."""
    try:
        async with store.users_lock:
            ids = [int(x) for x in store.users_df["usuario_id"].tolist()]
    except Exception:
        ids = []
    if ADMIN_ID not in ids:
        ids.append(ADMIN_ID)
    for uid in ids:
        try:
            await app.bot.send_message(chat_id=uid, text=text)
            await asyncio.sleep(0.05)  # leve throttle
        except Exception:
            pass  # usuario bloqueó el bot o no tiene chat; ignorar

async def _shutdown_after_delay__off(app):
    """Espera 120s, avisa y detiene la aplicación."""
    global OFF_TASK, OFF_DEADLINE
    try:
        await asyncio.sleep(120)
        # Aviso final de apagado (si ya tienes un post_shutdown que avisa, puedes comentar la línea de abajo)
        await _broadcast_to_all__off(app, "⏹️ El bot se está apagando ahora. Hasta pronto.")
    finally:
        OFF_TASK = None
        OFF_DEADLINE = None
        try:
            app.stop()  # detiene el run_polling de forma limpia
        except Exception:
            pass

async def off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/off — Solo admin: anuncia apagado en 2 minutos y luego detiene el bot."""
    if update.effective_user.id != ADMIN_ID:
        return
    global OFF_TASK, OFF_DEADLINE

    # Evitar programar múltiples apagados
    if OFF_TASK and not OFF_TASK.done():
        try:
            loop = asyncio.get_running_loop()
            secs = int(max(0, OFF_DEADLINE - loop.time()))
        except Exception:
            secs = 120
        await reply_text_retry(update.message, f"⏳ Apagado ya programado en ~{secs} s.")
        return

    await reply_text_retry(update.message, "⚠️ Se anunciará apagado y el bot se detendrá en 2 minutos.")
    await _broadcast_to_all__off(context.application, "⚠️ Aviso: el bot se apagará en 2 minutos. y volvera a las 8 am")

    loop = asyncio.get_running_loop()
    OFF_DEADLINE = loop.time() + 120
    OFF_TASK = asyncio.create_task(_shutdown_after_delay__off(context.application))

# =====================
# MAIN
# =====================
if __name__ == "__main__":
    try:
        # Load CSVs
        store.load_all_sync()
        load_rango_from_disk_sync()
        # Reconcile from logs (in case of previous crash)
        reconcile_from_logs_sync()

        # Telegram client with wider timeouts
        request = HTTPXRequest(connect_timeout=15.0, read_timeout=45.0, write_timeout=45.0, pool_timeout=45.0)
        app = ApplicationBuilder().token(TOKEN).request(request).concurrent_updates(True).build()

        # Handlers (single set — avoids duplicates)
        app.add_handler(CommandHandler("help", help_cmd))
        app.add_handler(CommandHandler("info", info_admin))
        app.add_handler(CommandHandler("usuarios", usuarios_cmd))
        app.add_handler(CommandHandler("kick", kick_cmd))
        app.add_handler(CommandHandler("id", whoami))
        app.add_handler(CommandHandler("lista", lista))
        app.add_handler(CommandHandler("v", mostrar_vendidos))
        app.add_handler(CommandHandler("c", enviar_directo))
        app.add_handler(CommandHandler("r", remover_carton))
        app.add_handler(CommandHandler("reset", reset_registro))
        app.add_handler(CommandHandler("lote", comando_lote))
        app.add_handler(CommandHandler("ver_lote", ver_lote))
        app.add_handler(CommandHandler("quitar_lote", quitar_lote))
        app.add_handler(CommandHandler("vendido", vendido_cmd))
        app.add_handler(CommandHandler("rango", definir_rango))
        app.add_handler(CommandHandler("disp", mostrar_disponibles))
        app.add_handler(CommandHandler("reload", reload_cmd))
        app.add_handler(CommandHandler("off", off_cmd))  # <- nuevo comando solo admin
        app.post_init = _on_startup
        app.post_shutdown = _on_shutdown
        

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

        app.add_error_handler(error_handler)

        print("🤖 Bot iniciado...")
        app.run_polling()
    except Exception:
        import traceback
        print("\n💥 Ocurrió un error al iniciar el bot:\n")
        traceback.print_exc()
        input("\nPresiona Enter para cerrar...")
