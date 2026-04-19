"""
Telegram-бот v4: магазин + POX Rewards (уровни, вывод, скидки) + отзывы + админка.
"""
import asyncio, logging, os, re, math
from contextlib import suppress
from datetime import datetime

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    Message, ReplyKeyboardMarkup, KeyboardButton,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "8673713148:AAHrH3-mlQGmyjvw1Fe1VaPTUt93tG8b1FU")
ADMIN_ID = 8265035616
ADMIN_USERNAME = "PihuiY_manager"
DB_PATH = "shop.db"
REVIEW_CHANNEL_ID = -1003927685434
PAGE_SIZE = 8

GOOGLE_CREDS_FILE = "credentials.json"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# === POX CONFIG (editable via admin) ===
# Хранится в БД таблица pox_config, загружается при старте
DEFAULT_POX = {
    "tier1_name": "Новичок",
    "tier1_min_refs": 0,
    "tier1_earn": 1.0,       # pox за покупку реферала
    "tier1_withdraw": 110,   # ₽ за 1 pox при выводе
    "tier1_discount": 130,   # ₽ за 1 pox при скидке
    "tier2_name": "Партнёр",
    "tier2_min_refs": 3,
    "tier2_earn": 1.2,
    "tier2_withdraw": 120,
    "tier2_discount": 140,
    "tier3_name": "Амбассадор",
    "tier3_min_refs": 15,
    "tier3_earn": 1.5,
    "tier3_withdraw": 130,
    "tier3_discount": 150,
    "min_withdraw_rub": 250,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bot")

# Google Sheets
gs_sheet = None
try:
    if os.path.exists(GOOGLE_CREDS_FILE) and GOOGLE_SHEET_ID:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
        gs_sheet = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        for n,h in [("Заказы",["ts","oid","uid","user","product","price","disc","final","status"]),
                     ("Рефералы",["ts","ref_id","ref_user","new_id","new_user"]),
                     ("Продажи",["ts","oid","uid","user","product","price","disc","final"])]:
            try: gs_sheet.worksheet(n)
            except: gs_sheet.add_worksheet(title=n,rows=1000,cols=12).append_row(h)
        log.info("GSheets OK")
except Exception as e:
    log.warning(f"GSheets fail: {e}"); gs_sheet = None

def gs_append(s, r):
    if not gs_sheet: return
    try: gs_sheet.worksheet(s).append_row([str(x) for x in r])
    except: pass

# ========== DB ==========
async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, ref_code TEXT UNIQUE,
            referred_by INTEGER, pox REAL DEFAULT 0, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price REAL,
            currency TEXT DEFAULT '₽', stock INTEGER DEFAULT 0,
            description TEXT, photo_id TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT,
            product_id INTEGER, product_name TEXT, price REAL, currency TEXT,
            discount_pct INTEGER DEFAULT 0, discount_flat REAL DEFAULT 0,
            final_price REAL, pox_spent REAL DEFAULT 0, status TEXT DEFAULT 'pending',
            admin_discount REAL DEFAULT 0, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS faq (id INTEGER PRIMARY KEY AUTOINCREMENT, question TEXT, answer TEXT);
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER,
            referral_id INTEGER, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT,
            order_id INTEGER, product_name TEXT, rating INTEGER, text TEXT,
            approved INTEGER DEFAULT 0, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pox_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS admin_ref_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE,
            label TEXT, earn_pox REAL, withdraw_rate REAL, discount_rate REAL,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT,
            pox_amount REAL, rub_amount REAL, status TEXT DEFAULT 'pending', created_at TEXT
        );
        """)
        await db.commit()
        # seed pox_config
        for k,v in DEFAULT_POX.items():
            await db.execute("INSERT OR IGNORE INTO pox_config (key,value) VALUES (?,?)",(k,str(v)))
        await db.commit()

async def get_cfg(key):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM pox_config WHERE key=?",(key,)) as c:
            r = await c.fetchone()
            return r[0] if r else None

async def get_cfg_float(key):
    v = await get_cfg(key); return float(v) if v else 0

async def get_cfg_int(key):
    v = await get_cfg(key); return int(float(v)) if v else 0

async def set_cfg(key, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO pox_config (key,value) VALUES (?,?)",(key,str(value)))
        await db.commit()

async def db_get_user(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id=?",(uid,)) as c:
            return await c.fetchone()

async def db_create_user(uid, uname, ref_by=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id,username,ref_code,referred_by,created_at) VALUES (?,?,?,?,?)",
            (uid, uname, f"ref{uid}", ref_by, datetime.now().isoformat(timespec="seconds")))
        await db.commit()

async def count_confirmed_refs(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT r.referral_id) FROM referrals r JOIN orders o ON o.user_id=r.referral_id WHERE r.referrer_id=? AND o.status='confirmed'",(uid,)) as c:
            return (await c.fetchone())[0]

async def get_tier(uid):
    n = await count_confirmed_refs(uid)
    t3 = await get_cfg_int("tier3_min_refs")
    t2 = await get_cfg_int("tier2_min_refs")
    if n >= t3: return 3, await get_cfg("tier3_name")
    if n >= t2: return 2, await get_cfg("tier2_name")
    return 1, await get_cfg("tier1_name")

async def get_tier_rates(tier):
    t = str(tier)
    return {
        "earn": await get_cfg_float(f"tier{t}_earn"),
        "withdraw": await get_cfg_float(f"tier{t}_withdraw"),
        "discount": await get_cfg_float(f"tier{t}_discount"),
    }

async def has_pending_order(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM orders WHERE user_id=? AND status='pending'",(uid,)) as c:
            return (await c.fetchone())[0] > 0

def fmt_price(p, c="₽"):
    return f"{int(p)}{c}" if p==int(p) else f"{p:.2f}{c}"

def parse_price(t):
    t = t.strip().replace(",",".")
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([^\d\s]*)$", t)
    if not m: raise ValueError
    return float(m.group(1)), (m.group(2) or "₽")

# ========== KEYBOARDS ==========
def main_menu(is_admin=False):
    rows = [
        [KeyboardButton(text="🛒 Товары"), KeyboardButton(text="👥 Реферальная система")],
        [KeyboardButton(text="❓ FAQ"), KeyboardButton(text="⭐ Отзывы")],
        [KeyboardButton(text="🤝 Партнёры")],
    ]
    if is_admin: rows.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить товар", callback_data="adm:add_product")],
        [InlineKeyboardButton(text="✏️ Редактировать товар", callback_data="adm:edit_product")],
        [InlineKeyboardButton(text="🗑 Удалить товар", callback_data="adm:del_product")],
        [InlineKeyboardButton(text="📦 Заказы (ожидают)", callback_data="adm:orders")],
        [InlineKeyboardButton(text="📋 Все заказы", callback_data="adm:all_orders:0")],
        [InlineKeyboardButton(text="💰 Запросы на вывод", callback_data="adm:withdrawals")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="🔄 Сброс статистики", callback_data="adm:reset_stats")],
        [InlineKeyboardButton(text="📝 FAQ", callback_data="adm:faq")],
        [InlineKeyboardButton(text="⭐ Отзывы (модерация)", callback_data="adm:reviews")],
        [InlineKeyboardButton(text="⚙️ Настройки POX", callback_data="adm:pox_cfg")],
        [InlineKeyboardButton(text="🔗 Реф-ссылки (админ)", callback_data="adm:ref_links")],
    ])

router = Router()

# ========== /start ==========
@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    args = msg.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    user = await db_get_user(msg.from_user.id)
    if not user:
        referred_by = None
        admin_link_id = None
        if ref_arg:
            if ref_arg.startswith("ref"):
                try:
                    rid = int(ref_arg.replace("ref",""))
                    if rid != msg.from_user.id and await db_get_user(rid):
                        referred_by = rid
                except: pass
            elif ref_arg.startswith("arl_"):
                # admin ref link
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute("SELECT * FROM admin_ref_links WHERE code=?",(ref_arg,)) as c:
                        arl = await c.fetchone()
                if arl:
                    referred_by = ADMIN_ID
                    admin_link_id = arl["id"]

        await db_create_user(msg.from_user.id, msg.from_user.username or "", referred_by)

        if referred_by and referred_by != ADMIN_ID:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT INTO referrals (referrer_id,referral_id,created_at) VALUES (?,?,?)",
                    (referred_by, msg.from_user.id, datetime.now().isoformat(timespec="seconds")))
                await db.commit()
            gs_append("Рефералы",[datetime.now().isoformat(timespec="seconds"),
                referred_by,(await db_get_user(referred_by))["username"],
                msg.from_user.id, msg.from_user.username or ""])
            with suppress(Exception):
                await msg.bot.send_message(referred_by,
                    f"🎉 По твоей ссылке зашёл @{msg.from_user.username or msg.from_user.id}\n"
                    f"pox будет начисляться за каждую его подтверждённую покупку.")
        elif referred_by == ADMIN_ID:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT INTO referrals (referrer_id,referral_id,created_at) VALUES (?,?,?)",
                    (ADMIN_ID, msg.from_user.id, datetime.now().isoformat(timespec="seconds")))
                await db.commit()

    await msg.answer(f"👋 Привет, {msg.from_user.first_name}!\n\nВыбирай раздел:",
        reply_markup=main_menu(msg.from_user.id == ADMIN_ID))

# ========== CATALOG ==========
async def build_catalog_kb(page=0):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id,name,price,currency,stock FROM products ORDER BY id DESC") as c:
            prods = await c.fetchall()
    total = len(prods)
    pages = max(1,(total+PAGE_SIZE-1)//PAGE_SIZE)
    page = max(0,min(page,pages-1))
    chunk = prods[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
    rows = []
    for p in chunk:
        mark = "✅" if p["stock"]>0 else "❌"
        rows.append([InlineKeyboardButton(text=f"{mark} {p['name']} — {fmt_price(p['price'],p['currency'])}",
            callback_data=f"prod:{p['id']}")])
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="◀️",callback_data=f"cat:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}",callback_data="cat:noop"))
    if page<pages-1: nav.append(InlineKeyboardButton(text="▶️",callback_data=f"cat:{page+1}"))
    if pages>1: rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows), total

@router.message(F.text=="🛒 Товары")
async def show_catalog(msg: Message):
    kb,total = await build_catalog_kb(0)
    if total==0: await msg.answer("Товаров нет 🙌"); return
    await msg.answer(f"🛍 <b>Каталог</b> ({total})\n\nВыбери товар:",reply_markup=kb)

@router.callback_query(F.data.startswith("cat:"))
async def cat_page(cb: CallbackQuery):
    v = cb.data.split(":")[1]
    if v=="noop": await cb.answer(); return
    kb,_ = await build_catalog_kb(int(v))
    with suppress(Exception): await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("prod:"))
async def show_product(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products WHERE id=?",(pid,)) as c:
            p = await c.fetchone()
    if not p: await cb.answer("Не найден",show_alert=True); return
    stock = f"{p['stock']} шт." if p["stock"]>0 else "❌ нет"
    text = f"<b>{p['name']}</b>\n\n💰 Цена: <b>{fmt_price(p['price'],p['currency'])}</b>\n📦 Наличие: {stock}\n"
    if p["description"]: text += f"\n{p['description']}"
    rows = []
    if p["stock"]>0: rows.append([InlineKeyboardButton(text="🚚 Заказать",callback_data=f"buy:{pid}")])
    rows.append([InlineKeyboardButton(text="◀️ Каталог",callback_data="cat:0")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    if p["photo_id"]:
        with suppress(Exception):
            await cb.message.answer_photo(p["photo_id"],caption=text,reply_markup=kb); await cb.answer(); return
    await cb.message.answer(text,reply_markup=kb); await cb.answer()

# ========== BUY with pox discount ==========
@router.callback_query(F.data.startswith("buy:"))
async def buy_start(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    if await has_pending_order(cb.from_user.id):
        await cb.answer("У тебя уже есть активный заказ.",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products WHERE id=?",(pid,)) as c:
            p = await c.fetchone()
    if not p or p["stock"]<=0: await cb.answer("Закончился",show_alert=True); return

    user = await db_get_user(cb.from_user.id)
    pox = user["pox"] if user else 0
    tier_num, tier_name = await get_tier(cb.from_user.id)
    rates = await get_tier_rates(tier_num)
    disc_rate = rates["discount"]  # ₽ за 1 pox скидки

    rows = []
    # без скидки
    rows.append([InlineKeyboardButton(text=f"💳 Без скидки — {fmt_price(p['price'],p['currency'])}",
        callback_data=f"cnf:{pid}:0")])
    # варианты скидки pox
    if pox >= 1 and disc_rate > 0:
        max_pox = min(pox, p["price"] / disc_rate)
        for use in [1, 2, 3, 5, math.floor(max_pox)]:
            if use > max_pox or use < 1: continue
            disc_rub = round(use * disc_rate, 2)
            final = round(p["price"] - disc_rub, 2)
            if final < 0: continue
            label = f"💎 -{int(disc_rub)}₽ ({use} pox) → {fmt_price(final,p['currency'])}"
            rows.append([InlineKeyboardButton(text=label, callback_data=f"cnf:{pid}:{use}")])
    rows.append([InlineKeyboardButton(text="❌ Отмена",callback_data=f"prod:{pid}")])

    text = (f"<b>Оформление</b>\n\n«{p['name']}» — {fmt_price(p['price'],p['currency'])}\n\n"
            f"💎 pox: <b>{pox:.1f}</b> ({tier_name})\n"
            f"Курс скидки: 1 pox = {int(disc_rate)}₽\n\n")
    if pox < 1: text += "Скидка будет доступна когда накопишь pox."
    else: text += "Выбери вариант:"
    await cb.message.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith("cnf:"))
async def buy_confirm(cb: CallbackQuery):
    parts = cb.data.split(":")
    pid, pox_use = int(parts[1]), float(parts[2])
    if await has_pending_order(cb.from_user.id):
        await cb.answer("Уже есть заказ",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products WHERE id=?",(pid,)) as c:
            p = await c.fetchone()
        if not p or p["stock"]<=0: await cb.answer("Закончился",show_alert=True); return
        user = await db_get_user(cb.from_user.id)
        if pox_use > 0 and (not user or user["pox"] < pox_use):
            await cb.answer("Недостаточно pox",show_alert=True); return
        tier_num,_ = await get_tier(cb.from_user.id)
        rates = await get_tier_rates(tier_num)
        disc_rub = round(pox_use * rates["discount"], 2)
        final = round(p["price"] - disc_rub, 2)
        if final < 0: final = 0

        cur = await db.execute(
            """INSERT INTO orders (user_id,username,product_id,product_name,price,currency,
               discount_flat,final_price,pox_spent,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (cb.from_user.id, cb.from_user.username or "", pid, p["name"],
             p["price"], p["currency"], disc_rub, final, pox_use,
             datetime.now().isoformat(timespec="seconds")))
        oid = cur.lastrowid
        if pox_use > 0:
            await db.execute("UPDATE users SET pox=pox-? WHERE user_id=?",(pox_use,cb.from_user.id))
        await db.commit()

    gs_append("Заказы",[datetime.now().isoformat(timespec="seconds"),oid,cb.from_user.id,
        cb.from_user.username or "",p["name"],p["price"],disc_rub,final,"pending"])

    disc_line = f"\nСкидка: {int(disc_rub)}₽ ({pox_use} pox)" if pox_use else ""
    # админу
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить",callback_data=f"adm_ord:confirm:{oid}"),
         InlineKeyboardButton(text="❌ Отменить",callback_data=f"adm_ord:cancel:{oid}")],
        [InlineKeyboardButton(text="💲 Изменить цену",callback_data=f"adm_ord:setprice:{oid}")],
        [InlineKeyboardButton(text="🎁 Выдать скидку",callback_data=f"adm_ord:give_disc:{oid}")],
        [InlineKeyboardButton(text="💬 ЛС покупателя",url=f"tg://user?id={cb.from_user.id}")],
    ])
    ulink = f"@{cb.from_user.username}" if cb.from_user.username else f"id{cb.from_user.id}"
    with suppress(Exception):
        await cb.bot.send_message(ADMIN_ID,
            f"🆕 <b>Заказ #{oid}</b>\n\n{ulink} (id <code>{cb.from_user.id}</code>)\n"
            f"Товар: <b>{p['name']}</b>\nЦена: {fmt_price(p['price'],p['currency'])}"
            f"{disc_line}\nК оплате: <b>{fmt_price(final,p['currency'])}</b>",
            reply_markup=admin_kb)

    buyer_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💬 Написать продавцу",url=f"https://t.me/{ADMIN_USERNAME}")]])
    with suppress(Exception):
        await cb.message.edit_text(
            f"✅ Заказ #{oid}!\n\n<b>{p['name']}</b>\nК оплате: <b>{fmt_price(final,p['currency'])}</b>"
            f"{disc_line}\n\nНапиши продавцу:",reply_markup=buyer_kb)
    await cb.answer("Заказ оформлен!")

# ========== ADMIN ORDER ACTIONS ==========
class EditOrderPrice(StatesGroup):
    waiting_price = State()

class GiveDiscount(StatesGroup):
    waiting_amount = State()

@router.callback_query(F.data.startswith("adm_ord:setprice:"))
async def adm_setprice(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    oid = int(cb.data.split(":")[2])
    await state.set_state(EditOrderPrice.waiting_price)
    await state.update_data(order_id=oid)
    await cb.message.answer(f"Введи новую цену для заказа #{oid}:"); await cb.answer()

@router.message(EditOrderPrice.waiting_price)
async def adm_setprice_do(msg: Message, state: FSMContext):
    data = await state.get_data()
    try: price,cur = parse_price(msg.text)
    except: await msg.answer("❗ Формат: 450 или 450₽"); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET final_price=?,currency=? WHERE id=?",(price,cur,data["order_id"]))
        await db.commit()
    await state.clear()
    await msg.answer(f"✅ Цена #{data['order_id']} → {fmt_price(price,cur)}",reply_markup=admin_menu_kb())

@router.callback_query(F.data.startswith("adm_ord:give_disc:"))
async def adm_give_disc(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    oid = int(cb.data.split(":")[2])
    await state.set_state(GiveDiscount.waiting_amount)
    await state.update_data(order_id=oid)
    await cb.message.answer(f"Введи сумму скидки в ₽ для заказа #{oid} (или % — например 20%):"); await cb.answer()

@router.message(GiveDiscount.waiting_amount)
async def adm_give_disc_do(msg: Message, state: FSMContext):
    data = await state.get_data(); oid = data["order_id"]
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE id=?",(oid,)) as c:
            order = await c.fetchone()
        if not order: await msg.answer("Заказ не найден"); await state.clear(); return
        txt = msg.text.strip()
        if txt.endswith("%"):
            pct = float(txt.replace("%",""))
            disc = round(order["price"] * pct / 100, 2)
        else:
            disc = float(txt.replace(",","."))
        new_final = round(order["final_price"] - disc, 2)
        if new_final < 0: new_final = 0
        await db.execute("UPDATE orders SET admin_discount=admin_discount+?, final_price=? WHERE id=?",(disc,new_final,oid))
        await db.commit()
    await state.clear()
    await msg.answer(f"✅ Скидка {int(disc)}₽ выдана. Новая цена #{oid}: {fmt_price(new_final,order['currency'])}",
        reply_markup=admin_menu_kb())
    with suppress(Exception):
        await msg.bot.send_message(order["user_id"],
            f"🎁 Тебе выдали скидку {int(disc)}₽ на заказ #{oid}!\nНовая цена: <b>{fmt_price(new_final,order['currency'])}</b>")

@router.callback_query(F.data.regexp(r"^adm_ord:(confirm|cancel):\d+$"))
async def admin_order_action(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    _,action,oid = cb.data.split(":"); oid = int(oid)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE id=?",(oid,)) as c:
            order = await c.fetchone()
        if not order: await cb.answer("Не найден",show_alert=True); return
        if order["status"]!="pending": await cb.answer(f"Уже {order['status']}",show_alert=True); return
        st = "confirmed" if action=="confirm" else "cancelled"
        await db.execute("UPDATE orders SET status=? WHERE id=?",(st,oid))
        if action=="confirm":
            await db.execute("UPDATE products SET stock=MAX(stock-1,0) WHERE id=?",(order["product_id"],))
        else:
            if order["pox_spent"]:
                await db.execute("UPDATE users SET pox=pox+? WHERE user_id=?",(order["pox_spent"],order["user_id"]))
        await db.commit()

    if action=="confirm":
        gs_append("Продажи",[datetime.now().isoformat(timespec="seconds"),oid,order["user_id"],
            order["username"],order["product_name"],order["price"],order["discount_flat"],order["final_price"]])
        buyer = await db_get_user(order["user_id"])
        if buyer and buyer["referred_by"]:
            ref_id = buyer["referred_by"]
            # check if admin ref link with custom rates
            # otherwise use tier rates
            tier_num,_ = await get_tier(ref_id)
            rates = await get_tier_rates(tier_num)
            earn = rates["earn"]
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE users SET pox=pox+? WHERE user_id=?",(earn,ref_id))
                await db.commit()
            with suppress(Exception):
                await cb.bot.send_message(ref_id,f"💎 +{earn} pox за покупку @{order['username'] or order['user_id']}!")
        with suppress(Exception):
            await cb.bot.send_message(order["user_id"],
                f"✅ Заказ #{oid} ({order['product_name']}) подтверждён!\n\n"
                f"Хочешь оставить отзыв?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="⭐ Оставить отзыв",callback_data=f"rev:start:{oid}")]]))
        suffix = "\n\n<b>✅ ПОДТВЕРЖДЁН</b>"
    else:
        with suppress(Exception):
            vb = f"\n💎 Возвращено {order['pox_spent']} pox." if order["pox_spent"] else ""
            await cb.bot.send_message(order["user_id"],f"❌ Заказ #{oid} отменён.{vb}")
        suffix = "\n\n<b>❌ ОТМЕНЁН</b>"
    with suppress(Exception): await cb.message.edit_text((cb.message.html_text or "")+suffix)
    await cb.answer("Готово")

# ========== REFERRAL SYSTEM ==========
@router.message(F.text=="👥 Реферальная система")
async def referral_section(msg: Message):
    user = await db_get_user(msg.from_user.id)
    if not user:
        await db_create_user(msg.from_user.id, msg.from_user.username or "")
        user = await db_get_user(msg.from_user.id)

    tier_num, tier_name = await get_tier(msg.from_user.id)
    rates = await get_tier_rates(tier_num)
    total_refs = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?",(msg.from_user.id,)) as c:
            total_refs = (await c.fetchone())[0]
    confirmed = await count_confirmed_refs(msg.from_user.id)

    # progress to next tier
    t2 = await get_cfg_int("tier2_min_refs"); t3 = await get_cfg_int("tier3_min_refs")
    if tier_num==1: next_info = f"До «{await get_cfg('tier2_name')}»: {confirmed}/{t2} подтв. реф."
    elif tier_num==2: next_info = f"До «{await get_cfg('tier3_name')}»: {confirmed}/{t3} подтв. реф."
    else: next_info = "Максимальный уровень 🏆"

    bot_info = await msg.bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user['ref_code']}"
    min_wd = await get_cfg_int("min_withdraw_rub")

    rows = []
    if user["pox"] >= 1:
        rows.append([InlineKeyboardButton(text=f"💸 Вывести pox ({int(rates['withdraw'])}₽/pox)",
            callback_data="wd:start")])
    text = (
        f"👤 <b>Твой профиль</b>\n"
        f"Уровень: <b>{tier_name}</b>\n"
        f"{next_info}\n\n"
        f"💎 pox: <b>{user['pox']:.1f}</b>\n"
        f"👥 Рефералов: {total_refs} (подтв. покупок: {confirmed})\n\n"
        f"📊 <b>Твои курсы:</b>\n"
        f"• Начисление: {rates['earn']} pox за покупку реферала\n"
        f"• Скидка: 1 pox = {int(rates['discount'])}₽\n"
        f"• Вывод: 1 pox = {int(rates['withdraw'])}₽ (мин. {min_wd}₽)\n\n"
        f"🔗 <b>Твоя ссылка:</b>\n<code>{ref_link}</code>")
    await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows) if rows else None)

# ========== WITHDRAW ==========
class WithdrawState(StatesGroup):
    amount = State()

@router.callback_query(F.data=="wd:start")
async def wd_start(cb: CallbackQuery, state: FSMContext):
    user = await db_get_user(cb.from_user.id)
    tier_num,_ = await get_tier(cb.from_user.id)
    rates = await get_tier_rates(tier_num)
    min_wd = await get_cfg_int("min_withdraw_rub")
    min_pox = math.ceil(min_wd / rates["withdraw"])
    if user["pox"] < min_pox:
        await cb.answer(f"Минимум {min_pox} pox ({min_wd}₽) для вывода",show_alert=True); return
    await state.set_state(WithdrawState.amount)
    await cb.message.answer(
        f"💸 Сколько pox вывести?\n\nУ тебя: {user['pox']:.1f} pox\n"
        f"Курс: 1 pox = {int(rates['withdraw'])}₽\nМинимум: {min_wd}₽ ({min_pox} pox)")
    await cb.answer()

@router.message(WithdrawState.amount)
async def wd_amount(msg: Message, state: FSMContext):
    try: amt = float(msg.text.strip().replace(",","."))
    except: await msg.answer("Введи число"); return
    user = await db_get_user(msg.from_user.id)
    if amt > user["pox"]: await msg.answer(f"У тебя только {user['pox']:.1f} pox"); return
    tier_num,_ = await get_tier(msg.from_user.id)
    rates = await get_tier_rates(tier_num)
    rub = round(amt * rates["withdraw"], 2)
    min_wd = await get_cfg_int("min_withdraw_rub")
    if rub < min_wd: await msg.answer(f"Минимум {min_wd}₽"); return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET pox=pox-? WHERE user_id=?",(amt,msg.from_user.id))
        await db.execute("INSERT INTO withdraw_requests (user_id,username,pox_amount,rub_amount,created_at) VALUES (?,?,?,?,?)",
            (msg.from_user.id,msg.from_user.username or "",amt,rub,datetime.now().isoformat(timespec="seconds")))
        await db.commit()
    await state.clear()
    await msg.answer(f"✅ Заявка на вывод создана!\n{amt} pox → {fmt_price(rub,'₽')}\n\nОжидай подтверждения.",
        reply_markup=main_menu(msg.from_user.id==ADMIN_ID))
    with suppress(Exception):
        await msg.bot.send_message(ADMIN_ID,
            f"💰 <b>Заявка на вывод</b>\n\n@{msg.from_user.username or msg.from_user.id}\n"
            f"{amt} pox → {fmt_price(rub,'₽')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Выплатить",callback_data=f"wd:approve:{msg.from_user.id}:{amt}"),
                 InlineKeyboardButton(text="❌ Отклонить",callback_data=f"wd:reject:{msg.from_user.id}:{amt}")]]))

@router.callback_query(F.data.startswith("wd:approve:"))
async def wd_approve(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    parts = cb.data.split(":"); uid=int(parts[2]); amt=float(parts[3])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdraw_requests SET status='approved' WHERE user_id=? AND pox_amount=? AND status='pending'",
            (uid,amt)); await db.commit()
    with suppress(Exception):
        await cb.bot.send_message(uid,f"✅ Вывод {amt} pox одобрен! Деньги будут переведены в течение 24ч.")
    await cb.message.edit_text((cb.message.html_text or "")+"\n\n<b>✅ ВЫПЛАЧЕНО</b>"); await cb.answer()

@router.callback_query(F.data.startswith("wd:reject:"))
async def wd_reject(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    parts = cb.data.split(":"); uid=int(parts[2]); amt=float(parts[3])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdraw_requests SET status='rejected' WHERE user_id=? AND pox_amount=? AND status='pending'",(uid,amt))
        await db.execute("UPDATE users SET pox=pox+? WHERE user_id=?",(amt,uid))
        await db.commit()
    with suppress(Exception):
        await cb.bot.send_message(uid,f"❌ Вывод отклонён. {amt} pox возвращено на баланс.")
    await cb.message.edit_text((cb.message.html_text or "")+"\n\n<b>❌ ОТКЛОНЕНО</b>"); await cb.answer()

# ========== FAQ ==========
@router.message(F.text=="❓ FAQ")
async def show_faq(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM faq ORDER BY id") as c:
            items = await c.fetchall()
    if not items: await msg.answer("FAQ пока пуст 🙏"); return
    text = "❓ <b>FAQ</b>\n\n"
    for i,q in enumerate(items,1): text += f"<b>{i}. {q['question']}</b>\n{q['answer']}\n\n"
    await msg.answer(text)

@router.message(F.text=="🤝 Партнёры")
async def show_partners(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 VPN",url="https://t.me/vpn_liberty_bot?start=promo_ABCD_ABCD_PROMO")]])
    await msg.answer("🤝 <b>Наши партнёры</b>",reply_markup=kb)

# ========== REVIEWS ==========
class WriteReview(StatesGroup):
    rating = State()
    text = State()

@router.message(F.text=="⭐ Отзывы")
async def show_reviews(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM reviews WHERE approved=1 ORDER BY id DESC LIMIT 15") as c:
            revs = await c.fetchall()
    if not revs: await msg.answer("Отзывов пока нет 🌟"); return
    text = "⭐ <b>Отзывы</b>\n\n"
    for r in revs:
        stars = "⭐"*r["rating"]+"☆"*(5-r["rating"])
        u = f"@{r['username']}" if r["username"] else f"id{r['user_id']}"
        text += f"{stars}\n<b>{r['product_name']}</b> — {u}\n{r['text']}\n\n"
    await msg.answer(text)

@router.callback_query(F.data.startswith("rev:start:"))
async def rev_start(cb: CallbackQuery, state: FSMContext):
    oid = int(cb.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE id=? AND user_id=? AND status='confirmed'",(oid,cb.from_user.id)) as c:
            o = await c.fetchone()
        if not o: await cb.answer("Заказ не найден",show_alert=True); return
        async with db.execute("SELECT id FROM reviews WHERE order_id=? AND user_id=?",(oid,cb.from_user.id)) as c:
            if await c.fetchone(): await cb.answer("Уже оставлял отзыв",show_alert=True); return
    await state.set_state(WriteReview.rating); await state.update_data(order_id=oid,product_name=o["product_name"])
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{'⭐'*i}",callback_data=f"rev:rate:{i}") for i in range(1,6)]])
    await cb.message.answer("Оцени покупку:",reply_markup=kb); await cb.answer()

@router.callback_query(WriteReview.rating, F.data.startswith("rev:rate:"))
async def rev_rate(cb: CallbackQuery, state: FSMContext):
    r = int(cb.data.split(":")[2]); await state.update_data(rating=r)
    await state.set_state(WriteReview.text)
    await cb.message.answer(f"{'⭐'*r} — напиши текст отзыва:"); await cb.answer()

@router.message(WriteReview.text)
async def rev_text(msg: Message, state: FSMContext):
    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO reviews (user_id,username,order_id,product_name,rating,text,created_at) VALUES (?,?,?,?,?,?,?)",
            (msg.from_user.id,msg.from_user.username or "",data["order_id"],data["product_name"],data["rating"],msg.text,
             datetime.now().isoformat(timespec="seconds"))); await db.commit()
        async with db.execute("SELECT id FROM reviews ORDER BY id DESC LIMIT 1") as c:
            rev_id = (await c.fetchone())[0]
    await state.clear(); await msg.answer("✅ Спасибо! Отзыв на модерации.")
    with suppress(Exception):
        await msg.bot.send_message(ADMIN_ID,
            f"⭐ Отзыв от @{msg.from_user.username or msg.from_user.id}\n{data['product_name']}\n{'⭐'*data['rating']}\n{msg.text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Одобрить",callback_data=f"rev:approve:{rev_id}"),
                InlineKeyboardButton(text="❌ Отклонить",callback_data=f"rev:reject:{rev_id}")]]))

@router.callback_query(F.data.startswith("rev:approve:"))
async def rev_approve(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    rid = int(cb.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM reviews WHERE id=?",(rid,)) as c:
            r = await c.fetchone()
        if not r: await cb.answer("Не найден",show_alert=True); return
        await db.execute("UPDATE reviews SET approved=1 WHERE id=?",(rid,)); await db.commit()
    stars = "⭐"*r["rating"]+"☆"*(5-r["rating"])
    u = f"@{r['username']}" if r["username"] else "Покупатель"
    with suppress(Exception):
        await cb.bot.send_message(REVIEW_CHANNEL_ID,f"{stars}\n\n<b>{r['product_name']}</b>\n\n{r['text']}\n\n— {u}")
    await cb.message.edit_text((cb.message.html_text or "")+"\n\n<b>✅ ОДОБРЕН</b>"); await cb.answer()

@router.callback_query(F.data.startswith("rev:reject:"))
async def rev_reject(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет доступа",show_alert=True); return
    rid = int(cb.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM reviews WHERE id=?",(rid,)); await db.commit()
    await cb.message.edit_text((cb.message.html_text or"")+"\n\n<b>❌ ОТКЛОНЁН</b>"); await cb.answer()

# ========== ADMIN PANEL ==========
class AddProduct(StatesGroup):
    name = State(); price = State(); stock = State(); description = State(); photo = State()
class EditProduct(StatesGroup):
    pick_field = State(); new_value = State()
class AddFAQ(StatesGroup):
    question = State(); answer = State()
class EditPoxCfg(StatesGroup):
    key = State(); value = State()
class CreateAdminRef(StatesGroup):
    label = State(); earn = State(); withdraw = State(); discount = State()

@router.message(F.text=="⚙️ Админ-панель")
async def admin_panel(msg: Message):
    if msg.from_user.id!=ADMIN_ID: await msg.answer("⛔"); return
    await msg.answer("⚙️ <b>Админ-панель</b>",reply_markup=admin_menu_kb())

# --- products ---
@router.callback_query(F.data=="adm:add_product")
async def adm_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    await state.set_state(AddProduct.name); await cb.message.answer("Введи <b>название</b>:"); await cb.answer()

@router.message(AddProduct.name)
async def ap_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text); await state.set_state(AddProduct.price); await m.answer("Введи <b>цену</b>:")

@router.message(AddProduct.price)
async def ap_price(m: Message, state: FSMContext):
    try: p,c = parse_price(m.text)
    except: await m.answer("❗ Формат: 500 или 5$"); return
    await state.update_data(price=p,currency=c); await state.set_state(AddProduct.stock); await m.answer("<b>Наличие</b> (число):")

@router.message(AddProduct.stock)
async def ap_stock(m: Message, state: FSMContext):
    if not m.text.strip().isdigit(): await m.answer("❗ Число"); return
    await state.update_data(stock=int(m.text)); await state.set_state(AddProduct.description); await m.answer("<b>Описание</b> (или -):")

@router.message(AddProduct.description)
async def ap_desc(m: Message, state: FSMContext):
    await state.update_data(description="" if m.text.strip()=="-" else m.text)
    await state.set_state(AddProduct.photo); await m.answer("<b>Фото</b> или -:")

@router.message(AddProduct.photo, F.photo)
async def ap_photo(m: Message, state: FSMContext):
    await _save_product(m, state, m.photo[-1].file_id)
@router.message(AddProduct.photo, F.text=="-")
async def ap_skip(m: Message, state: FSMContext):
    await _save_product(m, state, None)
@router.message(AddProduct.photo)
async def ap_other(m: Message): await m.answer("❗ Фото или -")

async def _save_product(m, state, pid):
    d = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO products (name,price,currency,stock,description,photo_id,created_at) VALUES (?,?,?,?,?,?,?)",
            (d["name"],d["price"],d["currency"],d["stock"],d["description"],pid,datetime.now().isoformat(timespec="seconds")))
        await db.commit()
    await state.clear(); await m.answer(f"✅ «{d['name']}» добавлен!",reply_markup=admin_menu_kb())

@router.callback_query(F.data=="adm:edit_product")
async def adm_editp(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id,name FROM products ORDER BY id DESC") as c: items = await c.fetchall()
    if not items: await cb.message.answer("Нет товаров"); await cb.answer(); return
    rows = [[InlineKeyboardButton(text=f"✏️ {p['name']}",callback_data=f"edp:{p['id']}")] for p in items]
    await cb.message.answer("Какой?",reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await cb.answer()

@router.callback_query(F.data.startswith("edp:"))
async def adm_editp_pick(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    pid = int(cb.data.split(":")[1]); await state.set_state(EditProduct.pick_field); await state.update_data(pid=pid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t,callback_data=f"edf:{f}")] for t,f in
        [("Название","name"),("Цена","price"),("Наличие","stock"),("Описание","description"),("Фото","photo")]])
    await cb.message.answer("Что меняем?",reply_markup=kb); await cb.answer()

@router.callback_query(EditProduct.pick_field, F.data.startswith("edf:"))
async def adm_editp_field(cb: CallbackQuery, state: FSMContext):
    f = cb.data.split(":")[1]; await state.update_data(field=f); await state.set_state(EditProduct.new_value)
    prompts = {"name":"название","price":"цену","stock":"наличие (число)","description":"описание (или -)","photo":"фото (или -)"}
    await cb.message.answer(f"Введи новое <b>{prompts[f]}</b>:"); await cb.answer()

@router.message(EditProduct.new_value)
async def adm_editp_apply(m: Message, state: FSMContext):
    d = await state.get_data(); pid=d["pid"]; f=d["field"]
    async with aiosqlite.connect(DB_PATH) as db:
        if f=="name": await db.execute("UPDATE products SET name=? WHERE id=?",(m.text,pid))
        elif f=="price":
            try: p,c = parse_price(m.text)
            except: await m.answer("❗"); return
            await db.execute("UPDATE products SET price=?,currency=? WHERE id=?",(p,c,pid))
        elif f=="stock":
            if not m.text.strip().isdigit(): await m.answer("❗ Число"); return
            await db.execute("UPDATE products SET stock=? WHERE id=?",(int(m.text),pid))
        elif f=="description":
            await db.execute("UPDATE products SET description=? WHERE id=?",("" if m.text.strip()=="-" else m.text,pid))
        elif f=="photo":
            if m.photo: await db.execute("UPDATE products SET photo_id=? WHERE id=?",(m.photo[-1].file_id,pid))
            elif m.text and m.text.strip()=="-": await db.execute("UPDATE products SET photo_id=NULL WHERE id=?",(pid,))
            else: await m.answer("❗ Фото или -"); return
        await db.commit()
    await state.clear(); await m.answer("✅ Сохранено",reply_markup=admin_menu_kb())

@router.callback_query(F.data=="adm:del_product")
async def adm_delp(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id,name FROM products ORDER BY id DESC") as c: items = await c.fetchall()
    if not items: await cb.message.answer("Нет"); await cb.answer(); return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🗑 {p['name']}",callback_data=f"delp:{p['id']}")] for p in items])
    await cb.message.answer("Удалить?",reply_markup=kb); await cb.answer()

@router.callback_query(F.data.startswith("delp:"))
async def adm_delp_do(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM products WHERE id=?",(int(cb.data.split(":")[1]),)); await db.commit()
    await cb.message.edit_text("✅ Удалён"); await cb.answer()

# --- orders ---
@router.callback_query(F.data=="adm:orders")
async def adm_orders(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE status='pending' ORDER BY id DESC LIMIT 20") as c:
            orders = await c.fetchall()
    if not orders: await cb.message.answer("Нет ожидающих 🎉"); await cb.answer(); return
    for o in orders:
        disc = f"\nСкидка pox: {int(o['discount_flat'])}₽" if o['discount_flat'] else ""
        adm_disc = f"\nСкидка админ: {int(o['admin_discount'])}₽" if o['admin_discount'] else ""
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅",callback_data=f"adm_ord:confirm:{o['id']}"),
             InlineKeyboardButton(text="❌",callback_data=f"adm_ord:cancel:{o['id']}")],
            [InlineKeyboardButton(text="💲 Цена",callback_data=f"adm_ord:setprice:{o['id']}"),
             InlineKeyboardButton(text="🎁 Скидка",callback_data=f"adm_ord:give_disc:{o['id']}")],
            [InlineKeyboardButton(text="💬 ЛС",url=f"tg://user?id={o['user_id']}")]])
        await cb.message.answer(
            f"#{o['id']} — <b>{o['product_name']}</b>\n"
            f"Цена: {fmt_price(o['price'],o['currency'])}{disc}{adm_disc}\n"
            f"К оплате: <b>{fmt_price(o['final_price'],o['currency'])}</b>\n"
            f"@{o['username'] or o['user_id']} | {o['created_at']}", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("adm:all_orders:"))
async def adm_all_orders(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    page = int(cb.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) FROM orders") as c: total=(await c.fetchone())[0]
        async with db.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 10 OFFSET ?",(page*10,)) as c:
            orders = await c.fetchall()
    if not orders: await cb.message.answer("Нет"); await cb.answer(); return
    se = {"pending":"⏳","confirmed":"✅","cancelled":"❌"}
    lines = [f"📋 <b>Заказы</b> (стр.{page+1})\n"]
    for o in orders: lines.append(f"{se.get(o['status'],'❓')} #{o['id']} {o['product_name']} {fmt_price(o['final_price'],o['currency'])} @{o['username'] or o['user_id']}")
    rows = [[InlineKeyboardButton(text=f"🗑 #{o['id']}",callback_data=f"adm:delord:{o['id']}")] for o in orders]
    nav = []
    pages = max(1,(total+9)//10)
    if page>0: nav.append(InlineKeyboardButton(text="◀️",callback_data=f"adm:all_orders:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}",callback_data="cat:noop"))
    if (page+1)*10<total: nav.append(InlineKeyboardButton(text="▶️",callback_data=f"adm:all_orders:{page+1}"))
    if nav: rows.append(nav)
    await cb.message.answer("\n".join(lines),reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await cb.answer()

@router.callback_query(F.data.startswith("adm:delord:"))
async def adm_delord(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    oid = int(cb.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE id=?",(oid,)) as c: o = await c.fetchone()
        if o:
            if o["status"]=="pending" and o["pox_spent"]:
                await db.execute("UPDATE users SET pox=pox+? WHERE user_id=?",(o["pox_spent"],o["user_id"]))
            if o["status"]=="confirmed":
                await db.execute("UPDATE products SET stock=stock+1 WHERE id=?",(o["product_id"],))
            await db.execute("DELETE FROM orders WHERE id=?",(oid,)); await db.commit()
    await cb.answer(f"#{oid} удалён",show_alert=True)

# --- withdrawals ---
@router.callback_query(F.data=="adm:withdrawals")
async def adm_wds(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM withdraw_requests WHERE status='pending' ORDER BY id DESC") as c:
            wds = await c.fetchall()
    if not wds: await cb.message.answer("Нет заявок 🎉"); await cb.answer(); return
    for w in wds:
        await cb.message.answer(
            f"💰 @{w['username'] or w['user_id']}\n{w['pox_amount']} pox → {fmt_price(w['rub_amount'],'₽')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Выплатить",callback_data=f"wd:approve:{w['user_id']}:{w['pox_amount']}"),
                InlineKeyboardButton(text="❌ Отклонить",callback_data=f"wd:reject:{w['user_id']}:{w['pox_amount']}")]]))
    await cb.answer()

# --- stats ---
@router.callback_query(F.data=="adm:stats")
async def adm_stats(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        async def one(q,*a):
            async with db.execute(q,a) as c: return (await c.fetchone())[0]
        users_c = await one("SELECT COUNT(*) FROM users")
        prods_c = await one("SELECT COUNT(*) FROM products")
        ord_t = await one("SELECT COUNT(*) FROM orders")
        ord_p = await one("SELECT COUNT(*) FROM orders WHERE status='pending'")
        ord_cf = await one("SELECT COUNT(*) FROM orders WHERE status='confirmed'")
        ord_cn = await one("SELECT COUNT(*) FROM orders WHERE status='cancelled'")
        rev_t = await one("SELECT COALESCE(SUM(final_price),0) FROM orders WHERE status='confirmed'")
        refs_t = await one("SELECT COUNT(*) FROM referrals")
        revs_t = await one("SELECT COUNT(*) FROM reviews WHERE approved=1")
        wd_t = await one("SELECT COALESCE(SUM(rub_amount),0) FROM withdraw_requests WHERE status='approved'")
        text = (
            f"📊 <b>Статистика</b>\n\n"
            f"👤 Пользователей: <b>{users_c}</b>\n"
            f"🛒 Товаров: <b>{prods_c}</b>\n\n"
            f"📦 Заказы:\n"
            f"  • всего: {ord_t}\n"
            f"  • ожидают: {ord_p}\n"
            f"  • подтверждены: {ord_cf}\n"
            f"  • отменены: {ord_cn}\n"
            f"💰 Выручка: <b>{rev_t:.0f}₽</b>\n\n"
            f"👥 Рефералов: {refs_t}\n"
            f"⭐ Отзывов: {revs_t}\n"
            f"💸 Выводов (одобр.): {wd_t:.0f}₽\n")
    await cb.message.answer(text); await cb.answer()

# --- reset ---
@router.callback_query(F.data=="adm:reset_stats")
async def adm_reset(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Все заказы",callback_data="adm:wipe:orders")],
        [InlineKeyboardButton(text="🗑 Отменённые",callback_data="adm:wipe:cancelled")],
        [InlineKeyboardButton(text="🗑 Рефералы",callback_data="adm:wipe:referrals")],
        [InlineKeyboardButton(text="🗑 Отзывы",callback_data="adm:wipe:reviews")],
        [InlineKeyboardButton(text="⚠️ Полный сброс",callback_data="adm:wipe:all")],
        [InlineKeyboardButton(text="◀️ Назад",callback_data="adm:back")]])
    await cb.message.answer("🔄 Что удалить?",reply_markup=kb); await cb.answer()

@router.callback_query(F.data=="adm:back")
async def adm_back(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: return
    await cb.message.answer("⚙️ <b>Админ-панель</b>",reply_markup=admin_menu_kb()); await cb.answer()

@router.callback_query(F.data.startswith("adm:wipe:"))
async def adm_wipe(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    t = cb.data.split(":")[2]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да",callback_data=f"adm:wipe_yes:{t}"),
         InlineKeyboardButton(text="❌ Нет",callback_data="adm:back")]])
    await cb.message.answer(f"⚠️ Точно удалить?",reply_markup=kb); await cb.answer()

@router.callback_query(F.data.startswith("adm:wipe_yes:"))
async def adm_wipe_yes(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: return
    t = cb.data.split(":")[2]
    async with aiosqlite.connect(DB_PATH) as db:
        if t=="orders": await db.execute("DELETE FROM orders")
        elif t=="cancelled": await db.execute("DELETE FROM orders WHERE status='cancelled'")
        elif t=="referrals": await db.execute("DELETE FROM referrals")
        elif t=="reviews": await db.execute("DELETE FROM reviews")
        elif t=="all":
            for tbl in ["orders","referrals","reviews","withdraw_requests"]: await db.execute(f"DELETE FROM {tbl}")
            await db.execute("UPDATE users SET pox=0")
        await db.commit()
    await cb.message.edit_text("✅ Удалено"); await cb.answer()

# --- FAQ admin ---
@router.callback_query(F.data=="adm:faq")
async def adm_faq(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM faq") as c: items = await c.fetchall()
    rows = [[InlineKeyboardButton(text="➕ Добавить",callback_data="faq:add")]]
    for i in items: rows.append([InlineKeyboardButton(text=f"🗑 {i['question'][:40]}",callback_data=f"faq:del:{i['id']}")])
    await cb.message.answer(f"📝 FAQ ({len(items)})",reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await cb.answer()

@router.callback_query(F.data=="faq:add")
async def faq_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: return
    await state.set_state(AddFAQ.question); await cb.message.answer("Вопрос:"); await cb.answer()
@router.message(AddFAQ.question)
async def faq_q(m: Message, state: FSMContext):
    await state.update_data(q=m.text); await state.set_state(AddFAQ.answer); await m.answer("Ответ:")
@router.message(AddFAQ.answer)
async def faq_a(m: Message, state: FSMContext):
    d = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO faq (question,answer) VALUES (?,?)",(d["q"],m.text)); await db.commit()
    await state.clear(); await m.answer("✅ Добавлено",reply_markup=admin_menu_kb())
@router.callback_query(F.data.startswith("faq:del:"))
async def faq_del(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM faq WHERE id=?",(int(cb.data.split(":")[2]),)); await db.commit()
    await cb.message.edit_text("✅ Удалён"); await cb.answer()

# --- reviews admin ---
@router.callback_query(F.data=="adm:reviews")
async def adm_revs(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM reviews WHERE approved=0 ORDER BY id DESC LIMIT 10") as c:
            revs = await c.fetchall()
    if not revs: await cb.message.answer("Нет на модерации 🎉"); await cb.answer(); return
    for r in revs:
        await cb.message.answer(f"@{r['username'] or r['user_id']} | {r['product_name']}\n{'⭐'*r['rating']}\n{r['text']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅",callback_data=f"rev:approve:{r['id']}"),
                InlineKeyboardButton(text="❌",callback_data=f"rev:reject:{r['id']}")]]))
    await cb.answer()

# --- POX config admin ---
@router.callback_query(F.data=="adm:pox_cfg")
async def adm_pox_cfg(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT key,value FROM pox_config ORDER BY key") as c:
            cfg = await c.fetchall()
    text = "⚙️ <b>Настройки POX</b>\n\n"
    for k,v in cfg: text += f"<code>{k}</code> = {v}\n"
    text += "\nНажми кнопку чтобы изменить:"
    rows = [[InlineKeyboardButton(text=k,callback_data=f"pcfg:{k}")] for k,v in cfg]
    rows.append([InlineKeyboardButton(text="◀️ Назад",callback_data="adm:back")])
    await cb.message.answer(text,reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await cb.answer()

@router.callback_query(F.data.startswith("pcfg:"))
async def adm_pcfg_pick(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: return
    key = cb.data.split(":",1)[1]
    await state.set_state(EditPoxCfg.value); await state.update_data(key=key)
    cur = await get_cfg(key)
    await cb.message.answer(f"<code>{key}</code> = {cur}\n\nВведи новое значение:"); await cb.answer()

@router.message(EditPoxCfg.value)
async def adm_pcfg_set(m: Message, state: FSMContext):
    d = await state.get_data()
    await set_cfg(d["key"], m.text.strip())
    await state.clear()
    await m.answer(f"✅ {d['key']} = {m.text.strip()}",reply_markup=admin_menu_kb())

# --- Admin ref links ---
@router.callback_query(F.data=="adm:ref_links")
async def adm_ref_links(cb: CallbackQuery):
    if cb.from_user.id!=ADMIN_ID: await cb.answer("Нет",show_alert=True); return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM admin_ref_links ORDER BY id DESC") as c:
            links = await c.fetchall()
    bot_info = await cb.bot.get_me()
    text = "🔗 <b>Админские реф-ссылки</b>\n\n"
    if links:
        for l in links:
            url = f"https://t.me/{bot_info.username}?start={l['code']}"
            text += (f"<b>{l['label']}</b>\n<code>{url}</code>\n"
                     f"Начисл: {l['earn_pox']} pox | Вывод: {l['withdraw_rate']}₽ | Скидка: {l['discount_rate']}₽\n\n")
    else:
        text += "Пока нет ссылок.\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать ссылку",callback_data="arl:create")],
        [InlineKeyboardButton(text="◀️ Назад",callback_data="adm:back")]])
    await cb.message.answer(text,reply_markup=kb); await cb.answer()

@router.callback_query(F.data=="arl:create")
async def arl_create(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id!=ADMIN_ID: return
    await state.set_state(CreateAdminRef.label)
    await cb.message.answer("Введи <b>название</b> ссылки (например «Для блогера Васи»):"); await cb.answer()

@router.message(CreateAdminRef.label)
async def arl_label(m: Message, state: FSMContext):
    await state.update_data(label=m.text); await state.set_state(CreateAdminRef.earn)
    await m.answer("Сколько <b>pox</b> начислять за покупку реферала? (число, например 1.5):")

@router.message(CreateAdminRef.earn)
async def arl_earn(m: Message, state: FSMContext):
    try: v = float(m.text.strip().replace(",","."))
    except: await m.answer("❗ Число"); return
    await state.update_data(earn=v); await state.set_state(CreateAdminRef.withdraw)
    await m.answer("Курс <b>вывода</b> (₽ за 1 pox, например 120):")

@router.message(CreateAdminRef.withdraw)
async def arl_wd(m: Message, state: FSMContext):
    try: v = float(m.text.strip().replace(",","."))
    except: await m.answer("❗ Число"); return
    await state.update_data(withdraw=v); await state.set_state(CreateAdminRef.discount)
    await m.answer("Курс <b>скидки</b> (₽ за 1 pox, например 140):")

@router.message(CreateAdminRef.discount)
async def arl_disc(m: Message, state: FSMContext):
    try: v = float(m.text.strip().replace(",","."))
    except: await m.answer("❗ Число"); return
    d = await state.get_data()
    import random, string
    code = "arl_" + "".join(random.choices(string.ascii_lowercase+string.digits, k=8))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO admin_ref_links (code,label,earn_pox,withdraw_rate,discount_rate,created_at) VALUES (?,?,?,?,?,?)",
            (code, d["label"], d["earn"], d["withdraw"], v, datetime.now().isoformat(timespec="seconds")))
        await db.commit()
    bot_info = await m.bot.get_me()
    url = f"https://t.me/{bot_info.username}?start={code}"
    await state.clear()
    await m.answer(f"✅ Ссылка создана!\n\n<b>{d['label']}</b>\n<code>{url}</code>\n\n"
                   f"Начисл: {d['earn']} pox | Вывод: {d['withdraw']}₽ | Скидка: {v}₽",
        reply_markup=admin_menu_kb())

# ========== MAIN ==========
async def main():
    await db_init()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    log.info("Бот запущен")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
