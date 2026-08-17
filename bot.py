#!/usr/bin/env python3
"""
Claw Royale agent bot.

Diselaraskan dengan skill.md (v1.15.0) + openapi.yaml resmi:
  - Death detection pakai agent_died.meta.youDied (BUKAN agentId, karena
    field itu self-token per-game, bukan uuid asli — lihat Core Rule 18).
  - action_rejected ditangani sama seperti agent_view/turn_advanced.
  - Error TARGET_DEAD (bukan AGENT_DEAD) dipakai buat "target sudah mati,
    ganti target di turn yang sama" — turn TIDAK habis (canAct: true).
  - Move ditolak total selagi inCave (canAct tetap true) -> exit cave
    (interact) diprioritaskan paling atas.
  - Ranking sekarang: alive > survival time DESC > kills DESC > EP ASC.
    HP akhir TIDAK lagi dihitung -> strategi digeser: lebih hati-hati soal
    fight, jangan korbankan waktu hidup demi 1 kill tambahan.
  - Resume ke game yang agent-nya sudah mati akan ditolak (4032 / 1013
    RESUME_TARGET_DEAD) -> re-dial /ws/join sekali, jangan retry-storm.
  - Slot FREE dan PAID itu independen (bisa jalan bersamaan) -> bot ini
    menjalankan keduanya sebagai dua loop terpisah secara default.
  - Fitur ekonomi baru: auto-redeem kode WELCOME, auto-belanja shop
    (pack ticket / material reforge), auto-reforge relic nganggur, cek
    notifikasi (mis. marketplace_sale_completed), dan (opsional, default
    OFF) auto-beli material murah di marketplace.

(Mode Barbar + Dashboard UI + Kill Counter + Smart Weapon Range +
 Fast DZ Escape + Inventory Tracker + Economy Manager + Rat Mode)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

# --------------------------------------------------------------------------
# Config & Setup
# --------------------------------------------------------------------------

API_KEY = os.environ.get("CLAW_API_KEY", "").strip()
BASE_HOST = os.environ.get("CLAW_HOST", "cdn.clawroyale.ai").strip()
REST_BASE = f"https://{BASE_HOST}/api"
WS_JOIN_URL = f"wss://{BASE_HOST}/ws/join"
WS_AGENT_URL = f"wss://{BASE_HOST}/ws/agent"

# "auto" = jalankan slot FREE dan PAID bersamaan (keduanya independen — lihat
# skill.md § State Router). Set "free" atau "paid" untuk cuma jalankan satu.
ENTRY_TYPE_PREFERENCE = os.environ.get("CLAW_ENTRY_TYPE", "auto").strip().lower()
RUN_FREE = ENTRY_TYPE_PREFERENCE in ("auto", "free")
RUN_PAID = ENTRY_TYPE_PREFERENCE in ("auto", "paid")

# Matikan log default INFO agar tidak merusak tampilan Dashboard
LOG_LEVEL = os.environ.get("CLAW_LOG_LEVEL", "WARNING").upper()
STATE_POLL_INTERVAL = float(os.environ.get("CLAW_STATE_POLL_INTERVAL", "5"))
RECONNECT_MIN_DELAY = float(os.environ.get("CLAW_RECONNECT_MIN_DELAY", "1"))
RECONNECT_MAX_DELAY = float(os.environ.get("CLAW_RECONNECT_MAX_DELAY", "30"))
INTER_GAME_DELAY = float(os.environ.get("CLAW_INTER_GAME_DELAY", "3"))

# --- Fitur ekonomi (belanja/reforge/marketplace otomatis) -----------------
CLAW_AUTO_REDEEM_WELCOME = os.environ.get("CLAW_AUTO_REDEEM_WELCOME", "true").strip().lower() == "true"
CLAW_AUTO_SHOP = os.environ.get("CLAW_AUTO_SHOP", "true").strip().lower() == "true"
CLAW_SHOP_MIN_RESERVE = float(os.environ.get("CLAW_SHOP_MIN_RESERVE", "500"))
CLAW_SHOP_MAX_SPEND_PER_CYCLE = float(os.environ.get("CLAW_SHOP_MAX_SPEND_PER_CYCLE", "2000"))
CLAW_AUTO_REFORGE = os.environ.get("CLAW_AUTO_REFORGE", "true").strip().lower() == "true"
# Marketplace = transaksi dengan pemain lain -> default OFF biar aman, aktifkan
# manual lewat env kalau memang mau bot belanja di marketplace juga.
CLAW_AUTO_MARKETPLACE = os.environ.get("CLAW_AUTO_MARKETPLACE", "false").strip().lower() == "true"
CLAW_MARKET_MAX_MATERIAL_PRICE = float(os.environ.get("CLAW_MARKET_MAX_MATERIAL_PRICE", "1500"))
CLAW_ECONOMY_INTERVAL = float(os.environ.get("CLAW_ECONOMY_INTERVAL", "60"))

# File terpisah untuk log error, supaya tidak merusak layar Dashboard (yang pakai stdout).
# Default ke /tmp karena direktori kerja/app di banyak container bersifat read-only.
LOG_FILE = os.environ.get("CLAW_LOG_FILE", "/tmp/clawroyale.log")
_LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
try:
    logging.basicConfig(level=LOG_LEVEL, format=_LOG_FORMAT, filename=LOG_FILE)
except OSError:
    # Tidak bisa menulis file log di lokasi manapun yang dicoba — jangan sampai
    # bot gagal start hanya karena logging. Pakai stderr sebagai fallback.
    logging.basicConfig(level=LOG_LEVEL, format=_LOG_FORMAT, stream=sys.stderr)

log = logging.getLogger("clawroyale")


# --------------------------------------------------------------------------
# DASHBOARD UI SYSTEM (mendukung slot FREE + PAID berjalan bersamaan)
# --------------------------------------------------------------------------

class SlotState:
    """Status tampilan untuk satu slot game (free ATAU paid)."""

    def __init__(self, label: str):
        self.label = label
        self.enabled = True

        self.game_id = "Idle"
        self.turn = 0             # <-- TAMBAHAN BARU
        self.alive_players = "?"  # <-- TAMBAHAN BARU
        self.kills = 0
        self.hp = "N/A"
        self.ep = "N/A"
        self.region = "N/A"
        self.is_dz = False
        self.weapon = "Kosong"
        self.armor = "Kosong"
        self.inventory: list[str] = []
        self.enemies = 0
        self.monsters = 0
        self.loot = 0

        self.last_action = "-"
        self.action_status = "-"
        self.reason = "Standby"

    def block(self) -> str:
        dz_warn = "⚠️ BAHAYA!" if self.is_dz else "✅ AMAN"
        inv_text = ", ".join(self.inventory[:5]) if self.inventory else "Tas Kosong"
        if len(self.inventory) > 5:
            inv_text += f" (+{len(self.inventory) - 5} lagi)"
        return (
            f"  Room      : {self.game_id} (Turn: {self.turn} | Sisa Agent: {self.alive_players})\n"
            f"  Kills     : 💀 {self.kills}    HP: {self.hp}    EP: {self.ep}\n"
            f"  Posisi    : {self.region}   [{dz_warn}]\n"
            f"  Equipment : Senjata={self.weapon} | Armor={self.armor}\n"
            f"  Radar     : {self.enemies} Musuh, {self.monsters} Monster, {self.loot} Loot\n"
            f"  Tas       : {inv_text}\n"
            f"  Aksi      : {self.last_action}  [{self.action_status}]\n"
            f"  Alasan    : {self.reason}\n"
        )


class Dashboard:
    def __init__(self):
        # Akun Info
        self.acc_name = "Loading..."
        self.acc_balance = "Loading..."
        self.acc_wallet = "Loading..."

        # Ekonomi
        self.pack_pity = "N/A"
        self.material_pity = "N/A"
        self.last_shop_action = "-"

        # Slot game (free / paid, independen — lihat State Router)
        self.slots: dict[str, SlotState] = {
            "free": SlotState("FREE"),
            "paid": SlotState("PAID"),
        }

        # Throttle biar tidak flicker/boros CPU saat banyak free actions beruntun
        self._min_render_interval = 0.08
        self._last_render_ts = 0.0

    def render(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_render_ts) < self._min_render_interval:
            return
        self._last_render_ts = now

        parts = ["\033[2J\033[H"]
        parts.append("=========================================================\n")
        parts.append(" CLAW ROYALE BOT - DASHBOARD \n")
        parts.append("=========================================================\n")
        parts.append("=== AKUN ===\n")
        parts.append(f"  Nama: {self.acc_name}   Balance: {self.acc_balance}   Wallet: {self.acc_wallet}\n\n")
        parts.append("=== EKONOMI ===\n")
        parts.append(f"  Pack Pity: {self.pack_pity}    Material Pity: {self.material_pity}\n")
        parts.append(f"  Aksi Terakhir: {self.last_shop_action}\n\n")

        for slot in self.slots.values():
            if not slot.enabled:
                continue
            parts.append(f"=== SLOT {slot.label} ===\n")
            parts.append(slot.block())
            parts.append("\n")

        parts.append("=========================================================\n")
        sys.stdout.write("".join(parts))
        sys.stdout.flush()


dash = Dashboard()


# --------------------------------------------------------------------------
# REST client
# --------------------------------------------------------------------------

class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


class RestClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.version = "1"
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "RestClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    def _headers(self) -> dict:
        return {"X-API-Key": self.api_key, "X-Version": self.version, "Content-Type": "application/json"}

    async def fetch_version(self) -> str:
        assert self._session
        async with self._session.get(f"{REST_BASE}/version") as resp:
            data = await resp.json()
            v = str(data.get("version") or data.get("data", {}).get("version") or "1")
            self.version = v
            return v

    async def request(self, method: str, path: str, extra_headers: Optional[dict] = None, **kwargs) -> dict:
        assert self._session
        url = f"{REST_BASE}{path}"
        headers = self._headers()
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(3):
            async with self._session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status == 426:
                    await self.fetch_version()
                    headers = self._headers()
                    if extra_headers:
                        headers.update(extra_headers)
                    continue
                text = await resp.text()
                try:
                    data = json.loads(text) if text else {}
                except json.JSONDecodeError:
                    data = {"raw": text}
                if resp.status >= 400:
                    err = data.get("error", {}) if isinstance(data, dict) else {}
                    raise ApiError(resp.status, err.get("code", "UNKNOWN"), err.get("message", text[:200]))
                return data
        raise ApiError(426, "VERSION_MISMATCH", "failed after retry")

    # -- account -----------------------------------------------------------
    async def get_me(self) -> dict:
        data = await self.request("GET", "/accounts/me")
        if "data" in data and isinstance(data.get("data"), dict) and "name" not in data:
            return data["data"]
        return data

    async def get_balance(self) -> dict:
        return await self.request("GET", "/accounts/me/balance")

    # -- loadout -------------------------------------------------------------
    async def get_loadout(self) -> dict:
        return await self.request("GET", "/loadout")

    async def get_inventory_relics(self) -> dict:
        return await self.request("GET", "/inventory/relics")

    async def get_inventory_packs(self) -> dict:
        return await self.request("GET", "/inventory/packs")

    async def get_inventory_items(self, category: str) -> dict:
        return await self.request("GET", "/inventory/items", params={"category": category})

    async def set_active_pack(self, pack_instance_id: int) -> dict:
        return await self.request("PUT", "/loadout/pack", json={"packInstanceId": pack_instance_id})

    async def set_sub_pack(self, pack_instance_id: int) -> dict:
        return await self.request("PUT", "/loadout/sub-pack", json={"packInstanceId": pack_instance_id})

    async def equip_relic(self, type_index: int, relic_instance_id: int) -> dict:
        return await self.request("PUT", f"/loadout/slot/{type_index}", json={"relicInstanceId": relic_instance_id})

    # -- shop (v1) -----------------------------------------------------------
    async def get_shop_listings(self) -> dict:
        return await self.request("GET", "/shop/listings")

    async def shop_purchase(self, listing_id: int, quantity: int = 1, idempotency_key: Optional[str] = None) -> dict:
        extra = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        return await self.request("POST", "/shop/purchase", extra_headers=extra,
                                   json={"listingId": listing_id, "quantity": quantity})

    async def get_shop_inventory_status(self) -> dict:
        return await self.request("GET", "/shop/inventory-status")

    # -- reforge ---------------------------------------------------------
    async def reforge(self, relic_instance_id: int, item_key: str, idempotency_key: str) -> dict:
        return await self.request("POST", "/reforge", json={
            "relicInstanceId": relic_instance_id,
            "itemKey": item_key,
            "idempotencyKey": idempotency_key,
        })

    # -- marketplace (P2P) ------------------------------------------------
    async def get_marketplace_listings(self, item_type: Optional[str] = None,
                                        cursor: Optional[str] = None, limit: int = 20) -> dict:
        params: dict[str, Any] = {"limit": limit}
        if item_type:
            params["itemType"] = item_type
        if cursor:
            params["cursor"] = cursor
        return await self.request("GET", "/marketplace/listings", params=params)

    async def marketplace_buy(self, listing_id: int, quantity: Optional[int] = None,
                               idempotency_key: Optional[str] = None) -> dict:
        extra = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        body: dict[str, Any] = {}
        if quantity is not None:
            body["quantity"] = quantity
        return await self.request("POST", f"/marketplace/listings/{listing_id}/buy", extra_headers=extra, json=body)

    async def marketplace_list(self, item_type: str, price: str, relic_instance_id: Optional[int] = None,
                                pack_instance_id: Optional[int] = None, item_key: Optional[str] = None,
                                quantity: Optional[int] = None, idempotency_key: Optional[str] = None) -> dict:
        body: dict[str, Any] = {"itemType": item_type, "price": price}
        if relic_instance_id is not None:
            body["relicInstanceId"] = relic_instance_id
        if pack_instance_id is not None:
            body["packInstanceId"] = pack_instance_id
        if item_key is not None:
            body["itemKey"] = item_key
        if quantity is not None:
            body["quantity"] = quantity
        extra = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        return await self.request("POST", "/marketplace/listings", extra_headers=extra, json=body)

    async def marketplace_cancel(self, listing_id: int) -> dict:
        return await self.request("DELETE", f"/marketplace/listings/{listing_id}")

    # -- notifications ------------------------------------------------------
    async def get_notifications(self, unread_only: bool = True, limit: int = 20) -> dict:
        return await self.request("GET", "/notifications",
                                   params={"unreadOnly": str(unread_only).lower(), "limit": limit})

    async def mark_notification_read(self, notification_id: int) -> dict:
        return await self.request("POST", f"/notifications/{notification_id}/read")

    async def mark_all_notifications_read(self) -> dict:
        return await self.request("POST", "/notifications/read-all")

    # -- redeem ------------------------------------------------------------
    async def redeem(self, code: str) -> dict:
        return await self.request("POST", "/redeem", json={"code": code})

    # -- dashboard (self-performance, read-only) -----------------------------
    async def get_dashboard_overview(self) -> dict:
        # Catatan: endpoint dashboard/* mengembalikan object view LANGSUNG,
        # TANPA amplop {success, data} — beda dari kebanyakan endpoint lain.
        return await self.request("GET", "/accounts/me/dashboard/overview")


async def ensure_loadout(rest: RestClient) -> None:
    """fullSet (Main pack + Sub pack + 3 relic) WAJIB supaya efek relic/pack
    aktif sama sekali — partial set = base stats doang, nol efek."""
    try:
        loadout = (await rest.get_loadout()).get("data", {})
    except ApiError as e:
        log.warning("ensure_loadout: gagal ambil loadout: %s", e)
        return

    if loadout.get("fullSet"):
        return

    try:
        packs = (await rest.get_inventory_packs()).get("data", [])
        relics = (await rest.get_inventory_relics()).get("data", [])
    except ApiError as e:
        log.warning("ensure_loadout: gagal ambil packs/relics: %s", e)
        return

    active_pack = loadout.get("activePack")
    slots = loadout.get("slots") or [None, None, None]

    if not active_pack and packs:
        try:
            await rest.set_active_pack(packs[0]["instanceId"])
        except ApiError as e:
            log.warning("set_active_pack gagal: %s", e)
        # Sub pack TIDAK opsional — tanpa itu fullSet tidak akan pernah true.
        if len(packs) > 1:
            try:
                await rest.set_sub_pack(packs[1]["instanceId"])
            except ApiError as e:
                log.warning("set_sub_pack gagal: %s", e)

    for type_index in range(3):
        if slots[type_index]:
            continue
        candidate = next((r for r in relics if r.get("typeIndex") == type_index), None)
        if candidate:
            try:
                await rest.equip_relic(type_index, candidate["instanceId"])
            except ApiError as e:
                log.warning("equip_relic gagal: %s", e)


# --------------------------------------------------------------------------
# Economy manager — redeem, belanja shop, reforge, notifikasi, marketplace
# --------------------------------------------------------------------------

async def _run_redeem_welcome(rest: RestClient) -> None:
    if not CLAW_AUTO_REDEEM_WELCOME:
        return
    try:
        res = await rest.redeem("WELCOME")
        data = res.get("data", res)
        if data.get("replayed"):
            dash.last_shop_action = "Kode WELCOME sudah pernah diklaim sebelumnya."
        else:
            n = len(data.get("items", []) or [])
            dash.last_shop_action = f"Redeem WELCOME sukses ({n} item didapat)!"
    except ApiError as e:
        # Kalau memang tidak eligible/sudah pernah, biarkan lewat — bukan fatal.
        log.warning("Redeem WELCOME gagal: %s", e)
    dash.render(force=True)


async def _update_economy_status(rest: RestClient) -> None:
    try:
        bal = (await rest.get_balance()).get("data", {})
        if "balance" in bal:
            dash.acc_balance = f"{bal['balance']} sMoltz"
    except ApiError as e:
        log.warning("Gagal ambil balance: %s", e)

    try:
        status = (await rest.get_shop_inventory_status())
        data = status.get("data", status)
        pack_pity = data.get("packPity", {}) or {}
        mat_pity = data.get("materialPity", {}) or {}
        dash.pack_pity = f"{pack_pity.get('counter', '?')}/{pack_pity.get('target', '?')}"
        if pack_pity.get("guaranteed"):
            dash.pack_pity += " (T1 GUARANTEED!)"
        dash.material_pity = f"{mat_pity.get('counter', '?')}/{mat_pity.get('target', '?')}"
    except ApiError as e:
        log.warning("Gagal ambil shop inventory-status: %s", e)


async def _check_notifications(rest: RestClient) -> None:
    try:
        notif = await rest.get_notifications(unread_only=True, limit=20)
        items = notif.get("data", notif).get("items", [])
    except ApiError as e:
        log.warning("Gagal ambil notifications: %s", e)
        return

    for it in items:
        if it.get("kind") == "marketplace_sale_completed":
            payload = it.get("payload") or {}
            dash.last_shop_action = f"💰 Listing terjual! +{payload.get('netAmount', '?')} sMoltz (setelah fee 7%)"
            dash.render(force=True)
        try:
            await rest.mark_notification_read(it.get("id"))
        except ApiError as e:
            log.warning("Gagal tandai notifikasi terbaca: %s", e)


async def _auto_shop(rest: RestClient) -> None:
    if not CLAW_AUTO_SHOP:
        return
    try:
        listings = (await rest.get_shop_listings()).get("data", {}).get("listings", [])
    except ApiError as e:
        log.warning("Gagal ambil shop listings: %s", e)
        return
    if not listings:
        return

    try:
        balance = (await rest.get_balance()).get("data", {}).get("balance", 0)
    except ApiError:
        return

    spend_cap = min(balance - CLAW_SHOP_MIN_RESERVE, CLAW_SHOP_MAX_SPEND_PER_CYCLE)
    if spend_cap <= 0:
        return

    # Prioritas: material bundle (buat stok batu reforge) dulu, baru pack ticket.
    materials = [l for l in listings if l.get("category") == "material"]
    tickets = [l for l in listings if l.get("category") == "gacha_ticket"]

    for listing in materials + tickets:
        try:
            price = float(listing.get("priceAmount", "0"))
        except (TypeError, ValueError):
            continue
        if price <= 0 or price > spend_cap:
            continue
        try:
            idem = str(uuid.uuid4())
            await rest.shop_purchase(listing["id"], quantity=1, idempotency_key=idem)
            dash.last_shop_action = f"🛒 Beli \"{listing.get('name')}\" seharga {price:.0f} sMoltz"
            dash.render(force=True)
            spend_cap -= price
        except ApiError as e:
            log.warning("Shop purchase gagal (%s): %s", listing.get("name"), e)
        if spend_cap <= 0:
            break


async def _auto_reforge(rest: RestClient) -> None:
    if not CLAW_AUTO_REFORGE:
        return
    try:
        materials = (await rest.get_inventory_items("material")).get("data", [])
    except ApiError as e:
        log.warning("Gagal ambil material stone: %s", e)
        return
    stones = [m.get("itemKey") for m in materials if (m.get("quantity") or 0) > 0]
    if not stones:
        return

    try:
        relics = (await rest.get_inventory_relics()).get("data", [])
    except ApiError as e:
        log.warning("Gagal ambil relics: %s", e)
        return

    # Cuma relic yang TIDAK sedang dipasang di pack aktif & tidak sedang
    # dilelang di marketplace yang boleh direforge (dilarang server kalau tidak).
    candidates = [r for r in relics if not r.get("equippedPackInstanceId") and not r.get("isListed")]
    if not candidates:
        return

    target = candidates[0]
    stone_key = stones[0]
    try:
        idem = str(uuid.uuid4())
        result = await rest.reforge(target["instanceId"], stone_key, idem)
        outcome = result.get("data", {}).get("outcome", "?")
        dash.last_shop_action = f"⚒️ Reforge {target.get('baseName', '?')} → {outcome}"
        dash.render(force=True)
    except ApiError as e:
        log.warning("Reforge gagal: %s", e)


async def _auto_marketplace(rest: RestClient) -> None:
    """OPSIONAL (default OFF, aktifkan via CLAW_AUTO_MARKETPLACE=true). Beli
    1 listing material termurah di bawah batas harga per siklus — supaya
    tidak sembarangan menghabiskan saldo untuk barang orang lain."""
    if not CLAW_AUTO_MARKETPLACE:
        return
    try:
        listings = (await rest.get_marketplace_listings(item_type="material")).get("data", {}).get("items", [])
    except ApiError as e:
        log.warning("Gagal ambil marketplace listings: %s", e)
        return

    try:
        balance = (await rest.get_balance()).get("data", {}).get("balance", 0)
    except ApiError:
        return

    for listing in listings:
        if listing.get("isMine") or listing.get("status") != "active":
            continue
        try:
            price = float(listing.get("price", "0"))
        except (TypeError, ValueError):
            continue
        if price <= 0 or price > CLAW_MARKET_MAX_MATERIAL_PRICE or price > (balance - CLAW_SHOP_MIN_RESERVE):
            continue
        try:
            idem = str(uuid.uuid4())
            await rest.marketplace_buy(listing["id"], quantity=1, idempotency_key=idem)
            dash.last_shop_action = f"Beli material di marketplace seharga {price:.0f} sMoltz"
            dash.render(force=True)
        except ApiError as e:
            log.warning("Marketplace buy gagal: %s", e)
        break  # satu transaksi per siklus, biar tidak boros request/saldo


async def economy_loop(rest: RestClient, stop: asyncio.Event) -> None:
    """Loop terpisah dari gameplay: cek saldo/pity, redeem, belanja, reforge,
    notifikasi, dan (opsional) marketplace, berjalan tiap CLAW_ECONOMY_INTERVAL
    detik tanpa mengganggu loop permainan."""
    await _run_redeem_welcome(rest)

    while not stop.is_set():
        try:
            await _update_economy_status(rest)
            await _check_notifications(rest)
            dash.render(force=True)
            await _auto_shop(rest)
            await _auto_reforge(rest)
            await _auto_marketplace(rest)
        except ApiError as e:
            log.warning("Economy cycle ApiError: %s", e)
        except Exception:
            log.exception("Economy cycle gagal")

        try:
            await asyncio.wait_for(stop.wait(), timeout=CLAW_ECONOMY_INTERVAL)
        except asyncio.TimeoutError:
            pass


# --------------------------------------------------------------------------
# Decision logic
# --------------------------------------------------------------------------

@dataclass
class Decision:
    kind: str
    target_region_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_monster_id: Optional[str] = None
    ruin_id: Optional[str] = None
    item_id: Optional[str] = None
    interactable_id: Optional[str] = None
    message: Optional[str] = None
    reason: str = ""


def is_cooldown_action(kind: str) -> bool:
    return kind in {"move", "attack", "explore", "use_item", "interact", "wait", "rest", "drop"}


def _get_weapon_range(w: dict) -> float:
    """Ambil jarak jangkau senjata, coba beberapa kemungkinan nama field
    (API bisa saja memakai nama berbeda), fallback ke 1 (Melee)."""
    for key in ("range", "atkRange", "attackRange", "weaponRange", "reach", "distance"):
        val = w.get(key)
        if isinstance(val, (int, float)) and val > 0:
            return val
    return 1


def _pick_engagement_target(view: dict, self_state: dict):
    """Tentukan target yang KEMUNGKINAN BESAR akan diserang oleh decide(),
    supaya pemilihan senjata (ranged/melee) sinkron dengan target itu — bukan
    cuma musuh yang paling dekat secara jarak, yang bisa saja beda dengan
    target yang benar-benar mau diserang."""
    visible_agents = view.get("visibleAgents") or []
    visible_monsters = view.get("visibleMonsters") or []
    all_enemies = visible_agents + visible_monsters
    if not all_enemies:
        return None

    hp = self_state.get("hp", 100)
    non_guardian = [a for a in visible_agents if not a.get("isGuardian")]
    # Margin "menang" dipersempit (lihat catatan ranking di decide()) supaya
    # weapon-switch juga konsisten dengan target yang benar-benar layak diserang.
    winnable = [a for a in non_guardian if a.get("hp", 999) <= hp * 1.15]
    pool = winnable or non_guardian

    if pool:
        return min(pool, key=lambda a: a.get("hp", 999))
    if visible_monsters:
        return min(visible_monsters, key=lambda m: m.get("hp", 999))
    return min(all_enemies, key=lambda e: e.get("distance", 0))


def decide_free_actions(view: dict) -> list[Decision]:
    """Free actions (0 cooldown): fast looting + smart equip (Jarak Musuh vs Weapon)."""
    free_decisions = []
    self_state = view.get("self", {}) or {}
    inventory = self_state.get("inventory") or []
    equipped_weapon = self_state.get("equippedWeapon")
    equipped_armor = self_state.get("equippedArmor")

    engagement_target = _pick_engagement_target(view, self_state)
    target_distance = engagement_target.get("distance", 0) if engagement_target else 0

    # ---------------------------------------------------------
    # 1. SMART AUTO-EQUIP SENJATA (RANGE VS MELEE)
    # ---------------------------------------------------------
    all_weapons = [i for i in inventory if i.get("category") == "weapon"]
    if isinstance(equipped_weapon, dict) and equipped_weapon.get("category") == "weapon":
        all_weapons.append(equipped_weapon)

    if all_weapons:
        def weapon_score(w):
            atk = w.get("atkBonus", 0) or w.get("damage", 0) or w.get("atk", 0) or w.get("power", 0) or 0
            w_range = _get_weapon_range(w)

            if target_distance > 1:
                if w_range > 1:
                    return atk + 1000
                return atk
            else:
                if w_range <= 1:
                    return atk + 1000
                return atk

        best_weapon = max(sorted(all_weapons, key=lambda w: str(w.get("id", ""))), key=weapon_score)

        is_already_equipped = False
        if equipped_weapon:
            if isinstance(equipped_weapon, dict) and equipped_weapon.get("id") == best_weapon.get("id"):
                is_already_equipped = True
            elif isinstance(equipped_weapon, str) and equipped_weapon == best_weapon.get("id"):
                is_already_equipped = True

        if not is_already_equipped and best_weapon.get("id"):
            w_type = "RANGED" if _get_weapon_range(best_weapon) > 1 else "MELEE"
            free_decisions.append(Decision(
                kind="equip",
                item_id=best_weapon.get("id"),
                reason=f"Ganti Senjata ({w_type}, jarak musuh~{target_distance}): {best_weapon.get('name')}"
            ))
            self_state["equippedWeapon"] = best_weapon

    # ---------------------------------------------------------
    # 2. AUTO-EQUIP ARMOR TERBAIK
    # ---------------------------------------------------------
    all_armors = [i for i in inventory if i.get("category") in ["armor", "equipment"]]
    if isinstance(equipped_armor, dict):
        all_armors.append(equipped_armor)

    if all_armors:
        def armor_score(a):
            return a.get("defBonus", 0) or a.get("defense", 0) or a.get("def", 0) or a.get("armor", 0) or a.get("hpMax", 0) or 0

        best_armor = max(sorted(all_armors, key=lambda a: str(a.get("id", ""))), key=armor_score)

        is_already_equipped_armor = False
        if equipped_armor:
            if isinstance(equipped_armor, dict) and equipped_armor.get("id") == best_armor.get("id"):
                is_already_equipped_armor = True
            elif isinstance(equipped_armor, str) and equipped_armor == best_armor.get("id"):
                is_already_equipped_armor = True

        if not is_already_equipped_armor and best_armor.get("id"):
            def_val = armor_score(best_armor)
            free_decisions.append(Decision(
                kind="equip",
                item_id=best_armor.get("id"),
                reason=f"Auto-equip Armor: {best_armor.get('name')} (DEF: {def_val})"
            ))

    # ---------------------------------------------------------
    # 3. FAST AUTO-PICKUP (Sapu Bersih Lootingan)
    # ---------------------------------------------------------
    raw_visible_items = []
    if isinstance(view.get("visibleItems"), list):
        raw_visible_items.extend(view.get("visibleItems"))
    current_region = view.get("currentRegion") or {}
    for key in ["items", "groundItems", "droppedItems"]:
        if isinstance(current_region.get(key), list):
            raw_visible_items.extend(current_region.get(key))

    unique_items = {item.get("id"): item for item in raw_visible_items if item.get("id")}
    if unique_items:
        inv_count = len(inventory)
        for item_id, item in unique_items.items():
            if inv_count >= 10:
                break
            free_decisions.append(Decision(
                kind="pickup",
                item_id=item_id,
                reason=f"Ambil {item.get('name', 'Item')}"
            ))
            inv_count += 1

    # ---------------------------------------------------------
    # 4. SMART INVENTORY MANAGEMENT (Buang Junk)
    # ---------------------------------------------------------
    if len(inventory) >= 10:
        equipped_ids = set()
        if isinstance(equipped_weapon, dict): equipped_ids.add(equipped_weapon.get("id"))
        elif isinstance(equipped_weapon, str): equipped_ids.add(equipped_weapon)
        if isinstance(equipped_armor, dict): equipped_ids.add(equipped_armor.get("id"))
        elif isinstance(equipped_armor, str): equipped_ids.add(equipped_armor)

        bag_items = [i for i in inventory if i.get("id") not in equipped_ids]
        junk_items = [i for i in bag_items if i.get("category") == "junk"]
        
        if junk_items:
            item_to_drop = junk_items[0]
            free_decisions.append(Decision(
                kind="drop",
                item_id=item_to_drop.get("id"),
                reason=f"Tas Penuh: Buang {item_to_drop.get('name')}."
            ))

    return free_decisions


def decide(view: dict, session: "GameSession") -> Decision:
    """Action Utama (Cooldown Action)."""
    self_state = view.get("self", {}) or {}
    hp = self_state.get("hp", 100)
    max_hp_guess = self_state.get("maxHp", 100) or 100
    ep = self_state.get("ep", 0)
    in_cave = self_state.get("inCave", False)

    current_region = view.get("currentRegion", {}) or {}
    is_death_zone = current_region.get("isDeathZone", False)
    connections = current_region.get("connections") or []

    visible_agents = view.get("visibleAgents") or []
    visible_monsters = view.get("visibleMonsters") or []
    visible_ruins = view.get("visibleRuins") or []
    pending_deathzones = view.get("pendingDeathzones") or []

    hp_ratio = hp / max_hp_guess if max_hp_guess else 1.0
    inventory_items = self_state.get("inventory") or []
    recovery_items = [i for i in inventory_items if i.get("category") in ["recovery", "consumable"]]

    # ---------------------------------------------------------
    # 0. KELUAR DARI GUA — move DITOLAK TOTAL selagi inCave (server memvalidasi
    #    ini SEBELUM cek adjacency, apapun kondisinya), jadi ini prioritas
    #    paling atas: percuma coba kabur/heal-position kalau masih di gua.
    # ---------------------------------------------------------
    if in_cave:
        facilities = view.get("visibleFacilities") or current_region.get("facilities") or current_region.get("interactables") or []
        if facilities:
            return Decision(kind="interact", interactable_id=facilities[0].get("id"), reason="🚪 Keluar dari Gua (Cave) dulu — move diblokir selagi di gua.")
        return Decision(kind="interact", reason="🚪 Mencoba keluar dari Gua.")

    # ---------------------------------------------------------
    # 1. SURVIVAL & RECOVERY
    # ---------------------------------------------------------

    # 1.1) FAST DEATH ZONE ESCAPE (Cari Jalur 100% Aman) — PRIORITAS TERTINGGI
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        safe_targets = [c for c in connections if c not in pending_here_ids]
        if safe_targets:
            really_safe = [c for c in safe_targets if c not in session.dangerous_regions]
            chosen = random.choice(really_safe) if really_safe else random.choice(safe_targets)
            return Decision(kind="move", target_region_id=chosen, reason="ZONA BERBAHAYA, KABUR!")
        elif connections:
            return Decision(kind="move", target_region_id=random.choice(connections), reason="KABUR DARURAT!")

    # 1.2) EARLY AUTO-HEAL (Naik ke 85%)
    if hp_ratio < 0.85 and recovery_items:
        best_hp_item = max(recovery_items, key=lambda i: i.get("hpRestore", 0))
        if best_hp_item.get("hpRestore", 0) > 0:
            return Decision(kind="use_item", item_id=best_hp_item.get("id"), reason=f"Heal Pakai {best_hp_item.get('name')}")

    # 1.3) Critical HP + No Potions -> Hard Retreat
    if hp_ratio < 0.25:
        if connections:
            return Decision(kind="move", target_region_id=random.choice(connections), reason=f"HP Sekarat ({hp_ratio:.0%}) & Habis Potion — Mundur!")
        return Decision(kind="wait", reason=f"HP Sekarat ({hp_ratio:.0%}) & Habis Potion, Tapi tidak ada jalan keluar.")

    # 1.4) Auto-Energy
    if ep < 2 and recovery_items:
        def ep_score(i):
            return i.get("epRestore", 0) or i.get("spRestore", 0) or i.get("energyRestore", 0) or 0
        best_ep_item = max(recovery_items, key=ep_score)
        if ep_score(best_ep_item) > 0:
            return Decision(kind="use_item", item_id=best_ep_item.get("id"), reason=f"Isi Stamina Pakai {best_ep_item.get('name')}")

    # 1.5) Hindari Kerumunan Massal (>12 orang)
    if len(visible_agents) > 12 and connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="Terlalu ramai, Reposisi!")

    # ---------------------------------------------------------
    # 2. FIGHT — MODE RAT/SURVIVAL (Prioritas Bertahan Hidup)
    # ---------------------------------------------------------
    if visible_agents and ep > 0:
        non_guardian = [a for a in visible_agents if not a.get("isGuardian")]
        
        # PRIORITAS 1: NYAMPAH (Kill Steal). Cuma serang kalau HP musuh <= 20%
        dying_enemies = [a for a in non_guardian if a.get("hp", 999) <= max_hp_guess * 0.20]
        if dying_enemies:
            weakest_dying = min(dying_enemies, key=lambda a: a.get("hp", 999))
            return Decision(kind="attack", target_agent_id=weakest_dying.get("id"), reason="Nyampah agent sekarat!")

        # PRIORITAS 2: KABUR! Kalau ada player sehat, jangan ladenin. Biarin mereka baku hantam.
        if connections:
            safe_routes = [c for c in connections if c not in session.dangerous_regions]
            chosen_route = random.choice(safe_routes) if safe_routes else random.choice(connections)
            return Decision(kind="move", target_region_id=chosen_route, reason="Ada player HP Jos, mending pindah aman!")

    # Cuma hunting monster kalau HP kita > 70% biar nggak gampang dibokong player lain.
    if visible_monsters and ep > 0 and hp_ratio >= 0.70:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        return Decision(kind="attack", target_monster_id=weakest_monster.get("id"), reason="Aman, hunting monster.")

    # ---------------------------------------------------------
    # 3. INTERAKSI & EKSPLORASI CERDAS
    # ---------------------------------------------------------
    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    # Stop explore kalau alert udah mulai merah (maksimal 3 biar ga dipanggil guardian)
    if visible_ruins and not alert_active and alert_gauge <= 3:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(kind="explore", ruin_id=ruin.get("ruinId"), reason=f"Eksplorasi Ruin (Alert: {alert_gauge})")

    if connections:
        safe_connections = [c for c in connections if c not in session.dangerous_regions]
        if safe_connections:
            return Decision(kind="move", target_region_id=random.choice(safe_connections), reason="Pindah ke zona aman.")
        return Decision(kind="move", target_region_id=random.choice(connections), reason="Pindah Semua map berisiko.")

    return Decision(kind="wait", reason="Standby nunggu energi penuh.")


def build_action_payload(decision: Decision) -> dict:
    data: dict[str, Any] = {}
    if decision.kind == "move" and decision.target_region_id:
        data = {"type": "move", "regionId": decision.target_region_id}
    elif decision.kind == "attack":
        target_id = decision.target_agent_id or decision.target_monster_id
        if target_id:
            target_type = "agent" if decision.target_agent_id else "monster"
            data = {"type": "attack", "targetId": target_id, "targetType": target_type}
    elif decision.kind == "explore":
        data = {"type": "explore"}
    elif decision.kind == "use_item" and decision.item_id:
        data = {"type": "use_item", "itemId": decision.item_id}
    elif decision.kind == "interact":
        data = {"type": "interact"}
        if decision.interactable_id:
            data["interactableId"] = decision.interactable_id
    elif decision.kind == "pickup" and decision.item_id:
        data = {"type": "pickup", "itemId": decision.item_id}
    elif decision.kind == "drop" and decision.item_id:
        data = {"type": "drop", "itemId": decision.item_id}
    elif decision.kind == "equip" and decision.item_id:
        data = {"type": "equip", "itemId": decision.item_id}
    elif decision.kind == "talk" and decision.message:
        data = {"type": "talk", "message": decision.message[:200]}
    elif decision.kind == "whisper" and decision.target_agent_id and decision.message:
        data = {"type": "whisper", "targetId": decision.target_agent_id, "message": decision.message[:200]}
    elif decision.kind == "broadcast" and decision.message:
        data = {"type": "broadcast", "message": decision.message[:500]}
    elif decision.kind == "wait":
        data = {"type": "rest"}
    else:
        data = {"type": decision.kind}

    payload: dict[str, Any] = {"type": "action", "data": data}
    if decision.reason:
        payload["thought"] = {"reasoning": decision.reason[:500]}
    return payload


# --------------------------------------------------------------------------
# Gameplay WebSocket loop
# --------------------------------------------------------------------------

@dataclass
class GameSession:
    entry_type: str
    can_act: bool = False
    alive: bool = True
    game_id: Optional[str] = None
    last_view: dict = field(default_factory=dict)
    last_view_turn: Optional[int] = None
    last_decision_kind: Optional[str] = None
    last_action_target: Optional[str] = None
    consecutive_same_target: int = 0
    confirmed_dead_targets: set = field(default_factory=set)
    last_seen_target_hp: dict = field(default_factory=dict)
    last_equipment_signature: Optional[str] = None
    dangerous_regions: set = field(default_factory=set)
    region_before_last_move: Optional[str] = None
    last_move_target_region: Optional[str] = None
    consecutive_failed_moves: int = 0
    last_explored_ruin_id: Optional[str] = None
    recently_attempted_free_actions: set = field(default_factory=set)
    acting_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def get_target_current_hp(view: dict, target_id: str) -> Optional[int]:
    for a in (view.get("visibleAgents") or []):
        if a.get("id") == target_id:
            return a.get("hp")
    for m in (view.get("visibleMonsters") or []):
        if m.get("id") == target_id:
            return m.get("hp")
    return None


async def send_hello(ws, entry_type: str) -> None:
    hello = {"type": "hello", "entryType": entry_type}
    await ws.send(json.dumps(hello))


def _is_in_danger(view: dict) -> bool:
    """Cek cepat apakah agent sedang di Death Zone atau region yang akan
    segera meledak — dipakai untuk memutuskan apakah free actions (looting/
    equip) perlu dilewati supaya perintah kabur bisa dikirim secepat mungkin."""
    current_region = view.get("currentRegion", {}) or {}
    pending_deathzones = view.get("pendingDeathzones") or []
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    return bool(current_region.get("isDeathZone", False) or current_region.get("id") in pending_here_ids)


async def update_dashboard_state(view: dict, slot: SlotState) -> None:
    """Memperbarui variabel dashboard (slot free/paid) agar sinkron dengan data server"""
    self_state = view.get("self", {}) or {}
    
    # === Info Dashboard Baru ===
    slot.turn = view.get("turn", slot.turn)
    slot.alive_players = view.get("aliveAgents", "?") 
    # ===========================

    slot.hp = f"{self_state.get('hp', 0)}/{self_state.get('maxHp', 0)}"
    slot.ep = str(self_state.get("ep", 0))
    slot.kills = self_state.get("kills", 0)

    current_region = view.get("currentRegion", {}) or {}
    slot.region = current_region.get("id", "N/A")
    slot.is_dz = _is_in_danger(view)

    slot.enemies = len(view.get("visibleAgents") or [])
    slot.monsters = len(view.get("visibleMonsters") or [])

    raw_items = (view.get("visibleItems") or []) + (current_region.get("items") or []) + (current_region.get("groundItems") or [])
    slot.loot = len(raw_items)

    equipped_w = self_state.get("equippedWeapon")
    slot.weapon = equipped_w.get("name") if isinstance(equipped_w, dict) else (str(equipped_w) if equipped_w else "(Kosong)")

    equipped_a = self_state.get("equippedArmor")
    slot.armor = equipped_a.get("name") if isinstance(equipped_a, dict) else (str(equipped_a) if equipped_a else "(Kosong)")

    inventory_items = self_state.get("inventory") or []
    item_counts: dict[str, int] = {}
    for item in inventory_items:
        name = item.get("name", "Unknown Item")
        qty = item.get("quantity", 1)
        item_counts[name] = item_counts.get(name, 0) + qty
    slot.inventory = [f"{name} x{qty}" for name, qty in item_counts.items()] if item_counts else []


async def maybe_act(ws, session: GameSession, view: dict, slot: SlotState) -> None:
    if not view:
        return

    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False or not session.alive:
        return

    if session.acting_lock.locked():
        return

    async with session.acting_lock:
        in_danger = _is_in_danger(view)

        if not in_danger:
            free_actions = decide_free_actions(view)
            for fd in free_actions:
                dedup_key = f"{fd.kind}:{fd.item_id or fd.interactable_id or fd.message or ''}"
                if dedup_key in session.recently_attempted_free_actions:
                    continue
                session.recently_attempted_free_actions.add(dedup_key)

                payload = build_action_payload(fd)
                slot.last_action = fd.kind.upper()
                slot.reason = fd.reason
                slot.action_status = "Terkirim => Tanpa cooldown"
                dash.render()
                await ws.send(json.dumps(payload))
                await asyncio.sleep(0.01)
        else:
            slot.reason = "BAHAYA! Skip looting/equip, prioritas KABUR..."
            dash.render()

        if not session.can_act:
            return

        decision = decide(view, session)

        current_target = decision.ruin_id or decision.target_monster_id or decision.target_agent_id

        if decision.kind == "attack" and current_target:
            target_confirmed_dead = current_target in session.confirmed_dead_targets
            current_target_hp = get_target_current_hp(view, current_target)
            previously_seen_hp = session.last_seen_target_hp.get(current_target)
            is_same_target_as_last_attack = (current_target == session.last_action_target and session.last_decision_kind == "attack")
            no_damage_landed = (is_same_target_as_last_attack and previously_seen_hp is not None
                                 and current_target_hp is not None and current_target_hp == previously_seen_hp)
            target_healing = (is_same_target_as_last_attack and previously_seen_hp is not None
                               and current_target_hp is not None and current_target_hp > previously_seen_hp)

            if target_confirmed_dead or no_damage_landed or target_healing:
                connections = (view.get("currentRegion") or {}).get("connections") or []
                if connections:
                    decision = Decision(kind="move", target_region_id=random.choice(connections), reason="Redirect target mati/stuck.")
                else:
                    decision = Decision(kind="wait", reason="Target mati/stuck tapi tidak ada jalan.")
                current_target = None
            else:
                if current_target_hp is not None:
                    session.last_seen_target_hp[current_target] = current_target_hp
                session.last_action_target = current_target

        elif decision.kind == "explore" and current_target:
            if current_target == session.last_explored_ruin_id:
                session.consecutive_same_target += 1
            else:
                session.consecutive_same_target = 0
                session.last_explored_ruin_id = current_target

            if session.consecutive_same_target >= 1:
                connections = (view.get("currentRegion") or {}).get("connections") or []
                if connections:
                    decision = Decision(kind="move", target_region_id=random.choice(connections), reason="Mencegah loop eksplorasi.")
                else:
                    decision = Decision(kind="wait", reason="Terjebak loop eksplorasi.")
                session.consecutive_same_target = 0
                session.last_explored_ruin_id = None
                session.last_action_target = None
            else:
                session.last_action_target = current_target
        else:
            session.consecutive_same_target = 0

        session.last_decision_kind = decision.kind

        if decision.kind == "move" and decision.target_region_id:
            session.region_before_last_move = (view.get("currentRegion") or {}).get("id")
            session.last_move_target_region = decision.target_region_id

        payload = build_action_payload(decision)

        slot.last_action = decision.kind.upper()
        slot.reason = decision.reason
        slot.action_status = "Terkirim => Cooldown"
        dash.render()

        await ws.send(json.dumps(payload))

        if is_cooldown_action(decision.kind):
            session.can_act = False


async def play_session(ws, session: GameSession, slot: SlotState) -> str:
    async for raw in ws:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Frame bukan JSON valid, diabaikan: %r", raw[:200])
            continue

        ftype = frame.get("type")

        if ftype == "welcome":
            if frame.get("decision") == "BLOCKED":
                return "closed"

        elif ftype == "assigned":
            session.game_id = frame.get("gameId")
            slot.game_id = session.game_id
            dash.render(force=True)

        elif ftype in ("agent_view", "turn_advanced", "handover_sync", "action_rejected"):
            # action_rejected (baru di 1.15.0) punya bentuk frame IDENTIK dengan
            # agent_view/turn_advanced, cuma reason-nya beda -> satu jalur saja.
            view = frame.get("view", {})
            if ftype != "action_rejected":
                session.recently_attempted_free_actions.clear()

            current_region_id = (view.get("currentRegion") or {}).get("id")
            if session.last_move_target_region is not None:
                if current_region_id == session.region_before_last_move:
                    session.consecutive_failed_moves += 1
                    session.dangerous_regions.add(session.last_move_target_region)
                else:
                    session.consecutive_failed_moves = 0
                session.last_move_target_region = None
                session.region_before_last_move = None

            session.last_view = view
            session.last_view_turn = frame.get("turn")
            await update_dashboard_state(view, slot)
            dash.render()
            await maybe_act(ws, session, view, slot)

        elif ftype == "action_result":
            success = frame.get("success", False)
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act

            error = frame.get("error") or {}
            if not success:
                code = error.get("code", "Unknown Error")
                if code == "TARGET_DEAD" and session.last_action_target:
                    # Target sudah mati, BUKAN kita — turn tidak habis (canAct
                    # sudah true), retry ke target lain di frame berikutnya.
                    session.confirmed_dead_targets.add(session.last_action_target)
                elif code == "AGENT_DEAD":
                    # Sinyal terminal punya sendiri — jangan kirim action lagi,
                    # tunggu frame agent_died (meta.youDied) yang otoritatif.
                    session.alive = False
                if session.last_decision_kind == "move" and session.last_move_target_region is not None:
                    session.consecutive_failed_moves = 0
                    session.last_move_target_region = None
                    session.region_before_last_move = None
                slot.action_status = f"GAGAL ({code})"
                log.warning("Action gagal: %s", error)
            else:
                slot.action_status = "SUKSES"
            dash.render()

            inline_view = frame.get("view")
            if inline_view:
                session.last_view = inline_view
                session.last_view_turn = frame.get("turn", session.last_view_turn)
                await update_dashboard_state(inline_view, slot)
                dash.render()

        elif ftype == "can_act_changed":
            session.can_act = frame.get("canAct", True)
            if session.can_act and session.last_view:
                await maybe_act(ws, session, session.last_view, slot)

        elif ftype == "agent_died":
            # SUMBER KEBENARAN "aku mati": meta.youDied dihitung per-viewer dan
            # cuma ditempel ke salinan milik kita sendiri. JANGAN bandingkan
            # agent_died.agentId dengan uuid asli — itu self-token per-game
            # ("st_..."), tidak akan pernah cocok (Core Rule 18).
            meta = frame.get("meta", {}) or {}
            if meta.get("youDied"):
                session.alive = False
                slot.action_status = "MATI"
                dash.render(force=True)
                return "died"

        elif ftype == "game_ended":
            slot.action_status = "GAME SELESAI"
            dash.render(force=True)
            return "ended"

    return "closed"


async def run_one_game(rest: RestClient, entry_type: str, slot: SlotState) -> str:
    headers = {"X-API-Key": rest.api_key, "X-Version": rest.version}
    try:
        async with websockets.connect(WS_JOIN_URL, additional_headers=headers, ping_interval=20, ping_timeout=20) as ws:
            welcome = json.loads(await ws.recv())
            if welcome.get("type") == "welcome" and welcome.get("decision") == "BLOCKED":
                return "blocked"

            # Selalu kirim hello walau decision == ALREADY_IN_GAME — kalau dua
            # channel (free+paid) sekaligus live, server butuh tahu entryType
            # mana yang mau di-resume, kalau tidak nanti 4003 HELLO_TIMEOUT.
            await send_hello(ws, entry_type)
            session = GameSession(entry_type=entry_type)
            return await play_session(ws, session, slot)
    except ConnectionClosed as e:
        if e.code == 1013:
            # RESUME_TARGET_DEAD: server TIDAK auto-fallback untuk paid (supaya
            # tidak kena entry fee dobel). Re-dial SEKALI di iterasi berikutnya,
            # jangan nunggu delay biasa.
            return "resume_dead"
        if e.code == 4032:
            # Agent sudah mati di game itu — module menolak re-entry.
            return "died"
        log.warning("WebSocket ditutup (code=%s reason=%s)", e.code, e.reason)
        return "closed"


def _slot_is_live(me: dict, entry_type: str) -> bool:
    games = me.get("currentGames") or []
    return any(
        g.get("entryType") == entry_type and g.get("isAlive") and g.get("gameStatus") != "finished"
        for g in games
    )


def _slot_is_startable(me: dict, entry_type: str) -> bool:
    if entry_type == "paid":
        readiness = me.get("readiness", {}) or {}
        return bool(readiness.get("paidReady"))
    # Free readiness pretty much always passes barring SC-wallet-policy
    # blockers — welcome frame's `decision` tetap jadi gerbang otoritatifnya.
    return True


async def game_slot_loop(rest: RestClient, entry_type: str, slot: SlotState, stop: asyncio.Event) -> None:
    """Loop mandiri untuk SATU slot (free ATAU paid). Slot free & paid itu
    independen (bisa hidup bersamaan) — lihat State Router di skill.md."""
    reconnect_delay = RECONNECT_MIN_DELAY

    while not stop.is_set():
        try:
            me = await rest.get_me()
            live = _slot_is_live(me, entry_type)
            startable = _slot_is_startable(me, entry_type)

            if not live and not startable:
                await asyncio.sleep(STATE_POLL_INTERVAL)
                continue

            if not live:
                # Cuma perlu setup loadout sebelum game BARU, bukan saat resume.
                await ensure_loadout(rest)

            slot.game_id = "Resuming..." if live else "Mencari Matchmaking..."
            dash.render(force=True)

            outcome = await run_one_game(rest, entry_type, slot)

            if outcome == "resume_dead":
                # Bukan game sungguhan — cuma butuh satu ronde re-dial ekstra,
                # jangan tunggu delay biasa (bukan retry-storm, cuma konvergen).
                continue
            elif outcome in ("died", "ended"):
                reconnect_delay = RECONNECT_MIN_DELAY
                await asyncio.sleep(INTER_GAME_DELAY)
            elif outcome == "blocked":
                await asyncio.sleep(STATE_POLL_INTERVAL)
            else:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)

        except Exception:
            log.exception("Unhandled exception di game_slot_loop(%s)", entry_type)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


async def main_loop() -> None:
    if not API_KEY:
        print("CLAW_API_KEY is not set — see .env.example")
        sys.exit(1)

    if not RUN_FREE and not RUN_PAID:
        print("CLAW_ENTRY_TYPE tidak mengaktifkan slot manapun. Set ke 'auto', 'free', atau 'paid'.")
        sys.exit(1)

    stop = asyncio.Event()

    def _handle_signal(*_args):
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    async with RestClient(API_KEY) as rest:
        await rest.fetch_version()

        try:
            me = await rest.get_me()
            readiness = me.get("readiness", {}) or {}
            dash.acc_name = me.get("name", "Unknown")
            dash.acc_balance = f"{me.get('balance', 0)} sMoltz"
            dash.acc_wallet = "Siap" if readiness.get("walletAddress") else "Belum Set"
        except ApiError as e:
            print(f"API Error saat mengambil info akun: {e}")
            sys.exit(1)

        dash.slots["free"].enabled = RUN_FREE
        dash.slots["paid"].enabled = RUN_PAID
        dash.render(force=True)

        tasks = [asyncio.create_task(economy_loop(rest, stop))]
        if RUN_FREE:
            tasks.append(asyncio.create_task(game_slot_loop(rest, "free", dash.slots["free"], stop)))
        if RUN_PAID:
            tasks.append(asyncio.create_task(game_slot_loop(rest, "paid", dash.slots["paid"], stop)))

        await stop.wait()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    # Paksa clear screen awal sebelum jalan
    sys.stdout.write("\033[2J\033[H")
    asyncio.run(main_loop())