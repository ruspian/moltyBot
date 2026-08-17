#!/usr/bin/env python3
"""
Claw Royale agent bot.
(Mode Barbar + Dashboard UI + Kill Counter + Smart Weapon Range + Fast DZ Escape + Inventory Tracker)
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

ENTRY_TYPE_PREFERENCE = os.environ.get("CLAW_ENTRY_TYPE", "auto").strip().lower()

# Matikan log default INFO agar tidak merusak tampilan Dashboard
LOG_LEVEL = os.environ.get("CLAW_LOG_LEVEL", "WARNING").upper()
STATE_POLL_INTERVAL = float(os.environ.get("CLAW_STATE_POLL_INTERVAL", "5"))
RECONNECT_MIN_DELAY = float(os.environ.get("CLAW_RECONNECT_MIN_DELAY", "1"))
RECONNECT_MAX_DELAY = float(os.environ.get("CLAW_RECONNECT_MAX_DELAY", "30"))
INTER_GAME_DELAY = float(os.environ.get("CLAW_INTER_GAME_DELAY", "3"))

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
# DASHBOARD UI SYSTEM
# --------------------------------------------------------------------------

class Dashboard:
    def __init__(self):
        # Akun Info
        self.acc_name = "Loading..."
        self.acc_balance = "Loading..."
        self.acc_wallet = "Loading..."

        # Game Info
        self.game_id = "Menunggu Match..."
        self.kills = 0

        # Status
        self.hp = "N/A"
        self.ep = "N/A"
        self.region = "N/A"
        self.is_dz = False
        self.weapon = "(Kosong)"
        self.armor = "(Kosong)"

        # Radar & Tas
        self.inventory: list[str] = []
        self.enemies = 0
        self.monsters = 0
        self.loot = 0

        # Action Log
        self.last_action = "-"
        self.action_status = "-"
        self.reason = "Standby"

        # Throttle biar tidak flicker/boros CPU saat banyak free actions beruntun
        self._min_render_interval = 0.08
        self._last_render_ts = 0.0

    def render(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_render_ts) < self._min_render_interval:
            return
        self._last_render_ts = now

        # ANSI Escape Code: Clear screen (\033[2J) & Move cursor to top-left (\033[H)
        dz_warn = "⚠️ (DEATH ZONE/BAHAYA!)" if self.is_dz else "✅ (Aman)"
        inv_text = "\n  ".join(self.inventory) if self.inventory else "  (Tas Kosong)"

        ui = f"""\033[2J\033[H
=========================================================
 🤖 CLAW ROYALE BOT - DASHBOARD (MODE BARBAR)
=========================================================
[ 👤 AKUN ]
  Nama      : {self.acc_name}
  Balance   : {self.acc_balance}
  Wallet    : {self.acc_wallet}

[ 🎮 GAME INFO ]
  Room ID   : {self.game_id}
  Kills     : 💀 {self.kills}

[ ❤️ STATUS ]
  HP        : {self.hp}
  EP        : {self.ep}
  Posisi    : {self.region}  {dz_warn}

[ 🛡️ EQUIPMENT ]
  Senjata   : {self.weapon}
  Armor     : {self.armor}

[ 🎒 INVENTORY (TAS) ]
  {inv_text}

[ 👁️ VISION (RADAR) ]
  Musuh     : {self.enemies} Agent(s) Terlihat
  Monster   : {self.monsters} Monster(s) Terlihat
  Loot      : {self.loot} Item(s) di Tanah

[ ⚡ LAST ACTION ]
  Action    : {self.last_action}
  Status    : {self.action_status}
  Alasan    : {self.reason}
=========================================================
"""
        sys.stdout.write(ui)
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

    async def request(self, method: str, path: str, **kwargs) -> dict:
        assert self._session
        url = f"{REST_BASE}{path}"
        for attempt in range(3):
            async with self._session.request(method, url, headers=self._headers(), **kwargs) as resp:
                if resp.status == 426:
                    await self.fetch_version()
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

    async def get_me(self) -> dict:
        data = await self.request("GET", "/accounts/me")
        if "data" in data and isinstance(data.get("data"), dict) and "name" not in data:
            return data["data"]
        return data

    async def get_loadout(self) -> dict:
        return await self.request("GET", "/loadout")

    async def get_inventory_relics(self) -> list:
        return (await self.request("GET", "/inventory/relics")).get("data", [])

    async def get_inventory_packs(self) -> list:
        return (await self.request("GET", "/inventory/packs")).get("data", [])

    async def set_active_pack(self, pack_instance_id: int) -> dict:
        return await self.request("PUT", "/loadout/pack", json={"packInstanceId": pack_instance_id})

    async def set_sub_pack(self, pack_instance_id: int) -> dict:
        return await self.request("PUT", "/loadout/sub-pack", json={"packInstanceId": pack_instance_id})

    async def equip_relic(self, type_index: int, relic_instance_id: int) -> dict:
        return await self.request("PUT", f"/loadout/slot/{type_index}", json={"relicInstanceId": relic_instance_id})


async def ensure_loadout(rest: RestClient) -> None:
    try:
        loadout = (await rest.get_loadout()).get("data", {})
    except ApiError as e:
        log.warning("ensure_loadout: gagal ambil loadout: %s", e)
        return

    if loadout.get("fullSet"):
        return

    try:
        packs = await rest.get_inventory_packs()
        relics = await rest.get_inventory_relics()
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
    return kind in {"move", "attack", "explore", "use_item", "interact", "wait", "rest"}


def _get_weapon_range(w: dict) -> float:
    """Ambil jarak jangkau senjata, coba beberapa kemungkinan nama field
    (beberapa API memakai nama field berbeda-beda), fallback ke 1 (Melee)."""
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
    winnable = [a for a in non_guardian if a.get("hp", 999) <= hp * 1.5]
    pool = winnable or non_guardian

    if pool:
        return min(pool, key=lambda a: a.get("hp", 999))
    if visible_monsters:
        return min(visible_monsters, key=lambda m: m.get("hp", 999))
    # Tidak ada kandidat serang yang jelas -> pakai musuh terdekat sebagai acuan
    return min(all_enemies, key=lambda e: e.get("distance", 0))


def decide_free_actions(view: dict) -> list[Decision]:
    """Free actions (0 cooldown): fast looting + smart equip (Jarak Musuh vs Weapon)."""
    free_decisions = []
    self_state = view.get("self", {}) or {}
    inventory = self_state.get("inventory") or []
    equipped_weapon = self_state.get("equippedWeapon")
    equipped_armor = self_state.get("equippedArmor")

    # Deteksi target yang relevan (bukan cuma musuh terdekat) untuk Smart Weapon Switch
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
                # Musuh Jauh -> Prioritaskan Senjata Ranged (Range > 1)
                if w_range > 1:
                    return atk + 1000
                return atk
            else:
                # Musuh Dekat (atau tidak ada musuh terlihat) -> Prioritaskan Senjata Melee/ATK Terbesar
                if w_range <= 1:
                    return atk + 1000
                return atk

        # Urutkan dulu berdasarkan id supaya tie-break stabil (hindari thrashing
        # equip bolak-balik saat dua senjata skornya sama).
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
                reason=f"Switch Senjata ({w_type}, jarak musuh~{target_distance}): {best_weapon.get('name')}"
            ))
            # Sinkronkan state lokal supaya decide() di frame yang sama (yang
            # dipanggil setelah free actions) sudah "tahu" senjata baru ini,
            # bukan menunggu update dari server di frame berikutnya.
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
                reason=f"⚡ FAST LOOT: Ambil {item.get('name', 'Item')}"
            ))
            inv_count += 1

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
    # 1. SURVIVAL & RECOVERY
    # ---------------------------------------------------------

    # 1.1) FAST DEATH ZONE ESCAPE (Cari Jalur 100% Aman) — PRIORITAS TERTINGGI
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        # Coret jalur yang mau meledak
        safe_targets = [c for c in connections if c not in pending_here_ids]
        if safe_targets:
            # Utamakan jalur yang tidak ada history bahaya
            really_safe = [c for c in safe_targets if c not in session.dangerous_regions]
            chosen = random.choice(really_safe) if really_safe else random.choice(safe_targets)
            return Decision(kind="move", target_region_id=chosen, reason="🚨 KABUR CEPAT DARI DEATH ZONE!")
        elif connections:
            # Darurat: Semua jalur mau meledak, pokoknya pindah dulu!
            return Decision(kind="move", target_region_id=random.choice(connections), reason="🚨 KABUR DARURAT! (Semua zona bahaya)")

    # 1.2) EARLY AUTO-HEAL (Naik ke 85%)
    if hp_ratio < 0.85 and recovery_items:
        best_hp_item = max(recovery_items, key=lambda i: i.get("hpRestore", 0))
        if best_hp_item.get("hpRestore", 0) > 0:
            return Decision(kind="use_item", item_id=best_hp_item.get("id"), reason=f"💊 Auto-Heal Dini: Pakai {best_hp_item.get('name')}")

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
            return Decision(kind="use_item", item_id=best_ep_item.get("id"), reason=f"🔋 Isi Stamina: Pakai {best_ep_item.get('name')}")

    # 1.5) Hindari Kerumunan Massal (>12 orang)
    if len(visible_agents) > 12 and connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="⚠️ Terlalu ramai (>12 agent), Reposisi!")

    # ---------------------------------------------------------
    # 2. FIGHT (AGRESIF)
    # ---------------------------------------------------------
    if visible_agents and ep > 0:
        non_guardian = [a for a in visible_agents if not a.get("isGuardian")]
        winnable = [a for a in non_guardian if a.get("hp", 999) <= hp * 1.5]
        pool = winnable or non_guardian
        if pool and hp_ratio >= 0.45:
            weakest = min(pool, key=lambda a: a.get("hp", 999))
            return Decision(kind="attack", target_agent_id=weakest.get("id"), reason="⚔️ SERANG! Menghajar Agent terlemah.")

    if visible_monsters and ep > 0 and hp_ratio >= 0.35:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        return Decision(kind="attack", target_monster_id=weakest_monster.get("id"), reason="⚔️ SERANG! Menghabisi Monster untuk loot.")

    # ---------------------------------------------------------
    # 3. INTERAKSI & EKSPLORASI
    # ---------------------------------------------------------
    if in_cave:
        facilities = view.get("visibleFacilities") or current_region.get("facilities") or current_region.get("interactables") or []
        if facilities:
            return Decision(kind="interact", interactable_id=facilities[0].get("id"), reason="Keluar dari Gua (Cave).")
        return Decision(kind="interact", reason="Mencoba keluar dari Gua (Cave).")

    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    if visible_ruins and not alert_active and alert_gauge <= 6:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(kind="explore", ruin_id=ruin.get("ruinId"), reason=f"Eksplorasi Ruin (Alert: {alert_gauge})")

    if connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="Hunting: Mencari musuh di region lain.")

    return Decision(kind="wait", reason="Standby, tidak ada aksi tersedia.")


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
    # Belum tahu status server, jadi jangan asumsikan bisa langsung beraksi
    # sebelum ada konfirmasi via agent_view/can_act_changed.
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
    # Mencegah dua maybe_act() jalan bersamaan (mis. dipicu agent_view lalu
    # can_act_changed hampir bersamaan) yang bisa mengirim aksi dobel.
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


async def update_dashboard_state(view: dict, session: GameSession):
    """Memperbarui variabel dashboard agar sinkron dengan data server"""
    self_state = view.get("self", {}) or {}

    dash.hp = f"{self_state.get('hp', 0)}/{self_state.get('maxHp', 0)}"
    dash.ep = str(self_state.get("ep", 0))
    dash.kills = self_state.get("kills", 0)

    current_region = view.get("currentRegion", {}) or {}
    dash.region = current_region.get("id", "N/A")
    dash.is_dz = _is_in_danger(view)

    dash.enemies = len(view.get("visibleAgents") or [])
    dash.monsters = len(view.get("visibleMonsters") or [])

    raw_items = (view.get("visibleItems") or []) + (current_region.get("items") or []) + (current_region.get("groundItems") or [])
    dash.loot = len(raw_items)

    equipped_w = self_state.get("equippedWeapon")
    dash.weapon = equipped_w.get("name") if isinstance(equipped_w, dict) else (str(equipped_w) if equipped_w else "(Kosong)")

    equipped_a = self_state.get("equippedArmor")
    dash.armor = equipped_a.get("name") if isinstance(equipped_a, dict) else (str(equipped_a) if equipped_a else "(Kosong)")

    # Hitung Rekap Tas (Inventory)
    inventory_items = self_state.get("inventory") or []
    item_counts: dict[str, int] = {}
    for item in inventory_items:
        name = item.get("name", "Unknown Item")
        qty = item.get("quantity", 1)
        item_counts[name] = item_counts.get(name, 0) + qty
    dash.inventory = [f"- {name} (x{qty})" for name, qty in item_counts.items()] if item_counts else []


async def maybe_act(ws, session: GameSession, view: dict) -> None:
    if not view:
        return

    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False:
        return

    # Cegah re-entrancy: kalau maybe_act masih berjalan (menunggu I/O), panggilan
    # kedua langsung dilewati alih-alih ikut antre dan mengirim aksi dobel.
    if session.acting_lock.locked():
        return

    async with session.acting_lock:
        in_danger = _is_in_danger(view)

        # ----- PROSES FREE ACTIONS -----
        # Kalau lagi dalam bahaya (Death Zone / region akan meledak), lewati
        # looting & auto-equip dulu (armor) supaya perintah KABUR (move) bisa
        # langsung dikirim tanpa delay antar-aksi. Weapon-equip tetap boleh
        # kalau butuh switch cepat, tapi di sini kita prioritaskan kecepatan
        # kabur di atas segalanya.
        if not in_danger:
            free_actions = decide_free_actions(view)
            for fd in free_actions:
                dedup_key = f"{fd.kind}:{fd.item_id or fd.interactable_id or fd.message or ''}"
                if dedup_key in session.recently_attempted_free_actions:
                    continue
                session.recently_attempted_free_actions.add(dedup_key)

                payload = build_action_payload(fd)
                dash.last_action = fd.kind.upper()
                dash.reason = fd.reason
                dash.action_status = "Terkirim (No Cooldown)"
                dash.render()
                await ws.send(json.dumps(payload))
                await asyncio.sleep(0.01)
        else:
            dash.reason = "🚨 BAHAYA! Skip looting/equip, prioritas KABUR..."
            dash.render()

        # ----- PROSES MAIN ACTION -----
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
                    decision = Decision(kind="move", target_region_id=random.choice(connections), reason="Redirect (Target Stuck/Mati)")
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

        # Update Dashboard
        dash.last_action = decision.kind.upper()
        dash.reason = decision.reason
        dash.action_status = "Terkirim (Cooldown)"
        dash.render()

        await ws.send(json.dumps(payload))

        if is_cooldown_action(decision.kind):
            session.can_act = False


async def play_session(ws, session: GameSession) -> str:
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
            dash.game_id = session.game_id
            dash.render(force=True)

        elif ftype in ("agent_view", "turn_advanced", "handover_sync", "action_rejected"):
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
            await update_dashboard_state(view, session)
            dash.render()
            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            success = frame.get("success", False)
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act

            error = frame.get("error") or {}
            if not success:
                if error.get("code") == "TARGET_DEAD" and session.last_action_target:
                    session.confirmed_dead_targets.add(session.last_action_target)
                if session.last_decision_kind == "move" and session.last_move_target_region is not None:
                    session.consecutive_failed_moves = 0
                    session.last_move_target_region = None
                    session.region_before_last_move = None
                dash.action_status = f"GAGAL ({error.get('code', 'Unknown Error')})"
                log.warning("Action gagal: %s", error)
            else:
                dash.action_status = "SUKSES"
            dash.render()

            inline_view = frame.get("view")
            if inline_view:
                session.last_view = inline_view
                session.last_view_turn = frame.get("turn", session.last_view_turn)
                await update_dashboard_state(inline_view, session)
                dash.render()

        elif ftype == "can_act_changed":
            session.can_act = frame.get("canAct", True)
            if session.can_act and session.last_view:
                await maybe_act(ws, session, session.last_view)

        elif ftype == "agent_died":
            meta = frame.get("meta", {}) or {}
            if meta.get("youDied"):
                session.alive = False
                dash.action_status = "MATI"
                dash.render(force=True)
                return "died"

        elif ftype == "game_ended":
            dash.action_status = "GAME SELESAI"
            dash.render(force=True)
            return "ended"

    return "closed"


async def run_one_game(rest: RestClient, entry_type: str) -> str:
    headers = {"X-API-Key": rest.api_key, "X-Version": rest.version}
    try:
        async with websockets.connect(WS_JOIN_URL, additional_headers=headers, ping_interval=20, ping_timeout=20) as ws:
            welcome = json.loads(await ws.recv())
            if welcome.get("type") == "welcome" and welcome.get("decision") == "BLOCKED":
                return "blocked"

            await send_hello(ws, entry_type)
            session = GameSession(entry_type=entry_type)
            return await play_session(ws, session)
    except ConnectionClosed as e:
        if e.code == 1013:
            return "resume_dead"
        if e.code == 4032:
            return "died"
        log.warning("WebSocket ditutup (code=%s reason=%s)", e.code, e.reason)
        return "closed"


async def choose_entry_type(rest: RestClient) -> Optional[str]:
    me = await rest.get_me()
    readiness = me.get("readiness", {}) or {}
    current_games = me.get("currentGames", []) or []

    def live(entry: str) -> bool:
        return any(g.get("entryType") == entry and g.get("isAlive") and g.get("gameStatus") != "finished" for g in current_games)

    free_live = live("free")
    paid_live = live("paid")

    if ENTRY_TYPE_PREFERENCE == "paid":
        return "paid" if paid_live or readiness.get("paidReady") else "free"
    if ENTRY_TYPE_PREFERENCE == "free":
        return "free"

    if paid_live:
        return "paid"
    if free_live:
        return "free"
    if readiness.get("paidReady"):
        return "paid"
    return "free"


async def main_loop() -> None:
    if not API_KEY:
        print("CLAW_API_KEY is not set — see .env.example")
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

        # Coba tarik data akun
        try:
            me = await rest.get_me()
            readiness = me.get("readiness", {}) or {}
            # Set Info ke Dashboard
            dash.acc_name = me.get("name", "Unknown")
            dash.acc_balance = f"{me.get('balance', 0)} sMoltz"
            dash.acc_wallet = "Siap" if readiness.get("walletAddress") else "Belum Set"
            dash.render(force=True)
        except ApiError as e:
            print(f"API Error saat mengambil info akun: {e}")
            sys.exit(1)

        reconnect_delay = RECONNECT_MIN_DELAY

        while not stop.is_set():
            try:
                entry_type = await choose_entry_type(rest)
                if entry_type is None:
                    await asyncio.sleep(STATE_POLL_INTERVAL)
                    continue

                await ensure_loadout(rest)

                dash.game_id = "Mencari Matchmaking..."
                dash.render(force=True)

                outcome = await run_one_game(rest, entry_type)

                if outcome in ("died", "ended", "resume_dead"):
                    reconnect_delay = RECONNECT_MIN_DELAY
                    await asyncio.sleep(INTER_GAME_DELAY)
                elif outcome == "blocked":
                    await asyncio.sleep(STATE_POLL_INTERVAL)
                else:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)

            except Exception:
                log.exception("Unhandled exception di main_loop")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


if __name__ == "__main__":
    # Paksa clear screen awal sebelum jalan
    sys.stdout.write("\033[2J\033[H")
    asyncio.run(main_loop())