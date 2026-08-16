#!/usr/bin/env python3
"""
Claw Royale agent bot. (Ultimate Edition - Hybrid & Smart Memory)
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
# Config
# --------------------------------------------------------------------------

API_KEY = os.environ.get("CLAW_API_KEY", "").strip()
BASE_HOST = os.environ.get("CLAW_HOST", "cdn.clawroyale.ai").strip()
REST_BASE = f"https://{BASE_HOST}/api"
WS_JOIN_URL = f"wss://{BASE_HOST}/ws/join"
WS_AGENT_URL = f"wss://{BASE_HOST}/ws/agent"

ENTRY_TYPE_PREFERENCE = os.environ.get("CLAW_ENTRY_TYPE", "auto").strip().lower()

LOG_LEVEL = os.environ.get("CLAW_LOG_LEVEL", "INFO").upper()
STATE_POLL_INTERVAL = float(os.environ.get("CLAW_STATE_POLL_INTERVAL", "5"))
RECONNECT_MIN_DELAY = float(os.environ.get("CLAW_RECONNECT_MIN_DELAY", "1"))
RECONNECT_MAX_DELAY = float(os.environ.get("CLAW_RECONNECT_MAX_DELAY", "30"))
INTER_GAME_DELAY = float(os.environ.get("CLAW_INTER_GAME_DELAY", "3"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("clawroyale")


def log_info_block(title: str, fields: dict) -> None:
    lines = [f"=== {title} ==="]
    label_width = max((len(k) for k in fields), default=0)
    for key, value in fields.items():
        if value is None:
            continue
        lines.append(f"  {key.ljust(label_width)} : {value}")
    log.info("\n" + "\n".join(lines))


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
        return {
            "X-API-Key": self.api_key,
            "X-Version": self.version,
            "Content-Type": "application/json",
        }

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
            async with self._session.request(
                method, url, headers=self._headers(), **kwargs
            ) as resp:
                if resp.status == 426:
                    log.warning("426 VERSION_MISMATCH on %s — refreshing X-Version", path)
                    await self.fetch_version()
                    continue
                text = await resp.text()
                try:
                    data = json.loads(text) if text else {}
                except json.JSONDecodeError:
                    data = {"raw": text}
                if resp.status >= 400:
                    err = data.get("error", {}) if isinstance(data, dict) else {}
                    raise ApiError(
                        resp.status,
                        err.get("code", "UNKNOWN"),
                        err.get("message", text[:200]),
                    )
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
        data = await self.request("GET", "/inventory/relics")
        return data.get("data", [])

    async def get_inventory_packs(self) -> list:
        data = await self.request("GET", "/inventory/packs")
        return data.get("data", [])

    async def set_active_pack(self, pack_instance_id: int) -> dict:
        return await self.request(
            "PUT", "/loadout/pack", json={"packInstanceId": pack_instance_id}
        )

    async def set_sub_pack(self, pack_instance_id: int) -> dict:
        return await self.request(
            "PUT", "/loadout/sub-pack", json={"packInstanceId": pack_instance_id}
        )

    async def equip_relic(self, type_index: int, relic_instance_id: int) -> dict:
        return await self.request(
            "PUT",
            f"/loadout/slot/{type_index}",
            json={"relicInstanceId": relic_instance_id},
        )


# --------------------------------------------------------------------------
# Loadout setup
# --------------------------------------------------------------------------

async def ensure_loadout(rest: RestClient) -> None:
    try:
        loadout = (await rest.get_loadout()).get("data", {})
    except ApiError as e:
        return

    if loadout.get("fullSet"):
        return

    try:
        packs = await rest.get_inventory_packs()
        relics = await rest.get_inventory_relics()
    except ApiError as e:
        return

    active_pack = loadout.get("activePack")
    slots = loadout.get("slots") or [None, None, None]

    if not active_pack and packs:
        main_candidate = packs[0]
        try:
            await rest.set_active_pack(main_candidate["instanceId"])
        except ApiError:
            pass

    if len(packs) > 1:
        try:
            await rest.set_sub_pack(packs[1]["instanceId"])
        except ApiError:
            pass

    for type_index in range(3):
        if slots[type_index]:
            continue
        candidate = next((r for r in relics if r.get("typeIndex") == type_index), None)
        if candidate:
            try:
                await rest.equip_relic(type_index, candidate["instanceId"])
            except ApiError:
                pass


# --------------------------------------------------------------------------
# Data Structures
# --------------------------------------------------------------------------

@dataclass
class Decision:
    kind: str  # "move" | "attack" | "explore" | "equip" | "wait"
    target_region_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_monster_id: Optional[str] = None
    ruin_id: Optional[str] = None
    item_id: Optional[str] = None
    reason: str = ""

@dataclass
class GameSession:
    entry_type: str
    can_act: bool = True
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
    visited_regions: list = field(default_factory=list) # Memory system


def is_cooldown_action(kind: str) -> bool:
    return kind in {"move", "attack", "explore"}


# --------------------------------------------------------------------------
# Decision logic (ULTIMATE: HYBRID + MEMORY + HEAL)
# --------------------------------------------------------------------------

def pick_smart_move(connections: list, visited: list) -> str:
    """Memory Tweak: Prioritaskan pindah ke region yang belum dikunjungi."""
    unvisited = [c for c in connections if c not in visited]
    if unvisited:
        return random.choice(unvisited)
    return random.choice(connections)


def decide(view: dict, session: GameSession) -> Decision:
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
    inventory = self_state.get("inventory") or []

    hp_ratio = hp / max_hp_guess if max_hp_guess else 1.0
    has_weapon = self_state.get("equippedWeapon") is not None

    # 1) PRIORITAS MUTLAK: KABUR DARI DEATH ZONE
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        safe_targets = [c for c in connections]
        if safe_targets:
            return Decision(
                kind="move",
                target_region_id=pick_smart_move(safe_targets, session.visited_regions),
                reason="[URGENT] Evakuasi dari Death Zone mutlak!",
            )

    # 2) AUTO-HEAL TWEAK (Gunakan item pemulih jika HP bocor)
    recovery_items = [i for i in inventory if i.get("category") == "recovery"]
    if hp_ratio <= 0.60 and recovery_items:
        item_to_use = recovery_items[0]
        # Pastikan tidak looping (nyangkut) di item yang sama jika server nge-bug
        if item_to_use.get("id") != session.last_action_target or session.consecutive_same_target < 2:
            return Decision(
                kind="equip", 
                item_id=item_to_use.get("id"),
                reason=f"[AUTO-HEAL] Mengkonsumsi {item_to_use.get('name')} (HP sisa: {hp_ratio:.0%})"
            )

    # ==========================================
    # LOGIKA JIKA BAWA SENJATA (MODE BARBAR)
    # ==========================================
    if has_weapon:
        # Hajar Musuh Paling Lemah (Prioritas Cari Kill)
        if visible_agents and ep > 0:
            non_guardian_targets = [a for a in visible_agents if not a.get("isGuardian")]
            if non_guardian_targets:
                weakest = min(non_guardian_targets, key=lambda a: a.get("hp", 999))
                # Sikat selama HP bot tidak sekarat (>30%)
                if hp_ratio >= 0.30:
                    return Decision(
                        kind="attack",
                        target_agent_id=weakest.get("id"),
                        reason=f"[MODE BARBAR] Eksekusi agen! (Target HP: {weakest.get('hp')})",
                    )
        
        # Kabur kalau sekarat mutlak (< 30%)
        if hp_ratio < 0.30:
            if connections:
                return Decision(
                    kind="move",
                    target_region_id=pick_smart_move(connections, session.visited_regions),
                    reason=f"[MODE BARBAR] HP Kritis ({hp_ratio:.0%}) - Mundur taktis!",
                )
            else:
                return Decision(kind="wait", reason="HP Kritis tapi buntu")
        
        # Farming Monster Santai
        if hp_ratio >= 0.5 and visible_monsters and ep > 0:
            weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
            return Decision(
                kind="attack",
                target_monster_id=weakest_monster.get("id"),
                reason=f"[MODE BARBAR] Farming monster (HP: {weakest_monster.get('hp')})",
            )

        # Hindari Kerumunan Terlalu Ekstrem (> 8 orang)
        CROWDED_THRESHOLD = 8
        if len(visible_agents) > CROWDED_THRESHOLD and connections:
            return Decision(
                kind="move",
                target_region_id=pick_smart_move(connections, session.visited_regions),
                reason=f"[MODE BARBAR] Terlalu ramai ({len(visible_agents)} orang), repo posisi",
            )

    # ==========================================
    # LOGIKA JIKA TANGAN KOSONG (MODE PEMULUNG)
    # ==========================================
    else:
        # Sangat Paranoid (Toleransi Kerumunan: 2 orang)
        if visible_agents:
            if hp_ratio < 0.90 or len(visible_agents) >= 2:
                if connections:
                    return Decision(
                        kind="move",
                        target_region_id=pick_smart_move(connections, session.visited_regions),
                        reason=f"[MODE PEMULUNG] Menghindar! (HP: {hp_ratio:.0%} / Musuh: {len(visible_agents)})",
                    )
                else:
                    return Decision(kind="wait", reason="Terkepung tanpa senjata!")

        # Kabur mutlak jika HP bocor (<30%)
        if hp_ratio < 0.30:
            if connections:
                return Decision(
                    kind="move",
                    target_region_id=pick_smart_move(connections, session.visited_regions),
                    reason=f"[MODE PEMULUNG] HP Kritis ({hp_ratio:.0%}) - Lari cari aman",
                )
            else:
                return Decision(kind="wait", reason="Sekarat dan buntu")

        # Numpang Nyampah Kill Agen
        if visible_agents:
            non_guardian_targets = [a for a in visible_agents if not a.get("isGuardian")]
            if non_guardian_targets:
                weakest = min(non_guardian_targets, key=lambda a: a.get("hp", 999))
                if weakest.get("hp", 999) <= hp * 0.3 and ep > 0:
                    return Decision(
                        kind="attack",
                        target_agent_id=weakest.get("id"),
                        reason=f"[MODE PEMULUNG] Nyampah target sekarat (HP musuh: {weakest.get('hp')})",
                    )
        
        # Numpang Nyampah Monster Sangat Lemah
        if visible_monsters and ep > 0:
            weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
            if weakest_monster.get("hp", 999) < 25:
                return Decision(
                    kind="attack",
                    target_monster_id=weakest_monster.get("id"),
                    reason=f"[MODE PEMULUNG] Gebuk monster lemah (HP: {weakest_monster.get('hp')})",
                )

    # ==========================================
    # LOGIKA UMUM (Nge-loot, Reposisi, dsb)
    # ==========================================
    
    # Eksplorasi Ruin (Hanya Jika Aman)
    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    if visible_ruins and not alert_active and alert_gauge <= 4:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(
                kind="explore",
                ruin_id=ruin.get("ruinId"),
                reason=f"Membongkar ruin (alert={alert_gauge}, hp={hp_ratio:.0%})",
            )

    # Terjebak di Gua
    if in_cave:
        return Decision(kind="wait", reason="Di dalam gua — menunggu interaksi")

    # Jalan-jalan Cari Ruin / Musuh menggunakan Smart Memory
    if connections:
        return Decision(
            kind="move",
            target_region_id=pick_smart_move(connections, session.visited_regions),
            reason="Repositioning — Menyisir area baru",
        )

    return Decision(kind="wait", reason="Tidak ada jalan keluar")


def build_action_payload(decision: Decision) -> dict:
    payload: dict[str, Any] = {"type": "action", "action": decision.kind}

    if decision.kind == "move" and decision.target_region_id:
        payload["targetRegionId"] = decision.target_region_id
    elif decision.kind == "attack":
        if decision.target_agent_id:
            payload["targetAgentId"] = decision.target_agent_id
        elif decision.target_monster_id:
            payload["targetMonsterId"] = decision.target_monster_id
    elif decision.kind == "explore" and decision.ruin_id:
        payload["ruinId"] = decision.ruin_id
    elif decision.kind == "equip" and decision.item_id:
        payload["itemId"] = decision.item_id

    return payload


# --------------------------------------------------------------------------
# Gameplay WebSocket loop
# --------------------------------------------------------------------------

async def send_hello(ws, entry_type: str) -> None:
    hello = {"type": "hello", "entryType": entry_type}
    await ws.send(json.dumps(hello))
    log.info("sent hello entryType=%s", entry_type)


async def play_session(ws, session: GameSession) -> str:
    async for raw in ws:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            continue

        ftype = frame.get("type")

        if ftype == "welcome":
            decision = frame.get("decision")
            if decision == "BLOCKED":
                log.error("join blocked by server: %s", frame)
                return "closed"

        elif ftype in ("waiting", "queued"):
            log.info("%s: %s", ftype, json.dumps(frame)[:300])

        elif ftype == "assigned":
            session.game_id = frame.get("gameId")
            log_info_block("Masuk Room", {
                "room/game id": session.game_id,
                "entry type": session.entry_type,
            })

        elif ftype in ("agent_view", "turn_advanced", "handover_sync"):
            view = frame.get("view", {})
            prev_hp = (session.last_view.get("self") or {}).get("hp")
            new_self = view.get("self") or {}
            new_hp = new_self.get("hp")
            new_max_hp = new_self.get("maxHp")
            session.last_view = view
            session.last_view_turn = frame.get("turn")
            
            # --- UPDATE SMART MEMORY ---
            current_region = view.get("currentRegion", {}) or {}
            curr_reg_id = current_region.get("id")
            if curr_reg_id:
                if not session.visited_regions or session.visited_regions[-1] != curr_reg_id:
                    session.visited_regions.append(curr_reg_id)
                    if len(session.visited_regions) > 5:
                        session.visited_regions.pop(0) # Simpan max 5 history

            visible_agents = view.get("visibleAgents") or []
            visible_monsters = view.get("visibleMonsters") or []
            visible_ruins = view.get("visibleRuins") or []

            hp_display = f"{new_hp}/{new_max_hp}" if new_max_hp else str(new_hp)

            log_info_block("Status", {
                "turn": session.last_view_turn,
                "hp": hp_display,
                "ep": new_self.get("ep"),
                "bisa aksi": session.can_act,
                "posisi (region)": curr_reg_id,
                "death zone": current_region.get("isDeathZone"),
                "musuh terlihat": len(visible_agents) or None,
                "monster terlihat": len(visible_monsters) or None,
                "ruin terlihat": len(visible_ruins) or None,
                "alert gauge": new_self.get("alertGauge"),
            })

            equipped_weapon = new_self.get("equippedWeapon")
            inventory_items = new_self.get("inventory") or []
            equip_signature = json.dumps(
                {"weapon": equipped_weapon, "inventory": inventory_items},
                sort_keys=True,
            )
            if equip_signature != session.last_equipment_signature:
                session.last_equipment_signature = equip_signature
                item_lines = {
                    it.get("name", f"item {i}"): f"x{it.get('quantity', 1)}"
                    for i, it in enumerate(inventory_items)
                }
                weapon_name = (
                    equipped_weapon.get("name") if isinstance(equipped_weapon, dict)
                    else equipped_weapon
                )
                log_info_block("Equipment / Inventory", {
                    "weapon": weapon_name or "(kosong)",
                    **(item_lines or {"item": "(kosong)"}),
                })
            
            # --- WARNING HP DROP ---
            if (
                prev_hp is not None
                and new_hp is not None
                and new_hp < prev_hp
                and session.last_decision_kind not in ["attack", "equip"]
            ):
                log_info_block("⚠️ TERJADI DAMAGE MISTERIUS!", {
                    "hp awal": prev_hp,
                    "hp sisa": new_hp,
                    "darah hilang": prev_hp - new_hp,
                    "aksi terakhir": session.last_decision_kind,
                    "penyebab": "Terkena hit saat cooldown, Cuaca, Guardian, atau Efek Status."
                })
            await maybe_act(ws, session, view)

        elif ftype == "action_rejected":
            view = frame.get("view", {})
            session.last_view = view
            session.last_view_turn = frame.get("turn")
            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            success = frame.get("success", True)
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act
            
            error = frame.get("error") or {}
            
            if session.last_decision_kind == "explore" and success:
                log.info("💎 [SUKSES EKSPLORASI] Berhasil membongkar ruin! Cek log game untuk detail item.")
            elif session.last_decision_kind == "equip" and success:
                log.info("🟢 [SUKSES HEALING] Item berhasil digunakan!")
            elif not success:
                log.warning(f"❌ [AKSI GAGAL] Sistem menolak aksi: {error.get('code')} - {error.get('message')}")
            
            if error.get("code") == "TARGET_DEAD" and session.last_action_target:
                session.confirmed_dead_targets.add(session.last_action_target)
            
            inline_view = frame.get("view")
            if inline_view:
                session.last_view = inline_view
                session.last_view_turn = frame.get("turn", session.last_view_turn)

        elif ftype == "can_act_changed":
            session.can_act = frame.get("canAct", True)
            if session.can_act and session.last_view:
                await maybe_act(ws, session, session.last_view)

        elif ftype == "log":
            msg = frame.get("message")
            if msg:
                log.info(f"📢 LOG GAME: {msg}")

        elif ftype == "agent_died":
            meta = frame.get("meta", {}) or {}
            if meta.get("youDied"):
                log_info_block("💀 AGEN TEWAS", {
                    "waktu bertahan": frame.get("survivalTime"),
                    "total kill": frame.get("kills")
                })
                session.alive = False
                return "died"

        elif ftype == "game_ended":
            log.info("🏁 GAME SELESAI.")
            return "ended"

    return "closed"


def get_target_current_hp(view: dict, target_id: str) -> Optional[int]:
    for a in (view.get("visibleAgents") or []):
        if a.get("id") == target_id:
            return a.get("hp")
    for m in (view.get("visibleMonsters") or []):
        if m.get("id") == target_id:
            return m.get("hp")
    return None


async def maybe_act(ws, session: GameSession, view: dict) -> None:
    if not view:
        return

    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False:
        return

    if not session.can_act:
        return

    decision = decide(view, session)

    current_target = (
        decision.ruin_id or decision.target_monster_id or decision.target_agent_id or decision.item_id
    )

    if decision.kind in ["attack", "equip"] and current_target:
        target_confirmed_dead = current_target in session.confirmed_dead_targets
        current_target_hp = get_target_current_hp(view, current_target)
        previously_seen_hp = session.last_seen_target_hp.get(current_target)
        
        is_same_target_as_last_attack = (
            current_target == session.last_action_target
            and session.last_decision_kind == decision.kind
        )
        no_damage_landed = (
            decision.kind == "attack"
            and is_same_target_as_last_attack
            and previously_seen_hp is not None
            and current_target_hp is not None
            and current_target_hp == previously_seen_hp
        )
        target_healing = (
            decision.kind == "attack"
            and is_same_target_as_last_attack
            and previously_seen_hp is not None
            and current_target_hp is not None
            and current_target_hp > previously_seen_hp
        )
        
        # Tambahan proteksi jika Heal loop/gagal
        is_failed_heal = (decision.kind == "equip" and session.consecutive_same_target >= 2)

        if target_confirmed_dead or no_damage_landed or target_healing or is_failed_heal:
            log_info_block("⚠️ AKSI DIBATALKAN (PROTEKSI STUCK)", {
                "target id": current_target,
                "alasan": "Serangan kebal/heal nyangkut",
                "tindakan": "Batal dan reposisi area"
            })
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(
                    kind="move",
                    target_region_id=pick_smart_move(connections, session.visited_regions),
                    reason="Membatalkan aksi macet, repo posisi",
                )
                current_target = None
            else:
                decision = Decision(kind="wait", reason="Aksi dibatalkan tapi buntu")
                current_target = None
        else:
            if current_target_hp is not None:
                session.last_seen_target_hp[current_target] = current_target_hp
            session.last_action_target = current_target

    elif decision.kind == "explore" and current_target:
        if (
            current_target == session.last_action_target
            and session.last_decision_kind == "explore"
        ):
            session.consecutive_same_target += 1
        else:
            session.consecutive_same_target = 0

        if session.consecutive_same_target >= 1:
            log_info_block("⚠️ RUIN NYANGKUT ATAU HABIS", {
                "tindakan": "Menghindari loop eksploitasi, otomatis pindah area."
            })
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(
                    kind="move",
                    target_region_id=pick_smart_move(connections, session.visited_regions),
                    reason="Meninggalkan ruin rusak, repo posisi",
                )
            else:
                decision = Decision(kind="wait", reason="Ruin buntu")
            session.consecutive_same_target = 0
            session.last_action_target = None
        else:
            session.last_action_target = current_target
    else:
        session.consecutive_same_target = 0

    session.last_decision_kind = decision.kind

    payload = build_action_payload(decision)
    
    # Ambil display untuk di log
    target_display = (
        decision.target_region_id
        or decision.target_agent_id
        or decision.target_monster_id
        or decision.ruin_id
        or decision.item_id
    )
    
    log_info_block("Aksi", {
        "action": decision.kind,
        "target": target_display,
        "alasan": decision.reason,
    })

    await ws.send(json.dumps(payload))

    if is_cooldown_action(decision.kind):
        session.can_act = False


async def run_one_game(rest: RestClient, entry_type: str) -> str:
    headers = {
        "X-API-Key": rest.api_key,
        "X-Version": rest.version,
    }
    join_started_at = time.monotonic()
    try:
        async with websockets.connect(
            WS_JOIN_URL, additional_headers=headers, ping_interval=20, ping_timeout=20
        ) as ws:
            welcome_raw = await ws.recv()
            welcome = json.loads(welcome_raw)
            
            if welcome.get("type") == "welcome":
                decision = welcome.get("decision")
                if decision == "BLOCKED":
                    log.error("account not ready to join (%s)", entry_type)
                    return "blocked"

            await send_hello(ws, entry_type)

            session = GameSession(entry_type=entry_type)
            outcome = await play_session(ws, session)
            return outcome

    except ConnectionClosed as e:
        waited = time.monotonic() - join_started_at
        if e.code == 1006 and waited < 120:
            log.info("WebSocket closed 1006 (matchmaking timeout), retrying...")
        elif e.code == 1013:
            return "resume_dead"
        elif e.code == 4032:
            return "died"
        return "closed"


async def choose_entry_type(rest: RestClient) -> Optional[str]:
    me = await rest.get_me()
    readiness = me.get("readiness", {}) or {}
    current_games = me.get("currentGames", []) or []

    def live(entry: str) -> bool:
        return any(
            g.get("entryType") == entry
            and g.get("isAlive")
            and g.get("gameStatus") != "finished"
            for g in current_games
        )

    free_live = live("free")
    paid_live = live("paid")

    if ENTRY_TYPE_PREFERENCE == "paid":
        if paid_live or readiness.get("paidReady"):
            return "paid"
        return "free"
    if ENTRY_TYPE_PREFERENCE == "free":
        return "free"

    if paid_live: return "paid"
    if free_live: return "free"
    if readiness.get("paidReady"): return "paid"
    return "free"


async def main_loop() -> None:
    if not API_KEY:
        log.error("CLAW_API_KEY is not set — see .env.example")
        sys.exit(1)

    stop = asyncio.Event()

    def _handle_signal(*_args):
        log.info("shutdown signal received")
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
            log_info_block("Akun", {
                "nama": me.get("name"),
                "balance": f"{me.get('balance')} sMoltz",
                "wallet ok": readiness.get("walletAddress"),
                "SC wallet": readiness.get("scWallet"),
                "sMoltz cukup": readiness.get("sMoltzSufficient"),
                "paid ready": readiness.get("paidReady"),
            })
        except ApiError as e:
            log.error("could not fetch account: %s", e)
            sys.exit(1)

        reconnect_delay = RECONNECT_MIN_DELAY

        while not stop.is_set():
            try:
                entry_type = await choose_entry_type(rest)
                if entry_type is None:
                    await asyncio.sleep(STATE_POLL_INTERVAL)
                    continue

                await ensure_loadout(rest)
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
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)

    log.info("bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main_loop())