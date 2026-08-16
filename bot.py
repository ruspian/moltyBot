#!/usr/bin/env python3
"""
Claw Royale agent bot. (Ultimate Skynet Edition - Panic Fixed)
Features: Hybrid Mode, Graph Mapping (BFS), Dynamic Risk, SL Plus Eject, Smart Target, Panic Flee.
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


async def ensure_loadout(rest: RestClient) -> None:
    try:
        loadout = (await rest.get_loadout()).get("data", {})
    except ApiError:
        return

    if loadout.get("fullSet"):
        return

    try:
        packs = await rest.get_inventory_packs()
        relics = await rest.get_inventory_relics()
    except ApiError:
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
# Data Structures & BFS Pathfinding
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
    
    # Advanced Memory (Graph Mapping)
    graph: dict = field(default_factory=dict)
    unexplored_regions: set = field(default_factory=set)
    visited_regions: list = field(default_factory=list)


def bfs_find_path(start: str, targets: set, graph: dict) -> Optional[str]:
    """Mencari rute terdekat menuju area yang belum terjamah."""
    if not targets or start not in graph:
        return None
    
    queue = [(start, [])]
    visited = {start}
    
    while queue:
        curr, path = queue.pop(0)
        if curr in targets and path:
            return path[0]  # Kembalikan langkah pertama
        
        for neighbor in graph.get(curr, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None

def smart_move(connections: list, session: GameSession, current_region: str) -> str:
    """Menggunakan BFS untuk ke ruang baru, atau fallback ke smart-random."""
    best_step = bfs_find_path(current_region, session.unexplored_regions, session.graph)
    if best_step and best_step in connections:
        return best_step
    
    unvisited = [c for c in connections if c not in session.visited_regions]
    if unvisited:
        return random.choice(unvisited)
    return random.choice(connections)

def is_cooldown_action(kind: str) -> bool:
    return kind in {"move", "attack", "explore"}


# --------------------------------------------------------------------------
# Decision logic (ULTIMATE SKYNET EDITION)
# --------------------------------------------------------------------------

def decide(view: dict, session: GameSession) -> Decision:
    self_state = view.get("self", {}) or {}
    hp = self_state.get("hp", 100)
    max_hp_guess = self_state.get("maxHp", 100) or 100
    ep = self_state.get("ep", 0)
    in_cave = self_state.get("inCave", False)
    
    current_region = view.get("currentRegion", {}) or {}
    curr_region_id = current_region.get("id")
    is_death_zone = current_region.get("isDeathZone", False)
    connections = current_region.get("connections") or []
    pending_deathzones = view.get("pendingDeathzones") or []
    
    visible_agents = view.get("visibleAgents") or []
    visible_monsters = view.get("visibleMonsters") or []
    visible_ruins = view.get("visibleRuins") or []
    inventory = self_state.get("inventory") or []

    hp_ratio = hp / max_hp_guess if max_hp_guess else 1.0
    has_weapon = self_state.get("equippedWeapon") is not None

    # Kalkulasi HP Drop (SL Plus Algorithm)
    prev_hp = (session.last_view.get("self") or {}).get("hp") if session.last_view else hp
    hp_drop = prev_hp - hp if prev_hp is not None else 0

    # 1) PRIORITAS MUTLAK 1: KABUR DARI DEATH ZONE (PANIC MODE)
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or curr_region_id in pending_here_ids:
        if connections:
            return Decision(
                kind="move",
                target_region_id=random.choice(connections), # Ganti jadi random.choice biar gak maksain rute nyangkut
                reason="[URGENT] Evakuasi dari Death Zone mutlak! (Pintu acak)",
            )

    # 2) PRIORITAS MUTLAK 2: EMERGENCY EJECT (SL PLUS) (PANIC MODE)
    if hp_drop >= 25:
        if connections:
            return Decision(
                kind="move",
                target_region_id=random.choice(connections), # Ganti jadi random.choice
                reason=f"[SL PLUS] Damage spike terdeteksi! Hilang {hp_drop} HP, lari acak!",
            )

    # 3) DYNAMIC RISK SCORING
    risk_score = 0
    guardian_present = False
    for a in visible_agents:
        if a.get("isGuardian"):
            risk_score += 50
            guardian_present = True
        elif a.get("hp", 100) < 30:
            risk_score -= 10  # Prasmanan kill, turunkan risiko
        else:
            risk_score += 15
            
    alert_gauge = self_state.get("alertGauge", 0) or 0
    risk_score += (alert_gauge * 5)
    
    # 4) AUTO-HEAL
    recovery_items = [i for i in inventory if i.get("category") == "recovery"]
    if hp_ratio <= 0.60 and recovery_items:
        item_to_use = recovery_items[0]
        if item_to_use.get("id") != session.last_action_target or session.consecutive_same_target < 2:
            return Decision(
                kind="equip", 
                item_id=item_to_use.get("id"),
                reason=f"[AUTO-HEAL] Konsumsi {item_to_use.get('name')} (Sisa HP: {hp_ratio:.0%})"
            )

    # ==========================================
    # HYBRID LOGIC & SMART TARGET SELECTION
    # ==========================================
    
    # Evaluasi Resiko Ekstrim
    # Jika tangan kosong, toleransi risk = 15. Jika bawa senjata, toleransi risk = 40.
    max_risk_tolerance = 40 if has_weapon else 15
    if risk_score > max_risk_tolerance and hp_ratio < 0.90:
        if connections:
            return Decision(
                kind="move",
                target_region_id=smart_move(connections, session, curr_region_id),
                reason=f"[RISK SCORING] Bahaya ekstrim (Skor: {risk_score}), mundur cari posisi!"
            )

    # Memilah Target (Smart Target Selection)
    non_guardian_targets = [a for a in visible_agents if not a.get("isGuardian")]
    if non_guardian_targets and ep > 0:
        weakest = min(non_guardian_targets, key=lambda a: a.get("hp", 999))
        weakest_hp = weakest.get("hp", 999)
        
        # SMART WAIT: Biarkan Guardian yang eksekusi
        if guardian_present and weakest_hp < 40:
            return Decision(
                kind="wait",
                reason=f"[SMART TARGET] Menunggu Guardian membunuh target lemah ({weakest_hp} HP)"
            )

        # Serang jika syarat terpenuhi
        if has_weapon and hp_ratio >= 0.30:
            return Decision(
                kind="attack",
                target_agent_id=weakest.get("id"),
                reason=f"[MODE BARBAR] Eksekusi agen! (Target HP: {weakest_hp})",
            )
        elif not has_weapon and weakest_hp <= hp * 0.3:
            return Decision(
                kind="attack",
                target_agent_id=weakest.get("id"),
                reason=f"[MODE PEMULUNG] Nyampah agen sekarat (Target HP: {weakest_hp})",
            )

    # Serang Monster (Priority Low)
    if visible_monsters and ep > 0:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        mon_hp = weakest_monster.get("hp", 999)
        if has_weapon and hp_ratio >= 0.5:
            return Decision(
                kind="attack",
                target_monster_id=weakest_monster.get("id"),
                reason=f"[MODE BARBAR] Farming monster (HP: {mon_hp})",
            )
        elif not has_weapon and mon_hp < 25:
            return Decision(
                kind="attack",
                target_monster_id=weakest_monster.get("id"),
                reason=f"[MODE PEMULUNG] Gebuk monster lemah (HP: {mon_hp})",
            )

    # Eksplorasi Ruin (Nge-loot)
    alert_active = self_state.get("alertActive", False)
    if visible_ruins and not alert_active and alert_gauge <= 4:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(
                kind="explore",
                ruin_id=ruin.get("ruinId"),
                reason=f"Membongkar ruin (alert={alert_gauge}, hp={hp_ratio:.0%})",
            )

    if in_cave:
        return Decision(kind="wait", reason="Di dalam gua — menunggu interaksi")

    if connections:
        return Decision(
            kind="move",
            target_region_id=smart_move(connections, session, curr_region_id),
            reason="Repositioning — BFS Pathfinding ke area baru",
        )

    return Decision(kind="wait", reason="Buntu, tidak ada yang bisa dilakukan")


def build_action_payload(decision: Decision) -> dict:
    payload: dict[str, Any] = {"type": "action", "action": decision.kind}
    if decision.kind == "move" and decision.target_region_id:
        payload["targetRegionId"] = decision.target_region_id
    elif decision.kind == "attack":
        if decision.target_agent_id: payload["targetAgentId"] = decision.target_agent_id
        elif decision.target_monster_id: payload["targetMonsterId"] = decision.target_monster_id
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
                log.error("join blocked by server")
                return "closed"

        elif ftype in ("waiting", "queued"):
            pass # Keep it clean

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
            
            # --- GRAPH UPDATE (MEMORY) ---
            current_region = view.get("currentRegion", {}) or {}
            curr_reg_id = current_region.get("id")
            connections = current_region.get("connections", [])
            
            if curr_reg_id:
                session.graph[curr_reg_id] = connections
                for c in connections:
                    if c not in session.graph:
                        session.unexplored_regions.add(c)
                if curr_reg_id in session.unexplored_regions:
                    session.unexplored_regions.remove(curr_reg_id)

                if not session.visited_regions or session.visited_regions[-1] != curr_reg_id:
                    session.visited_regions.append(curr_reg_id)
                    if len(session.visited_regions) > 5:
                        session.visited_regions.pop(0) 

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
                log.info("💎 [SUKSES EKSPLORASI] Berhasil membongkar ruin! Cek log game untuk item.")
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


async def maybe_act(ws, session: GameSession, view: dict) -> None:
    if not view: return
    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False: return
    if not session.can_act: return

    decision = decide(view, session)
    current_target = (
        decision.ruin_id or decision.target_monster_id or decision.target_agent_id or decision.item_id
    )

    if decision.kind in ["attack", "equip", "explore"] and current_target:
        if current_target == session.last_action_target and session.last_decision_kind == decision.kind:
            session.consecutive_same_target += 1
        else:
            session.consecutive_same_target = 0

        # Proteksi Anti-Stuck (Maks 2x coba)
        if session.consecutive_same_target >= 2:
            log_info_block("⚠️ AKSI DIBATALKAN (PROTEKSI STUCK)", {
                "tindakan": "Mencegah spam ke target/ruin yang sama, nabrak pintu acak."
            })
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(
                    kind="move",
                    target_region_id=random.choice(connections), # Ganti jadi random biar pasti lepas
                    reason="Membatalkan aksi macet, repo posisi acak",
                )
                current_target = None
            else:
                decision = Decision(kind="wait", reason="Aksi macet tapi buntu")
                current_target = None
        else:
            session.last_action_target = current_target
    else:
        session.consecutive_same_target = 0

    session.last_decision_kind = decision.kind
    payload = build_action_payload(decision)
    
    target_display = (
        decision.target_region_id or decision.target_agent_id or 
        decision.target_monster_id or decision.ruin_id or decision.item_id
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
        if paid_live or readiness.get("paidReady"): return "paid"
        return "free"
    if ENTRY_TYPE_PREFERENCE == "free": return "free"
    
    if paid_live: return "paid"
    if free_live: return "free"
    if readiness.get("paidReady"): return "paid"
    return "free"


async def main_loop() -> None:
    if not API_KEY:
        log.error("CLAW_API_KEY is not set")
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