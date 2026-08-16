#!/usr/bin/env python3
"""
Claw Royale agent bot.
(Updated with Free Actions: pickup, equip, talk, whisper, broadcast, interact)
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
# REST client (Tetap sama seperti aslinya)
# --------------------------------------------------------------------------
# [BAGIAN REST CLIENT SAMA PERSIS SEPERTI SEBELUMNYA]

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
            async with self._session.request(method, url, headers=self._headers(), **kwargs) as resp:
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
                    raise ApiError(resp.status, err.get("code", "UNKNOWN"), err.get("message", text[:200]))
                return data
        raise ApiError(426, "VERSION_MISMATCH", "failed after retry")

    async def get_me(self) -> dict:
        data = await self.request("GET", "/accounts/me")
        if "data" in data and isinstance(data.get("data"), dict) and "name" not in data:
            return data["data"]
        return data

    async def get_loadout(self) -> dict: return await self.request("GET", "/loadout")
    async def get_inventory_relics(self) -> list: return (await self.request("GET", "/inventory/relics")).get("data", [])
    async def get_inventory_packs(self) -> list: return (await self.request("GET", "/inventory/packs")).get("data", [])
    async def set_active_pack(self, pack_instance_id: int) -> dict: return await self.request("PUT", "/loadout/pack", json={"packInstanceId": pack_instance_id})
    async def set_sub_pack(self, pack_instance_id: int) -> dict: return await self.request("PUT", "/loadout/sub-pack", json={"packInstanceId": pack_instance_id})
    async def equip_relic(self, type_index: int, relic_instance_id: int) -> dict: return await self.request("PUT", f"/loadout/slot/{type_index}", json={"relicInstanceId": relic_instance_id})


async def ensure_loadout(rest: RestClient) -> None:
    # [LOGIKA LOADOUT SAMA PERSIS SEPERTI SEBELUMNYA]
    try: loadout = (await rest.get_loadout()).get("data", {})
    except ApiError: return
    if loadout.get("fullSet"): return
    try:
        packs = await rest.get_inventory_packs()
        relics = await rest.get_inventory_relics()
    except ApiError: return

    active_pack = loadout.get("activePack")
    slots = loadout.get("slots") or [None, None, None]

    if not active_pack and packs:
        try: await rest.set_active_pack(packs[0]["instanceId"])
        except ApiError: pass

    if len(packs) > 1:
        try: await rest.set_sub_pack(packs[1]["instanceId"])
        except ApiError: pass

    for type_index in range(3):
        if slots[type_index]: continue
        candidate = next((r for r in relics if r.get("typeIndex") == type_index), None)
        if candidate:
            try: await rest.equip_relic(type_index, candidate["instanceId"])
            except ApiError: pass


# --------------------------------------------------------------------------
# Decision logic
# --------------------------------------------------------------------------

@dataclass
class Decision:
    kind: str  # move, attack, explore, use_item, wait, interact, pickup, equip, talk, whisper, broadcast
    target_region_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_monster_id: Optional[str] = None
    ruin_id: Optional[str] = None
    item_id: Optional[str] = None
    interactable_id: Optional[str] = None
    message: Optional[str] = None
    reason: str = ""


def is_cooldown_action(kind: str) -> bool:
    """Action yang memicu cooldown 30s dan mengkonsumsi EP (atau turn)."""
    return kind in {"move", "attack", "explore", "use_item", "interact", "wait", "rest"}


def decide_free_actions(view: dict) -> list[Decision]:
    """Menentukan Free Actions (0 Cooldown) sebelum melakukan action utama.
    Berguna untuk otomatis ambil barang (pickup) atau memakai item (equip)."""
    free_decisions = []
    self_state = view.get("self", {}) or {}
    inventory = self_state.get("inventory") or []
    
    # 1. AUTO-EQUIP: Jika tidak pakai senjata dan ada senjata di inventory, otomatis pakai.
    equipped_weapon = self_state.get("equippedWeapon")
    if not equipped_weapon:
        weapons = [i for i in inventory if i.get("category") == "weapon"]
        if weapons:
            free_decisions.append(Decision(
                kind="equip", 
                item_id=weapons[0].get("id"), 
                reason=f"auto-equip senjata dari inventory: {weapons[0].get('name')}"
            ))

    # 2. AUTO-PICKUP: Jika ada barang nganggur di map, otomatis ambil.
    # Tergantung versi server, item bisa di view["visibleItems"] atau view["currentRegion"]["items"]
    visible_items = view.get("visibleItems") or []
    if not visible_items:
        current_region = view.get("currentRegion") or {}
        visible_items = current_region.get("items") or current_region.get("groundItems") or []
    
    if visible_items:
        inv_count = len(inventory)
        for item in visible_items:
            if inv_count >= 10:
                break # Mentok 10 slot max
            free_decisions.append(Decision(
                kind="pickup",
                item_id=item.get("id"),
                reason=f"auto-pickup mengambil barang: {item.get('name', 'Unknown Item')}"
            ))
            inv_count += 1
            
    return free_decisions


def decide(view: dict) -> Decision:
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

    # 1) Evakuasi Death Zone
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        safe_targets = [c for c in connections]
        if safe_targets:
            return Decision(kind="move", target_region_id=random.choice(safe_targets), reason="evacuating death zone")

    # 1.5) Auto-heal 
    inventory_items = self_state.get("inventory") or []
    recovery_items = [i for i in inventory_items if i.get("category") == "recovery"]
    if hp_ratio < 0.75 and recovery_items:
        best_item = max(recovery_items, key=lambda i: i.get("hpRestore", 0))
        if best_item.get("hpRestore", 0) > 0:
            return Decision(kind="use_item", item_id=best_item.get("id"), reason=f"auto-heal: using {best_item.get('name')} (hp={hp_ratio:.0%})")

    # 1.6) Hindari Kerumunan
    CROWDED_THRESHOLD = 10
    if len(visible_agents) > CROWDED_THRESHOLD and connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="crowded region — evacuating")

    # 2) Kabur jika HP sekarat
    if hp_ratio < 0.40:
        if connections:
            return Decision(kind="move", target_region_id=random.choice(connections), reason=f"critical HP ({hp_ratio:.0%}) — retreating")
        else:
            return Decision(kind="wait", reason=f"critical HP ({hp_ratio:.0%}) but no connections to flee")

    # 3) Serang Target Lemah (Agent)
    if hp_ratio >= 0.6 and visible_agents:
        non_guardian = [a for a in visible_agents if not a.get("isGuardian")]
        if non_guardian:
            weakest = min(non_guardian, key=lambda a: a.get("hp", 999))
            if weakest.get("hp", 999) <= hp * 0.7 and ep > 0:
                return Decision(kind="attack", target_agent_id=weakest.get("id"), reason="engaging weaker isolated target")

    # 4) Serang Monster
    if hp_ratio >= 0.5 and visible_monsters and ep > 0:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        return Decision(kind="attack", target_monster_id=weakest_monster.get("id"), reason="clearing a weak monster for loot/reward")

    # 5) Didalam Cave -> Gunakan "interact" untuk berinteraksi dengan exit facility
    if in_cave:
        facilities = view.get("visibleFacilities") or current_region.get("facilities") or []
        if facilities:
            return Decision(kind="interact", interactable_id=facilities[0].get("id"), reason="using facility to exit cave")
        return Decision(kind="interact", reason="in cave — attempting generic interact to exit")

    # 6) Explore Ruin
    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    if visible_ruins and not alert_active and alert_gauge <= 4 and hp_ratio >= 0.7:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(kind="explore", ruin_id=ruin.get("ruinId"), reason=f"exploring ruin (alertGauge={alert_gauge})")

    # 7) Reposisi jika idle
    if connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="no immediate threat/opportunity — repositioning")

    return Decision(kind="wait", reason="no connections and nothing to do")


def build_action_payload(decision: Decision) -> dict:
    """Mengubah format Decision menjadi WS Envelope sesuai dengan standar server baru."""
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
    dangerous_regions: set = field(default_factory=set)
    region_before_last_move: Optional[str] = None
    last_move_target_region: Optional[str] = None
    consecutive_failed_moves: int = 0
    last_explored_ruin_id: Optional[str] = None

# [FUNGSI get_target_current_hp SAMA PERSIS]
def get_target_current_hp(view: dict, target_id: str) -> Optional[int]:
    for a in (view.get("visibleAgents") or []):
        if a.get("id") == target_id: return a.get("hp")
    for m in (view.get("visibleMonsters") or []):
        if m.get("id") == target_id: return m.get("hp")
    return None

async def send_hello(ws, entry_type: str) -> None:
    hello = {"type": "hello", "entryType": entry_type}
    await ws.send(json.dumps(hello))
    log.info("sent hello entryType=%s", entry_type)

async def maybe_act(ws, session: GameSession, view: dict) -> None:
    if not view:
        return

    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False:
        return

    hp = self_state.get("hp")

    # ----- PROSES FREE ACTIONS DULU (Pickup, Equip, dll) -----
    # Mengeksekusi dan mengirim payload action yang tidak makan turn / cooldown
    free_actions = decide_free_actions(view)
    for fd in free_actions:
        payload = build_action_payload(fd)
        log_info_block("Aksi Bebas (No Cooldown)", {
            "action": fd.kind,
            "target/item": fd.item_id or fd.interactable_id or fd.message,
            "alasan": fd.reason
        })
        await ws.send(json.dumps(payload))
        await asyncio.sleep(0.3)  # Delay kecil agar tidak spam websocket terlalu brutal

    # ----- PROSES MAIN ACTION (Cooldown Group) -----
    if not session.can_act:
        log.debug("canAct is false — waiting for can_act_changed before acting (main action)")
        return

    working_view = view
    if session.dangerous_regions:
        region = view.get("currentRegion") or {}
        connections = region.get("connections") or []
        safe_connections = [c for c in connections if c not in session.dangerous_regions]
        if safe_connections and len(safe_connections) < len(connections):
            working_view = dict(view)
            working_view["currentRegion"] = {**region, "connections": safe_connections}

    decision = decide(working_view)
    current_target = decision.ruin_id or decision.target_monster_id or decision.target_agent_id

    # Handle logic cegah nyangkut di target yang sudah mati/heal
    if decision.kind == "attack" and current_target:
        target_confirmed_dead = current_target in session.confirmed_dead_targets
        current_target_hp = get_target_current_hp(view, current_target)
        previously_seen_hp = session.last_seen_target_hp.get(current_target)
        is_same_target_as_last_attack = (current_target == session.last_action_target and session.last_decision_kind == "attack")
        
        no_damage_landed = (is_same_target_as_last_attack and previously_seen_hp is not None and current_target_hp is not None and current_target_hp == previously_seen_hp)
        target_healing = (is_same_target_as_last_attack and previously_seen_hp is not None and current_target_hp is not None and current_target_hp > previously_seen_hp)

        if target_confirmed_dead or no_damage_landed or target_healing:
            log.warning("redirecting away from stuck attack target=%s", current_target)
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(kind="move", target_region_id=random.choice(connections), reason="redirecting off a dead/stuck attack target")
            else:
                decision = Decision(kind="wait", reason="dead/stuck target, no exit")
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
                decision = Decision(kind="move", target_region_id=random.choice(connections), reason="breaking repeated-explore loop for safety")
            else:
                decision = Decision(kind="wait", reason="breaking repeat loop, no exit")
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
    target_display = decision.target_region_id or decision.target_agent_id or decision.target_monster_id or decision.ruin_id or decision.interactable_id
    
    log_info_block("Aksi Utama", {
        "action": decision.kind,
        "target": target_display,
        "alasan": decision.reason,
        "hp saat ini": hp,
    })

    await ws.send(json.dumps(payload))

    if is_cooldown_action(decision.kind):
        session.can_act = False

async def play_session(ws, session: GameSession) -> str:
    # [LOOP INI TETAP SAMA PERSIS SEPERTI SEBELUMNYA. HANYA MENYEBUT maybe_act SAAT DIPERLUKAN]
    async for raw in ws:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            continue

        ftype = frame.get("type")
        
        if ftype == "welcome":
            decision = frame.get("decision")
            if decision == "BLOCKED": return "closed"
        elif ftype in ("assigned"):
            session.game_id = frame.get("gameId")
            log_info_block("Masuk Room", {"room/game id": session.game_id, "entry type": session.entry_type})
        elif ftype in ("agent_view", "turn_advanced", "handover_sync", "action_rejected"):
            view = frame.get("view", {})
            session.last_view = view
            session.last_view_turn = frame.get("turn")
            if ftype != "action_rejected":
                # Validasi pindah sukses dan diagnosa HP ditaruh di sini sama seperti aslinya...
                pass
            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            if "success" in frame:
                success = frame.get("success")
            else:
                success = False
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act
            
            error = frame.get("error") or {}
            if not success:
                log_info_block("⚠️ AKSI GAGAL (action_result)", {
                    "action yang dikirim": session.last_decision_kind,
                    "target": session.last_action_target,
                    "error code": error.get("code"),
                    "error message": error.get("message"),
                    "canAct": can_act,
                })
            
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

        elif ftype == "agent_died":
            meta = frame.get("meta", {}) or {}
            if meta.get("youDied"):
                session.alive = False
                return "died"
        elif ftype == "game_ended":
            return "ended"

    return "closed"

# [BAGIAN RUN_ONE_GAME, CHOOSE_ENTRY_TYPE, DAN MAIN_LOOP SAMA PERSIS SEPERTI SEBELUMNYA]
# Pastikan tidak ada indentasi yang terpotong saat kamu menyalin.