#!/usr/bin/env python3
"""
Claw Royale agent bot.
(Aggressive Mode: Flash Looting, Smart Equip Armor & Weapon, Auto-Energy)

--------------------------------------------------------------------------
Tuning notes (why this isn't pure "never retreat"):

The game's ranking rule is: alive first -> survival time DESC -> kills
DESC -> EP used ASC -> agent id ASC. Survival time outranks kills. That
means a full "no retreat, ever" policy actively fights the ranking system
- dying for one more kill is a net loss, not a win. This keeps the
aggressive spirit (attack readily, don't flee ordinary fights, keep
flash-looting/auto-equip) but adds two cheap guards that cost nothing
when a game is going fine and only ever fire right before it would
otherwise end badly:
  - a critical-HP escape valve (only when there's no heal item left -
    healing is still tried first, same as before)
  - skipping *starting* a fight in an extremely crowded region (12+
    visible agents) - this threshold is high on purpose so normal 1-3
    agent skirmishes are completely untouched; it's grounded in an
    earlier live incident where big unexplained HP drops correlated
    specifically with 15-19+ visible agents in one region

Also fixed: auto-equip was scoring weapons/armor with field names
("damage"/"atk"/"power", "defense"/"def"/"armor") that don't exist on
real items - every weapon scored 0 and "best weapon" was arbitrary. Real
field is `atkBonus` for weapons and `defBonus` for armor, confirmed both
from live game data seen earlier in this session AND from this game's
own combat formula doc: "Base damage = attacker ATK + weapon atkBonus".
`hpRestore` / `epRestore` for recovery items were already correct.
--------------------------------------------------------------------------
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
    return kind in {"move", "attack", "explore", "use_item", "interact", "wait", "rest"}


def decide_free_actions(view: dict) -> list[Decision]:
    """Free actions (0 cooldown): fast looting + smart equip (weapon + armor).

    Scoring uses `atkBonus` (weapons) / `defBonus` (armor) as the primary
    key - confirmed against real item payloads seen in this game (e.g. a
    Pistol with atkBonus=15, a Chainmail with defBonus=12) and against
    this game's own combat formula: damage = ATK + weapon atkBonus. The
    old damage/atk/power/defense/def/armor keys are kept as harmless
    fallbacks in case a future item type uses different naming, but they
    don't exist on any item schema seen so far.
    """
    free_decisions = []
    self_state = view.get("self", {}) or {}
    inventory = self_state.get("inventory") or []
    equipped_weapon = self_state.get("equippedWeapon")
    equipped_armor = self_state.get("equippedArmor")

    # ---------------------------------------------------------
    # 1. AUTO-EQUIP SENJATA TERBAIK
    # ---------------------------------------------------------
    all_weapons = [i for i in inventory if i.get("category") == "weapon"]

    if equipped_weapon:
        if isinstance(equipped_weapon, dict) and equipped_weapon.get("category") == "weapon":
            all_weapons.append(equipped_weapon)

    if all_weapons:
        def weapon_score(w):
            return w.get("atkBonus", 0) or w.get("damage", 0) or w.get("atk", 0) or w.get("power", 0) or 0

        best_weapon = max(all_weapons, key=weapon_score)

        is_already_equipped = False
        if equipped_weapon:
            if isinstance(equipped_weapon, dict) and equipped_weapon.get("id") == best_weapon.get("id"):
                is_already_equipped = True
            elif isinstance(equipped_weapon, str) and equipped_weapon == best_weapon.get("id"):
                is_already_equipped = True

        if not is_already_equipped and best_weapon.get("id"):
            atk = weapon_score(best_weapon)
            free_decisions.append(Decision(
                kind="equip",
                item_id=best_weapon.get("id"),
                reason=f"auto-equip senjata TERKUAT: {best_weapon.get('name')} (atkBonus: {atk})"
            ))

    # ---------------------------------------------------------
    # 2. AUTO-EQUIP ARMOR TERBAIK
    # ---------------------------------------------------------
    all_armors = [i for i in inventory if i.get("category") in ["armor", "equipment"]]

    if equipped_armor:
        if isinstance(equipped_armor, dict):
            all_armors.append(equipped_armor)

    if all_armors:
        def armor_score(a):
            return a.get("defBonus", 0) or a.get("defense", 0) or a.get("def", 0) or a.get("armor", 0) or a.get("hpMax", 0) or 0

        best_armor = max(all_armors, key=armor_score)

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
                reason=f"auto-equip ARMOR TERKUAT: {best_armor.get('name')} (defBonus: {def_val})"
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
                break  # Mentok 10 slot max
            free_decisions.append(Decision(
                kind="pickup",
                item_id=item_id,
                reason=f"⚡ FAST LOOT: Mengambil {item.get('name', 'Unknown Item')}"
            ))
            inv_count += 1

    return free_decisions


def decide(view: dict) -> Decision:
    """Cooldown-group action. See the module-level tuning notes at the top
    of this file for why the two safety checks below exist."""
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

    # 1.1) Death zone -> always leave, no exceptions.
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        safe_targets = [c for c in connections]
        if safe_targets:
            return Decision(kind="move", target_region_id=random.choice(safe_targets), reason="evacuating death zone")

    # 1.2) Auto-heal when hurt.
    if hp_ratio < 0.60 and recovery_items:
        best_hp_item = max(recovery_items, key=lambda i: i.get("hpRestore", 0))
        if best_hp_item.get("hpRestore", 0) > 0:
            return Decision(kind="use_item", item_id=best_hp_item.get("id"), reason=f"auto-heal: using {best_hp_item.get('name')}")

    # 1.3) Critical HP with NO heal item left -> the one hard retreat in
    #    this bot. Ranking is alive > survival time > kills; dying here
    #    forfeits everything already earned this game for nothing. Only
    #    reachable when 1.2 couldn't heal (no item, or item exhausted).
    if hp_ratio < 0.25:
        if connections:
            return Decision(kind="move", target_region_id=random.choice(connections), reason=f"critical HP ({hp_ratio:.0%}), no heal item — forced retreat to stay alive")
        return Decision(kind="wait", reason=f"critical HP ({hp_ratio:.0%}), no heal item, no exit")

    # 1.4) Auto-Energy (Isi Stamina jika EP < 2 supaya bisa nyerang terus)
    if ep < 2 and recovery_items:
        def ep_score(i):
            return i.get("epRestore", 0) or i.get("spRestore", 0) or i.get("energyRestore", 0) or 0

        best_ep_item = max(recovery_items, key=ep_score)
        if ep_score(best_ep_item) > 0:
            return Decision(kind="use_item", item_id=best_ep_item.get("id"), reason=f"auto-energy: ngisi stamina pakai {best_ep_item.get('name')}")

    # 1.5) Extremely crowded region -> reposition instead of picking a
    #    fight here. High threshold on purpose: normal 1-3 agent
    #    skirmishes below this are completely untouched. Grounded in an
    #    earlier live incident where big unexplained HP drops correlated
    #    specifically with 15-19+ visible agents in one region.
    CROWDED_THRESHOLD = 12
    if len(visible_agents) > CROWDED_THRESHOLD and connections:
        return Decision(
            kind="move", target_region_id=random.choice(connections),
            reason=f"extremely crowded ({len(visible_agents)} agents visible) — repositioning before engaging",
        )

    # ---------------------------------------------------------
    # 2. FIGHT — aggressive, but skip fights that are clearly hopeless
    #    rather than trading survival time for them.
    # ---------------------------------------------------------

    if visible_agents and ep > 0:
        non_guardian = [a for a in visible_agents if not a.get("isGuardian")]
        # Generous on purpose - only filters out targets with a big HP
        # edge over us, not just anyone tougher. Falls back to the full
        # pool if that's genuinely all there is and we're still healthy.
        winnable = [a for a in non_guardian if a.get("hp", 999) <= hp * 1.5]
        pool = winnable or non_guardian
        if pool and hp_ratio >= 0.45:
            weakest = min(pool, key=lambda a: a.get("hp", 999))
            return Decision(kind="attack", target_agent_id=weakest.get("id"), reason="MODE AGRESIF: menghajar agent terlemah yang realistis dimenangkan")

    if visible_monsters and ep > 0 and hp_ratio >= 0.35:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        return Decision(kind="attack", target_monster_id=weakest_monster.get("id"), reason="MODE AGRESIF: menghabisi monster untuk loot")

    # ---------------------------------------------------------
    # 3. INTERAKSI & EKSPLORASI
    # ---------------------------------------------------------

    if in_cave:
        facilities = view.get("visibleFacilities") or current_region.get("facilities") or current_region.get("interactables") or []
        if facilities:
            return Decision(kind="interact", interactable_id=facilities[0].get("id"), reason="using facility to exit cave")
        return Decision(kind="interact", reason="in cave — attempting generic interact to exit")

    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    if visible_ruins and not alert_active and alert_gauge <= 6:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(kind="explore", ruin_id=ruin.get("ruinId"), reason=f"exploring ruin (alertGauge={alert_gauge})")

    if connections:
        return Decision(kind="move", target_region_id=random.choice(connections), reason="Hunting: mencari musuh/loot di region lain")

    return Decision(kind="wait", reason="no connections and nothing to do")


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
    # Dedup guard for free actions (pickup/equip): without this, calling
    # maybe_act multiple times against the same stale view (e.g. two
    # can_act_changed frames before a fresh agent_view arrives) would
    # resend the exact same pickup/equip repeatedly. Cleared whenever a
    # genuinely new agent_view/turn_advanced/handover_sync frame arrives.
    recently_attempted_free_actions: set = field(default_factory=set)


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

    # ----- PROSES FREE ACTIONS (Flash Looting & Smart Equip) -----
    free_actions = decide_free_actions(view)
    for fd in free_actions:
        dedup_key = f"{fd.kind}:{fd.item_id or fd.interactable_id or fd.message}"
        if dedup_key in session.recently_attempted_free_actions:
            continue
        session.recently_attempted_free_actions.add(dedup_key)
        payload = build_action_payload(fd)
        log_info_block("Aksi Bebas (No Cooldown)", {
            "action": fd.kind,
            "target/item": fd.item_id or fd.interactable_id or fd.message,
            "alasan": fd.reason
        })
        await ws.send(json.dumps(payload))
        # Jeda super cepat 0.01 detik agar server tidak pusing tapi barang kesapu bersih
        await asyncio.sleep(0.01)

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

    if decision.kind == "attack" and current_target:
        target_confirmed_dead = current_target in session.confirmed_dead_targets
        current_target_hp = get_target_current_hp(view, current_target)
        previously_seen_hp = session.last_seen_target_hp.get(current_target)
        is_same_target_as_last_attack = (current_target == session.last_action_target and session.last_decision_kind == "attack")

        no_damage_landed = (is_same_target_as_last_attack and previously_seen_hp is not None and current_target_hp is not None and current_target_hp == previously_seen_hp)
        target_healing = (is_same_target_as_last_attack and previously_seen_hp is not None and current_target_hp is not None and current_target_hp > previously_seen_hp)

        if target_confirmed_dead or no_damage_landed or target_healing:
            log.warning("redirecting away from stuck attack target=%s (confirmedDead=%s noDamageLanded=%s targetHealing=%s)", current_target, target_confirmed_dead, no_damage_landed, target_healing)
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

            if ftype != "action_rejected":
                # A genuinely new world-state snapshot arrived — safe to
                # retry any free actions that were deduped against the
                # previous (now stale) view.
                session.recently_attempted_free_actions.clear()

                current_region = view.get("currentRegion", {}) or {}
                current_region_id = current_region.get("id")
                new_self = view.get("self") or {}
                new_hp = new_self.get("hp")
                new_max_hp = new_self.get("maxHp")
                visible_agents = view.get("visibleAgents") or []
                visible_monsters = view.get("visibleMonsters") or []
                visible_ruins = view.get("visibleRuins") or []

                if session.last_move_target_region is not None:
                    moved_from = session.region_before_last_move
                    if current_region_id == moved_from:
                        session.consecutive_failed_moves += 1
                        if session.consecutive_failed_moves >= 3:
                            log.warning(
                                "move hasn't changed region in %d consecutive attempts "
                                "(stuck in %s) — worth a look if this keeps happening",
                                session.consecutive_failed_moves, moved_from,
                            )
                    else:
                        session.consecutive_failed_moves = 0
                    session.last_move_target_region = None
                    session.region_before_last_move = None

                hp_display = f"{new_hp}/{new_max_hp}" if new_max_hp else str(new_hp)
                reason = frame.get("reason")

                log_info_block("Status", {
                    "turn": frame.get("turn"),
                    "hp": hp_display,
                    "ep": new_self.get("ep"),
                    "bisa aksi": session.can_act,
                    "posisi (region)": current_region_id,
                    "death zone": current_region.get("isDeathZone"),
                    "musuh terlihat": len(visible_agents) or None,
                    "monster terlihat": len(visible_monsters) or None,
                    "ruin terlihat": len(visible_ruins) or None,
                    "update type": f"{ftype} ({reason})" if reason else ftype,
                })

                equipped_weapon = new_self.get("equippedWeapon")
                equipped_armor = new_self.get("equippedArmor")
                inventory_items = new_self.get("inventory") or []
                equip_signature = json.dumps({"weapon": equipped_weapon, "armor": equipped_armor, "inventory": inventory_items}, sort_keys=True)

                if equip_signature != session.last_equipment_signature:
                    session.last_equipment_signature = equip_signature
                    item_lines = {it.get("name", f"item {i}"): f"x{it.get('quantity', 1)}" for i, it in enumerate(inventory_items)}
                    weapon_name = equipped_weapon.get("name") if isinstance(equipped_weapon, dict) else equipped_weapon
                    armor_name = equipped_armor.get("name") if isinstance(equipped_armor, dict) else equipped_armor
                    log_info_block("Equipment", {
                        "weapon": weapon_name or "(kosong)",
                        "armor": armor_name or "(kosong)",
                        **(item_lines or {"item": "(kosong)"}),
                    })

            session.last_view = view
            session.last_view_turn = frame.get("turn")

            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            if "success" in frame:
                success = frame.get("success")
            else:
                log.warning("action_result missing 'success' entirely — treating as failure: %s", frame)
                success = False
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act

            error = frame.get("error") or {}
            if not success:
                # last_action_target only tracks attack/explore targets -
                # for a failed move, show the move's actual target region
                # instead of a stale attack target id.
                failed_target = (
                    session.last_move_target_region
                    if session.last_decision_kind == "move"
                    else session.last_action_target
                )
                log_info_block("⚠️ AKSI GAGAL (action_result)", {
                    "action yang dikirim": session.last_decision_kind,
                    "target": failed_target,
                    "error code": error.get("code"),
                    "error message": error.get("message"),
                    "canAct": can_act,
                })

            if error.get("code") == "TARGET_DEAD" and session.last_action_target:
                session.confirmed_dead_targets.add(session.last_action_target)

            # An explicit rejection (e.g. COOLDOWN_ACTIVE right after a
            # TARGET_DEAD redirect, when our client-side can_act tracking
            # was briefly ahead of the server's real cooldown state)
            # already explains exactly why this move failed. Don't also
            # let the generic silent-move diagnostic below pile a
            # "N consecutive failures" warning on top of an
            # already-understood, unrelated failure mode - that
            # diagnostic exists specifically to catch moves the server
            # reports as successful but silently doesn't apply.
            if (
                not success
                and session.last_decision_kind == "move"
                and session.last_move_target_region is not None
            ):
                session.consecutive_failed_moves = 0
                session.last_move_target_region = None
                session.region_before_last_move = None

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

async def run_one_game(rest: RestClient, entry_type: str) -> str:
    headers = {
        "X-API-Key": rest.api_key,
        "X-Version": rest.version,
    }

    try:
        async with websockets.connect(
            WS_JOIN_URL, additional_headers=headers, ping_interval=20, ping_timeout=20
        ) as ws:
            welcome_raw = await ws.recv()
            welcome = json.loads(welcome_raw)

            if welcome.get("type") == "welcome":
                decision = welcome.get("decision")
                if decision == "BLOCKED":
                    return "blocked"

            await send_hello(ws, entry_type)
            session = GameSession(entry_type=entry_type)
            outcome = await play_session(ws, session)
            return outcome

    except ConnectionClosed as e:
        if e.code == 1013: return "resume_dead"
        if e.code == 4032: return "died"
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
        try: loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError: pass

    async with RestClient(API_KEY) as rest:
        await rest.fetch_version()
        try:
            me = await rest.get_me()
            readiness = me.get("readiness", {}) or {}
            log_info_block("Akun", {
                "nama": me.get("name"),
                "balance": f"{me.get('balance')} sMoltz",
                "wallet ok": readiness.get("walletAddress"),
                "whitelist": readiness.get("whitelistApproved"),
                "sMoltz cukup": readiness.get("sMoltzSufficient"),
                "paid ready": readiness.get("paidReady"),
            })
        except ApiError as e:
            log.error("could not fetch account — check CLAW_API_KEY: %s", e)
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

            except ApiError:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            except (ConnectionClosed, OSError):
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            except Exception:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


if __name__ == "__main__":
    asyncio.run(main_loop())