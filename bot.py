#!/usr/bin/env python3
"""
Claw Royale agent bot.

Single agent, single account/wallet — by design. Claw Royale enforces
"1 SC wallet = 1 active free game + 1 active paid game, primary agent only"
and actively detects/penalizes in-game teaming, so running many accounts
against the same rooms is against the platform's own rules. This bot plays
one agent as well as it can instead.

Flow (per skill.md / openapi.yaml):
  GET /api/version         -> X-Version header value for every request
  GET /api/accounts/me     -> readiness + currentGames (resume vs fresh join)
  GET/PUT /api/loadout*    -> ensure a full loadout (Main+Sub pack + 3 relics)
                              before joining a NEW game (skip on resume)
  wss://.../ws/join         -> hello {type:"hello", entryType, mode} -> welcome
                              -> queued/assigned -> becomes the gameplay socket
  gameplay loop:
    agent_view / turn_advanced -> decide() -> send action (or free action)
    action_result (canAct=false after a cooldown action) -> wait for
      can_act_changed before sending another cooldown-group action
    agent_died with meta.youDied == true -> stop, this run is over
    game_ended -> read result, go back to matchmaking

Config is via environment variables — see .env.example / docker-compose.yml.

--------------------------------------------------------------------------
Changelog:

1. Move-success verification (region-changed check on the turn after a
   move) and 2. an explore-repeat guard fix (tracks the ruin id directly
   instead of only comparing against the immediately-previous action)
   were added after live runs showed a "stuck 9 turns, HP fine, died
   anyway" pattern.

3. ROOT CAUSE FOUND AND FIXED: the WS action envelope itself was wrong.
   This bot was sending flat payloads like
     {"type": "action", "action": "move", "targetRegionId": "..."}
   but the real server contract (references/actions.md + a working
   reference implementation, both from github.com/ruspian/molty5) wants
   the action nested under "data" with its OWN "type" key, and different
   field names:
     {"type": "action", "data": {"type": "move", "regionId": "..."}}
   actions.md even calls this out by name under its gotchas: "do not
   wrap actions in the old HTTP `{ "action": ... }` body shape" — which
   is exactly the shape this bot was using. This one bug plausibly
   explains every prior symptom at once: moves that never changed
   currentRegion.id, attacks that never landed damage, and possibly
   silent no-ops on every other action too, all while action_result's
   `success` field defaulted to True in this bot whenever it happened to
   be missing/misread — masking outright rejections as "success".

   Corrected field names (source: references/actions.md, cross-checked
   against ruspian/molty5's bot/game/action_sender.py which builds
   these same envelopes and is presumably working against the real
   server):
     move:     {"type": "move", "regionId": "..."}
     attack:   {"type": "attack", "targetId": "...", "targetType": "agent"|"monster"}
     use_item: {"type": "use_item", "itemId": "..."}
     explore:  {"type": "explore"}  — NOTE: actions.md marks this
               "currently disabled (action rebuild in progress) — do not
               submit" server-side. Left wired up rather than guessed
               away, since this bot's own logs showed explore actions
               going out during real games; watch the (now-fixed)
               action_result error logging for INVALID_ACTION-style
               rejections to see whether that applies to this specific
               deployment.

   Two more bugs fell out of implementing the real spec correctly:
     a. use_item is actually IN the cooldown group per actions.md
        (costs 1 EP, triggers the 60s cooldown) — this bot had it
        classified as a free action and was sending a heal AND a
        separate move/attack/explore in the same turn. That second
        action would have been sent while still on cooldown from the
        first, which the real server most likely rejects. The special
        "send heal then immediately decide again" path is removed;
        use_item now goes through the same single-action-per-turn path
        as everything else.
     b. "wait" was being sent to the server as a literal
        {"action": "wait"} — not a real action type in actions.md at
        all. It's now mapped to the real "rest" action (0 EP, still
        consumes the cooldown turn, but grants +1 bonus EP instead of
        silently doing nothing / getting rejected).

   Left untouched (and worth flagging, not fixing blind): actions.md's
   own join/connection docs describe a REST `POST /api/join` (long-poll)
   flow followed by connecting straight to `/ws/agent` with no join
   handshake message — different from this bot's `/ws/join` + `hello`
   WebSocket handshake. Since this bot's handshake has empirically been
   getting into real games (turns advancing, HP changing, deaths), and
   the reference docs' host (cdn.moltyroyale.com) differs from this
   bot's default (cdn.clawroyale.ai), that connection layer was left as
   is rather than "fixed" against what may be a different game/host.
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
# "auto" -> paid if ready else free ; "free" ; "paid"

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
    """Print a readable multi-line block like:
        === Account ===
        nama       : Otooong
        balance    : 0 sMoltz
        ...
    Skips fields whose value is None so optional data doesn't clutter it.
    """
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
        self.version = "1"  # refreshed by fetch_version()
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
            # Server payload shape is loosely typed in the spec
            # (additionalProperties: true) — try common keys.
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
                    # VERSION_MISMATCH — refresh and retry once
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
        # Some deployments wrap this in {"success": true, "data": {...}},
        # others return MeResponse fields at the top level directly.
        # Handle both without guessing wrong every time.
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
# Loadout setup — best-effort: fill Main+Sub pack + 3 relic slots from
# whatever the account already owns. This does NOT buy anything from the
# shop; it only equips what is already in inventory. Run the shop / gacha
# flow manually first if your inventory is empty (see references/shop.md).
# --------------------------------------------------------------------------

async def ensure_loadout(rest: RestClient) -> None:
    try:
        loadout = (await rest.get_loadout()).get("data", {})
    except ApiError as e:
        log.warning("could not read loadout: %s", e)
        return

    log.info(
        "current loadout: fullSet=%s activePack=%s subPack=%s slots=%s",
        loadout.get("fullSet"),
        loadout.get("activePack"),
        loadout.get("subPack"),
        loadout.get("slots"),
    )

    if loadout.get("fullSet"):
        log.info("loadout already fullSet — skipping setup")
        return

    log.info("loadout incomplete — attempting to auto-fill from inventory")

    try:
        packs = await rest.get_inventory_packs()
        relics = await rest.get_inventory_relics()
    except ApiError as e:
        log.warning("could not read inventory: %s", e)
        return

    active_pack = loadout.get("activePack")
    slots = loadout.get("slots") or [None, None, None]

    # Fill main pack if missing
    if not active_pack and packs:
        main_candidate = packs[0]
        try:
            await rest.set_active_pack(main_candidate["instanceId"])
            log.info("equipped main pack instanceId=%s", main_candidate["instanceId"])
        except ApiError as e:
            log.warning("failed to equip main pack: %s", e)

    # Fill sub pack if there's a second distinct pack available
    # (server rejects Main-only packs in the sub slot; we just try and log).
    if len(packs) > 1:
        try:
            await rest.set_sub_pack(packs[1]["instanceId"])
            log.info("equipped sub pack instanceId=%s", packs[1]["instanceId"])
        except ApiError as e:
            log.info("sub pack equip skipped/failed: %s", e)

    # Fill relic slots by typeIndex (0..2) from owned relics matching each slot
    for type_index in range(3):
        if slots[type_index]:
            continue
        candidate = next(
            (r for r in relics if r.get("typeIndex") == type_index), None
        )
        if candidate:
            try:
                await rest.equip_relic(type_index, candidate["instanceId"])
                log.info(
                    "equipped relic slot=%s instanceId=%s",
                    type_index,
                    candidate["instanceId"],
                )
            except ApiError as e:
                log.warning("failed to equip relic slot %s: %s", type_index, e)

    if not packs or len(relics) < 3:
        log.warning(
            "inventory insufficient for a full loadout (packs=%d relics=%d) — "
            "entering with partial/base stats. Buy packs/relics via the shop "
            "to improve this.",
            len(packs), len(relics),
        )


# --------------------------------------------------------------------------
# Decision logic
# --------------------------------------------------------------------------
#
# Ranking (1.15.0+): alive first -> survival time DESC -> kills DESC ->
# EP used ASC -> agent id ASC. Remaining HP does not matter by itself.
# So the guiding principle is: SURVIVE FIRST, fight only when it does not
# cost you survival time, never trade survival time for a kill.

@dataclass
class Decision:
    kind: str  # "move" | "attack" | "explore" | "use_item" | "wait" | "flee"
    target_region_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_monster_id: Optional[str] = None
    ruin_id: Optional[str] = None
    item_id: Optional[str] = None
    reason: str = ""


def is_cooldown_action(kind: str) -> bool:
    # Per references/actions.md §2, the real cooldown group is:
    # move, attack, use_item, interact, rest (and explore, though currently
    # disabled server-side). use_item was previously (wrongly) treated as
    # free here. "wait" isn't a real server action - build_action_payload()
    # maps it to "rest", which DOES consume the cooldown, so it belongs in
    # this set too. Only pickup/equip/talk/whisper/broadcast are free.
    return kind in {"move", "attack", "explore", "use_item", "wait"}


def decide(view: dict) -> Decision:
    """Pure function: game view -> next action. Keep this readable and
    tune it here — this is the whole 'strategy' of the bot."""

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

    # 1) Currently standing in a death zone (or one is about to activate
    #    here) -> leave immediately, this outranks everything else.
    pending_here_ids = {dz.get("id") for dz in pending_deathzones}
    if is_death_zone or current_region.get("id") in pending_here_ids:
        safe_targets = [c for c in connections]
        if safe_targets:
            return Decision(
                kind="move",
                target_region_id=random.choice(safe_targets),
                reason="evacuating death zone",
            )

    # 1.5) Auto-heal: use a recovery item from inventory when HP is low.
    #    NOTE: use_item is a COOLDOWN action per actions.md (costs 1 EP,
    #    triggers the 60s cooldown) — it is NOT free. This just returns
    #    the heal as THIS turn's action like any other; maybe_act no
    #    longer sends a second action in the same turn afterward.
    inventory_items = self_state.get("inventory") or []
    recovery_items = [i for i in inventory_items if i.get("category") == "recovery"]
    if hp_ratio < 0.75 and recovery_items:
        best_item = max(recovery_items, key=lambda i: i.get("hpRestore", 0))
        if best_item.get("hpRestore", 0) > 0:
            return Decision(
                kind="use_item",
                item_id=best_item.get("id"),
                reason=(
                    f"auto-heal: using {best_item.get('name', 'recovery item')} "
                    f"(hp={hp_ratio:.0%}, restores {best_item.get('hpRestore')})"
                ),
            )

    # 1.6) Crowded/contested region -> too many visible agents means high
    #    risk of being attacked from multiple directions at once, even if
    #    our own HP looks fine right now. Observed pattern: large
    #    unexplained HP drops (e.g. 87 -> 58 in one turn) happening in
    #    regions with 15-19+ visible agents, with no death zone / alert
    #    gauge / weather flag explaining it — most likely damage from
    #    agents the fight-or-flee logic below doesn't account for when
    #    the crowd is this dense. Evacuate before engaging with anything.
    CROWDED_THRESHOLD = 10
    if len(visible_agents) > CROWDED_THRESHOLD and connections:
        return Decision(
            kind="move",
            target_region_id=random.choice(connections),
            reason=(
                f"crowded region ({len(visible_agents)} agents visible) — "
                "evacuating before engaging, high risk of multi-attacker damage"
            ),
        )

    # 2) Low HP -> disengage and retreat rather than fight, since survival
    #    time outranks kills in the current ranking rules. This must fire
    #    regardless of WHY hp is low (agent damage, monster counter-hit,
    #    zone/weather tick) — critical HP always means "get out", not just
    #    when a hostile agent happens to be visible. Only reached when we
    #    have no usable recovery item (the auto-heal branch above would
    #    have returned already if we did).
    if hp_ratio < 0.40:
        if connections:
            return Decision(
                kind="move",
                target_region_id=random.choice(connections),
                reason=f"critical HP ({hp_ratio:.0%}) — retreating unconditionally",
            )
        else:
            return Decision(
                kind="wait",
                reason=f"critical HP ({hp_ratio:.0%}) but no connections to flee to",
            )

    # 4) Healthy and a weak, isolated target is adjacent -> only then
    #    consider a fight. Never chase; only engage what's already here.
    if hp_ratio >= 0.6 and visible_agents:
        non_guardian_targets = [a for a in visible_agents if not a.get("isGuardian")]
        if non_guardian_targets:
            weakest = min(
                non_guardian_targets,
                key=lambda a: a.get("hp", 999),
            )
            if weakest.get("hp", 999) <= hp * 0.7 and ep > 0:
                return Decision(
                    kind="attack",
                    target_agent_id=weakest.get("id"),
                    reason="engaging weaker isolated target while healthy",
                )

    # 4) A monster is visible and we're healthy with EP -> fine to fight
    #    (monsters are usually a safer EP/reward trade than agents).
    if hp_ratio >= 0.5 and visible_monsters and ep > 0:
        weakest_monster = min(visible_monsters, key=lambda m: m.get("hp", 999))
        return Decision(
            kind="attack",
            target_monster_id=weakest_monster.get("id"),
            reason="clearing a weak monster for loot/reward",
        )

    # 5) In a cave -> the only way out is to interact the same
    #    interactableId used to enter; this bot does not track that id
    #    across turns (kept out of scope), so default to waiting it out
    #    unless a specific integration adds tracking. (references/actions.md
    #    documents a real "interact" action - {"type": "interact",
    #    "interactableId": "..."} - so wiring this up properly is a
    #    reasonable next step, just not done here.)
    if in_cave:
        return Decision(kind="wait", reason="in cave — awaiting explicit exit handling")

    # 6) A ruin is nearby and alert risk is low -> explore it for
    #    relics/packs. Each explore raises alertGauge +2 (+4 more on full
    #    clear); at gauge 10 guardians actively hunt you. Back off well
    #    before alertActive actually triggers, and require more HP margin
    #    than other actions since a ruin ambush can hit hard.
    #    NOTE: references/actions.md marks explore as "currently disabled
    #    (action rebuild in progress)" server-side as of that doc's
    #    version. Left enabled here since this bot's own logs showed
    #    explore actions going out in real games — watch for an
    #    INVALID_ACTION-style rejection in the (now-fixed) action_result
    #    error logging to confirm whether that applies to this deployment.
    alert_active = self_state.get("alertActive", False)
    alert_gauge = self_state.get("alertGauge", 0) or 0
    if visible_ruins and not alert_active and alert_gauge <= 4 and hp_ratio >= 0.7:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(
                kind="explore",
                ruin_id=ruin.get("ruinId"),
                reason=f"exploring ruin (alertGauge={alert_gauge}, hp={hp_ratio:.0%})",
            )

    # 7) Nothing urgent -> reposition toward an unexplored-looking
    #    connection to keep finding ruins/loot rather than idling in place
    #    (idling in one region for too long is a common way to get
    #    cornered as the map shrinks).
    if connections:
        return Decision(
            kind="move",
            target_region_id=random.choice(connections),
            reason="no immediate threat/opportunity — repositioning",
        )

    return Decision(kind="wait", reason="no connections and nothing to do")


def build_action_payload(decision: Decision) -> dict:
    """Build the WS action envelope per references/actions.md, cross-checked
    against a real working implementation (ruspian/molty5's
    bot/game/action_sender.py). The envelope nests the action under "data"
    with its OWN "type" key - NOT the flat {"action": kind, ...} shape this
    bot was sending before, which actions.md explicitly calls out as wrong:
    "do not wrap actions in the old HTTP `{ "action": ... }` body shape."

        {"type": "action", "data": {"type": "move", "regionId": "..."}}

    "wait" isn't a real action type in the spec - closest real equivalent
    is "rest" (0 EP, still consumes the cooldown turn, grants +1 bonus EP
    instead of silently doing nothing).
    """

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
    elif decision.kind == "wait":
        data = {"type": "rest"}
    else:
        data = {"type": decision.kind}

    payload: dict[str, Any] = {"type": "action", "data": data}
    if decision.reason:
        # Cap per actions.md's "Thought reasoning: 500 chars, exceeding
        # causes validation failure".
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
    last_action_target: Optional[str] = None  # ruinId, targetAgentId, or targetMonsterId
    consecutive_same_target: int = 0
    confirmed_dead_targets: set = field(default_factory=set)
    last_seen_target_hp: dict = field(default_factory=dict)  # targetId -> hp
    last_equipment_signature: Optional[str] = None
    dangerous_regions: set = field(default_factory=set)  # regionId -> seen a big HP drop here
    # Move-success verification (see the agent_view handler in
    # play_session): tracks what region we were in and what we asked to
    # move to, then checks on the next state update whether it actually
    # changed. Kept as a standing sanity check even now that the payload
    # bug is fixed, so a regression shows up immediately instead of
    # silently again.
    region_before_last_move: Optional[str] = None
    last_move_target_region: Optional[str] = None
    consecutive_failed_moves: int = 0
    # Explore-repeat guard target, tracked by ruin id independently of
    # last_decision_kind so an explore -> move -> explore sequence on the
    # SAME ruin still gets caught even with "move" sitting in between.
    last_explored_ruin_id: Optional[str] = None


async def send_hello(ws, entry_type: str) -> None:
    hello = {"type": "hello", "entryType": entry_type}
    await ws.send(json.dumps(hello))
    log.info("sent hello entryType=%s", entry_type)


async def play_session(ws, session: GameSession) -> str:
    """Consume frames until the agent dies or the game ends.
    Returns 'died' | 'ended' | 'closed' to tell the caller what happened."""

    async for raw in ws:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("non-JSON frame: %r", raw[:200])
            continue

        ftype = frame.get("type")
        log.debug("frame: %s", json.dumps(frame)[:500])

        if ftype == "welcome":
            decision = frame.get("decision")
            log.info("welcome decision=%s", decision)
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
            reason = frame.get("reason")

            current_region = view.get("currentRegion", {}) or {}
            current_region_id = current_region.get("id")
            visible_agents = view.get("visibleAgents") or []
            visible_monsters = view.get("visibleMonsters") or []
            visible_ruins = view.get("visibleRuins") or []

            # Move-success verification: did currentRegion.id actually
            # change after we sent a move last turn?
            if session.last_move_target_region is not None:
                moved_from = session.region_before_last_move
                if current_region_id == moved_from:
                    session.consecutive_failed_moves += 1
                    log.warning(
                        "move appears to have FAILED: still in region=%s after "
                        "requesting move to %s (consecutive_failed_moves=%d)",
                        moved_from, session.last_move_target_region,
                        session.consecutive_failed_moves,
                    )
                    if session.consecutive_failed_moves >= 3:
                        log.error(
                            "move has failed to change region %d turns in a row "
                            "while stuck in region=%s — dumping full raw view for "
                            "inspection: %s",
                            session.consecutive_failed_moves, moved_from,
                            json.dumps(view, default=str),
                        )
                else:
                    if session.consecutive_failed_moves:
                        log.info(
                            "move succeeded — region changed %s -> %s after %d "
                            "failed attempt(s)",
                            moved_from, current_region_id,
                            session.consecutive_failed_moves,
                        )
                    session.consecutive_failed_moves = 0
                session.last_move_target_region = None
                session.region_before_last_move = None

            hp_display = (
                f"{new_hp}/{new_max_hp}" if new_max_hp else str(new_hp)
            )

            log_info_block("Status", {
                "turn": session.last_view_turn,
                "hp": hp_display,
                "ep": new_self.get("ep"),
                "bisa aksi": session.can_act,
                "posisi (region)": current_region_id,
                "death zone": current_region.get("isDeathZone"),
                "musuh terlihat": len(visible_agents) or None,
                "monster terlihat": len(visible_monsters) or None,
                "ruin terlihat": len(visible_ruins) or None,
                "alert gauge": new_self.get("alertGauge"),
                "update type": f"{ftype} ({reason})" if reason else ftype,
            })

            # Equipment snapshot — weapon + inventory items, logged only when
            # it actually changes so this doesn't spam every single turn.
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
                log_info_block("Equipment", {
                    "weapon": weapon_name or "(kosong)",
                    **(item_lines or {"item": "(kosong)"}),
                })
            # Diagnostic: if HP dropped since the last view and it wasn't
            # from an attack we just sent, dump the ENTIRE raw view so any
            # field we haven't modeled (weather, events, guardian proximity,
            # status effects — whatever the server actually uses) becomes
            # visible instead of guessed at. Fires on any large-ish drop
            # regardless of last action, since large drops have now been
            # observed both on non-attack turns AND right after our own
            # attack action (e.g. 57->21 following an attack) — excluding
            # attack turns was hiding half the relevant data.
            HP_DROP_DIAGNOSTIC_THRESHOLD = 10
            if (
                prev_hp is not None
                and new_hp is not None
                and (prev_hp - new_hp) >= HP_DROP_DIAGNOSTIC_THRESHOLD
            ):
                danger_region_id = current_region_id
                if danger_region_id:
                    session.dangerous_regions.add(danger_region_id)
                log.warning(
                    "HP dropped %s -> %s (delta=%s) last_action=%s region=%s "
                    "(marked as dangerous, will avoid revisiting) "
                    "— FULL raw view=%s",
                    prev_hp, new_hp, prev_hp - new_hp, session.last_decision_kind,
                    danger_region_id, json.dumps(view, default=str),
                )
            await maybe_act(ws, session, view)

        elif ftype == "action_rejected":
            # Same frame shape as agent_view/turn_advanced but tagged as a
            # failed-action snapshot (1.15.0). Treat identically — it is
            # the authoritative state at this moment, action was refused.
            view = frame.get("view", {})
            session.last_view = view
            session.last_view_turn = frame.get("turn")
            log.info(
                "action_rejected — refreshed state turn=%s hp=%s",
                session.last_view_turn, (view.get("self") or {}).get("hp"),
            )
            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            # actions.md guarantees "success" is always present on this
            # frame. Previously this defaulted a missing key to True,
            # which would have silently treated an unrecognized/malformed
            # response as a success. Default to False instead — fail
            # closed, not open — and log loudly if it's ever truly absent.
            if "success" in frame:
                success = frame.get("success")
            else:
                log.warning(
                    "action_result frame is missing 'success' entirely — "
                    "treating as a failure defensively: %s", frame,
                )
                success = False
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act
            cooldown_ms = frame.get("cooldownRemainingMs")
            error = frame.get("error") or {}

            if not success:
                log_info_block("⚠️ AKSI GAGAL (action_result)", {
                    "action yang dikirim": session.last_decision_kind,
                    "target": session.last_action_target,
                    "error code": error.get("code"),
                    "error message": error.get("message"),
                    "canAct": can_act,
                })
            else:
                log.debug(
                    "action_result success=%s canAct=%s cooldownRemainingMs=%s",
                    success, can_act, cooldown_ms,
                )
            # TARGET_DEAD (1.15.0) is the authoritative "that target is
            # already dead" signal — distinct from AGENT_DEAD (our own
            # death). Remember it so the repeat-attack guard can retarget
            # instead of blindly refusing every repeat.
            if error.get("code") == "TARGET_DEAD" and session.last_action_target:
                session.confirmed_dead_targets.add(session.last_action_target)
                log.info(
                    "target=%s confirmed dead via TARGET_DEAD",
                    session.last_action_target,
                )
            # Some server versions attach a fresh view directly on the
            # action_result frame itself — capture it if present so the
            # next decision isn't made from a stale cached view.
            inline_view = frame.get("view")
            if inline_view:
                session.last_view = inline_view
                session.last_view_turn = frame.get("turn", session.last_view_turn)

        elif ftype == "can_act_changed":
            session.can_act = frame.get("canAct", True)
            log.info("can_act_changed -> %s", session.can_act)
            if session.can_act and session.last_view:
                await maybe_act(ws, session, session.last_view)

        elif ftype == "agent_died":
            meta = frame.get("meta", {}) or {}
            if meta.get("youDied"):
                log.info(
                    "we died — survivalTime=%s kills=%s",
                    frame.get("survivalTime"), frame.get("kills"),
                )
                session.alive = False
                return "died"
            else:
                log.debug("another agent died (not us)")

        elif ftype == "game_ended":
            log.info("game_ended: %s", json.dumps(frame)[:800])
            return "ended"

        elif ftype == "log":
            log.debug("game log: %s", frame.get("message"))

        else:
            log.debug("unhandled frame type=%s", ftype)

    return "closed"


def get_target_current_hp(view: dict, target_id: str) -> Optional[int]:
    """Look up a target's current HP from visibleAgents/visibleMonsters by id,
    so the attack-repeat guard can tell if a previous hit actually landed."""
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

    hp = self_state.get("hp")

    # Free actions (talk/whisper) go BEFORE the main action and never
    # consume the turn — placeholder hook, extend with real chat logic
    # if you want the agent to communicate.
    # await send_free_action(ws, {"type": "action", "data": {"type": "whisper", ...}})

    if not session.can_act:
        log.debug("canAct is false — waiting for can_act_changed before acting")
        return

    # Steer away from regions we've already seen cause big HP drops. We
    # build a filtered copy of the view rather than changing decide()'s
    # signature, so decide() stays a simple, testable pure function.
    working_view = view
    if session.dangerous_regions:
        region = view.get("currentRegion") or {}
        connections = region.get("connections") or []
        safe_connections = [c for c in connections if c not in session.dangerous_regions]
        if safe_connections and len(safe_connections) < len(connections):
            log.info(
                "avoiding %d known-dangerous connection(s) out of %d options",
                len(connections) - len(safe_connections), len(connections),
            )
            working_view = dict(view)
            working_view["currentRegion"] = {**region, "connections": safe_connections}

    decision = decide(working_view)

    # use_item (auto-heal) is a normal cooldown action per actions.md — it
    # is sent as THIS turn's action via the same path as everything else
    # below, not as an extra free action followed by a second real action
    # in the same turn (that assumption was wrong and would have raced
    # against the server's cooldown).

    current_target = (
        decision.ruin_id or decision.target_monster_id or decision.target_agent_id
    )

    if decision.kind == "attack" and current_target:
        # Redirect only when we have a real reason to believe repeating is
        # pointless: the target was already confirmed dead (TARGET_DEAD),
        # its HP hasn't moved since our last hit (miss/blocked — no damage
        # landed), or its HP went UP (regenerating/healing faster than we
        # can damage it). Otherwise, an alive target that's taking damage
        # is exactly what we WANT to keep hitting to close out the kill.
        target_confirmed_dead = current_target in session.confirmed_dead_targets
        current_target_hp = get_target_current_hp(view, current_target)
        previously_seen_hp = session.last_seen_target_hp.get(current_target)
        is_same_target_as_last_attack = (
            current_target == session.last_action_target
            and session.last_decision_kind == "attack"
        )
        no_damage_landed = (
            is_same_target_as_last_attack
            and previously_seen_hp is not None
            and current_target_hp is not None
            and current_target_hp == previously_seen_hp
        )
        target_healing = (
            is_same_target_as_last_attack
            and previously_seen_hp is not None
            and current_target_hp is not None
            and current_target_hp > previously_seen_hp
        )

        if target_confirmed_dead or no_damage_landed or target_healing:
            log.warning(
                "redirecting away from attack target=%s (confirmedDead=%s "
                "noDamageLanded=%s targetHealing=%s prevHp=%s curHp=%s ep=%s) "
                "— last attack payload sent was targeting this same id",
                current_target, target_confirmed_dead, no_damage_landed,
                target_healing, previously_seen_hp, current_target_hp,
                self_state.get("ep"),
            )
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(
                    kind="move",
                    target_region_id=random.choice(connections),
                    reason="redirecting off a dead/stuck/healing attack target",
                )
                current_target = None
            else:
                decision = Decision(kind="wait", reason="dead/stuck/healing target, no exit")
                current_target = None
        else:
            if current_target_hp is not None:
                session.last_seen_target_hp[current_target] = current_target_hp
            session.last_action_target = current_target

    # Explore uses a ruin-id-based repeat guard: track the ruin we last
    # tried to explore directly, independent of session.last_decision_kind,
    # so an explore -> move -> explore sequence on the SAME ruin still
    # trips the guard even with a move sitting in between.
    elif decision.kind == "explore" and current_target:
        if current_target == session.last_explored_ruin_id:
            session.consecutive_same_target += 1
        else:
            session.consecutive_same_target = 0
        session.last_explored_ruin_id = current_target

        if session.consecutive_same_target >= 1:
            log.warning(
                "refusing to repeat explore on target=%s again without a fresh "
                "turn (hp=%s) — falling back to repositioning instead",
                current_target, hp,
            )
            connections = (view.get("currentRegion") or {}).get("connections") or []
            if connections:
                decision = Decision(
                    kind="move",
                    target_region_id=random.choice(connections),
                    reason="breaking repeated-explore loop for safety",
                )
            else:
                decision = Decision(kind="wait", reason="breaking repeat loop, no exit")
            session.consecutive_same_target = 0
            session.last_explored_ruin_id = None
            session.last_action_target = None
        else:
            session.last_action_target = current_target
    else:
        # move/wait/other kinds don't carry a repeatable target — leave
        # last_action_target as-is so a subsequent attack/explore decision
        # can still be compared against the last REAL target we acted on.
        session.consecutive_same_target = 0

    session.last_decision_kind = decision.kind

    if decision.kind == "move" and decision.target_region_id:
        # Record what we're about to attempt so the agent_view handler can
        # verify next turn whether the region actually changed.
        session.region_before_last_move = (view.get("currentRegion") or {}).get("id")
        session.last_move_target_region = decision.target_region_id

    payload = build_action_payload(decision)
    target_display = (
        decision.target_region_id
        or decision.target_agent_id
        or decision.target_monster_id
        or decision.ruin_id
    )
    log_info_block("Aksi", {
        "action": decision.kind,
        "target": target_display,
        "alasan": decision.reason,
        "hp saat ini": hp,
    })

    await ws.send(json.dumps(payload))

    if is_cooldown_action(decision.kind):
        # Optimistically mark canAct False until the server confirms via
        # action_result / can_act_changed — avoids double-sending before
        # the response arrives.
        session.can_act = False


async def run_one_game(rest: RestClient, entry_type: str) -> str:
    """Join (or resume) a single game and play it to completion.
    Returns the outcome string from play_session, or 'error'."""

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
            log.info("welcome frame: %s", json.dumps(welcome)[:400])

            if welcome.get("type") == "welcome":
                decision = welcome.get("decision")
                if decision == "BLOCKED":
                    log.error("account not ready to join (%s) — see readiness in /accounts/me", entry_type)
                    return "blocked"

            await send_hello(ws, entry_type)

            session = GameSession(entry_type=entry_type)
            outcome = await play_session(ws, session)
            return outcome

    except ConnectionClosed as e:
        waited = time.monotonic() - join_started_at
        log.warning(
            "websocket closed: code=%s reason=%s (after %.1fs since connect)",
            e.code, e.reason, waited,
        )
        if e.code == 1006 and waited < 120:
            log.info(
                "1006 while still in matchmaking queue — likely server-side "
                "idle/matchmaking timeout, not a bot bug. Will retry."
            )
        if e.code == 1013:
            log.info("RESUME_TARGET_DEAD — will re-dial for a fresh assignment")
            return "resume_dead"
        if e.code == 4032:
            log.info("agent already dead in that game — dropping it")
            return "died"
        return "closed"


async def choose_entry_type(rest: RestClient) -> Optional[str]:
    """Consult /accounts/me readiness + currentGames to decide what to do
    next, honoring the free/paid independent-slot rules from skill.md."""

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
        log.info("paid requested but not ready/live — falling back to free")
        return "free"

    if ENTRY_TYPE_PREFERENCE == "free":
        return "free"

    # auto: resume whichever is live; prefer paid for a fresh join if ready
    if paid_live:
        return "paid"
    if free_live:
        return "free"
    if readiness.get("paidReady"):
        return "paid"
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
            pass  # Windows dev fallback

    async with RestClient(API_KEY) as rest:
        await rest.fetch_version()
        log.info("using X-Version=%s", rest.version)

        try:
            me = await rest.get_me()
            readiness = me.get("readiness", {}) or {}
            log_info_block("Akun", {
                "nama": me.get("name"),
                "balance": f"{me.get('balance')} sMoltz",
                "wallet ok": readiness.get("walletAddress"),
                "whitelist": readiness.get("whitelistApproved"),
                "SC wallet": readiness.get("scWallet"),
                "identity": readiness.get("identity"),
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
                    log.info("nothing to do right now — idling")
                    await asyncio.sleep(STATE_POLL_INTERVAL)
                    continue

                await ensure_loadout(rest)

                outcome = await run_one_game(rest, entry_type)
                log.info("game outcome: %s", outcome)

                if outcome in ("died", "ended", "resume_dead"):
                    reconnect_delay = RECONNECT_MIN_DELAY
                    await asyncio.sleep(INTER_GAME_DELAY)
                elif outcome == "blocked":
                    await asyncio.sleep(STATE_POLL_INTERVAL)
                else:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)

            except ApiError as e:
                log.error("API error in main loop: %s", e)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            except (ConnectionClosed, OSError) as e:
                log.warning("connection issue: %s — backing off %.1fs", e, reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            except Exception:
                log.exception("unexpected error in main loop")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)

    log.info("bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main_loop())