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
        return await self.request("GET", "/accounts/me")

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
#
# Exact WS action payload field names are not pinned down in the material
# available to this bot, so `decide()` returns an abstract Decision and
# `send_action()` renders it defensively (tries the most standard shape;
# logs the raw server response either way so you can see exactly what the
# server accepted/rejected and tighten this mapping over time).

@dataclass
class Decision:
    kind: str  # "move" | "attack" | "explore" | "pickup" | "wait" | "flee"
    target_region_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_monster_id: Optional[str] = None
    ruin_id: Optional[str] = None
    reason: str = ""


def is_cooldown_action(kind: str) -> bool:
    # move / attack / explore consume the turn and enter a cooldown group;
    # pickup / equip / talk / whisper / broadcast are free per the docs.
    return kind in {"move", "attack", "explore"}


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

    # 2) Low HP -> disengage and retreat rather than fight, since survival
    #    time outranks kills in the current ranking rules.
    if hp_ratio < 0.35:
        threats = [a for a in visible_agents if not a.get("isGuardian")]
        if threats and connections:
            return Decision(
                kind="move",
                target_region_id=random.choice(connections),
                reason=f"low HP ({hp_ratio:.0%}) — retreating from contested region",
            )

    # 3) Healthy and a weak, isolated target is adjacent -> only then
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
    #    unless a specific integration adds tracking.
    if in_cave:
        return Decision(kind="wait", reason="in cave — awaiting explicit exit handling")

    # 6) A ruin is nearby and not yet at max alert -> explore it for
    #    relics/packs; back off once alertActive is a real risk.
    alert_active = self_state.get("alertActive", False)
    if visible_ruins and not alert_active and hp_ratio >= 0.5:
        ruin = next((r for r in visible_ruins if not r.get("isEmpty")), None)
        if ruin:
            return Decision(
                kind="explore",
                ruin_id=ruin.get("ruinId"),
                reason="exploring non-empty ruin while alert is safe",
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
    """Best-effort mapping from Decision -> the WS action message.
    Confirm/adjust field names against real `action_result` responses —
    the bot logs the full raw frame for exactly this purpose."""

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
            log.info("%s: %s", ftype, frame.get("message", ""))

        elif ftype == "assigned":
            session.game_id = frame.get("gameId")
            log.info("assigned to game %s", session.game_id)

        elif ftype in ("agent_view", "turn_advanced", "handover_sync"):
            view = frame.get("view", {})
            session.last_view = view
            turn = frame.get("turn")
            reason = frame.get("reason")
            log.info(
                "state update type=%s reason=%s turn=%s hp=%s canAct=%s",
                ftype, reason, turn,
                (view.get("self") or {}).get("hp"),
                session.can_act,
            )
            await maybe_act(ws, session, view)

        elif ftype == "action_result":
            success = frame.get("success", True)
            can_act = frame.get("canAct")
            if can_act is not None:
                session.can_act = can_act
            cooldown_ms = frame.get("cooldownRemainingMs")
            log.info(
                "action_result success=%s canAct=%s cooldownRemainingMs=%s error=%s",
                success, can_act, cooldown_ms, frame.get("error"),
            )

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


async def maybe_act(ws, session: GameSession, view: dict) -> None:
    if not view:
        return

    self_state = view.get("self", {}) or {}
    if self_state.get("isAlive") is False:
        return

    # Free actions (talk/whisper) go BEFORE the main action and never
    # consume the turn — placeholder hook, extend with real chat logic
    # if you want the agent to communicate.
    # await send_free_action(ws, {"type": "action", "action": "whisper", ...})

    if not session.can_act:
        log.debug("canAct is false — waiting for can_act_changed before acting")
        return

    decision = decide(view)
    payload = build_action_payload(decision)
    log.info("decision=%s reason=%r payload=%s", decision.kind, decision.reason, payload)

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
        log.warning("websocket closed: code=%s reason=%s", e.code, e.reason)
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
            log.info(
                "account name=%s balance=%s readiness=%s",
                me.get("name"), me.get("balance"), me.get("readiness"),
            )
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
