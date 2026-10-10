import asyncio
import os
import uuid
from functools import wraps
from pathlib import Path

from aiohttp import web, ClientSession, ClientWSTimeout, WSMsgType

from auth import (
    SESSION_COOKIE, create_session, authenticate, destroy_session, get_session,
    init_auth_db, list_users, create_user, set_enabled, delete_user, change_password,
    get_session_ttl, set_session_ttl, SESSION_TTL_MIN, SESSION_TTL_MAX,
    register_or_check_device, list_devices, set_device_status, delete_device,
)

ROOT = Path(__file__).resolve().parent / "public"
API_URL = "wss://developer.mig33.id/developer/ws"
SUBPROTOCOL = "mig33.developer.ws.v1"
PORT = int(os.environ.get("PORT", "3000"))

# Registry of upstream sockets currently used by the browser's WS1-WS10 slots.
ACTIVE_UPSTREAM = {}
# Per-login-session upstream registry. A browser session owns only its own WS1-WS10.
SESSION_UPSTREAM = {}
WS_SESSION = {}
SOCKET_LOCKS = {}
KICK_RATE_LOCKS = {}
KICK_RATE_STATE = {}
SESSION_BROWSERS = {}
BROWSER_SESSION = {}
BROWSER_CLIENTS = set()


def _current_user(request):
    return get_session(request.cookies.get(SESSION_COOKIE))


def require_auth(handler):
    @wraps(handler)
    async def wrapped(request):
        user = _current_user(request)
        if not user:
            if request.path.startswith("/api/") or request.path == "/ws":
                return web.json_response({"ok": False, "error": "Login diperlukan"}, status=401)
            raise web.HTTPFound("/login")
        request["user"] = user
        request["session_id"] = request.cookies.get(SESSION_COOKIE)
        return await handler(request)
    return wrapped

# Active KICK ALL jobs grouped by room. Each job tracks targets that have
# already been confirmed kicked by an upstream room event.
ACTIVE_KICK_JOBS = {}
ACTIVE_KICK_TASKS = {}
# Actual asyncio tasks, kept separately so LOGOUT ALL can cancel stale jobs.
ACTIVE_KICK_TASK_HANDLES = {}

def _norm_room_key(room):
    return str(room or "").strip().casefold()

def _event_payload_dict(payload):
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload

def _kick_event_info(payload):
    data = _event_payload_dict(payload)
    typ = str(payload.get("type", data.get("type", data.get("event_type", "")))).strip().lower()
    room = str(payload.get("room", data.get("room", ""))).strip()
    target = str(payload.get("target_username", data.get("target_username", data.get("username", data.get("target", ""))))).strip()
    status = str(payload.get("status_message", data.get("status_message", data.get("message", "")))).strip().lower()
    return typ, room, target, status

def _is_confirmed_kicked_event(payload):
    typ, room, target, status = _kick_event_info(payload)
    if not target:
        return False
    exact = {
        "room.participant.kicked", "room.member.kicked", "room.user.kicked",
        "participant.kicked", "member.kicked", "user.kicked",
    }
    if typ in exact:
        return True
    if "vote" in typ or typ in {"room.kick.result", "room.command.result"}:
        return False
    return any(x in status for x in (
        "has been kicked", "was kicked", "have been kicked", "kicked from the room"
    ))

async def mark_confirmed_kick(payload, session_id):
    if not _is_confirmed_kicked_event(payload):
        return None
    typ, room, target, status = _kick_event_info(payload)
    room_key = _norm_room_key(room)
    if not room_key or not target or not session_id:
        return None
    target_key = target.casefold()
    job_key = (session_id, room_key)
    jobs = ACTIVE_KICK_JOBS.get(job_key, [])
    newly_marked = False
    for state in list(jobs):
        if not isinstance(state, dict):
            continue
        lock = state.get("lock")
        if lock is None:
            lock = asyncio.Lock()
            state["lock"] = lock
        async with lock:
            events = state.get("events", {})
            if target_key not in events:
                continue
            kicked_targets = state.setdefault("kicked", set())
            if target_key not in kicked_targets:
                kicked_targets.add(target_key)
                events[target_key].set()
                newly_marked = True
                callback = state.get("on_kicked")
                if callback is not None:
                    task = asyncio.create_task(callback(target))
                    tasks = state.setdefault("replacement_tasks", set())
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
    if newly_marked:
        print(f"[KICK EVENT] session={session_id[:8]} confirmed kicked room={room!r} target={target!r} type={typ}", flush=True)
    return {"room": room, "target": target, "type": typ}


# Maximum configurable kick rate per upstream WebSocket.
# The UI exposes 1..200 kicks/second/WS; 100 preserves the previous default.
KICK_MAX_PER_SECOND = 200
KICK_LIMIT_LABEL = f"{KICK_MAX_PER_SECOND}/socket/second"
# Network/browser sends must never be allowed to stall a KICKALL worker forever.
KICK_SEND_TIMEOUT = float(os.environ.get("KICK_SEND_TIMEOUT", "5"))
BROWSER_EVENT_TIMEOUT = float(os.environ.get("BROWSER_EVENT_TIMEOUT", "2"))

DOWNLOAD_DIRS = [
    Path.home() / "storage" / "downloads",
    Path.home() / "storage" / "shared" / "Download",
    Path("/sdcard/Download"),
    Path.home() / "Downloads",
    Path(__file__).resolve().parent / "configs",
]



def safe_config_filename(name):
    name = str(name or "").strip()
    if name.lower().endswith(".json"):
        name = name[:-5]
    # Keep the filename simple and safe for Android Downloads.
    name = "".join(ch if ch.isalnum() or ch in "-_ ." else "_" for ch in name).strip(" .")
    if not name:
        raise ValueError("Nama file kosong")
    return name + ".json"

def find_download_dir():
    for d in DOWNLOAD_DIRS:
        try:
            if d.exists() and d.is_dir():
                return d
        except Exception:
            pass
    # If no platform-specific folder exists, use a local project folder.
    # This makes the same build work in GitHub Codespaces/Linux as well as Termux.
    d = Path(__file__).resolve().parent / "configs"
    try:
        d.mkdir(parents=True, exist_ok=True)
        return d
    except Exception:
        return None

def find_config_file(filename):
    for d in DOWNLOAD_DIRS:
        f = d / filename
        try:
            if f.is_file():
                return f
        except Exception:
            pass
    return None

async def save_config_file(request):
    try:
        data = await request.json()
        filename = safe_config_filename(data.get("name"))
        cfg = data.get("config")
        if not isinstance(cfg, dict):
            raise ValueError("Config tidak valid")
        d = find_download_dir()
        if d is None:
            return web.json_response({"ok": False, "error": "Folder konfigurasi tidak tersedia."}, status=500)
        path = d / filename
        import json
        path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return web.json_response({"ok": True, "filename": filename, "path": str(path)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)

async def load_config_file(request):
    try:
        filename = safe_config_filename(request.query.get("name", ""))
        path = find_config_file(filename)
        if path is None:
            return web.json_response({"ok": False, "error": f"File {filename} tidak ditemukan di Download."}, status=404)
        import json
        cfg = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(cfg, dict):
            raise ValueError("Isi file bukan config JSON")
        return web.json_response({"ok": True, "filename": filename, "config": cfg})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)


def register_socket(session_id, username, ws):
    if not session_id or not username:
        return
    key = username.strip().casefold()
    if not key:
        return
    SESSION_UPSTREAM.setdefault(session_id, {})[key] = ws
    WS_SESSION[id(ws)] = session_id
    # Keep a legacy global registry only for diagnostics/protection; kick jobs never use it.
    ACTIVE_UPSTREAM[(session_id, key)] = ws
    SOCKET_LOCKS.setdefault(id(ws), asyncio.Lock())
    KICK_RATE_LOCKS.setdefault(id(ws), asyncio.Lock())
    KICK_RATE_STATE.setdefault(id(ws), {"window_start": 0.0, "count": 0})

def unregister_socket(session_id, username, ws):
    if session_id:
        sockets = SESSION_UPSTREAM.get(session_id)
        if sockets is not None and username:
            key = username.strip().casefold()
            if sockets.get(key) is ws:
                sockets.pop(key, None)
            if not sockets:
                SESSION_UPSTREAM.pop(session_id, None)
        elif sockets is not None:
            for key, current in list(sockets.items()):
                if current is ws:
                    sockets.pop(key, None)
            if not sockets:
                SESSION_UPSTREAM.pop(session_id, None)
    for key, current in list(ACTIVE_UPSTREAM.items()):
        if current is ws:
            ACTIVE_UPSTREAM.pop(key, None)
    WS_SESSION.pop(id(ws), None)
    SOCKET_LOCKS.pop(id(ws), None)
    KICK_RATE_LOCKS.pop(id(ws), None)
    KICK_RATE_STATE.pop(id(ws), None)

def session_sockets(session_id):
    sockets = SESSION_UPSTREAM.get(session_id, {})
    result = []
    seen = set()
    for username, ws in list(sockets.items()):
        if ws is None or getattr(ws, "closed", False):
            continue
        if id(ws) in seen:
            continue
        seen.add(id(ws))
        result.append((username, ws))
    return result

def all_active_ws_usernames(exclude_session=None):
    names = set()
    for sid, sockets in list(SESSION_UPSTREAM.items()):
        if exclude_session is not None and sid == exclude_session:
            continue
        for name, ws in list(sockets.items()):
            if ws is not None and not getattr(ws, "closed", False):
                names.add(str(name).strip().casefold())
    return names


async def send_upstream(ws, payload):
    """Send one upstream payload without allowing a stalled socket to freeze KICKALL."""
    lock = SOCKET_LOCKS.setdefault(id(ws), asyncio.Lock())
    try:
        async with asyncio.timeout(KICK_SEND_TIMEOUT):
            async with lock:
                if ws.closed:
                    return False
                await ws.send_json(payload)
                return True
    except (asyncio.TimeoutError, ConnectionError, OSError):
        print(f"[KICK SEND TIMEOUT/ERROR] ws={id(ws)}", flush=True)
        return False

async def send_kick(ws, room, target, max_per_second=None):
    """Send one room.kick with a per-upstream-WebSocket rate limit.

    ``max_per_second`` is supplied by KICK ALL. The limiter remains shared
    per upstream WS, so concurrent dispatches cannot bypass the configured
    rate. Target pacing is handled by the Kick All batch dispatcher.
    """
    limit = KICK_MAX_PER_SECOND if max_per_second is None else min(
        KICK_MAX_PER_SECOND, max(1, int(max_per_second))
    )
    wsid = id(ws)
    rate_lock = KICK_RATE_LOCKS.setdefault(wsid, asyncio.Lock())
    state = KICK_RATE_STATE.setdefault(wsid, {"window_start": 0.0, "count": 0, "last_send": 0.0})

    while True:
        async with rate_lock:
            if ws.closed:
                return False
            now = asyncio.get_running_loop().time()
            if state["window_start"] <= 0 or now - state["window_start"] >= 1.0:
                state["window_start"] = now
                state["count"] = 0
            if state["count"] < limit:
                state["count"] += 1
                state["last_send"] = now
                break
            else:
                wait_for = max(0.001, 1.0 - (now - state["window_start"]))
        await asyncio.sleep(wait_for)

    return await send_upstream(ws, {
        "type": "room.kick",
        "room": room,
        "target_username": target,
    })

async def publish_browser_event(session_id, payload):
    """Queue browser UI updates without blocking the Kick engine.

    Browser/WebView delivery is best-effort: each send runs in its own task and
    has a short timeout. A slow or frozen browser therefore cannot hold up the
    Kick All dispatcher or its Delay Target/Delay Socket timing.
    """
    if not isinstance(payload, dict) or not session_id:
        return

    browsers = SESSION_BROWSERS.get(session_id, set())

    async def deliver(browser):
        if browser.closed:
            browsers.discard(browser)
            BROWSER_CLIENTS.discard(browser)
            BROWSER_SESSION.pop(id(browser), None)
            return
        try:
            await asyncio.wait_for(
                browser.send_json(payload),
                timeout=max(0.05, BROWSER_EVENT_TIMEOUT),
            )
        except Exception:
            # UI delivery is best-effort. Never propagate a browser/WebView
            # failure into the Kick engine.
            browsers.discard(browser)
            BROWSER_CLIENTS.discard(browser)
            BROWSER_SESSION.pop(id(browser), None)

    for browser in list(browsers):
        if browser.closed:
            browsers.discard(browser)
            BROWSER_CLIENTS.discard(browser)
            BROWSER_SESSION.pop(id(browser), None)
            continue
        asyncio.create_task(deliver(browser))



@require_auth
async def stop_kick_loop(request):
    """Stop the active KICK ALL job for this authenticated browser session."""
    session_id = request.get("session_id")
    room = str((await request.json()).get("room", "")).strip() if request.can_read_body else ""
    room_key = _norm_room_key(room) if room else None
    stopped = 0

    for job_key in list(ACTIVE_KICK_TASK_HANDLES.keys()):
        if job_key[0] != session_id:
            continue
        if room_key and job_key[1] != room_key:
            continue

        # Remove state first so late upstream kick events cannot schedule
        # replacement kicks after STOP has been pressed.
        states = ACTIVE_KICK_JOBS.pop(job_key, [])
        for state in states:
            state["stopped"] = True
            for replacement_task in list(state.get("replacement_tasks", set())):
                if replacement_task and not replacement_task.done():
                    replacement_task.cancel()

        handles = ACTIVE_KICK_TASK_HANDLES.pop(job_key, {})
        for job_id, task in list(handles.items()):
            if task and not task.done():
                task.cancel()
                stopped += 1

        ACTIVE_KICK_TASKS.pop(job_key, None)

    return web.json_response({"ok": True, "stopped": stopped})


@require_auth
async def kick_loop(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "JSON tidak valid"}, status=400)
    room = str(data.get("room", "")).strip()
    targets = [str(x).strip() for x in data.get("targets", []) if str(x).strip()][:10]
    try:
        brute_mode = min(10, max(1, int(data.get("bruteMode", len(targets) or 1))))
        targets = targets[:brute_mode]
        loops = max(1, int(data.get("loop", 1)))
        delay_socket = max(0, int(data.get("delaySocket", 10)))
        kick_limit_per_ws = min(
            KICK_MAX_PER_SECOND, max(1, int(data.get("kickLimitPerWs", KICK_MAX_PER_SECOND)))
        )
        delay_target = max(0, int(data.get("delayTarget", 10)))
    except Exception:
        return web.json_response({"ok": False, "error": "Parameter kick tidak valid"}, status=400)
    if not room or not targets:
        return web.json_response({"ok": False, "error": "Room dan TARGET wajib diisi"}, status=400)
    session_id = request.get("session_id")
    socket_entries = session_sockets(session_id)
    own_ws_names = {name.casefold() for name, _ws in socket_entries}
    targets = [t for t in targets if t.casefold() not in own_ws_names][:10]
    if not socket_entries:
        return web.json_response({"ok": False, "error": "Tidak ada WebSocket yang aktif"}, status=409)
    if not targets:
        return web.json_response({"ok": False, "error": "Semua target termasuk WebSocket milik session ini"}, status=400)

    room_key = _norm_room_key(room)
    job_key = (session_id, room_key)
    active = ACTIVE_KICK_JOBS.get(job_key, [])
    if active or job_key in ACTIVE_KICK_TASKS:
        active_ids = [x for x in ACTIVE_KICK_TASKS.get(job_key, [])] if isinstance(ACTIVE_KICK_TASKS.get(job_key), list) else []
        return web.json_response({"ok": False, "error": "Masih ada job aktif untuk room ini", "active": True, "jobIds": active_ids}, status=409)

    # The backend owns the job lifecycle. The HTTP request only starts the job;
    # progress and completion are delivered through the persistent browser WS.
    job_id = uuid.uuid4().hex[:12]
    ACTIVE_KICK_TASKS[job_key] = [job_id]
    task = asyncio.create_task(_run_kick_job(
        job_id, session_id, room, list(targets), socket_entries, loops,
        kick_limit_per_ws, delay_socket, delay_target
    ))
    ACTIVE_KICK_TASK_HANDLES.setdefault(job_key, {})[job_id] = task
    def _job_done(_task):
        current = ACTIVE_KICK_TASKS.get(job_key, [])
        if job_id in current:
            current.remove(job_id)
        if not current:
            ACTIVE_KICK_TASKS.pop(job_key, None)
        handles = ACTIVE_KICK_TASK_HANDLES.get(job_key, {})
        handles.pop(job_id, None)
        if not handles:
            ACTIVE_KICK_TASK_HANDLES.pop(job_key, None)
        # A failed worker must not leave a stale job in this user's room.
        active_states = ACTIVE_KICK_JOBS.get(job_key, [])
        if active_states:
            ACTIVE_KICK_JOBS[job_key] = [state for state in active_states if state.get("jobId") != job_id]
            if not ACTIVE_KICK_JOBS[job_key]:
                ACTIVE_KICK_JOBS.pop(job_key, None)
    task.add_done_callback(_job_done)
    return web.json_response({
        "ok": True, "accepted": True, "jobId": job_id,
        "websockets": len(socket_entries), "targets": len(targets),
        "loop": loops,
        "bruteMode": brute_mode, "kickLimitPerWs": kick_limit_per_ws,
        "delaySocket": delay_socket, "delayTarget": delay_target,
        "transport": "browser-ws-events"
    }, status=202)

async def _run_kick_job(job_id, session_id, room, targets, socket_entries, loops, kick_limit_per_ws, delay_socket, delay_target):
    # Keep worker failures observable by the frontend instead of leaving a
    # permanent RUNNING state. Cancellation is intentionally silent because
    # LOGOUT/RESET explicitly cancels the worker.
    try:
        return await _run_kick_job_impl(
            job_id, session_id, room, targets, socket_entries, loops,
            kick_limit_per_ws, delay_socket, delay_target
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[KICK JOB ERROR] job={job_id} session={session_id[:8]} room={room!r}: {exc!r}", flush=True)
        raise

async def _run_kick_job_impl(job_id, session_id, room, targets, socket_entries, loops, kick_limit_per_ws, delay_socket, delay_target):
    """Run Kick All with direct parallel fan-out and no per-socket queue.

    Each target is dispatched directly to every active upstream socket with
    asyncio.gather(). The existing per-WS rate limiter and per-socket send
    lock remain the transport safeguards. Repeats a fixed number of complete
    target-list passes as configured by the Loop textbox.
    Confirmed room-kick events update shared target state and may trigger an
    immediate replacement fan-out.
    """
    room_key = _norm_room_key(room)
    replacement_state = {
        "jobId": job_id,
        "sessionId": session_id,
        "targets": list(targets),
        "kicked": set(),
        "claimed": set(),
        "events": {t.casefold(): asyncio.Event() for t in targets},
        "replacement_tasks": set(),
        "replacement_dispatched": 0,
        "stopped": False,
        "lock": asyncio.Lock(),
    }

    def target_is_kicked(target):
        key = str(target or "").strip().casefold()
        return bool(key) and key in replacement_state["kicked"]

    async def send_direct(ws_name, ws, target, is_replacement=False):
        target_key = str(target or "").strip().casefold()
        if not target_key or ws is None or ws.closed:
            return False
        async with replacement_state["lock"]:
            if replacement_state.get("stopped") or target_key in replacement_state["kicked"]:
                return False
        try:
            result = await send_kick(ws, room, target, kick_limit_per_ws)
            return result is not False
        except Exception as exc:
            print(f"[KICK DIRECT ERROR] job={job_id} ws={ws_name} target={target!r}: {exc!r}", flush=True)
            return False

    async def fanout_target(target, is_replacement=False):
        """Dispatch WS in a flat cadence controlled by the Delay Socket textbox.

        The configured value is the gap between consecutive WS starts, not a
        cumulative delay value. For example, 10ms means WS1 starts, then WS2
        about 10ms later, WS3 about 10ms after WS2, and so on. Deadlines are
        anchored to a monotonic clock so task-creation overhead does not add a
        fresh delay on top of every interval.
        """
        if replacement_state.get("stopped") or target_is_kicked(target):
            return 0
        active = [(name, ws) for name, ws in socket_entries if not ws.closed]
        if not active:
            return 0

        # Delay Socket is a per-transition pause: launch one WS action, then
        # wait the configured duration before launching the next WS action.
        # Do not calculate deadlines from a common start time or accumulate
        # a separate delay on top of the configured interval.
        interval = max(0, delay_socket) / 1000.0
        tasks = []
        for index, (name, ws) in enumerate(active):
            if replacement_state.get("stopped"):
                break
            tasks.append(asyncio.create_task(
                send_direct(name, ws, target, is_replacement=is_replacement)
            ))
            if index < len(active) - 1 and interval:
                await asyncio.sleep(interval)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for result in results if result is True)

    async def dispatch_replacement(kicked_target):
        """Directly fan out a replacement target to all active sockets."""
        if replacement_state.get("stopped"):
            return
        async with replacement_state["lock"]:
            kicked_key = str(kicked_target).strip().casefold()
            candidates = [
                t for t in replacement_state["targets"]
                if t.casefold() not in replacement_state["kicked"]
                and t.casefold() != kicked_key
                and t.casefold() not in replacement_state["claimed"]
            ]
            if not candidates:
                return
            replacement = candidates[0]
            replacement_state["claimed"].add(replacement.casefold())
            replacement_state["replacement_dispatched"] += 1
        await fanout_target(replacement, is_replacement=True)

    replacement_state["on_kicked"] = dispatch_replacement
    ACTIVE_KICK_JOBS.setdefault((session_id, room_key), []).append(replacement_state)

    loop_no = 0
    try:
        while True:
            if loop_no >= loops:
                break

            active_sockets = [(name, ws) for name, ws in socket_entries if not ws.closed]
            if not active_sockets:
                print(f"[KICK JOB] no active sockets job={job_id}", flush=True)
                break

            loop_no += 1
            await publish_browser_event(session_id, {
                "type": "kick.batch.started",
                "jobId": job_id,
                "room": room,
                "loop": loop_no,
                "targets": len(targets),
                "sockets": len(active_sockets),
                "mode": "parallel-fanout-direct",
            })

            # Targets are grouped for status reporting only. Delay Target is
            # applied between every consecutive target, including group edges.
            for batch_start in range(0, len(targets), 2):
                if replacement_state.get("stopped"):
                    break
                batch_targets = [
                    target for target in targets[batch_start:batch_start + 2]
                    if not target_is_kicked(target)
                ]
                if not batch_targets:
                    continue

                await publish_browser_event(session_id, {
                    "type": "kick.target.batch.started",
                    "jobId": job_id,
                    "room": room,
                    "loop": loop_no,
                    "batchSize": len(batch_targets),
                    "batchTargets": batch_targets,
                    "sockets": len(active_sockets),
                })

                for target_index, target in enumerate(batch_targets):
                    await publish_browser_event(session_id, {
                        "type": "kick.target.fanout.started",
                        "jobId": job_id,
                        "room": room,
                        "target_username": target,
                        "loop": loop_no,
                        "batchIndex": target_index,
                        "batchSize": len(batch_targets),
                        "sockets": len(active_sockets),
                    })

                    result = await fanout_target(target)
                    sent = result if isinstance(result, int) else 0
                    await publish_browser_event(session_id, {
                        "type": "kick.target.fanout.completed",
                        "jobId": job_id,
                        "room": room,
                        "target_username": target,
                        "loop": loop_no,
                        "batchIndex": target_index,
                        "batchSize": len(batch_targets),
                        "sockets": len(active_sockets),
                        "sent": sent,
                    })

                    if replacement_state.get("stopped"):
                        break
                    has_more_targets = (
                        target_index + 1 < len(batch_targets)
                        or batch_start + 2 < len(targets)
                    )
                    if has_more_targets and delay_target and not replacement_state.get("stopped"):
                        await asyncio.sleep(delay_target / 1000.0)

                await publish_browser_event(session_id, {
                    "type": "kick.target.batch.completed",
                    "jobId": job_id,
                    "room": room,
                    "loop": loop_no,
                    "batchSize": len(batch_targets),
                })
            await publish_browser_event(session_id, {
                "type": "kick.batch.completed",
                "jobId": job_id,
                "room": room,
                "loop": loop_no,
            })

            if replacement_state.get("stopped"):
                break
            if loop_no >= loops:
                break

        while replacement_state.get("replacement_tasks"):
            pending = list(replacement_state["replacement_tasks"])
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await publish_browser_event(session_id, {
            "type": "kick.job.finished",
            "jobId": job_id,
            "room": room,
            "stopped": bool(replacement_state.get("stopped")),
            "loopsCompleted": loop_no,
            "mode": "parallel-fanout-direct",
        })
        replacement_state["stopped"] = True
        job_key = (session_id, room_key)
        active_jobs = ACTIVE_KICK_JOBS.get(job_key, [])
        if replacement_state in active_jobs:
            active_jobs.remove(replacement_state)
        if not active_jobs:
            ACTIVE_KICK_JOBS.pop(job_key, None)

@require_auth
async def reset_troop_session(request):
    """Clear troop-side jobs/state without logging the browser application out."""
    session_id = request.get("session_id")
    cancelled = 0
    for job_key in list(ACTIVE_KICK_JOBS.keys()):
        if job_key[0] == session_id:
            for state in ACTIVE_KICK_JOBS.get(job_key, []):
                state["stopped"] = True
                for replacement_task in list(state.get("replacement_tasks", set())):
                    if replacement_task and not replacement_task.done():
                        replacement_task.cancel()
            ACTIVE_KICK_JOBS.pop(job_key, None)
    for job_key in list(ACTIVE_KICK_TASK_HANDLES.keys()):
        if job_key[0] != session_id:
            continue
        handles = ACTIVE_KICK_TASK_HANDLES.pop(job_key, {})
        for task in list(handles.values()):
            if task and not task.done():
                task.cancel()
                cancelled += 1
        ACTIVE_KICK_TASKS.pop(job_key, None)
    # Close every upstream belonging to this troop session so the next LOGIN ALL
    # always starts with fresh developer sessions.
    sockets = list((SESSION_UPSTREAM.get(session_id) or {}).values())
    for ws in sockets:
        try:
            if ws is not None and not ws.closed:
                await ws.close(code=1000, message=b"troop reset")
        except Exception:
            pass
    SESSION_UPSTREAM.pop(session_id, None)
    return web.json_response({"ok": True, "cancelledJobs": cancelled, "closedSockets": len(sockets), "session": "troop-reset"})


def require_admin(handler):
    @wraps(handler)
    async def wrapped(request):
        user = _current_user(request)
        if not user:
            if request.path.startswith("/api/"):
                return web.json_response({"ok": False, "error": "Login diperlukan"}, status=401)
            raise web.HTTPFound("/login")
        if user.get("role") != "admin":
            if request.path.startswith("/api/"):
                return web.json_response({"ok": False, "error": "Akses admin diperlukan"}, status=403)
            raise web.HTTPForbidden(text="Akses admin diperlukan")
        request["user"] = user
        request["session_id"] = request.cookies.get(SESSION_COOKIE)
        return await handler(request)
    return wrapped


async def login_page(request):
    return web.FileResponse(ROOT / "login.html")


async def app_page(request):
    return web.FileResponse(ROOT / "index.html")


async def admin_page(request):
    return web.FileResponse(ROOT / "admin.html")


async def auth_login(request):
    try:
        data = await request.json()
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", ""))
    except Exception:
        return web.json_response({"ok": False, "error": "JSON tidak valid"}, status=400)
    if not username or not password:
        return web.json_response({"ok": False, "error": "Username dan password wajib diisi"}, status=400)
    user = authenticate(username, password)
    if not user:
        return web.json_response({"ok": False, "error": "Username atau password salah"}, status=401)
    device_id = None
    if user.get("role") != "admin":
        device_id = str(data.get("deviceId", ""))
        device_secret = str(data.get("deviceSecret", ""))
        device_label = str(data.get("deviceLabel", "Perangkat"))
        try:
            device_status = register_or_check_device(
                user["username"], device_id, device_secret, device_label,
                request.headers.get("User-Agent", ""),
            )
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        if device_status == "pending":
            return web.json_response({
                "ok": False, "devicePending": True,
                "error": "Perangkat menunggu persetujuan admin. Silakan coba login kembali setelah disetujui.",
            }, status=403)
        if device_status == "rejected":
            return web.json_response({"ok": False, "deviceRejected": True, "error": "Perangkat ditolak admin."}, status=403)
        if device_status == "revoked":
            return web.json_response({"ok": False, "deviceRevoked": True, "error": "Akses perangkat telah dicabut admin."}, status=403)
        if device_status != "approved":
            return web.json_response({"ok": False, "error": "Kredensial perangkat tidak valid. Hapus data perangkat lokal lalu ajukan pendaftaran baru."}, status=403)
    token = create_session(user, device_id=device_id)
    response = web.json_response({"ok": True, "user": user})
    response.set_cookie(
        SESSION_COOKIE, token, max_age=get_session_ttl(), httponly=True,
        samesite="Lax", secure=request.scheme == "https", path="/"
    )
    return response


async def auth_logout(request):
    destroy_session(request.cookies.get(SESSION_COOKIE))
    response = web.json_response({"ok": True})
    response.del_cookie(SESSION_COOKIE, path="/")
    return response


async def auth_me(request):
    user = _current_user(request)
    if not user:
        return web.json_response({"ok": False, "authenticated": False}, status=401)
    return web.json_response({"ok": True, "authenticated": True, "user": user})


@require_admin
async def admin_change_password(request):
    try:
        data = await request.json()
        change_password(request["user"]["username"], str(data.get("password", "")))
        return web.json_response({"ok": True})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)


@require_admin
async def admin_users(request):
    return web.json_response({"ok": True, "users": list_users()})


@require_admin
async def admin_session_settings(request):
    if request.method == "GET":
        ttl = get_session_ttl()
        return web.json_response({
            "ok": True,
            "seconds": ttl,
            "minutes": ttl // 60,
            "minSeconds": SESSION_TTL_MIN,
            "maxSeconds": SESSION_TTL_MAX,
        })
    try:
        data = await request.json()
        seconds = int(data.get("seconds"))
        ttl = set_session_ttl(seconds)
        return web.json_response({"ok": True, "seconds": ttl, "minutes": ttl // 60})
    except (TypeError, ValueError):
        return web.json_response({"ok": False, "error": "Durasi session tidak valid"}, status=400)


@require_admin
async def admin_create_user(request):
    try:
        data = await request.json()
        create_user(str(data.get("username", "")), str(data.get("password", "")), "user")
        return web.json_response({"ok": True})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)
    except Exception:
        return web.json_response({"ok": False, "error": "Data user tidak valid"}, status=400)


@require_admin
async def admin_set_user_enabled(request):
    username = request.match_info.get("username", "")
    try:
        data = await request.json()
        set_enabled(username, bool(data.get("enabled")))
        return web.json_response({"ok": True})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)


@require_admin
async def admin_delete_user(request):
    username = request.match_info.get("username", "")
    try:
        delete_user(username)
        return web.json_response({"ok": True})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)

@require_admin
async def admin_devices(request):
    status = str(request.query.get("status", "")).strip().lower()
    return web.json_response({"ok": True, "devices": list_devices(status)})


@require_admin
async def admin_delete_device(request):
    device_id = request.match_info.get("device_id", "")
    try:
        result = delete_device(device_id)
        return web.json_response({"ok": True, "device": result})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=404)


@require_admin
async def admin_device_action(request):
    device_id = request.match_info.get("device_id", "")
    action = request.match_info.get("action", "")
    status_map = {"approve": "approved", "reject": "rejected", "revoke": "revoked"}
    status = status_map.get(action)
    if not status:
        return web.json_response({"ok": False, "error": "Aksi perangkat tidak valid"}, status=400)
    try:
        result = set_device_status(device_id, status, request["user"]["username"])
        return web.json_response({"ok": True, "device": result})
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=404)


async def index(request):
    if _current_user(request):
        raise web.HTTPFound("/app")
    raise web.HTTPFound("/login")

@require_auth
async def proxy(request):
    browser = web.WebSocketResponse(heartbeat=20)
    await browser.prepare(request)
    session_id = request.get("session_id") or request.cookies.get(SESSION_COOKIE)
    BROWSER_CLIENTS.add(browser)
    BROWSER_SESSION[id(browser)] = session_id
    SESSION_BROWSERS.setdefault(session_id, set()).add(browser)
    upstream = None
    session = None
    relay_task = None
    try:
        session = ClientSession()
        upstream_username = None
        # No receive timeout: the API is a long-lived WebSocket and has its own JSON ping rule.
        ws_timeout = ClientWSTimeout(ws_close=10, ws_receive=None)
        last_error = None
        for attempt in range(1, 3):
            try:
                upstream = await session.ws_connect(
                    API_URL,
                    protocols=(SUBPROTOCOL,),
                    heartbeat=20,
                    autoping=True,
                    timeout=ws_timeout,
                )
                print(f"UPSTREAM CONNECTED attempt={attempt} protocol={upstream.protocol!r} status={getattr(upstream, "_response", None).status if getattr(upstream, "_response", None) else "?"}", flush=True)
                break
            except Exception as e:
                last_error = e
                print(f"UPSTREAM CONNECT FAILED attempt={attempt}: {type(e).__name__}: {e!r}", flush=True)
                if attempt < 2:
                    await asyncio.sleep(0.5)
        if upstream is None:
            detail = f"{type(last_error).__name__}: {last_error}" if last_error else "unknown error"
            payload = {"type":"proxy.error","data":{"stage":"upstream_connect","message":detail[:500]}}
            if not browser.closed:
                await browser.send_json(payload)
                await browser.close(code=1011, message=b"upstream connection failed")
            return browser

        async def upstream_to_browser():
            async for msg in upstream:
                if msg.type == WSMsgType.TEXT:
                    try:
                        payload = __import__("json").loads(msg.data)
                        confirmed = await mark_confirmed_kick(payload, session_id)
                        if confirmed:
                            await publish_browser_event(session_id, {
                                "type": "kick.target.confirmed",
                                "room": confirmed["room"],
                                "target_username": confirmed["target"],
                                "source_type": confirmed["type"],
                            })
                    except Exception:
                        pass
                    await browser.send_str(msg.data)
                elif msg.type == WSMsgType.BINARY:
                    await browser.send_bytes(msg.data)
                elif msg.type == WSMsgType.PING:
                    await upstream.pong()
                elif msg.type == WSMsgType.PONG:
                    continue
                elif msg.type == WSMsgType.CLOSE:
                    print(f"UPSTREAM CLOSE code={upstream.close_code}", flush=True)
                    break
                elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                    print(f"UPSTREAM END type={msg.type} exception={upstream.exception()!r}", flush=True)
                    break

        relay_task = asyncio.create_task(upstream_to_browser())

        async for msg in browser:
            if msg.type == WSMsgType.TEXT:
                if upstream.closed:
                    await browser.send_json({"type":"proxy.error","data":{"stage":"upstream_send","message":"upstream socket is closed"}})
                    break
                try:
                    payload = __import__("json").loads(msg.data)
                    if payload.get("type") == "developer.login":
                        upstream_username = str(payload.get("username", "")).strip() or None
                        register_socket(session_id, upstream_username, upstream)
                except Exception:
                    pass
                await upstream.send_str(msg.data)
            elif msg.type == WSMsgType.BINARY:
                if upstream.closed:
                    break
                await upstream.send_bytes(msg.data)
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                break

    except Exception as e:
        print(f"WebSocket proxy error: {type(e).__name__}: {e!r}", flush=True)
        if not browser.closed:
            try:
                await browser.send_json({"type":"proxy.error","data":{"stage":"proxy","message":f"{type(e).__name__}: {e}"[:500]}})
            except Exception:
                pass
            await browser.close(code=1011, message=b"proxy error")
    finally:
        BROWSER_CLIENTS.discard(browser)
        browsers = SESSION_BROWSERS.get(session_id, set())
        browsers.discard(browser)
        if not browsers and session_id:
            SESSION_BROWSERS.pop(session_id, None)
        BROWSER_SESSION.pop(id(browser), None)
        if relay_task is not None:
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)
        if upstream is not None:
            unregister_socket(session_id, upstream_username, upstream)
            if not upstream.closed:
                await upstream.close()
        if session is not None:
            await session.close()
    return browser

async def health(request):
    return web.json_response({
        "ok": True,
        "service": "migsock",
        "port": PORT,
        "upstream": API_URL,
        "ws_path": "/ws",
    })

init_auth_db()

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/login", login_page)
app.router.add_get("/app", require_auth(app_page))
app.router.add_get("/admin", require_admin(admin_page))
app.router.add_post("/api/auth/login", auth_login)
app.router.add_post("/api/auth/logout", require_auth(auth_logout))
app.router.add_get("/api/auth/me", auth_me)
app.router.add_get("/api/admin/users", admin_users)
app.router.add_get("/api/admin/session-settings", admin_session_settings)
app.router.add_post("/api/admin/session-settings", admin_session_settings)
app.router.add_post("/api/admin/password", admin_change_password)
app.router.add_post("/api/admin/users", admin_create_user)
app.router.add_post("/api/admin/users/{username}/enabled", admin_set_user_enabled)
app.router.add_get("/api/admin/devices", admin_devices)
app.router.add_delete("/api/admin/devices/{device_id}", admin_delete_device)
app.router.add_post("/api/admin/devices/{device_id}/{action}", admin_device_action)
app.router.add_delete("/api/admin/users/{username}", admin_delete_user)
app.router.add_get("/health", health)
app.router.add_get("/ws", proxy)
app.router.add_post("/api/kick-loop", kick_loop)
app.router.add_post("/api/kick-stop", stop_kick_loop)
app.router.add_post("/api/troop/reset", reset_troop_session)
app.router.add_static("/static/", ROOT)

if __name__ == "__main__":
    print(f"migsock listening on 0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT)
