from flask import Flask, render_template, jsonify, request, redirect, url_for, make_response
from ws_manager import GlobalCoordinator
import time
import threading
import gc
import os
import json
import secrets

app = Flask(__name__)
app.secret_key = "dualhedge_secret_session_encryption_key_99"

# Local Storage for allowed IDs
ALLOWED_IDS_FILE = os.path.join(os.path.dirname(__file__), "allowed_ids.json")

def load_allowed_ids():
    try:
        with open(ALLOWED_IDS_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {
            "access_ids": {
                "DH-KEY-ADMIN1": {"label": "Master Admin Key", "status": "active", "created_at": 1721112345}
            },
            "admin_secret": "dualhedge_secret_99"
        }

def save_allowed_ids(data):
    try:
        with open(ALLOWED_IDS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving allowed_ids.json: {e}")

# In-memory dictionary to track live user sessions
# Format: { session_token: { access_id, label, ip, user_agent, last_active } }
active_access_sessions = {}

@app.before_request
def check_access_control():
    return None

@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r

# Access Gate GET / POST Routes
@app.route('/access', methods=['GET'])
def access_gate():
    return render_template('access.html')

@app.route('/api/access/login', methods=['POST'])
def access_login():
    data = request.json or {}
    access_id = data.get('access_id', '').strip()
    
    allowed_data = load_allowed_ids()
    id_info = allowed_data.get('access_ids', {}).get(access_id)
    
    if id_info and id_info.get('status') == 'active':
        # 🚫 SINGLE SESSION RULE: Terminate any previous session using this same Access ID
        stale_tokens = [tok for tok, sess in active_access_sessions.items() if sess['access_id'] == access_id]
        for tok in stale_tokens:
            active_access_sessions.pop(tok, None)
            
        session_token = secrets.token_hex(24)
        active_access_sessions[session_token] = {
            'access_id': access_id,
            'label': id_info.get('label', 'Unknown'),
            'ip': request.headers.get('X-Forwarded-For', request.remote_addr),
            'user_agent': request.headers.get('User-Agent', 'Unknown'),
            'last_active': time.time()
        }
        
        resp = make_response(jsonify({"success": True, "message": "Access granted"}))
        resp.set_cookie('dh_session_token', session_token, max_age=30*86400, httponly=True, samesite='Lax')
        return resp
        
    return jsonify({"success": False, "message": "Invalid or locked Access ID"}), 401

# Admin Control Panel Verification Helpers & Routes
def verify_admin_secret():
    secret_param = request.args.get('secret')
    allowed_data = load_allowed_ids()
    return secret_param == allowed_data.get('admin_secret', 'dualhedge_secret_99')

@app.route('/admin', methods=['GET'])
def admin_panel():
    if not verify_admin_secret():
        return "Not Found", 404
    return render_template('admin.html')

@app.route('/api/admin/active_sessions', methods=['GET'])
def admin_active_sessions():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    sessions_list = []
    for token, data in active_access_sessions.items():
        rounds = 0
        profit = 0.0
        status = "Inactive"
        balance_info = "P: 0.00 / B: 0.00"
        
        # Look up corresponding coordinator in active_sessions
        x_sess_id = data.get('x_session_id')
        if x_sess_id and x_sess_id in active_sessions:
            coord = active_sessions[x_sess_id]['coordinator']
            if coord:
                rounds = len(coord.bet_history)
                # Sum profit from all rounds
                profit = sum(
                    r.get('total_profit', 0) for r in coord.bet_history 
                    if r.get('total_profit') is not None
                )
                status = "Armed" if coord.auto_bet_requested else "Not Armed"
                if coord.bet_state == "placing":
                    status = "PLACING BETS"
                
                balance_info = f"P: {coord.account1.balance:.2f} / B: {coord.account2.balance:.2f}"
                
        sessions_list.append({
            'session_token': token,
            'access_id': data['access_id'],
            'label': data['label'],
            'ip': data['ip'],
            'user_agent': data['user_agent'],
            'last_active': data['last_active'],
            'rounds': rounds,
            'profit': profit,
            'status': status,
            'balances': balance_info
        })
    return jsonify({"success": True, "sessions": sessions_list})

@app.route('/api/admin/keys', methods=['GET'])
def admin_get_keys():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    allowed_data = load_allowed_ids()
    return jsonify({"success": True, "keys": allowed_data.get('access_ids', {})})

@app.route('/api/admin/keys/add', methods=['POST'])
def admin_add_key():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    data = request.json or {}
    key = data.get('key', '').strip()
    label = data.get('label', '').strip()
    if not key or not label:
        return jsonify({"success": False, "message": "Missing key or label"}), 400
        
    allowed_data = load_allowed_ids()
    if key in allowed_data['access_ids']:
        return jsonify({"success": False, "message": "Key already exists"}), 400
        
    allowed_data['access_ids'][key] = {
        "label": label,
        "status": "active",
        "created_at": int(time.time())
    }
    save_allowed_ids(allowed_data)
    return jsonify({"success": True, "message": "Key added successfully"})

@app.route('/api/admin/keys/status', methods=['POST'])
def admin_key_status():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    data = request.json or {}
    key = data.get('key', '').strip()
    status = data.get('status', '').strip()
    if not key or status not in ('active', 'locked'):
        return jsonify({"success": False, "message": "Invalid parameters"}), 400
        
    allowed_data = load_allowed_ids()
    if key not in allowed_data['access_ids']:
        return jsonify({"success": False, "message": "Key not found"}), 404
        
    allowed_data['access_ids'][key]['status'] = status
    save_allowed_ids(allowed_data)
    return jsonify({"success": True, "message": f"Key status updated to {status}"})

@app.route('/api/admin/keys/delete', methods=['POST'])
def admin_delete_key():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    data = request.json or {}
    key = data.get('key', '').strip()
    
    allowed_data = load_allowed_ids()
    if key not in allowed_data['access_ids']:
        return jsonify({"success": False, "message": "Key not found"}), 404
        
    allowed_data['access_ids'].pop(key)
    save_allowed_ids(allowed_data)
    
    tokens_to_remove = [tok for tok, sess in active_access_sessions.items() if sess['access_id'] == key]
    for tok in tokens_to_remove:
        active_access_sessions.pop(tok, None)
        
    return jsonify({"success": True, "message": "Key deleted successfully"})

@app.route('/api/admin/sessions/terminate', methods=['POST'])
def admin_terminate_session():
    if not verify_admin_secret():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    data = request.json or {}
    token = data.get('session_token', '').strip()
    
    if token in active_access_sessions:
        active_access_sessions.pop(token)
        return jsonify({"success": True, "message": "Session terminated"})
    return jsonify({"success": False, "message": "Session not found"}), 404


# Dictionary to store per-session GlobalCoordinator instances
# Format: { 'session_id': { 'coordinator': GlobalCoordinator, 'last_active': timestamp } }
active_sessions = {}

def get_coordinator():
    session_id = request.headers.get('X-Session-ID')
    if not session_id:
        return None
        
    if session_id not in active_sessions:
        active_sessions[session_id] = {
            'coordinator': GlobalCoordinator(),
            'last_active': time.time()
        }
    else:
        active_sessions[session_id]['last_active'] = time.time()
        
    return active_sessions[session_id]['coordinator']

def get_ram_usage():
    try:
        # Read from /proc/meminfo on Linux (Render)
        with open('/proc/meminfo', 'r') as f:
            lines = f.readlines()
        mem_total = 0
        mem_free = 0
        mem_available = 0
        for line in lines:
            if "MemTotal" in line:
                mem_total = int(line.split()[1])
            elif "MemAvailable" in line:
                mem_available = int(line.split()[1])
            elif "MemFree" in line:
                mem_free = int(line.split()[1])
        if mem_total > 0:
            avail = mem_available if mem_available > 0 else mem_free
            used = mem_total - avail
            percent = (used / mem_total) * 100.0
            return percent
    except Exception:
        pass
    try:
        import psutil
        return psutil.virtual_memory().percent
    except Exception:
        return 12.5  # Safe default fallback

def cleanup_stale_sessions():
    while True:
        time.sleep(600)  # Check every 10 minutes
        current_time = time.time()
        stale_keys = []
        for sid, data in active_sessions.items():
            # If a session has been inactive for 2 hours (7200 seconds), remove it
            if current_time - data['last_active'] > 7200:
                stale_keys.append(sid)
                
        for sid in stale_keys:
            # Stop the coordinators nicely if they are running
            coord = active_sessions[sid]['coordinator']
            coord.running = False
            if coord.account1: coord.account1.running = False
            if coord.account2: coord.account2.running = False
            del active_sessions[sid]

        # Force garbage collection to reclaim cyclic refs from dead WS/thread objects
        gc.collect()

threading.Thread(target=cleanup_stale_sessions, daemon=True).start()

@app.route('/api/debug_active_sessions', methods=['GET'])
def debug_active_sessions():
    out = {}
    for sid, data in active_sessions.items():
        coord = data['coordinator']
        out[sid] = {
            "last_active": data['last_active'],
            "acc1_tables": list(coord.account1.tables.keys()),
            "acc2_tables": list(coord.account2.tables.keys()),
            "acc1_login_status": coord.account1.login_status,
            "acc2_login_status": coord.account2.login_status,
            "acc1_running": coord.account1.running,
            "acc2_running": coord.account2.running
        }
    return jsonify(out)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status', methods=['GET'])
def status():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
        
    # Calculate health metrics
    ram_usage = get_ram_usage()
    
    # ── Snapshot both tables dicts ONCE — prevents RuntimeError if asyncio
    # adds/removes a table while this thread is iterating (Bug 2 fix)
    tables1_snap = dict(coordinator.account1.tables)
    tables2_snap = dict(coordinator.account2.tables)

    latencies = []
    for t, info in tables1_snap.items():
        if info.get("status") in ["Connected", "Betting Open"] and info.get("latency", -1) > 0:
            latencies.append(info["latency"])
    for t, info in tables2_snap.items():
        if info.get("status") in ["Connected", "Betting Open"] and info.get("latency", -1) > 0:
            latencies.append(info["latency"])
            
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    max_latency = max(latencies) if latencies else 0.0
    
    # Time remaining calculation
    time_remaining = 0.0
    betting_open_active = False
    for t, info in tables1_snap.items():
        if info.get("is_betting_open"):
            opened_at = info.get("betting_opened_at", 0)
            if opened_at > 0:
                betting_open_active = True
                rem = max(0.0, 15.0 - (time.time() - opened_at))
                if time_remaining == 0.0 or rem < time_remaining:
                    time_remaining = rem
                    
    # Compute signals — mirrors EXACTLY what _check_auto_bet_locked checks
    is_p1_logged = "logged in" in coordinator.account1.login_status.lower() or "connected" in coordinator.account1.login_status.lower()
    is_p2_logged = "logged in" in coordinator.account2.login_status.lower() or "connected" in coordinator.account2.login_status.lower()
    
    ready_tables = 0
    total_acc1_tables = len(tables1_snap)
    total_acc2_tables = len(tables2_snap)
    tables_with_ws1 = 0
    tables_with_ws2 = 0
    
    for token, info in tables1_snap.items():
        ws1 = info.get('ws')
        if ws1 and getattr(ws1, 'open', True):
            tables_with_ws1 += 1
            if token in tables2_snap:
                info2 = tables2_snap[token]
                ws2 = info2.get('ws')
                if ws2 and getattr(ws2, 'open', True):
                    lat1 = info.get('latency', -1)
                    lat2 = info2.get('latency', -1)
                    if (lat1 == -1 or lat1 <= 800) and (lat2 == -1 or lat2 <= 800):
                        ready_tables += 1
    
    for token, info in tables2_snap.items():
        ws2 = info.get('ws')
        if ws2 and getattr(ws2, 'open', True):
            tables_with_ws2 += 1
    
    is_armed = coordinator.auto_bet_requested
    connection_ok = is_p1_logged and is_p2_logged
    tables_ok = ready_tables >= 2
    network_ok = max_latency < 350.0 if latencies else (True if connection_ok else False)
    ram_ok = ram_usage < 85.0
    
    if connection_ok and tables_ok and network_ok and ram_ok and is_armed:
        signal = "green"
    elif connection_ok and tables_ok and network_ok and ram_ok and not is_armed:
        signal = "yellow"
    else:
        signal = "red"
    
    reasons = []
    if not is_p1_logged:
        reasons.append("Acc1 down")
    if not is_p2_logged:
        reasons.append("Acc2 down")
    if total_acc1_tables == 0:
        reasons.append("No tables (Acc1)")
    if total_acc2_tables == 0:
        reasons.append("No tables (Acc2)")
    if connection_ok and not tables_ok:
        reasons.append(f"Only {ready_tables} table pair ready (need 2+)")
    if tables_with_ws1 < total_acc1_tables and total_acc1_tables > 0:
        reasons.append(f"Acc1 WS: {tables_with_ws1}/{total_acc1_tables}")
    if tables_with_ws2 < total_acc2_tables and total_acc2_tables > 0:
        reasons.append(f"Acc2 WS: {tables_with_ws2}/{total_acc2_tables}")
    if not network_ok:
        reasons.append(f"High Lat ({max_latency:.0f}ms)")
    if not ram_ok:
        reasons.append(f"RAM ({ram_usage:.1f}%)")
    if not is_armed and connection_ok and tables_ok:
        reasons.append("Not Armed")

    # ── Smart Guidance: contextual advice for the user ──
    frames_ready_count = 0
    betting_open_count = 0
    for t, info in tables1_snap.items():
        if info.get('frame_ready') and info.get('is_betting_open'):
            frames_ready_count += 1
        if info.get('is_betting_open'):
            betting_open_count += 1
    for t, info in tables2_snap.items():
        if info.get('frame_ready') and info.get('is_betting_open'):
            frames_ready_count += 1
        if info.get('is_betting_open'):
            betting_open_count += 1
    
    bet_state = coordinator.bet_state
    
    if bet_state == "placing":
        guidance = {"icon": "⚡", "text": "Bet fire ho rahi hai — page band mat karo!", "color": "#00e676"}
    elif bet_state == "placed":
        guidance = {"icon": "✅", "text": "Hedge lag gayi — result ka wait karo", "color": "#2ecc71"}
    elif not is_p1_logged and not is_p2_logged:
        guidance = {"icon": "🔐", "text": "Pehle dono accounts login karo", "color": "#e74c3c"}
    elif not is_p1_logged:
        guidance = {"icon": "⏳", "text": "Account 1 connect nahi hai — re-login karo", "color": "#e74c3c"}
    elif not is_p2_logged:
        guidance = {"icon": "⏳", "text": "Account 2 connect ho raha hai — thoda wait karo", "color": "#f39c12"}
    elif total_acc1_tables == 0 or total_acc2_tables == 0:
        guidance = {"icon": "📡", "text": "Tables load ho rahi hain — wait karo", "color": "#f39c12"}
    elif tables_with_ws1 < total_acc1_tables or tables_with_ws2 < total_acc2_tables:
        alive = tables_with_ws1 + tables_with_ws2
        total = total_acc1_tables + total_acc2_tables
        guidance = {"icon": "🔄", "text": f"Connection jud raha hai ({alive}/{total} ready)", "color": "#f39c12"}
    elif not ram_ok:
        guidance = {"icon": "💾", "text": f"RAM full hai ({ram_usage:.0f}%) — restart karo", "color": "#e74c3c"}
    elif not network_ok:
        guidance = {"icon": "📶", "text": f"Network slow hai ({max_latency:.0f}ms) — bet mat lagao abhi", "color": "#e74c3c"}
    elif not tables_ok:
        guidance = {"icon": "⏳", "text": f"Sirf {ready_tables} table pair ready — 2 chahiye", "color": "#f39c12"}
    elif not is_armed:
        guidance = {"icon": "👆", "text": "Sab ready hai — ARM dabao scanning shuru hogi", "color": "#2ecc71"}
    elif is_armed and betting_open_count >= 4 and frames_ready_count >= 4:
        guidance = {"icon": "🔥", "text": f"FIRE hone wali hai — {frames_ready_count} frames tayyar!", "color": "#00e676"}
    elif is_armed and betting_open_count >= 2:
        guidance = {"icon": "👁️", "text": f"Dekh raha hai — {betting_open_count} tables khuli hain, timing check ho rahi", "color": "#f1c40f"}
    elif is_armed:
        guidance = {"icon": "👁️", "text": "Scanning chal rahi hai — betting window ka wait", "color": "#f1c40f"}
    else:
        guidance = {"icon": "⏳", "text": "Wait karo...", "color": "#95a5a6"}

    return jsonify({
        # ── Andar Bahar tables ──
        "tables1": [
            {
                "token": t, "name": info["name"],
                "status": info["status"],
                "is_open": info.get("is_betting_open", False),
                "roundId": info.get("roundId"),
                "last_result": info.get("last_result"),
                "latency": info.get("latency", -1),
                "frame_ready": info.get("frame_ready", False)
            } for t, info in tables1_snap.items()
        ],
        "tables2": [
            {
                "token": t, "name": info["name"],
                "status": info["status"],
                "is_open": info.get("is_betting_open", False),
                "roundId": info.get("roundId"),
                "last_result": info.get("last_result"),
                "latency": info.get("latency", -1),
                "frame_ready": info.get("frame_ready", False)
            } for t, info in tables2_snap.items()
        ],
        # ── Andar Bahar ──
        "balance1": coordinator.account1.balance,
        "balance2": coordinator.account2.balance,
        "initial_balance1": coordinator.initial_balance1,
        "initial_balance2": coordinator.initial_balance2,
        "is_successful": coordinator.is_successful,
        "last_bet_result": coordinator.last_bet_result,
        "bet_state": coordinator.bet_state,
        "auto_bet_armed": coordinator.auto_bet_requested,
        "login_status1": coordinator.account1.login_status,
        "login_status2": coordinator.account2.login_status,
        "nickname1": getattr(coordinator.account1, 'temp_nickname', ''),
        "nickname2": getattr(coordinator.account2, 'temp_nickname', ''),
        "round_results1": coordinator.account1.round_results[-20:],
        "round_results2": coordinator.account2.round_results[-20:],
        "burned_account": coordinator.burned_account,
        "burn_detected_at": coordinator._burn_detected_at,
        "telegram_queue_size": coordinator.telegram.get_queue_size() if hasattr(coordinator, 'telegram') else 0,
        
        # ── Health & Signal ──
        "system_signal": signal,
        "system_ram": ram_usage,
        "system_latency": max_latency,
        "system_time_remaining": time_remaining,
        "system_signal_reasons": reasons,
        
        # ── Smart Guidance ──
        "guidance": guidance,
        "ready_pairs": ready_tables,
        "frames_ready": frames_ready_count,
        "betting_open_tables": betting_open_count
    })

@app.route('/api/burn', methods=['POST'])
def burn_account():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"success": False, "message": "Missing X-Session-ID header"}), 400

    data = request.json
    account_num = data.get('account_num')
    if not account_num or int(account_num) not in (1, 2):
        return jsonify({"success": False, "message": "Please provide a valid account number (1 or 2)."}), 400

    account_num = int(account_num)
    coordinator.burned_account = account_num
    coordinator._burn_detected_at = 0  # 0 to bypass countdown on frontend
    coordinator.auto_bet_requested = False
    coordinator.bet_state = "idle"

    # Auto-rotate if a Telegram replacement account exists
    from urllib.parse import urlparse
    import threading
    rotated = False
    if coordinator._last_login_base_url:
        parsed = urlparse(coordinator._last_login_base_url)
        domain = parsed.netloc or coordinator._last_login_base_url
        next_acct = coordinator.telegram.pull_next_account(domain)
        if next_acct:
            username = next_acct['username']
            password = next_acct['password']
            coordinator.telegram.send_message(f"🔄 Account {account_num} burned manually. Automatically rotating to {username}.")
            threading.Thread(target=coordinator.relogin_account, args=(account_num, username, password), daemon=True).start()
            rotated = True
        else:
            coordinator.telegram.send_message(f"⚠️ Account {account_num} burned manually. No replacement account in queue!")

    return jsonify({"success": True, "message": f"Account {account_num} marked as burned.", "rotated": rotated})

@app.route('/api/telegram/status', methods=['GET'])
def telegram_status():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
    q_size = coordinator.telegram.get_queue_size() if hasattr(coordinator, 'telegram') else 0
    return jsonify({"queue_size": q_size})

@app.route('/api/relogin', methods=['POST'])
def relogin():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"success": False, "message": "Missing X-Session-ID header"}), 400

    data = request.json
    account_num = data.get('account_num')
    username = data.get('username')
    password = data.get('password')

    if not account_num or not username or not password:
        return jsonify({"success": False, "message": "Please provide account number, username, and password."})

    result = coordinator.relogin_account(int(account_num), username, password)
    return jsonify(result)

@app.route('/api/login', methods=['POST'])
def login():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"success": False, "message": "Missing X-Session-ID header"}), 400
        
    data = request.json
    base_url = data.get('baseUrl')
    user1 = data.get('username1')
    pass1 = data.get('password1')
    user2 = data.get('username2')
    pass2 = data.get('password2')
    
    if not user1 or not pass1 or not user2 or not pass2:
        return jsonify({"success": False, "message": "Please provide credentials for both accounts."})
        
    result = coordinator.perform_login(base_url, user1, pass1, user2, pass2)
    return jsonify(result)

@app.route('/api/login_status', methods=['GET'])
def get_login_status():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
        
    return jsonify({
        "status1": coordinator.account1.login_status,
        "status2": coordinator.account2.login_status
    })

@app.route('/api/bet', methods=['POST'])
def place_bet():
    import logging
    logger = logging.getLogger("ws_manager")
    sid = request.headers.get('X-Session-ID', 'NONE')
    logger.info(f"🎯 /api/bet POST received — Session: {sid}")
    
    coordinator = get_coordinator()
    if not coordinator:
        logger.warning(f"🚨 /api/bet REJECTED — no coordinator (session={sid})")
        return jsonify({"error": "Missing X-Session-ID header"}), 400
        
    data = request.json or {}
    mode = data.get('mode', 'auto')
    amount = data.get('amount', 100.0)
    
    logger.info(f"🎯 Calling arm_auto_bet(mode={mode}, amount={amount})")
    result = coordinator.arm_auto_bet(mode, amount)
    logger.info(f"🎯 arm_auto_bet result: {result}")
    return jsonify(result)

@app.route('/api/disarm', methods=['POST'])
def disarm_bet():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
        
    result = coordinator.disarm_auto_bet()
    return jsonify(result)

@app.route('/api/fire_all_4', methods=['POST'])
def fire_all_4():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
        
    data = request.json or {}
    mode = data.get('mode', 'auto')
    amount = data.get('amount', 100.0)
    
    result = coordinator.fire_all_4_manual(mode, amount)
    return jsonify(result)

@app.route('/api/test_stack', methods=['POST'])
def test_stack():
    """🧪 Test if 7Mojos server accumulates multiple bets on the same table."""
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400
    
    result = coordinator.test_stack_bet()
    return jsonify(result)



@app.route('/api/clear_cache', methods=['POST'])
def clear_cache():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400

    # Clear Andar Bahar round results and acks
    coordinator.account1.round_results.clear()
    coordinator.account2.round_results.clear()
    coordinator.account1.pending_bet_acks.clear()
    coordinator.account2.pending_bet_acks.clear()

    # Clear last_result from each Andar Bahar table
    for info in coordinator.account1.tables.values():
        info.pop('last_result', None)
    for info in coordinator.account2.tables.values():
        info.pop('last_result', None)

    # Reset Andar Bahar bet state
    coordinator.last_bet_result   = None
    coordinator.bet_state         = "idle"
    coordinator.auto_bet_requested = False

    return jsonify({"success": True, "message": "Cache cleared successfully."})

@app.route('/api/export_data', methods=['GET'])
def export_data():
    coordinator = get_coordinator()
    if not coordinator:
        return jsonify({"error": "Missing X-Session-ID header"}), 400

    # Filter: only include rounds with positive total_profit (ignore negative/losing rounds)
    positive_records = [
        r for r in coordinator.bet_history
        if r.get('total_profit') is not None and r.get('total_profit', 0) > 0
    ]

    from flask import Response
    import json as json_module
    
    export_data = {
        "exported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_rounds": len(coordinator.bet_history),
        "profitable_rounds": len(positive_records),
        "records": positive_records
    }
    
    response = Response(
        json_module.dumps(export_data, indent=2),
        mimetype='application/json',
        headers={'Content-Disposition': 'attachment; filename=bet_history.json'}
    )
    return response

@app.route('/api/logs', methods=['GET'])
def get_logs():
    from ws_manager import LOG_BUFFER
    return "\n".join(list(LOG_BUFFER)), 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/api/js_error', methods=['POST'])
def js_error():
    data = request.json or {}
    msg = data.get('message', 'Unknown')
    stack = data.get('stack', '')
    logger.error(f"🚨 CLIENT JS ERROR: {msg}\nStack: {stack}")
    return jsonify({"success": True})

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
