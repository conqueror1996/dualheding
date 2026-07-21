import asyncio
import json
import logging
import os
import threading
import time
import requests
import websockets
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import config
from urllib.parse import urlparse
import socket as _socket_mod
import gc

import collections

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Global log buffer — stores only INFO+ messages for /api/logs
# Keeping it small (300) saves ~2-3MB RAM vs 1000 DEBUG entries
LOG_BUFFER = collections.deque(maxlen=300)

class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            LOG_BUFFER.append(msg)
        except Exception:
            self.handleError(record)

# Add memory handler to root logger
_buf_handler = BufferLogHandler()
_buf_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(_buf_handler)

# Thread pool for running blocking IO inside async loops
_executor = ThreadPoolExecutor(max_workers=4)

# ── Reconnection settings ──
MAX_RECONNECT_DELAY = 5    # seconds (was 15 — ultra-fast recovery)
INITIAL_RECONNECT_DELAY = 0.5  # seconds (was 2 — reconnect INSTANTLY)
BETTING_OPEN_TIMEOUT = 15  # auto-expire stale betting windows after this many seconds


class TelegramBotManager:
    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.running = False
        self.last_update_id = 0
        self.accounts_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telegram_accounts.json")
        self.token = config.TELEGRAM_BOT_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID

    def load_accounts(self):
        if not os.path.exists(self.accounts_file):
            return []
        try:
            with open(self.accounts_file, 'r') as f:
                return json.load(f)
        except Exception:
            return []

    def save_accounts(self, accounts):
        try:
            with open(self.accounts_file, 'w') as f:
                json.dump(accounts, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save telegram_accounts.json: {e}")

    def send_message(self, text):
        if not self.token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            requests.post(url, json={"chat_id": self.chat_id, "text": text}, timeout=5)
        except Exception as e:
            logger.error(f"Telegram send_message failed: {e}")

    def get_queue_size(self):
        accounts = self.load_accounts()
        return len([a for a in accounts if not a.get('used')])

    def pull_next_account(self, domain):
        accounts = self.load_accounts()
        for idx, acct in enumerate(accounts):
            if not acct.get('used') and acct.get('domain') == domain:
                accounts[idx]['used'] = True
                self.save_accounts(accounts)
                return acct
        return None

    def start(self):
        if not config.TELEGRAM_ENABLED or not self.token:
            return
        self.running = True
        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()

    def stop(self):
        self.running = False

    def _poll_loop(self):
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        logger.info("Started Telegram Bot Manager polling.")
        while self.running:
            try:
                params = {"timeout": 10, "offset": self.last_update_id + 1}
                resp = requests.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data.get("result", []):
                        update_id = item["update_id"]
                        self.last_update_id = max(self.last_update_id, update_id)
                        message = item.get("message", {})
                        text = message.get("text", "")
                        
                        if text.startswith("/add "):
                            parts = text.split()
                            if len(parts) == 4:
                                _, domain, username, password = parts
                                # Clean domain
                                parsed = urlparse(domain if "://" in domain else "http://" + domain)
                                clean_domain = parsed.netloc or domain
                                
                                accounts = self.load_accounts()
                                accounts.append({
                                    "domain": clean_domain,
                                    "username": username,
                                    "password": password,
                                    "used": False,
                                    "timestamp": time.time()
                                })
                                self.save_accounts(accounts)
                                self.send_message(f"✅ Account saved for {clean_domain}!")
                            else:
                                self.send_message("❌ Usage: /add <domain> <username> <password>")
                        elif text.startswith("/status"):
                            q_size = self.get_queue_size()
                            self.send_message(f"📊 Current Queue: {q_size} available accounts.")
            except Exception as e:
                logger.debug(f"Telegram poll error: {e}")
            time.sleep(3)


class BaccaratManager:
    # Map winner codes to labels (Andar Bahar: 0=Andar, 1=Bahar)
    WINNER_MAP = {0: 'Andar', 1: 'Bahar', 2: 'Tie', 'andar': 'Andar', 'bahar': 'Bahar', 'tie': 'Tie', 'player': 'Andar', 'banker': 'Bahar'}

    def __init__(self, name, bet_type):
        self.name    = name
        self.bet_type = bet_type
        import random
        import string
        prefix = random.choice(["Player", "User", "Gamer", "Pro", "sasa", "pagal", "robin", "terrence", "brenda", "sam", "max", "lucky"])
        suffix = "".join(random.choices(string.digits, k=6))
        self.temp_nickname = f"{prefix}{suffix}"
        self.tables  = {}  # token -> {name, status, is_betting_open, ws, ...}
        self.balance = 0
        self.running = False
        self.loop    = None
        self.active_tasks = set()  # Use set: O(1) add/discard, no duplicates
        self.login_status = "Not logged in"
        self._coordinator_ref  = None
        self.playcric_session  = None
        self.playcric_url      = None
        self.csrf_token        = None
        self.player_token      = None
        self.operator_token    = None
        self.pending_bet_acks  = {}   # token -> {'status': 'pending'|'confirmed'|'rejected', 'raw': str}
        self.round_results     = []   # list of {table, roundId, winner, playerTotal, bankerTotal, timestamp}

    def rotate_proxy_session(self):
        import random
        import time
        name_part = self.name.replace(" ", "_")
        self.proxy_session_id = f"{name_part}_{int(time.time())}_{random.randint(1000, 9999)}"
        proxy_url = config.get_current_proxy(self.proxy_session_id)
        if proxy_url and self.playcric_session:
            try:
                self.playcric_session.close()  # Clear connection pool cache
            except Exception:
                pass
            self.playcric_session.proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
        logger.info(f"[{self.name}] Rotated proxy session to {self.proxy_session_id} to bypass IP block/geographic restrictions.")

    @staticmethod
    def _log_raw_frame(direction, account_name, table_token, text):
        try:
            with open("ws_raw_frames.log", "a") as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{direction}] [{account_name}] [{table_token}] {text.strip()}\n")
        except Exception:
            pass

    def perform_login(self, base_url, username, password):
        self._username = username
        self._password = password
        self.login_status = f"Starting login for {self.name}..."
        logger.info(self.login_status)
        self.playcric_url     = base_url
        self.csrf_token       = None
        self.player_token     = None
        self.operator_token   = None
        self.pending_bet_acks = {}
        self.playcric_session = requests.Session()
        
        # Unique session ID for proxy stickiness to avoid IP rotation issues
        import random
        import time
        self.proxy_session_id = f"{username}_{int(time.time())}_{random.randint(1000, 9999)}"
        
        proxy_url = config.get_current_proxy(self.proxy_session_id)
        if proxy_url:
            self.playcric_session.proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
        self.playcric_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        })
        
        try:
            # 🚀 Bypasses 403: Fetch root domain '/' instead of '/login' to get CSRF token
            resp = self.playcric_session.get(self.playcric_url, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            token_input = soup.find('input', {'name': '_token'})
            if token_input:
                self.csrf_token = token_input.get('value')
            else:
                meta_token = soup.find('meta', {'name': 'csrf-token'})
                if meta_token:
                    self.csrf_token = meta_token.get('content')
                    
            if not self.csrf_token:
                raise ValueError("CSRF token missing in response body.")
        except Exception as e:
            if config.switch_to_backup_proxy():
                logger.warning(f"[{self.name}] Connection to Cricmatch failed. Swapping to BACKUP proxy and retrying...")
                backup_proxy = config.get_current_proxy(self.proxy_session_id)
                self.playcric_session.proxies = {
                    "http": backup_proxy,
                    "https": backup_proxy
                }
                try:
                    # 🚀 Bypasses 403: Fetch root domain '/' on backup proxy as well
                    resp = self.playcric_session.get(self.playcric_url, timeout=15)
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    token_input = soup.find('input', {'name': '_token'})
                    if token_input:
                        self.csrf_token = token_input.get('value')
                    else:
                        meta_token = soup.find('meta', {'name': 'csrf-token'})
                        if meta_token:
                            self.csrf_token = meta_token.get('content')
                    if not self.csrf_token:
                        status = getattr(resp, 'status_code', '?')
                        url_used = getattr(resp, 'url', '?')
                        logger.error(f"[{self.name}] CSRF extraction failed on backup proxy (HTTP {status}). URL: {url_used}. Body snippet: {resp.text[:300]}")
                        return {"success": False, "message": f"Failed to extract CSRF token after proxy failover. (HTTP {status})"}
                except Exception as retry_err:
                    logger.warning(f"[{self.name}] Proxy failover SSL error ({retry_err}). Falling back to direct connection...")
                    self.playcric_session.proxies = {}  # Fallback to direct connection if proxy fails
                    try:
                        resp = self.playcric_session.get(self.playcric_url, timeout=15)
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        token_input = soup.find('input', {'name': '_token'})
                        if token_input:
                            self.csrf_token = token_input.get('value')
                        else:
                            meta_token = soup.find('meta', {'name': 'csrf-token'})
                            if meta_token:
                                self.csrf_token = meta_token.get('content')
                        if not self.csrf_token:
                            return {"success": False, "message": "Failed to extract CSRF token (direct connection)."}
                    except Exception as direct_err:
                        return {"success": False, "message": f"Connection error after retry: {direct_err}"}
            else:
                status = getattr(locals().get('resp'), 'status_code', '?')
                logger.error(f"[{self.name}] Connection/CSRF error: {e}. HTTP Status: {status}")
                return {"success": False, "message": f"Connection/CSRF error: {e} (HTTP {status})"}
            
        login_payload = {
            "username": username,
            "password": password,
            "remember_me": "1",
            "_token": self.csrf_token
        }
        
        self.playcric_session.headers.update({
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.playcric_url,
            "Referer": f"{self.playcric_url}/login",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        })
        
        try:
            resp_login = self.playcric_session.post(f"{self.playcric_url}/login", data=login_payload, timeout=15)
            login_json = resp_login.json()
            if login_json.get("status") != 200:
                return {"success": False, "message": f"Login failed: {login_json.get('message', 'Invalid credentials')}"}
        except Exception as e:
            logger.warning(f"[{self.name}] Login POST failed: {e}")
            return {"success": False, "message": "Invalid login response."}
            
        try:
            self.playcric_session.headers.pop("X-Requested-With", None)
            self.playcric_session.headers.pop("Content-Type", None)
            self.playcric_session.headers.update({"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"})
            
            # Fetch login redirect path if returned to fully bind cookies/session on Cricmatch backend
            redirect_path = login_json.get("url")
            if redirect_path:
                redirect_url = f"{self.playcric_url}/{redirect_path.lstrip('/')}"
                logger.info(f"[{self.name}] Fetching login redirect URL: {redirect_url}")
                try:
                    self.playcric_session.get(redirect_url, timeout=15)
                except Exception as redirect_err:
                    logger.warning(f"[{self.name}] Login redirect fetch failed: {redirect_err}")
            
            launcher_url = f"{self.playcric_url}/7mojos/launcher?q=427"
            # Step 1: Get token from first redirect WITHOUT following (prevents token consumption)
            resp_launcher = self.playcric_session.get(launcher_url, timeout=15, allow_redirects=False)
            
            # Extract tokens from redirect Location header
            location = resp_launcher.headers.get('Location')
            if location:
                import urllib.parse
                token_source = urllib.parse.urljoin(resp_launcher.url, location)
            else:
                token_source = resp_launcher.url
            logger.info(f"[{self.name}] Launcher token source: {token_source[:120]}...")
            
            pt_match = re.search(r'playerToken=([^&"\']+)', token_source)
            ot_match = re.search(r'operatorToken=([^&"\']+)', token_source)
            
            if pt_match and ot_match:
                self.player_token = pt_match.group(1)
                self.operator_token = ot_match.group(1)
                
                # Step 2: Follow the full redirect chain to activate server-side session
                # This is required for table-level authentication to work
                try:
                    self.playcric_session.get(token_source, timeout=15, allow_redirects=True)
                    logger.info(f"[{self.name}] Session activation redirect chain completed")
                except Exception as chain_err:
                    logger.warning(f"[{self.name}] Session activation redirect failed (non-fatal): {chain_err}")
                
                if not self.running:
                    self.start()
                    
                self.login_status = "Connected!"
                return {"success": True, "message": f"{self.name} Logged in!"}
            else:
                return {"success": False, "message": "Could not find tokens in launcher response."}
                
        except Exception as e:
            logger.warning(f"[{self.name}] Launcher fetch failed: {e}")
            return {"success": False, "message": f"Error fetching launcher: {e}"}

    def get_lobby_games(self):
        auth_url = f"{config.LOBBY_BASE_URL}/api/lobbyv2/authenticate"
        auth_params = {
            "operatorToken": self.operator_token,
            "playerToken": self.player_token
        }
        
        jwt_token = None
        try:
            # 7Mojos lobby does not require proxy on French VPS (direct is faster)
            proxies = None
            resp_auth = requests.get(auth_url, params=auth_params, headers=config.HEADERS, proxies=proxies, timeout=10)
            if resp_auth.status_code == 200:
                jwt_token = resp_auth.json().get('token')
            else:
                logger.warning(f"[{self.name}] Lobby auth failed: HTTP {resp_auth.status_code}")
                return False
        except Exception as e:
            logger.warning(f"[{self.name}] Lobby auth error: {e}")
            if config.switch_to_backup_proxy():
                logger.warning(f"[{self.name}] Lobby auth failed. Swapped to backup proxy.")
            return False
            
        if not jwt_token:
            logger.warning(f"[{self.name}] No JWT token received from lobby auth")
            return False

        url = f"{config.LOBBY_BASE_URL}/api/lobbyv2/games?type=any"
        headers = config.HEADERS.copy()
        headers["Authorization"] = f"Basic {jwt_token}"
        
        try:
            # 7Mojos lobby does not require proxy on French VPS
            proxies = None
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            if resp.status_code == 200:
                games = resp.json()
                # Match Andar Bahar twin tables by exact token (ab-3, ab-4)
                ab_games = [g for g in games if g.get('token') in config.ANDAR_BAHAR_TOKENS]
                ab_games.sort(key=lambda g: g.get('token', ''))
                logger.info(f"[{self.name}] Andar Bahar twin tables found: {[g.get('name') for g in ab_games]}")
                if len(ab_games) > 0:
                    logger.info(f"[{self.name}] Sample Andar Bahar Game JSON: {ab_games[0]}")
                for g in ab_games:
                    token = g.get('token')
                    if token and token not in self.tables:
                        self.tables[token] = {
                            "name": g.get('name'),
                            "status": "Disconnected",
                            "balance": 0.0,
                            "roundId": None,
                            "is_betting_open": False,
                            "betting_opened_at": 0,
                            "max_bet": 100.0,  # Default safety limit, updated dynamically in WS init
                            "ws": None,
                            "latency": -1,
                            "last_msg_at": 0,  # Timestamp of last WS message received (silence watchdog)
                        }
                return True
            else:
                logger.warning(f"[{self.name}] Lobby games fetch failed: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"[{self.name}] Lobby games error: {e}")
        return False

    def update_balance(self):
        if not self.playcric_session or not self.csrf_token:
            return
            
        balance_url = f"{self.playcric_url}/api2/v2/getBalance"
        bal_payload = {
            "_token": self.csrf_token
        }
        
        # FIX: Use per-request headers instead of mutating shared session headers
        # This prevents thread-safety issues when balance updates run concurrently
        request_headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }
        
        try:
            resp_bal = self.playcric_session.post(
                balance_url,
                data=bal_payload,
                headers=request_headers,
                timeout=10
            )
            try:
                bal_json = resp_bal.json()
            except Exception:
                # Response is not JSON — session likely expired (getting HTML login page)
                logger.warning(f"[{self.name}] Balance response is not JSON (session expired?). Status={resp_bal.status_code}, body={resp_bal.text[:100]}")
                return
            if bal_json.get("status") == 200:
                b = bal_json.get("balance", {}).get("balance", "0")
                old_val = self.balance
                self.balance = float(str(b).replace(',',''))
                if self.balance != old_val:
                    logger.info(f"💰 [Balance Update] {self.name}: Rs. {self.balance:.2f} (was: Rs. {old_val:.2f})")
                if self._coordinator_ref:
                    if self.name == "Account 1 (Player)":
                        if self._coordinator_ref.initial_balance1 == 0.0 and self.balance > 0:
                            self._coordinator_ref.initial_balance1 = self.balance
                    elif self.name == "Account 2 (Banker)":
                        if self._coordinator_ref.initial_balance2 == 0.0 and self.balance > 0:
                            self._coordinator_ref.initial_balance2 = self.balance
                    self._coordinator_ref.check_burned_accounts()
        except Exception as e:
            logger.debug(f"[{self.name}] Balance update failed: {e}")

    def authenticate_table(self, game_token):
        url = f"{config.LCE_BASE_URL}/api/players/identity/authenticate"
        payload = {
            "GameToken": game_token,
            "OperatorToken": self.operator_token,
            "PlayerToken": self.player_token,
            "DeviceType": 1,
            "OS": 1,
            "Referrer": f"{config.HEADERS.get('Origin', 'https://de3-clb.horances.com')}/",
            "ClientVersion": "1.10.0.1084"
        }
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                # 7Mojos game servers do NOT block European IPs (direct is faster & avoids country block)
                proxies = None
                resp = requests.post(url, json=payload, headers=config.HEADERS, proxies=proxies, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success'):
                        return data.get('data', {}).get('accessToken')
                    else:
                        logger.warning(f"[{self.name}] Table auth rejected for {game_token}: success=false, response={resp.text[:200]}")
                        
                        # 🔄 AUTO-RELOGIN ON EXPIRED SESSION (errorCode 402)
                        if data.get('errorCode') == 402 or "402" in resp.text:
                            logger.warning(f"[{self.name}] 🚨 Session expired (errorCode 402). Attempting auto-relogin to refresh tokens...")
                            if getattr(self, '_username', None) and getattr(self, '_password', None):
                                relogin_res = self.perform_login(self.playcric_url, self._username, self._password)
                                if relogin_res.get('success'):
                                    logger.info(f"[{self.name}] ✅ Auto-relogin successful! Retrying table auth...")
                                    payload["OperatorToken"] = self.operator_token
                                    payload["PlayerToken"] = self.player_token
                                    if attempt < max_retries:
                                        time.sleep(0.5)
                                        continue
                                else:
                                    logger.error(f"[{self.name}] ❌ Auto-relogin failed: {relogin_res.get('message')}")
                        
                        if "blocked" in resp.text.lower() or "country" in resp.text.lower() or data.get('errorCode') == 11:
                            self.rotate_proxy_session()
                            if attempt < max_retries:
                                logger.info(f"[{self.name}] Country blocked — retry {attempt+1}/{max_retries} with new proxy IP...")
                                time.sleep(0.5)  # Brief pause before retry
                                continue
                        # Non-country error, don't retry
                        return None
                else:
                    logger.warning(f"[{self.name}] Table auth failed for {game_token[:8]}: HTTP {resp.status_code}, body={resp.text[:200]}")
                    return None
            except Exception as e:
                logger.warning(f"[{self.name}] Table auth error for {game_token[:8]}: {e}")
                if config.switch_to_backup_proxy():
                    logger.warning(f"[{self.name}] Table auth failed. Swapped to backup proxy.")
                if attempt < max_retries:
                    self.rotate_proxy_session()
                    time.sleep(0.5)
                    continue
                return None
        logger.warning(f"[{self.name}] Table auth exhausted {max_retries} retries for {game_token[:8]} (all Country blocked)")
        return None

    async def connect_websocket(self, game_token, access_token):
        """Connect to a table's WebSocket with automatic reconnection on failure."""
        self.tables[game_token]['reconnect_delay'] = INITIAL_RECONNECT_DELAY

        while self.running:
            if access_token:
                try:
                    await self._connect_websocket_once(game_token, access_token)
                except Exception as e:
                    logger.warning(f"[{self.name}] WS connection error for {game_token[:8]}: {e}")
            else:
                logger.warning(f"[{self.name}] Skipping connection attempt for {game_token[:8]} (no access token)")

            # Connection dropped or auth failed — check if we should reconnect
            if not self.running:
                break
            if game_token not in self.tables:
                break

            self.tables[game_token]['status'] = "Reconnecting..."
            self.tables[game_token]['is_betting_open'] = False
            self.tables[game_token]['ws'] = None

            delay = self.tables[game_token].get('reconnect_delay', INITIAL_RECONNECT_DELAY)
            logger.info(f"[{self.name}] Reconnecting {self.tables[game_token].get('name', game_token[:8])} in {delay}s...")
            await asyncio.sleep(delay)

            # Exponential backoff (unless overridden for fast reconnect)
            if delay >= INITIAL_RECONNECT_DELAY:
                self.tables[game_token]['reconnect_delay'] = min(delay * 2, MAX_RECONNECT_DELAY)
            else:
                # Reset to normal reconnect delay after fast trigger
                self.tables[game_token]['reconnect_delay'] = INITIAL_RECONNECT_DELAY

            # Re-authenticate before reconnecting (tokens may have expired or need initial fetch)
            loop = asyncio.get_event_loop()
            try:
                new_access_token = await loop.run_in_executor(
                    _executor, self.authenticate_table, game_token
                )
            except Exception as e:
                logger.warning(f"[{self.name}] Re-auth failed for {game_token[:8]}: {e}")
                new_access_token = None

            if new_access_token:
                access_token = new_access_token
                self.tables[game_token]['reconnect_delay'] = INITIAL_RECONNECT_DELAY  # Reset backoff on successful auth
            else:
                access_token = None
                # Cap delay at 10s for auth failures — fast retry with new proxy IP is key
                self.tables[game_token]['reconnect_delay'] = min(self.tables[game_token].get('reconnect_delay', INITIAL_RECONNECT_DELAY), 10)
                logger.warning(f"[{self.name}] Re-auth failed for {game_token[:8]}, retrying in {self.tables[game_token]['reconnect_delay']}s...")

    async def _connect_websocket_once(self, game_token, access_token):
        """Single WebSocket connection attempt. Raises on disconnect."""
        # FIX: Run blocking negotiate call in executor to avoid blocking event loop
        loop = asyncio.get_event_loop()
        neg_url = f"{config.LCE_BASE_URL}/wssr/player/negotiate"
        neg_params = {"access_token": access_token}
        try:
            # 7Mojos negotiate does NOT require proxy on French VPS
            proxies = None
            neg_resp = await loop.run_in_executor(
                _executor,
                lambda: requests.post(neg_url, params=neg_params, headers=config.HEADERS, proxies=proxies, timeout=10)
            )
            if neg_resp.status_code != 200:
                logger.warning(f"[{self.name}] Negotiate failed: HTTP {neg_resp.status_code}")
                if config.switch_to_backup_proxy():
                    logger.warning(f"[{self.name}] Negotiate failed. Swapped to backup proxy.")
                return
            connection_id = neg_resp.json().get("connectionId")
        except Exception as e:
            logger.warning(f"[{self.name}] Negotiate error: {e}")
            if config.switch_to_backup_proxy():
                logger.warning(f"[{self.name}] Negotiate error. Swapped to backup proxy.")
            return

        # FIX: Use config-derived URL instead of hardcoded
        lce_host = config.LCE_BASE_URL.replace("https://", "").replace("http://", "")
        ws_url = f"wss://{lce_host}/wssr/player?id={connection_id}&access_token={access_token}"
        origin = config.HEADERS.get("Origin", "https://de3-clb.horances.com")

        if game_token in self.tables:
            self.tables[game_token]['status'] = "Connecting..."
        
        # NOTE: WebSocket connects WITHOUT proxy for minimum latency.
        # Auth (negotiate/authenticate) still uses Indian proxy for geo-access.
        # Direct WS from Render → 7Mojos avoids the double-hop penalty (~400ms saved).
        try:
            async with websockets.connect(
                ws_url,
                additional_headers={"Origin": origin},
                open_timeout=15,   # If handshake hangs (proxy drop), raise after 15s
                close_timeout=5,   # Don't wait forever on graceful close
                ping_interval=8,   # Send WS-level ping every 8s (detect dead connection fast)
                ping_timeout=5,    # Close connection if pong not received within 5s
            ) as ws:
                if game_token not in self.tables:
                    return

                self.tables[game_token]['ws'] = ws
                self.tables[game_token]['status'] = "Connected"
                
                # ⚡ Socket tuning: TCP_NODELAY + Keep-Alive
                try:
                    transport = ws.transport
                    if transport:
                        sock = transport.get_extra_info('socket')
                        if sock:
                            # 1. Disable Nagle's algorithm for instant sends
                            sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_NODELAY, 1)
                            # 2. Enable SO_KEEPALIVE to detect half-open connections
                            sock.setsockopt(_socket_mod.SOL_SOCKET, _socket_mod.SO_KEEPALIVE, 1)
                            # 3. Configure aggressive Keep-Alive parameters (if supported by OS)
                            if hasattr(_socket_mod, "TCP_KEEPIDLE"):
                                sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_KEEPIDLE, 5)
                                sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_KEEPINTVL, 2)
                                sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_KEEPCNT, 3)
                            elif hasattr(_socket_mod, "TCP_KEEPALIVE") and not hasattr(_socket_mod, "TCP_KEEPIDLE"):
                                # macOS fallback
                                sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_KEEPALIVE, 5)
                            logger.debug(f"[{self.name}] TCP tuning (NODELAY + KEEPALIVE) set for {self.tables[game_token].get('name', game_token[:8])}")
                except Exception as e:
                    logger.debug(f"[{self.name}] TCP socket tuning failed: {e}")
                
                logger.info(f"[{self.name}] Connected to {self.tables[game_token].get('name', game_token[:8])}")
                
                # Start background latency ping loop
                # NOTE: latency_task is NOT added to active_tasks — it is scoped
                # to this WS connection and cancelled in the finally block below.
                # Adding it would cause a leak across reconnects.
                latency_task = asyncio.create_task(self._latency_ping_loop(game_token, ws))
                
                try:
                    BaccaratManager._log_raw_frame("-->", self.name, game_token, '{"protocol":"json","version":1}\x1e')
                    await ws.send('{"protocol":"json","version":1}\x1e')
                    BaccaratManager._log_raw_frame("-->", self.name, game_token, '{"arguments":[],"invocationId":"0","target":"Ready","type":1}\x1e')
                    await ws.send('{"arguments":[],"invocationId":"0","target":"Ready","type":1}\x1e')
                    
                    # 🔒 Nickname Obfuscation: Send random pregenerated nickname
                    nickname_payload = {
                        "arguments": [{
                            "type": 11,
                            "data": json.dumps({"nickname": self.temp_nickname})
                        }],
                        "target": "Message",
                        "type": 1
                    }
                    nickname_frame = json.dumps(nickname_payload) + "\x1e"
                    BaccaratManager._log_raw_frame("-->", self.name, game_token, nickname_frame)
                    await ws.send(nickname_frame)
                    logger.info(f"[{self.name}] Sent nickname obfuscation frame: {self.temp_nickname}")
                    
                    async for message in ws:
                        if not self.running:
                            break
                        BaccaratManager._log_raw_frame("<--", self.name, game_token, message)
                        messages = message.split('\x1e')
                        for msg in messages:
                            if not msg or msg == '{}':
                                continue
                            try:
                                data = json.loads(msg)
                                self._handle_ws_message(game_token, data)
                            except json.JSONDecodeError:
                                pass
                finally:
                    latency_task.cancel()
                    try:
                        await latency_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    if game_token in self.tables:
                        self.tables[game_token]['latency'] = -1
        except Exception as e:
            if config.switch_to_backup_proxy():
                logger.warning(f"[{self.name}] WebSocket connection failed. Swapped to backup proxy.")
            raise e

        # Connection closed normally — mark table
        if game_token in self.tables:
            self.tables[game_token]['status'] = "Disconnected"
            self.tables[game_token]['is_betting_open'] = False
            self.tables[game_token]['ws'] = None

    def _handle_ws_message(self, game_token, data):
        if game_token not in self.tables:
            return
        # Update last-message timestamp on every received WS message (silence watchdog)
        self.tables[game_token]['last_msg_at'] = time.time()

        # ── SignalR Completion Message (type: 3) — Bet Confirmation ──
        # In Andar Bahar, type:3 with result:null and no error IS the only
        # bet confirmation 7Mojos sends. There is no separate inner_type
        # game-engine confirmation for AB (unlike Baccarat).
        if data.get('type') == 3:
            tname = self.tables[game_token].get('name', game_token[:8])
            error_info = data.get('error')
            if error_info:
                if game_token in self.pending_bet_acks and self.pending_bet_acks[game_token]['status'] == 'pending':
                    self.pending_bet_acks[game_token]['status'] = 'rejected'
                    self.pending_bet_acks[game_token]['raw'] = str(error_info)[:200]
                logger.warning(f"[{self.name}] [{tname}] ❌ SignalR invocation error: {error_info}")
            else:
                if game_token in self.pending_bet_acks and self.pending_bet_acks[game_token]['status'] == 'pending':
                    self.pending_bet_acks[game_token]['status'] = 'confirmed'
                    self.pending_bet_acks[game_token]['raw'] = 'SignalR type:3 completion ACK'
                    logger.info(f"[{self.name}] [{tname}] 🎉 BET CONFIRMED (SignalR type:3 ACK)")
                else:
                    logger.debug(f"[{self.name}] [{tname}] 📩 SignalR type:3 ACK (no pending bet)")

        if data.get('type') == 1 and data.get('target') == 'Message':
            args = data.get('arguments', [])
            if args and len(args) > 0:
                arg_str = args[0]
                if isinstance(arg_str, str):
                    try:
                        payload = json.loads(arg_str)
                        msg_type = payload.get('type')

                        # ── Log ALL messages to help debug bet confirmation format ──
                        tname = self.tables[game_token].get('name', game_token[:8])
                        logger.debug(f"[{self.name}] [{tname}] WS msg_type={msg_type} data_keys={list(payload.get('data', {}).keys()) if isinstance(payload.get('data'), dict) else payload.get('type', '?')}")

                        # Process all message types for bet confirmation and roundId updates
                        if msg_type == 2:
                            inner_data = payload.get('data', {})
                            if isinstance(inner_data, str):
                                try:
                                    inner_data = json.loads(inner_data)
                                except Exception:
                                    inner_data = {}
                            inner_type = inner_data.get('type') if isinstance(inner_data, dict) else None
                            # Log at INFO level when bet is pending so we can debug confirmation
                            if game_token in self.pending_bet_acks and self.pending_bet_acks[game_token]['status'] == 'pending':
                                logger.info(f"[{self.name}] [{tname}] 🔍 BET PENDING — got msg inner_type={inner_type} keys={list(inner_data.keys()) if isinstance(inner_data, dict) else 'none'}")
                            else:
                                logger.debug(f"[{self.name}] [{tname}] type=2 inner_type={inner_type}")
                            if inner_type == 3:
                                self.tables[game_token]['status'] = "Betting Open"
                                self.tables[game_token]['is_betting_open'] = True
                                self.tables[game_token]['betting_opened_at'] = time.time()
                                self._prebuild_bet_frames(game_token)
                                self._on_betting_open()
                            elif inner_type == 7:
                                self.tables[game_token]['status'] = "Dealing"
                                self.tables[game_token]['is_betting_open'] = False
                                self.tables[game_token]['frame_ready'] = False
                            elif inner_type == 1:
                                # inner_type:1 = game state update (may contain bet info)
                                if game_token in self.pending_bet_acks:
                                    if self.pending_bet_acks[game_token]['status'] == 'pending':
                                        self.pending_bet_acks[game_token]['status'] = 'confirmed'
                                        self.pending_bet_acks[game_token]['raw'] = str(inner_data)[:200]
                                        logger.info(f"[{self.name}] [{tname}] 🎉 BET CONFIRMED by game engine (inner_type=1)")
                            # NOTE: inner_type:6 = Bahar card dealt, inner_type:7 = Andar card dealt
                            # These are NOT bet result messages in Andar Bahar!
                            elif inner_type == 0:
                                self.tables[game_token]['roundId'] = inner_data.get('roundId')
                                currency_dict = inner_data.get('currencyConfig', {})
                                if isinstance(currency_dict, dict):
                                    if 'maxBet' in currency_dict:
                                        self.tables[game_token]['max_bet'] = float(currency_dict['maxBet'])
                                    if 'minBet' in currency_dict:
                                        self.tables[game_token]['min_bet'] = float(currency_dict['minBet'])
                                logger.info(
                                    f"[{self.name}] [{tname}] Table limits parsed — "
                                    f"Min: {self.tables[game_token].get('min_bet', 50.0):.2f} | "
                                    f"Max: {self.tables[game_token].get('max_bet', 100.0):.2f}"
                                )

                            if isinstance(inner_data, dict):
                                incoming_rid = inner_data.get('roundId')
                                if incoming_rid and incoming_rid != self.tables[game_token].get('roundId'):
                                    self.tables[game_token]['roundId'] = incoming_rid

                                self._try_extract_result(game_token, inner_data, tname)

                                possible_bet_fields = [
                                    inner_data.get('playerBets'),
                                    inner_data.get('currentBets'),
                                    inner_data.get('bets'),
                                    inner_data.get('acceptedBets'),
                                    inner_data.get('data', {}).get('playerBets') if isinstance(inner_data.get('data'), dict) else None,
                                ]
                                ab_bet_obj = inner_data.get('bet')
                                has_ab_bet = False
                                if isinstance(ab_bet_obj, dict):
                                    andar_amt = ab_bet_obj.get('andarBet', 0) or 0
                                    bahar_amt = ab_bet_obj.get('baharBet', 0) or 0
                                    if andar_amt > 0 or bahar_amt > 0:
                                        has_ab_bet = True

                                has_bet_data = has_ab_bet or inner_data.get('successful') in [True, "true", "True", 1] or any(
                                    v for v in possible_bet_fields
                                    if v and (isinstance(v, list) and len(v) > 0 or isinstance(v, dict))
                                )
                                if has_bet_data and game_token in self.pending_bet_acks:
                                    old_status = self.pending_bet_acks[game_token]['status']
                                    if old_status == 'pending':
                                        self.pending_bet_acks[game_token]['status'] = 'confirmed'
                                        self.pending_bet_acks[game_token]['raw'] = str(inner_data)[:200]
                                        logger.info(f"[{self.name}] [{tname}] BET CONFIRMED by server")

                                is_explicit_fail = inner_data.get('successful') in [False, "false", "False", 0]
                                err_msg = inner_data.get('failReason') or inner_data.get('errorMessage')
                                if is_explicit_fail and not err_msg:
                                    err_msg = (inner_data.get('error') or 
                                               inner_data.get('errorCode') or 
                                               inner_data.get('errorType') or
                                               "Transaction unsuccessful")

                                if err_msg and not has_bet_data and game_token in self.pending_bet_acks:
                                    if self.pending_bet_acks[game_token]['status'] == 'pending':
                                        self.pending_bet_acks[game_token]['status'] = 'rejected'
                                        self.pending_bet_acks[game_token]['raw'] = str(err_msg)[:200]
                                        logger.warning(f"[{self.name}] [{tname}] BET REJECTED by server: {err_msg}")

                        # NOTE: Removed dangerous catch-all that auto-confirmed on ANY message.
                        # Real bet confirmations ONLY come from:
                        # 1. andarBet/baharBet > 0 in inner_data.bet
                        # 2. successful: true in inner_data  
                        # 3. playerBets/currentBets/acceptedBets arrays

                        elif msg_type == 9:
                            # Real-time balance update from server (type:9 = newBalance)
                            # Keeps cached balance accurate so next round's bet sizing
                            # uses the REAL server balance, not a stale HTTP-polled value
                            tname9 = self.tables[game_token].get('name', game_token[:8])
                            bal_data = payload.get('data', {})
                            if isinstance(bal_data, str):
                                try:
                                    bal_data = json.loads(bal_data)
                                except Exception:
                                    bal_data = {}
                            if isinstance(bal_data, dict):
                                new_bal = bal_data.get('newBalance')
                                if new_bal is not None:
                                    try:
                                        old_bal = self.balance
                                        self.balance = float(new_bal)
                                        if abs(self.balance - old_bal) > 0.01:
                                            logger.info(f"[{self.name}] [{tname9}] 💰 RT balance: {self.balance:.2f} (was {old_bal:.2f})")
                                    except (ValueError, TypeError):
                                        pass

                        # Log unknown message types for discovery
                        elif msg_type not in (6, 9):
                            logger.debug(f"[{self.name}] Unknown msg_type={msg_type}: {str(payload)[:120]}")

                    except json.JSONDecodeError:
                        pass

    def _try_extract_result(self, game_token, data, tname):
        """Try to extract round result (Player/Banker/Tie) from a message payload.
        
        7Mojos sends results in type-2 messages with these keys:
          - type (inner_type, usually 8 or 9)
          - gameRoundId: the round identifier
          - result: 0=Player, 1=Banker, 2=Tie
          - playerCards: list of card objects
          - bankerCards: list of card objects
          - win: amount won
          - winningPlayers: list of winning player types
        
        Also handles the connection snapshot which has playerValue/bankerValue.
        """
        if not isinstance(data, dict):
            return

        # ── Primary: Look for the dedicated result message ──
        # Baccarat: 'gameRoundId' + 'result' + 'playerCards' + 'bankerCards'
        # Andar Bahar: 'gameRoundId' + 'winnerSide' + 'cardsDealt'
        has_baccarat_result = 'gameRoundId' in data and 'result' in data and 'playerCards' in data
        has_ab_result = 'gameRoundId' in data and 'winnerSide' in data and 'cardsDealt' in data
        
        if has_baccarat_result or has_ab_result:
            winner_raw = data.get('winnerSide') if has_ab_result else data.get('result')
            round_id = data.get('gameRoundId')
            player_cards = data.get('playerCards', []) if has_baccarat_result else []
            banker_cards = data.get('bankerCards', []) if has_baccarat_result else []
            win_raw = data.get('win', 0)
            # Andar Bahar sends win as dict: {"total": 171.0, "mainWin": 171.0, ...}
            # Baccarat sends win as number: 171.0
            if isinstance(win_raw, dict):
                win_amount = float(win_raw.get('total') or win_raw.get('mainWin') or 0)
            else:
                win_amount = float(win_raw or 0)
            cards_dealt = data.get('cardsDealt', 0)
            joker_card = data.get('joker')

            # Calculate totals from cards if available (Baccarat only)
            player_total = self._calc_card_total(player_cards) if has_baccarat_result else None
            banker_total = self._calc_card_total(banker_cards) if has_baccarat_result else None

            # Normalize winner
            if isinstance(winner_raw, int):
                winner = self.WINNER_MAP.get(winner_raw, f'Unknown({winner_raw})')
            elif isinstance(winner_raw, str):
                winner = self.WINNER_MAP.get(winner_raw.lower(), winner_raw.capitalize())
            else:
                winner = str(winner_raw)

            result_entry = {
                'table': tname,
                'table_token': game_token,
                'roundId': round_id,
                'winner': winner,
                'playerTotal': player_total,
                'bankerTotal': banker_total,
                'playerCards': player_cards,
                'bankerCards': banker_cards,
                'winAmount': win_amount,
                'cardsDealt': cards_dealt,
                'jokerCard': joker_card,
                'timestamp': time.time(),
                'account': self.name
            }

            # Store on the table itself
            if game_token in self.tables:
                self.tables[game_token]['last_result'] = result_entry

            # Append to history (cap at 50)
            self.round_results.append(result_entry)
            if len(self.round_results) > 50:
                self.round_results = self.round_results[-50:]

            if has_ab_result:
                logger.info(f"[{self.name}] [{tname}] 🎯 RESULT: {winner} | Cards:{cards_dealt} | Win:{win_amount} | Round:{round_id}")
            else:
                logger.info(f"[{self.name}] [{tname}] 🎯 RESULT: {winner} | P:{player_total} B:{banker_total} | Win:{win_amount} | Round:{round_id}")
            return

        # ── Secondary: Check for winner/winSide in other message formats ──
        winner_raw = data.get('winner') or data.get('winSide') or data.get('winnerType')
        if winner_raw is not None:
            if isinstance(winner_raw, int):
                winner = self.WINNER_MAP.get(winner_raw, f'Unknown({winner_raw})')
            elif isinstance(winner_raw, str):
                winner = self.WINNER_MAP.get(winner_raw.lower(), winner_raw.capitalize())
            else:
                winner = str(winner_raw)

            round_id = data.get('roundId') or self.tables.get(game_token, {}).get('roundId')
            player_total = data.get('playerValue') or data.get('playerTotal')
            banker_total = data.get('bankerValue') or data.get('bankerTotal')

            result_entry = {
                'table': tname,
                'table_token': game_token,
                'roundId': round_id,
                'winner': winner,
                'playerTotal': player_total,
                'bankerTotal': banker_total,
                'playerCards': None,
                'bankerCards': None,
                'winAmount': 0,
                'timestamp': time.time(),
                'account': self.name
            }

            if game_token in self.tables:
                self.tables[game_token]['last_result'] = result_entry
            self.round_results.append(result_entry)
            if len(self.round_results) > 50:
                self.round_results = self.round_results[-50:]
            logger.info(f"[{self.name}] [{tname}] 🎯 RESULT (alt): {winner} | P:{player_total} B:{banker_total} | Round:{round_id}")

    @staticmethod
    def _calc_card_total(cards):
        """Calculate baccarat hand total from a list of card objects."""
        if not cards or not isinstance(cards, list):
            return None
        total = 0
        for card in cards:
            if isinstance(card, dict):
                val = card.get('value', 0)
                if isinstance(val, int):
                    total += val if val < 10 else 0
                elif isinstance(val, str) and val.isdigit():
                    v = int(val)
                    total += v if v < 10 else 0
            elif isinstance(card, (int, float)):
                v = int(card)
                total += v if v < 10 else 0
        return total % 10

    def _on_betting_open(self):
        """Called when any table's betting window opens. Override in subclasses."""
        if self._coordinator_ref:
            self._coordinator_ref.check_auto_bet()

    async def _heartbeat_loop(self):
        # Andar Bahar has natural 7-12s silence between rounds (result → new round).
        # Old 8s timeout caused false zombie detection. 25s safely covers worst-case gaps.
        SILENCE_TIMEOUT = 25
        while self.running:
            # Snapshot to avoid dict-changed-size-during-iteration errors
            items = list(self.tables.items())
            now = time.time()
            for token, info in items:
                ws = info.get('ws')
                if ws and info.get('status') in ['Connected', 'Betting Open', 'Dealing']:
                    # 1. Send SignalR keepalive ping
                    try:
                        await ws.send('{"type":6}\x1e')
                    except Exception as e:
                        logger.debug(f"[{self.name}] Heartbeat send failed for {info.get('name', token[:8])}: {e}")

                    # 2. Silence watchdog: if WS reports open but no messages for 25s, force-close
                    last_msg = info.get('last_msg_at', 0)
                    if last_msg > 0 and (now - last_msg) > SILENCE_TIMEOUT:
                        tname = info.get('name', token[:8])
                        logger.warning(
                            f"[{self.name}] [{tname}] 🚨 SILENCE WATCHDOG: No message for "
                            f"{now - last_msg:.0f}s — force-closing zombie WS to trigger reconnect."
                        )
                        try:
                            await ws.close()
                        except Exception:
                            pass
            await asyncio.sleep(1)  # Heartbeat every 1s — ultra-fast zombie detection

    async def _balance_loop(self):
        """Balance loop — runs blocking HTTP in thread executor to not block event loop."""
        while self.running:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(_executor, self.update_balance)
            except Exception as e:
                logger.debug(f"[{self.name}] Balance loop error: {e}")
            await asyncio.sleep(5)

    async def _betting_expiry_loop(self):
        """Auto-expire stale is_betting_open states to prevent betting on closed windows."""
        while self.running:
            now = time.time()
            items = list(self.tables.items())
            for token, info in items:
                if info.get('is_betting_open'):
                    elapsed = now - info.get('betting_opened_at', 0)
                    if elapsed > BETTING_OPEN_TIMEOUT:
                        info['is_betting_open'] = False
                        if info.get('status') == 'Betting Open':
                            info['status'] = 'Connected'
                        logger.debug(f"[{self.name}] Auto-expired betting window for {info.get('name', token[:8])} after {elapsed:.1f}s")
            await asyncio.sleep(2)

    async def _latency_ping_loop(self, game_token, ws):
        """Periodically ping the websocket to calculate real-time network latency (RTT)."""
        consecutive_failures = 0
        while self.running and ws.open:
            try:
                start_time = time.time()
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=4.0)
                latency_ms = (time.time() - start_time) * 1000
                consecutive_failures = 0
                if game_token in self.tables:
                    self.tables[game_token]['latency'] = int(latency_ms)
                    tname = self.tables[game_token].get('name', game_token[:8])
                    if latency_ms > 300:
                        logger.warning(f"[{self.name}] [{tname}] ⚠️ HIGH LATENCY: {latency_ms:.0f}ms — bets may miss window!")
                    elif latency_ms > 150:
                        logger.debug(f"[{self.name}] [{tname}] 📶 Latency: {latency_ms:.0f}ms (acceptable)")
            except Exception as e:
                consecutive_failures += 1
                if game_token in self.tables:
                    self.tables[game_token]['latency'] = -1
                tname = self.tables[game_token].get('name', game_token[:8]) if game_token in self.tables else game_token[:8]
                logger.warning(f"[{self.name}] [{tname}] ⚠️ Ping failed ({consecutive_failures}/3): {e}")
                if consecutive_failures >= 3:
                    logger.error(f"[{self.name}] [{tname}] 🚨 WebSocket zombie connection detected (3 consecutive ping failures). Force closing.")
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    break
            await asyncio.sleep(2)  # Latency ping every 2s — fast health monitoring

    async def start_all_tables_async(self):
        self.running = True
        self.update_balance()
        if not self.get_lobby_games():
            self.running = False
            return

        for token in list(self.tables.keys()):
            # Always start the reconnect loop task for all tables, letting it retry auth internally
            access_token = self.authenticate_table(token)
            task = asyncio.create_task(self.connect_websocket(token, access_token))
            self.active_tasks.add(task)
                
        self.active_tasks.add(asyncio.create_task(self._heartbeat_loop()))
        self.active_tasks.add(asyncio.create_task(self._balance_loop()))
        self.active_tasks.add(asyncio.create_task(self._betting_expiry_loop()))
        await asyncio.gather(*self.active_tasks, return_exceptions=True)

    def start(self):
        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.start_all_tables_async())
        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        
    def stop(self):
        self.running = False
        if self.loop:
            for task in list(self.active_tasks):  # snapshot set before iteration
                self.loop.call_soon_threadsafe(task.cancel)
        self.active_tasks.clear()
        # Don't clear tables here; running WS tasks still reference them
    def _prebuild_bet_frames(self, game_token):
        """Pre-build raw WebSocket frames for both bet types when betting opens.
        Warms JIT code path. Actual amount is replaced at fire time in <1ms."""
        if game_token not in self.tables:
            return
        for bet_type in [config.ANDAR_BET_TYPE, config.BAHAR_BET_TYPE]:
            try:
                frame = self._build_raw_bet_frame(50.0, bet_type)
                self.tables[game_token][f'prebuilt_frame_{bet_type}'] = frame
            except Exception as e:
                logger.debug(f"[{self.name}] Pre-build frame failed for {game_token[:8]}: {e}")
        # ✅ Mark this table as frame-ready for UI indicator
        self.tables[game_token]['frame_ready'] = True
        tname = self.tables[game_token].get('name', game_token[:8])
        logger.info(f"[{self.name}] [{tname}] ✅ FRAME READY — betting window open")

    @staticmethod
    def _build_raw_bet_frame(amount, side=0, round_id=None):
        """Build a masked WebSocket TEXT frame for a bet. Returns raw bytes."""
        clean_amt = int(amount) if isinstance(amount, (int, float)) and float(amount).is_integer() else amount
        andar_val = clean_amt if side == 0 else 0
        bahar_val = clean_amt if side == 1 else 0
        inner_data = {
            "bets": {"andarBet": andar_val, "baharBet": bahar_val, "sideBets": [], "win": 0, "processed": False},
            "gameplayMessageType": 0
        }
        if round_id:
            inner_data["roundId"] = round_id
        payload_obj = {
            "arguments": [{"type": 1, "data": json.dumps(inner_data)}],
            "target": "Message", "type": 1
        }
        raw_payload = (json.dumps(payload_obj) + '\x1e').encode('utf-8')
        length = len(raw_payload)
        frame = bytearray([0x81])  # FIN + TEXT
        if length < 126:
            frame.append(0x80 | length)
        elif length <= 65535:
            frame.append(0x80 | 126)
            frame.extend(length.to_bytes(2, 'big'))
        else:
            frame.append(0x80 | 127)
            frame.extend(length.to_bytes(8, 'big'))
        mask_key = os.urandom(4)
        frame.extend(mask_key)
        for i in range(length):
            frame.append(raw_payload[i] ^ mask_key[i % 4])
        return bytes(frame)

    async def _place_multiple_bets_async(self, wss_with_names, amount, bet_type):
        """
        Fire bets to all tables simultaneously to trigger race condition.
        wss_with_names: list of (ws, table_name)
        Returns: list of {table, status: 'sent'|'failed', error: str|None}
        """
        payload_0 = {
            "arguments": [{"type": 1, "data": json.dumps({"areBetsInZeroCommMode": False, "bets": [{"type": bet_type, "bet": amount}], "gameplayMessageType": 0})}],
            "target": "Message", "type": 1
        }
        msg0 = json.dumps(payload_0) + '\x1e'

        valid_wss = [(ws, name) for ws, name in wss_with_names if ws is not None and getattr(ws, 'open', True)]
        results_map = {name: {"table": name, "status": "sent", "error": None} for _, name in wss_with_names}
        for ws, name in wss_with_names:
            if ws is None or not getattr(ws, 'open', True):
                results_map[name] = {"table": name, "status": "failed", "error": "No WebSocket connection" if ws is None else "WebSocket not open"}

        # STRICT ALL-OR-NOTHING: If any target table's WebSocket is closed, do not bet on ANY tables for this account.
        if len(valid_wss) < len(wss_with_names):
            logger.warning(f"[{self.name}] 🚨 place_multiple_bets aborting: only {len(valid_wss)}/{len(wss_with_names)} WebSockets are open.")
            for _, name in wss_with_names:
                results_map[name] = {"table": name, "status": "failed", "error": "WebSocket connection not open for all tables"}
            return [results_map[name] for _, name in wss_with_names]

        # Send bets to all tables — track per-table success to prevent double bets
        raw_sent_tables = set()  # Tables that successfully sent via raw transport
        try:
            # Build raw websocket TEXT frame (masked) to bypass asyncio overhead
            raw_payload = msg0.encode('utf-8')
            length = len(raw_payload)
            frame = bytearray([0x81])  # FIN + TEXT
            if length < 126:
                frame.append(0x80 | length)
            elif length <= 65535:
                frame.append(0x80 | 126)
                frame.extend(length.to_bytes(2, 'big'))
            else:
                frame.append(0x80 | 127)
                frame.extend(length.to_bytes(8, 'big'))
            mask_key = os.urandom(4)
            frame.extend(mask_key)
            for i in range(length):
                frame.append(raw_payload[i] ^ mask_key[i % 4])
            raw_frame = bytes(frame)

            # Try raw transport write — only works if ws.transport exists (websockets < v13)
            for ws, name in valid_wss:
                try:
                    if not getattr(ws, 'open', True):
                        raise ConnectionError("WebSocket closed before raw write")
                    transport = getattr(ws, 'transport', None)
                    if transport is None:
                        raise AttributeError("No transport attribute")
                    transport.write(raw_frame)
                    raw_sent_tables.add(name)
                except Exception as e:
                    logger.debug(f"[{self.name}] Raw write failed for {name}: {e}")
                    break  # Stop trying raw for remaining tables
        except Exception as e:
            logger.debug(f"[{self.name}] Raw frame build failed, using ws.send: {e}")

        # Fallback: use ws.send ONLY for tables that were NOT already sent via raw
        fallback_wss = [(ws, name) for ws, name in valid_wss if name not in raw_sent_tables]
        if fallback_wss:
            async def _send_one(ws, name):
                try:
                    await ws.send(msg0)
                    results_map[name]["status"] = "sent"
                    results_map[name]["error"] = None
                except Exception as e:
                    results_map[name]["status"] = "failed"
                    results_map[name]["error"] = str(e)

            await asyncio.gather(*[_send_one(ws, name) for ws, name in fallback_wss])

        return [results_map[name] for _, name in wss_with_names]

    async def _undo_bets_async(self, targets, bet_type=None, amount=None):
        # FORMAT 2 (gameplayMessageType:7): The TRUE cancel/clear command from 7Mojos client.
        #   Inner message: {"bets": [], "gameplayMessageType": 7} — cancels all confirmed bets.
        # We fire BOTH to maximize the chance of the server processing the undo.

        # FORMAT 1: type:7 unbet (from pentest extension protocol capture)
        undo_type7_payload = {
            "arguments": [json.dumps({"type": 7})],
            "target": "Message", "type": 1
        }
        undo_type7_msg = json.dumps(undo_type7_payload) + '\x1e'

        # FORMAT 2: gameplayMessageType:1 with empty bets (game client clear)
        undo_clear_payload = {
            "arguments": [{"type": 1, "data": json.dumps({
                "areBetsInZeroCommMode": False,
                "bets": [],
                "gameplayMessageType": 1
            })}],
            "target": "Message", "type": 1
        }
        undo_clear_msg = json.dumps(undo_clear_payload) + '\x1e'

        async def _send_undo(target):
            # Support both game token (string) and tuple (ws, name)
            if isinstance(target, str):
                info = self.tables.get(target, {})
                ws = info.get('ws')
                name = info.get('name', target[:8])
            elif isinstance(target, tuple) and len(target) == 2:
                ws, name = target
            else:
                ws = target
                name = "Unknown"

            try:
                # Resolve token
                tok = target if isinstance(target, str) else (getattr(ws, 'game_token', 'unknown_tok') if ws else 'unknown_tok')
                if ws and getattr(ws, 'open', True):
                    # Send BOTH undo formats for maximum reliability
                    BaccaratManager._log_raw_frame("-->", self.name, tok, f"[UNDO type:7] {undo_type7_msg}")
                    await ws.send(undo_type7_msg)
                    BaccaratManager._log_raw_frame("-->", self.name, tok, f"[UNDO clear] {undo_clear_msg}")
                    await ws.send(undo_clear_msg)
                    logger.info(f"[{self.name}] [{name}] ↩️ UNDO sent (type:7 + empty bets clear)")
                    return {"table": name, "status": "undo_sent", "error": None}
                else:
                    # WS reports closed — try transport.write() as emergency fallback
                    # Transport buffer may still flush before TCP teardown
                    transport = getattr(ws, 'transport', None) if ws else None
                    if transport:
                        # Build raw undo frame manually
                        raw_payload = undo_type7_msg.encode('utf-8')
                        length = len(raw_payload)
                        frame = bytearray([0x81])
                        if length < 126:
                            frame.append(0x80 | length)
                        elif length <= 65535:
                            frame.append(0x80 | 126)
                            frame.extend(length.to_bytes(2, 'big'))
                        mask_key = os.urandom(4)
                        frame.extend(mask_key)
                        for i in range(length):
                            frame.append(raw_payload[i] ^ mask_key[i % 4])
                        transport.write(bytes(frame))
                        logger.warning(f"[{self.name}] [{name}] ↩️ UNDO via transport.write() (WS closed, transport alive)")
                        return {"table": name, "status": "undo_transport", "error": None}
                    else:
                        logger.warning(f"[{self.name}] [{name}] ↩️ UNDO skipped: WebSocket AND transport dead")
                        return {"table": name, "status": "failed", "error": "WebSocket not open"}
            except Exception as e:
                logger.warning(f"[{self.name}] [{name}] ↩️ UNDO failed: {e}")
                return {"table": name, "status": "failed", "error": str(e)}

        if targets:
            await asyncio.gather(*[_send_undo(t) for t in targets])


class GlobalCoordinator:
    def __init__(self):
        self.account1 = BaccaratManager("Account 1 (Andar)", config.ANDAR_BET_TYPE)
        self.account2 = BaccaratManager("Account 2 (Bahar)", config.BAHAR_BET_TYPE)
        self.auto_bet_requested = False
        self.bet_state = "idle"  # idle | armed | placing | placed
        self.last_bet_result = None
        self._hedge_undone = False
        self.bet_history = []
        self.bet_mode = "auto"   # auto | fixed
        self.bet_target_amount = 100.0
        self._bet_lock = threading.Lock()  # Prevents race condition from dual _on_betting_open calls
        self.burned_account = None  # None | 1 | 2 — set when an account goes negative
        self._burn_detected_at = None  # timestamp when burn was detected
        self._last_login_base_url = None  # remember base_url for relogin
        self.initial_balance1 = 0.0
        self.initial_balance2 = 0.0
        self._hunting_thread = None  # Continuous hunting loop thread
        self._hunting_active = False  # Flag to stop hunting
        # Inject coordinator references
        self.account1._coordinator_ref = self
        self.account2._coordinator_ref = self
        self.telegram = TelegramBotManager(self)
        self.telegram.start()
        
    def perform_login(self, base_url, user1, pass1, user2, pass2):
        # Stop existing background tasks to prevent ghost connections
        for mgr in [self.account1, self.account2]:
            if mgr.running:
                mgr.stop()

        # Force garbage collect old WS objects, frame buffers, cached data
        gc.collect()
        
        # Fully recreate instances to wipe all cache, tables, and balances
        self.account1    = BaccaratManager("Account 1 (Andar)", config.ANDAR_BET_TYPE)
        self.account2    = BaccaratManager("Account 2 (Bahar)", config.BAHAR_BET_TYPE)
        self.account1._coordinator_ref    = self
        self.account2._coordinator_ref    = self
        self.auto_bet_requested  = False
        self.bet_state           = "idle"
        self.last_bet_result     = None
        self._hedge_undone       = False
        self.bet_history         = []
        self.burned_account      = None
        self._burn_detected_at   = None
        self._last_login_base_url = base_url
        self.initial_balance1 = 0.0
        self.initial_balance2 = 0.0
        # is_successful is a @property computed from balances — resets automatically

        # Force another gc pass after clearing references
        gc.collect()

        logger.info("🧹 FRESH START: All cache, tables, WS, balances cleared for new login")

        r1 = self.account1.perform_login(base_url, user1, pass1)
        if not r1["success"]: return r1

        r2 = self.account2.perform_login(base_url, user2, pass2)
        if not r2["success"]: return r2

        self.auto_bet_requested = False
        self.bet_state = "idle"
        return {"success": True, "message": "Both accounts logged in! Andar Bahar twin tables connected. Press Arm to start."}
        
    def check_auto_bet(self):
        if not self._bet_lock.acquire(blocking=False):
            return  # Another thread is already inside check_auto_bet
        try:
            self._check_auto_bet_locked()
        finally:
            self._bet_lock.release()

    def _check_auto_bet_locked(self):
        if not self.auto_bet_requested:
            return
        if self.bet_state == "placing":
            return  # Already firing, don't double-fire
            
        now = time.time()
        valid_tables = []
        skip_reasons = {}  # token -> reason string
        # FIX: Snapshot dict to prevent RuntimeError during iteration
        for token, info in list(self.account1.tables.items()):
            tname = info.get('name', token[:8])
            # Check account1 table conditions (must have an active, open WS connection)
            if not info.get('is_betting_open'):
                skip_reasons[tname] = "Acc1 betting not open"
                continue
            if not info.get('ws') or not getattr(info.get('ws'), 'open', True):
                skip_reasons[tname] = "Acc1 WS not open"
                continue
            # Check active responsiveness / latency (reject only if measured and > 800ms)
            acc1_latency = info.get('latency', -1)
            if acc1_latency != -1 and acc1_latency > 800:
                skip_reasons[tname] = f"Acc1 latency={acc1_latency}ms (>800)"
                continue
            # Check account2 has matching table with active, open WS connection
            if token not in self.account2.tables:
                skip_reasons[tname] = "Acc2 no matching table"
                continue
            if not self.account2.tables[token].get('ws') or not getattr(self.account2.tables[token].get('ws'), 'open', True):
                skip_reasons[tname] = "Acc2 WS not open"
                continue
            # Check active responsiveness / latency for Acc2 (reject only if measured and > 800ms)
            acc2_latency = self.account2.tables[token].get('latency', -1)
            if acc2_latency != -1 and acc2_latency > 800:
                skip_reasons[tname] = f"Acc2 latency={acc2_latency}ms (>800)"
                continue
            # Check account2 betting is also open
            if not self.account2.tables[token].get('is_betting_open'):
                skip_reasons[tname] = "Acc2 betting not open"
                continue

            # Calculate elapsed times since betting opened on both accounts
            elapsed1 = now - info.get('betting_opened_at', 0)
            elapsed2 = now - self.account2.tables[token].get('betting_opened_at', 0)

            # Ensure window is not closing in less than 2.5s (safety threshold)
            worst_elapsed = max(elapsed1, elapsed2)
            remaining = max(15.0 - worst_elapsed, 0)  # Andar Bahar 15s timer
            if remaining < 2.5:
                skip_reasons[tname] = f"window closing soon ({remaining:.1f}s left)"
                continue

            valid_tables.append((token, info, remaining))

        # ═══ PRE-CHECK: BOTH TWIN TABLES MUST BE CONNECTED ═══
        # Before firing, verify both ab-3 and ab-4 are alive on both accounts
        all_tables_connected = True
        for token, info in list(self.account1.tables.items()):
            tname = info.get('name', token[:8])
            ws1 = self.account1.tables.get(token, {}).get('ws')
            ws2 = self.account2.tables.get(token, {}).get('ws')
            status1 = self.account1.tables.get(token, {}).get('status', 'Unknown')
            status2 = self.account2.tables.get(token, {}).get('status', 'Unknown')
            if ws1 is None or status1 in ['Disconnected', 'Reconnecting...', 'Connecting...']:
                logger.debug(f"🚫 {tname} Acc1 not connected (status={status1}) — holding fire")
                all_tables_connected = False
            if ws2 is None or status2 in ['Disconnected', 'Reconnecting...', 'Connecting...']:
                logger.debug(f"🚫 {tname} Acc2 not connected (status={status2}) — holding fire")
                all_tables_connected = False
        
        if not all_tables_connected:
            return  # Silent return — hunter will retry in 100ms

        # ═══ SMART TABLE SELECTION ═══
        # All 4 tables confirmed connected — now pick freshest 2 to fire on
        if len(valid_tables) >= 2:
            valid_tables.sort(key=lambda x: x[2], reverse=True)  # Most remaining time first for SELECTION
            best_remaining = min(valid_tables[0][2], valid_tables[1][2])
            logger.info(
                f"🧠 SMART TIMING: {len(valid_tables)} tables ready (all 4 connected) | "
                f"Best pair: {valid_tables[0][1].get('name','?')} ({valid_tables[0][2]:.1f}s) + "
                f"{valid_tables[1][1].get('name','?')} ({valid_tables[1][2]:.1f}s) | "
                f"Worst remaining: {best_remaining:.1f}s"
            )
            # Pick top 2, then re-sort by LEAST remaining first for FIRE ORDER
            # Table whose window closes soonest gets fired first → less rejection risk
            top2 = valid_tables[:2]
            top2.sort(key=lambda x: x[2])  # Ascending — least remaining fires first
            logger.info(
                f"🎯 FIRE ORDER: 1st={top2[0][1].get('name','?')} ({top2[0][2]:.1f}s) → "
                f"2nd={top2[1][1].get('name','?')} ({top2[1][2]:.1f}s)"
            )
            valid_tables = [(tok, info) for tok, info, _ in top2]

        # Log summary of table evaluation
        if skip_reasons:
            reasons_str = " | ".join(f"{k}: {v}" for k, v in skip_reasons.items())
            logger.debug(f"🔍 Table scan: {len(valid_tables)} valid, {len(skip_reasons)} skipped [{reasons_str}]")
        
        if len(valid_tables) >= 2:
            t1_min = valid_tables[0][1].get('min_bet', 1.0)
            t2_min = valid_tables[1][1].get('min_bet', 1.0)
            min_required = max(t1_min, t2_min, 1.0)
            if self.account1.balance < min_required or self.account2.balance < min_required:
                logger.warning(f"Insufficient balance (need ₹{min_required:.0f} min). Acc1={self.account1.balance:.2f} Acc2={self.account2.balance:.2f}")
                self.auto_bet_requested = False
                self.bet_state = "idle"
                return
                
            target_tables = valid_tables[:2]
            
            # Target bet size calculation based on bet_mode
            t1_token, t1_info = target_tables[0]
            t2_token, t2_info = target_tables[1]
            t1_max = t1_info.get('max_bet', 900000.0)
            t2_max = self.account2.tables.get(t2_token, {}).get('max_bet', 900000.0)
            max_allowed_bet = min(t1_max, t2_max)

            # ═══ RACE CONDITION STRATEGY ═══
            # We intentionally bet the FULL balance on EACH table simultaneously.
            # The transport.write() burst fires both bets before the server can
            # deduct the first bet's amount — both pass the balance check.
            # If race wins: account goes negative, but ALL bets are placed. ✓
            # If race loses: 1 bet rejected → undo all → re-arm → try next round.
            # DO NOT divide by num_tables — that defeats the race condition.
            num_tables = len(target_tables)
            effective_balance = min(self.account1.balance, self.account2.balance)

            # No chip step restriction — we send via raw WebSocket, not UI
            # Use the FULL balance as bet amount on each table
            min_bet = max(t1_info.get('min_bet', 1.0), t2_info.get('min_bet', 1.0), 1.0)
            if self.bet_mode == 'fixed':
                bet_amount = min(float(self.bet_target_amount), effective_balance, max_allowed_bet)
            else:  # auto
                bet_amount = min(effective_balance, max_allowed_bet)
            
            # Server expects integer amounts
            bet_amount = float(int(bet_amount))
            
            if bet_amount < min_bet:
                logger.warning(f"Bet amount ₹{bet_amount:.0f} below table minimum ₹{min_bet:.0f}. Skipping round.")
                self.auto_bet_requested = False
                self.bet_state = "idle"
                return
            
            logger.info(f"💰 RACE BET: ₹{bet_amount:.0f}/table × {num_tables} tables = ₹{bet_amount * num_tables:.0f} total (balance: ₹{effective_balance:.0f}) → race condition covers the gap")
            
            bal1_before = self.account1.balance
            bal2_before = self.account2.balance
            table_names = [info.get('name', token[:8]) for token, info in target_tables]
            
            logger.info(f"🏦 Auto-Bet Sizing: Acc1={self.account1.balance:.2f} Acc2={self.account2.balance:.2f} | Hedge Bet={bet_amount:.2f} (Max limit: {max_allowed_bet:.2f})")

            # Build individual bet objects for UI tracking
            individual_bets = [
                {"id": 0, "account": "Account 1", "bet_on": "Andar", "table": table_names[0], "amount": bet_amount, "status": "placing", "error": None},
                {"id": 1, "account": "Account 1", "bet_on": "Andar", "table": table_names[1], "amount": bet_amount, "status": "placing", "error": None},
                {"id": 2, "account": "Account 2", "bet_on": "Bahar", "table": table_names[0], "amount": bet_amount, "status": "placing", "error": None},
                {"id": 3, "account": "Account 2", "bet_on": "Bahar", "table": table_names[1], "amount": bet_amount, "status": "placing", "error": None},
            ]

            self.bet_state = "placing"
            self.last_bet_result = {
                "type": "placing",
                "tables": table_names,
                "bet1": bet_amount,
                "bet2": bet_amount,
                "bets": individual_bets,
            }
            self._hedge_undone = False
            self.auto_bet_requested = False

            # ═══════════════════════════════════════════════════════════════════
            # SIMULTANEOUS ALL-OR-NOTHING HEDGE
            # Fire all 4 bets at once (max speed) → verify all 4 confirmed
            # If ANY bet rejected → UNDO ALL bets on ALL tables
            # ═══════════════════════════════════════════════════════════════════
            def _fire_and_verify_all(tables, bets, bet_amt, acc1, acc2, bal1_bef, bal2_bef):
              try:
                tokens_used = [tok for tok, info in tables]

                # ── Pre-flight: verify ALL WebSockets are open ──
                wss1 = []  # Account 1 (Player) WS list
                wss2 = []  # Account 2 (Banker) WS list
                for tok, info in tables:
                    tname = info.get('name', tok[:8])
                    ws1 = acc1.tables.get(tok, {}).get('ws')
                    ws2 = acc2.tables.get(tok, {}).get('ws')
                    if ws1 is None or not getattr(ws1, 'open', True):
                        logger.warning(f"🚨 Account 1 WS for {tname} NOT open — aborting all")
                        self._abort_and_rearm(bets, tables, "WS not open")
                        return
                    if ws2 is None or not getattr(ws2, 'open', True):
                        logger.warning(f"🚨 Account 2 WS for {tname} NOT open — aborting all")
                        self._abort_and_rearm(bets, tables, "WS not open")
                        return
                    wss1.append((ws1, tname))
                    wss2.append((ws2, tname))

                # ── Register pending acks for ALL tables ──
                for tok, info in tables:
                    acc1.pending_bet_acks[tok] = {'status': 'pending', 'raw': None}
                    acc2.pending_bet_acks[tok] = {'status': 'pending', 'raw': None}

                # ══════════════════════════════════════════════════
                # 🚀 SAFE SEQUENTIAL FIRE: Pre-built frames + transport.write()
                # No threads — thread-safe, no WS disconnect risk
                # transport.write() = buffer copy only = <0.1ms per call
                # ══════════════════════════════════════════════════
                
                # Step 1: Pre-build ALL frames (amount is now known)
                prebuilt = {}  # (tok, account) -> frame bytes
                for tok, info in tables:
                    prebuilt[(tok, 'acc1')] = BaccaratManager._build_raw_bet_frame(bet_amt, acc1.bet_type)
                    prebuilt[(tok, 'acc2')] = BaccaratManager._build_raw_bet_frame(bet_amt, acc2.bet_type)
                    if tok in acc1.tables:
                        acc1.tables[tok]['frame_ready'] = True
                    if tok in acc2.tables:
                        acc2.tables[tok]['frame_ready'] = True
                
                # Step 2: Build text payloads for ws.send() with SignalR invocationId: "0" and active roundId
                bet_payloads = {}  # (tok, account) -> text string
                clean_amt = int(bet_amt) if isinstance(bet_amt, (int, float)) and float(bet_amt).is_integer() else bet_amt
                for tok, info in tables:
                    rid = info.get('roundId') or acc1.tables.get(tok, {}).get('roundId') or acc2.tables.get(tok, {}).get('roundId')
                    
                    data1 = {"bets": {"andarBet": clean_amt, "baharBet": 0, "sideBets": [], "win": 0, "processed": False}, "gameplayMessageType": 0}
                    data2 = {"bets": {"andarBet": 0, "baharBet": clean_amt, "sideBets": [], "win": 0, "processed": False}, "gameplayMessageType": 0}
                    if rid:
                        data1["roundId"] = rid
                        data2["roundId"] = rid
                        
                    payload1 = json.dumps({
                        "arguments": [{"type": 1, "data": json.dumps(data1)}],
                        "target": "Message",
                        "type": 1
                    }) + '\x1e'
                    payload2 = json.dumps({
                        "arguments": [{"type": 1, "data": json.dumps(data2)}],
                        "target": "Message",
                        "type": 1
                    }) + '\x1e'
                    bet_payloads[(tok, 'acc1')] = payload1
                    bet_payloads[(tok, 'acc2')] = payload2
                
                # Collect ws objects for firing
                acc1_ws_jobs = []  # (ws, payload, tname)
                acc2_ws_jobs = []
                for tok, info in tables:
                    tname = info.get('name', tok[:8])
                    ws1 = acc1.tables.get(tok, {}).get('ws')
                    ws2 = acc2.tables.get(tok, {}).get('ws')
                    if ws1 and acc1.loop:
                        acc1_ws_jobs.append((ws1, bet_payloads[(tok, 'acc1')], tname))
                    if ws2 and acc2.loop:
                        acc2_ws_jobs.append((ws2, bet_payloads[(tok, 'acc2')], tname))
                
                # ══════════════════════════════════════════════════
                # 🚀 RACE CONDITION BURST: transport.write() blast
                # Write raw WS frames directly to kernel TCP buffers
                # Both table bets per account fire in single burst <0.1ms
                # Server receives both BEFORE deducting balance from 1st
                # Same proven technique used by undo blast (line ~1900)
                # ══════════════════════════════════════════════════
                fire_start = time.time()
                send_failed = False

                # Build raw masked WebSocket TEXT frames from payloads
                raw_bet_frames = {}
                for tok_f, info_f in tables:
                    for acct_key in ['acc1', 'acc2']:
                        payload_str = bet_payloads[(tok_f, acct_key)]
                        raw = payload_str.encode('utf-8')
                        length = len(raw)
                        frame = bytearray([0x81])  # FIN + TEXT
                        if length < 126:
                            frame.append(0x80 | length)
                        elif length <= 65535:
                            frame.append(0x80 | 126)
                            frame.extend(length.to_bytes(2, 'big'))
                        else:
                            frame.append(0x80 | 127)
                            frame.extend(length.to_bytes(8, 'big'))
                        mask_key = os.urandom(4)
                        frame.extend(mask_key)
                        for i in range(length):
                            frame.append(raw[i] ^ mask_key[i % 4])
                        raw_bet_frames[(tok_f, acct_key)] = bytes(frame)

                # Per-account burst callbacks: write ALL table bets in
                # one synchronous burst → both reach server simultaneously
                acc1_blast_ok = [0]  # mutable counter for closure
                acc2_blast_ok = [0]

                def _burst_acc1():
                    for tok_b, info_b in tables:
                        tname_b = info_b.get('name', tok_b[:8])
                        ws_b = acc1.tables.get(tok_b, {}).get('ws')
                        if ws_b:
                            tr = getattr(ws_b, 'transport', None)
                            if tr:
                                tr.write(raw_bet_frames[(tok_b, 'acc1')])
                                BaccaratManager._log_raw_frame("-->", acc1.name, tname_b, bet_payloads[(tok_b, 'acc1')])
                                acc1_blast_ok[0] += 1

                def _burst_acc2():
                    for tok_b, info_b in tables:
                        tname_b = info_b.get('name', tok_b[:8])
                        ws_b = acc2.tables.get(tok_b, {}).get('ws')
                        if ws_b:
                            tr = getattr(ws_b, 'transport', None)
                            if tr:
                                tr.write(raw_bet_frames[(tok_b, 'acc2')])
                                BaccaratManager._log_raw_frame("-->", acc2.name, tname_b, bet_payloads[(tok_b, 'acc2')])
                                acc2_blast_ok[0] += 1

                try:
                    # Fire BOTH accounts simultaneously on their separate event loops
                    # Each burst writes all table bets without yielding = maximum race window
                    if acc1.loop and acc1.loop.is_running():
                        acc1.loop.call_soon_threadsafe(_burst_acc1)
                    if acc2.loop and acc2.loop.is_running():
                        acc2.loop.call_soon_threadsafe(_burst_acc2)

                    # Brief wait for burst callbacks to execute on their loops
                    time.sleep(0.010)

                    blast_count = acc1_blast_ok[0] + acc2_blast_ok[0]
                    expected_jobs = len(tables) * 2
                    if blast_count < expected_jobs:
                        logger.warning(f"⚡ Burst incomplete: {blast_count}/{expected_jobs} frames sent!")
                        send_failed = True
                except Exception as e:
                    logger.error(f"⚡ Burst fire failed: {e}")
                    send_failed = True

                fire_elapsed = (time.time() - fire_start) * 1000
                total_jobs = acc1_blast_ok[0] + acc2_blast_ok[0]

                # Log timing and status
                for _, _, tname_r in acc1_ws_jobs:
                    logger.info(f"⚡ Acc1 [{tname_r}] bet BURST via transport.write()")
                for _, _, tname_r in acc2_ws_jobs:
                    logger.info(f"⚡ Acc2 [{tname_r}] bet BURST via transport.write()")

                if send_failed:
                    logger.warning(f"⚡ BURST FIRE FAILED ({fire_elapsed:.1f}ms) — undoing ALL")
                    self._undo_all_tables(acc1, acc2, tokens_used, tables, bet_amt, bal1_bef, bal2_bef)
                    self._abort_and_rearm(bets, tables, "Burst fire failed")
                    return

                logger.info(f"🚀 All {total_jobs} bets BURST-FIRED in {fire_elapsed:.1f}ms via transport.write()! Waiting for confirmation...")

                # Save fire timing for UI display
                self.last_bet_result['fire_elapsed_ms'] = round(fire_elapsed, 1)
                avg_ms = fire_elapsed / max(total_jobs, 1)
                self.last_bet_result['fire_details'] = []
                for _, _, tname_r in acc1_ws_jobs:
                    self.last_bet_result['fire_details'].append({'table': tname_r, 'account': 'Acc1', 'ok': True, 'ms': round(avg_ms, 2)})
                for _, _, tname_r in acc2_ws_jobs:
                    self.last_bet_result['fire_details'].append({'table': tname_r, 'account': 'Acc2', 'ok': True, 'ms': round(avg_ms, 2)})

                # ══════════════════════════════════════════════════
                # ⏱️ POLL FOR ALL 4 CONFIRMATIONS
                # ══════════════════════════════════════════════════
                # Andar Bahar confirmations can take 1-3s through proxy.
                # Old 900ms window caused false HEDGE BROKEN events.
                max_polls = 20  # 20 × 150ms = 3s max
                fire_time = time.time()
                all_confirmed = False

                for poll in range(max_polls):
                    time.sleep(0.15)

                    if time.time() - fire_time > 4.0:
                        logger.warning(f"⏰ HARD DEADLINE reached ({time.time()-fire_time:.1f}s)")
                        break

                    # Check each table's ack status
                    statuses = {}
                    any_rejected = False
                    all_done = True

                    for tok, info in tables:
                        tname = info.get('name', tok[:8])
                        ack1 = acc1.pending_bet_acks.get(tok, {}).get('status', 'pending')
                        ack2 = acc2.pending_bet_acks.get(tok, {}).get('status', 'pending')
                        statuses[tname] = (ack1, ack2)

                        if ack1 == 'rejected' or ack2 == 'rejected':
                            any_rejected = True
                        if ack1 == 'pending' or ack2 == 'pending':
                            all_done = False

                    if any_rejected:
                        logger.warning(f"❌ REJECTION detected: {statuses}")
                        break

                    if all_done:
                        # Check if ALL confirmed (not just all done)
                        all_confirmed = all(
                            acc1.pending_bet_acks.get(tok, {}).get('status') == 'confirmed' and
                            acc2.pending_bet_acks.get(tok, {}).get('status') == 'confirmed'
                            for tok, info in tables
                        )
                        if all_confirmed:
                            logger.info(f"✅ ALL 4 BETS CONFIRMED in {(poll+1)*150}ms! 🎉")
                        break

                # ══════════════════════════════════════════════════
                # 📊 FINAL VERDICT: ALL OR NOTHING
                # ══════════════════════════════════════════════════

                # Update individual bet statuses from acks
                for idx, (tok, info) in enumerate(tables):
                    ack1 = acc1.pending_bet_acks.get(tok, {}).get('status', 'pending')
                    ack2 = acc2.pending_bet_acks.get(tok, {}).get('status', 'pending')
                    bets[idx]['status'] = ack1 if ack1 in ('confirmed', 'rejected') else 'sent'
                    bets[idx + 2]['status'] = ack2 if ack2 in ('confirmed', 'rejected') else 'sent'

                if all_confirmed:
                    # ✅ ALL 4 CONFIRMED — SAFE!
                    logger.info(f"✅✅ HEDGE SAFE: All 4 bets confirmed on {[i.get('name') for _,i in tables]}")
                    self.bet_state = "placed"
                    tbl_names = [info.get('name', tok[:8]) for tok, info in tables]
                    self.last_bet_result = {
                        "type": "placed", "tables": tbl_names,
                        "bet1": bet_amt, "bet2": bet_amt,
                        "bets": bets, "hedge_status": "safe"
                    }
                    record = {
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "epoch": time.time(),
                        "tables": tbl_names,
                        "bet1_amount": bet_amt, "bet2_amount": bet_amt,
                        "bet1_type": "Player", "bet2_type": "Banker",
                        "bet_statuses": [b['status'] for b in bets],
                        "hedge_status": "safe",
                        "bal1_before": bal1_bef, "bal2_before": bal2_bef,
                        "profit1": None, "profit2": None, "total_profit": None,
                        "bal1_after": None, "bal2_after": None,
                        "status": "placed"
                    }
                    self.bet_history.append(record)
                    self._save_history_to_file()
                    if acc1.loop:
                        asyncio.run_coroutine_threadsafe(
                            self._track_bet_result(tbl_names, bal1_bef, bal2_bef),
                            acc1.loop
                        )
                else:
                    # ❌ NOT ALL CONFIRMED → UNDO EVERYTHING
                    status_summary = []
                    for tok, info in tables:
                        tname = info.get('name', tok[:8])
                        a1 = acc1.pending_bet_acks.get(tok, {}).get('status', '?')
                        a2 = acc2.pending_bet_acks.get(tok, {}).get('status', '?')
                        status_summary.append(f"{tname}:P={a1}/B={a2}")

                    logger.error(f"🚨 HEDGE BROKEN — UNDOING ALL BETS: {status_summary}")
                    self._hedge_undone = True

                    self._undo_all_tables(acc1, acc2, tokens_used, tables, bet_amt, bal1_bef, bal2_bef)

                    for tok, _ in tables:
                        acc1.pending_bet_acks.pop(tok, None)
                        acc2.pending_bet_acks.pop(tok, None)

                    for b in bets:
                        if b['status'] != 'confirmed':
                            b['status'] = 'undone'
                            b['error'] = b.get('error') or 'Hedge broken — ALL bets undone'

                    self.last_bet_result = {
                        "type": "undone",
                        "tables": [info.get('name', tok[:8]) for tok, info in tables],
                        "bet1": bet_amt, "bet2": bet_amt,
                        "bets": bets, "hedge_status": "broken",
                        "reason": f"Not all 4 confirmed: {status_summary}"
                    }
                    logger.info("🔄 Auto re-arming for next round...")
                    self.auto_bet_requested = True
                    self.bet_state = "armed"

              except Exception as e:
                logger.error(f"🚨 CRITICAL: _fire_and_verify_all CRASHED: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self.bet_state = "idle"
                self.auto_bet_requested = False
                if self.telegram:
                    self.telegram.send_message(f"🚨 SYSTEM CRASH: {e}")

            threading.Thread(
                target=_fire_and_verify_all,
                args=(target_tables, individual_bets, bet_amount, self.account1, self.account2, bal1_before, bal2_before),
                daemon=True
            ).start()
    def _abort_and_rearm(self, bets, tables, reason):
        """Quick abort without undo — used for pre-flight failures."""
        for b in bets:
            b['status'] = 'aborted'
            b['error'] = reason
        self.last_bet_result = {
            "type": "undone",
            "tables": [info.get('name', tok[:8]) for tok, info in tables],
            "bets": bets, "hedge_status": "aborted",
            "reason": reason
        }
        logger.info(f"🔄 Aborted ({reason}) — re-arming for next round...")
        self.auto_bet_requested = True
        self.bet_state = "armed"

    def _undo_all_tables(self, acc1, acc2, tokens, tables, bet_amt, bal1_bef, bal2_bef):
        """Undo bets on ALL tables: instant blast first, then retry+verify."""
        logger.info(f"↩️↩️ UNDOING ALL {len(tokens)} TABLES...")
        
        # ══════════════════════════════════════════════════
        # STEP 1: INSTANT RAW BLAST (transport.write) — <1ms
        # Send undo frames BEFORE any async/thread overhead
        # This ensures undo reaches server while window is open
        # ══════════════════════════════════════════════════
        undo_type7 = json.dumps({"arguments": [json.dumps({"type": 7})], "target": "Message", "type": 1}) + '\x1e'
        undo_clear = json.dumps({"arguments": [{"type": 1, "data": json.dumps({"areBetsInZeroCommMode": False, "bets": [], "gameplayMessageType": 1})}], "target": "Message", "type": 1}) + '\x1e'
        
        blast_count = 0
        for tok, info in tables:
            tname = info.get('name', tok[:8])
            for acct, label in [(acc1, 'Acc1'), (acc2, 'Acc2')]:
                ws = acct.tables.get(tok, {}).get('ws')
                if ws:
                    transport = getattr(ws, 'transport', None)
                    if transport:
                        try:
                            # Build raw WS frame for undo type:7
                            for msg in [undo_type7, undo_clear]:
                                raw = msg.encode('utf-8')
                                length = len(raw)
                                frame = bytearray([0x81])
                                if length < 126:
                                    frame.append(0x80 | length)
                                elif length <= 65535:
                                    frame.append(0x80 | 126)
                                    frame.extend(length.to_bytes(2, 'big'))
                                mask_key = os.urandom(4)
                                frame.extend(mask_key)
                                for i in range(length):
                                    frame.append(raw[i] ^ mask_key[i % 4])
                                if acct.loop:
                                    acct.loop.call_soon_threadsafe(transport.write, bytes(frame))
                                else:
                                    transport.write(bytes(frame))
                            blast_count += 1
                        except Exception as e:
                            logger.warning(f"↩️ {label} [{tname}] raw undo blast failed: {e}")
        
        logger.info(f"↩️ INSTANT BLAST: {blast_count}/{len(tokens)*2} undo frames sent via transport.write()")
        
        # ══════════════════════════════════════════════════
        # STEP 2: ASYNC RETRY + VERIFY (threads) — backup
        # In case raw blast was ignored, retry via ws.send()
        # ══════════════════════════════════════════════════
        undo_threads = []
        for tok, info in tables:
            tname = info.get('name', tok[:8])
            t = threading.Thread(
                target=self._undo_single_table,
                args=(acc1, acc2, tok, tname, bet_amt, bal1_bef, bal2_bef),
                daemon=True
            )
            undo_threads.append(t)
            t.start()
        for t in undo_threads:
            t.join(timeout=8)
        logger.info(f"↩️↩️ ALL TABLES UNDO COMPLETE")

    def _undo_single_table(self, acc1, acc2, token, tname, bet_amt, bal1_bef, bal2_bef):
        """Undo bets on a single table for both accounts with retry logic."""
        logger.info(f"↩️ Undoing bets on '{tname}' for both accounts...")

        def _undo_one_account(acct, label, undo_bet_type, bal_bef):
            if not acct.loop or not acct.loop.is_running():
                logger.error(f"🚨 {label}: loop not running — CANNOT UNDO!")
                return

            # If the bet on this table was never confirmed, it means no money was deducted.
            # We don't need to verify any refund since no bet was placed on this table.
            initial_status = acct.pending_bet_acks.get(token, {}).get('status', 'pending')
            if initial_status != 'confirmed':
                logger.info(f"✅ {label} UNDO on '{tname}' pre-verified (bet status was '{initial_status}', no locked funds)")
                acct.pending_bet_acks[token] = {'status': 'undone', 'raw': 'Pre-verified'}
                return

            # Loop to send and verify the undo request
            for attempt in range(3):
                # Calculate expected balance for this specific table's undo.
                # It should be the start balance (bal_bef) minus any OTHER tables that are still confirmed.
                other_confirmed_count = 0
                for t_tok, _ in acct.tables.items():
                    if t_tok != token:
                        t_status = acct.pending_bet_acks.get(t_tok, {}).get('status')
                        if t_status == 'confirmed':
                            other_confirmed_count += 1
                expected_bal = bal_bef - (other_confirmed_count * bet_amt)

                try:
                    # 🚀 If attempt > 0, we suspect negative balance lockout! 
                    # Let's perform a Hard Session Reset.
                    if attempt > 0:
                        logger.warning(f"🚨 {label} [{tname}] UNDO verification failed (attempt {attempt}). Suspecting negative balance lockout! Hard-killing transport to force instant reconnect...")
                        ws = acct.tables.get(token, {}).get('ws')
                        if ws:
                            # 1. Force reconnect delay to 50ms (0.05s)
                            acct.tables[token]['reconnect_delay'] = 0.05
                            # 2. Hard close TCP transport
                            try:
                                if hasattr(ws, 'transport') and ws.transport:
                                    ws.transport.close()
                                    logger.info(f"✅ {label} [{tname}] Transport hard closed.")
                            except Exception as transport_err:
                                logger.error(f"🚨 {label} [{tname}] Transport close error: {transport_err}")
                            
                            # 3. Wait for new socket to connect
                            reconnected = False
                            for wait_idx in range(40):  # max 4 seconds
                                time.sleep(0.1)
                                current_ws = acct.tables.get(token, {}).get('ws')
                                if current_ws and current_ws != ws:
                                    logger.info(f"✅ {label} [{tname}] Reconnection detected! Proceeding to retry UNDO on fresh socket...")
                                    reconnected = True
                                    break
                            if not reconnected:
                                logger.warning(f"⚠️ {label} [{tname}] Reconnection timed out (4s), retrying on whatever is available...")

                    fut = asyncio.run_coroutine_threadsafe(
                        acct._undo_bets_async([token], bet_type=undo_bet_type, amount=bet_amt),
                        acct.loop
                    )
                    fut.result(timeout=3)
                    logger.info(f"✅ {label} UNDO sent on '{tname}' (attempt {attempt+1})")
                    time.sleep(0.15)
                    acct.update_balance()
                    
                    if acct.balance >= expected_bal - 0.5:
                        logger.info(f"✅ {label} UNDO VERIFIED: Rs. {acct.balance:.2f}")
                        acct.pending_bet_acks[token] = {'status': 'undone', 'raw': 'Verified'}
                        return
                    else:
                        logger.warning(f"🚨 {label} UNDO verification failed: bal={acct.balance:.2f} expected={expected_bal:.2f}")
                        time.sleep(0.1)
                except Exception as e:
                    logger.error(f"🚨 {label} UNDO attempt {attempt+1} FAILED: {e}")
                    time.sleep(0.15)

            # Standard undo failed 3x — log warning, but do NOT close WebSocket
            # Once window is closed, closing the socket does not cancel the bet on the server anyway.
            err = f"🚨 CRITICAL: {label} UNDO FAILED on '{tname}' after 3 attempts!"
            logger.error(err)
            if self.telegram:
                self.telegram.send_message(err)

        t1 = threading.Thread(target=_undo_one_account, args=(acc1, "Acc1 (Andar)", acc1.bet_type, bal1_bef), daemon=True)
        t2 = threading.Thread(target=_undo_one_account, args=(acc2, "Acc2 (Bahar)", acc2.bet_type, bal2_bef), daemon=True)
        t1.start()
        t2.start()
        t1.join(timeout=6)
        t2.join(timeout=6)

    async def _track_bet_result(self, table_names, bal1_before, bal2_before):
        await asyncio.sleep(45)

        # FIX: If hedge was undone, skip result tracking — don't overwrite the re-armed state
        if self._hedge_undone:
            logger.info("⏭️ Skipping _track_bet_result — hedge was undone and re-armed.")
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(_executor, self.account1.update_balance)
        await loop.run_in_executor(_executor, self.account2.update_balance)
        
        diff1 = self.account1.balance - bal1_before
        diff2 = self.account2.balance - bal2_before
        
        self.last_bet_result = {
            "type": "result",
            "tables": table_names,
            "profit1": diff1,
            "profit2": diff2,
            "total_profit": diff1 + diff2,
            "bal1": self.account1.balance,
            "bal2": self.account2.balance
        }
        self.bet_state = "idle"

        # Update the last bet_history record with P&L data
        if self.bet_history:
            last_record = self.bet_history[-1]
            if last_record.get('status') == 'placed':
                last_record['profit1'] = diff1
                last_record['profit2'] = diff2
                last_record['total_profit'] = diff1 + diff2
                last_record['bal1_after'] = self.account1.balance
                last_record['bal2_after'] = self.account2.balance
                last_record['status'] = 'completed'
                self._save_history_to_file()

    def _save_history_to_file(self):
        """Persist bet_history to a JSON file, keeping it lightweight."""
        try:
            if len(self.bet_history) > 150:
                self.bet_history = self.bet_history[-150:]
            with open('bet_history.json', 'w') as f:
                json.dump(self.bet_history, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save bet_history.json: {e}")


    def arm_auto_bet(self, mode="auto", amount=100.0):
        # Check cached balances instantly (non-blocking)
        if self.account1.balance > 0 and self.account1.balance < 50 or self.account2.balance > 0 and self.account2.balance < 50:
            return {"success": False, "message": "Insufficient balance in one or both accounts to arm."}

        self.bet_mode = mode
        self.bet_target_amount = float(amount)
        self.auto_bet_requested = True
        self.bet_state = "armed"
        
        # Trigger immediate check (0ms overhead)
        self.check_auto_bet()
        
        # Start continuous hunting loop — scans every 50ms like a hawk
        self._start_hunting()
        
        # Asynchronously refresh balances in background thread
        threading.Thread(target=self._async_update_balances, daemon=True).start()

        logger.info(f"🎯 AUTO-BET ARMED ({mode.upper()}: {amount:.0f} Rs) — Instant Hunter active, scanning every 50ms")
        return {"success": True, "message": f"Dual Auto-Bet armed ({mode.upper()}: {amount:.0f} Rs)! Hunting for tables..."}

    def _async_update_balances(self):
        try:
            self.account1.update_balance()
            self.account2.update_balance()
        except Exception:
            pass

    def _start_hunting(self):
        """Start the continuous hunting loop thread."""
        self._hunting_active = True
        if self._hunting_thread and self._hunting_thread.is_alive():
            return  # Already hunting
        self._hunting_thread = threading.Thread(target=self._hunter_loop, daemon=True)
        self._hunting_thread.start()
    
    def _stop_hunting(self):
        """Stop the hunting loop."""
        self._hunting_active = False
    
    def _hunter_loop(self):
        """Continuous scan loop — checks every 50ms while armed.
        Like a hawk waiting to pounce the INSTANT 2 tables are ready."""
        logger.info("🦅 Hunter loop STARTED — scanning every 50ms")
        while self._hunting_active and self.auto_bet_requested:
            self.check_auto_bet()
            # If bet was placed, stop hunting
            if self.bet_state == "placing" or self.bet_state == "placed":
                logger.info("🦅 Hunter loop STOPPED — bet fired!")
                return
            time.sleep(0.05)  # Ultra-fast 50ms scan interval
        logger.info("🦅 Hunter loop STOPPED — disarmed or completed")

    def disarm_auto_bet(self):
        self.auto_bet_requested  = False
        self.bet_state           = "idle"
        self._stop_hunting()  # Stop the hunter
        return {"success": True, "message": "Auto-Bet disarmed."}

    def fire_all_4_manual(self, mode="auto", amount=100.0):
        """⚡ Force immediate burst firing on ALL 4 bets across both target tables."""
        if self.account1.balance <= 0 or self.account2.balance <= 0:
            return {"success": False, "message": f"Cannot fire: Acc1 bal={self.account1.balance:.2f}, Acc2 bal={self.account2.balance:.2f}"}

        # Ensure we have connected tables on both accounts
        tables1 = [t for t in self.account1.tables.values() if t.get('ws') and getattr(t.get('ws'), 'open', True)]
        tables2 = [t for t in self.account2.tables.values() if t.get('ws') and getattr(t.get('ws'), 'open', True)]
        if len(tables1) < 2 or len(tables2) < 2:
            return {"success": False, "message": f"Need at least 2 connected tables per account. (Acc1: {len(tables1)}, Acc2: {len(tables2)})"}

        self.bet_mode = mode
        self.bet_target_amount = float(amount)
        self.auto_bet_requested = True
        self.bet_state = "armed"
        
        # Trigger immediate check to pounce on all 4 bets
        with self._bet_lock:
            self._check_auto_bet_locked()

        return {"success": True, "message": "⚡ Burst-fired ALL 4 bets on 2 tables!"}

    def check_burned_accounts(self):
        """Check if either account has gone negative. Called from balance polling.
        Disarms auto-bet, but does NOT automatically burn the account."""
        for acct_num, acct in [(1, self.account1), (2, self.account2)]:
            if acct.balance < 0:
                if self.auto_bet_requested or self.bet_state != "idle":
                    self.auto_bet_requested = False
                    self.bet_state = "idle"
                    logger.warning(
                        f"⚠️ ACCOUNT {acct_num} went negative ({acct.balance}). Auto-bet disarmed."
                    )

    @property
    def is_successful(self):
        """Check if either account's current balance is >= 3x of its initial balance."""
        cond1 = (self.initial_balance1 > 0 and self.account1.balance >= 3 * self.initial_balance1)
        cond2 = (self.initial_balance2 > 0 and self.account2.balance >= 3 * self.initial_balance2)
        return cond1 or cond2

    def relogin_account(self, account_num, username, password):
        """Re-login only the burned account with new credentials.
        The healthy account's session, WS connections, and tables remain untouched."""
        if account_num not in (1, 2):
            return {"success": False, "message": "Invalid account number (must be 1 or 2)"}

        base_url = self._last_login_base_url
        if not base_url:
            return {"success": False, "message": "No base URL stored — do a full login first"}

        # Stop ONLY the burned account
        old_acct = self.account1 if account_num == 1 else self.account2
        if old_acct.running:
            old_acct.stop()

        # Create fresh BaccaratManager for the burned account
        if account_num == 1:
            new_acct = BaccaratManager("Account 1 (Andar)", config.ANDAR_BET_TYPE)
            self.account1 = new_acct
            self.initial_balance1 = 0.0
        else:
            new_acct = BaccaratManager("Account 2 (Bahar)", config.BAHAR_BET_TYPE)
            self.account2 = new_acct
            self.initial_balance2 = 0.0

        new_acct._coordinator_ref = self

        # Login with new credentials
        result = new_acct.perform_login(base_url, username, password)
        if not result["success"]:
            return result

        # Clear burned state
        self.burned_account = None
        self._burn_detected_at = None

        logger.info(f"✅ Account {account_num} re-logged with new ID: {username}")
        return {
            "success": True,
            "message": f"Account {account_num} re-logged as {username}! Press Arm to resume."
        }

    # ══════════════════════════════════════════════════════════════════
    # 🧪 STACK TEST: Send 2 rapid bets on SAME table to test if server
    #    accumulates them (proof-of-concept for single-table stacking)
    # ══════════════════════════════════════════════════════════════════
    def test_stack_bet(self):
        """Send 2 × ₹50 Player bets on one table from Account 1 only.
        Checks if server accumulates (total ₹100) or replaces/rejects."""
        logger.info("🧪 [STACK TEST] Starting single-table stacking test...")

        # Find a table with betting window open on Account 1
        target_token = None
        target_name = None
        for token, info in list(self.account1.tables.items()):
            if info.get('is_betting_open') and info.get('ws'):
                ws = info.get('ws')
                transport = getattr(ws, 'transport', None)
                if transport:
                    target_token = token
                    target_name = info.get('name', token[:8])
                    break

        if not target_token:
            return {"success": False, "message": "No table with open betting window found. Wait for a table to open."}

        logger.info(f"🧪 [STACK TEST] Target: {target_name} | Sending 2 × ₹50 Player bets...")

        # Record balance before
        self.account1.update_balance()
        bal_before = self.account1.balance
        if bal_before < 100:
            return {"success": False, "message": f"Balance too low for test ({bal_before:.2f}). Need at least ₹100."}

        # Register pending acks
        self.account1.pending_bet_acks[target_token] = {'status': 'pending', 'raw': None}

        # Build 2 separate ₹50 bet frames
        frame1 = BaccaratManager._build_raw_bet_frame(50.0, config.ANDAR_BET_TYPE)
        frame2 = BaccaratManager._build_raw_bet_frame(50.0, config.ANDAR_BET_TYPE)

        ws = self.account1.tables[target_token]['ws']
        transport = ws.transport

        # 🚀 Fire both frames with GC disabled for maximum speed
        fire_start = time.time()
        gc.disable()
        try:
            self.account1.loop.call_soon_threadsafe(transport.write, frame1)
            self.account1.loop.call_soon_threadsafe(transport.write, frame2)
        finally:
            gc.enable()
        fire_elapsed = (time.time() - fire_start) * 1000

        logger.info(f"🧪 [STACK TEST] 2 × ₹50 bets FIRED in {fire_elapsed:.2f}ms on {target_name}")

        # Wait for confirmations (up to 2 seconds)
        results = []
        for poll in range(20):
            time.sleep(0.1)
            ack = self.account1.pending_bet_acks.get(target_token, {})
            status = ack.get('status', 'pending')
            if status != 'pending':
                results.append(status)
                # Reset and check for second ack
                self.account1.pending_bet_acks[target_token] = {'status': 'pending', 'raw': None}
                if len(results) >= 2:
                    break

        # Check balance after
        time.sleep(0.3)
        self.account1.update_balance()
        bal_after = self.account1.balance
        bal_diff = bal_before - bal_after

        # Analyze results
        if bal_diff >= 95:  # ~₹100 deducted (both bets accepted)
            verdict = "✅ STACKING WORKS! Server accumulated both bets."
            stacking_works = True
        elif bal_diff >= 45:  # ~₹50 deducted (only one bet accepted)
            verdict = "⚠️ Only 1 bet accepted. Server may replace or ignore duplicates."
            stacking_works = False
        elif bal_diff <= 5:  # No deduction (both rejected or undo needed)
            verdict = "❌ No bets accepted. Server rejected rapid stacking."
            stacking_works = False
        else:
            verdict = f"🤔 Unexpected balance change: ₹{bal_diff:.2f}"
            stacking_works = False

        result_msg = (
            f"🧪 STACK TEST RESULT\n"
            f"Table: {target_name}\n"
            f"Bets sent: 2 × ₹50 in {fire_elapsed:.2f}ms\n"
            f"Balance before: ₹{bal_before:.2f}\n"
            f"Balance after: ₹{bal_after:.2f}\n"
            f"Deducted: ₹{bal_diff:.2f}\n"
            f"Ack statuses: {results}\n"
            f"Verdict: {verdict}"
        )
        logger.info(f"🧪 [STACK TEST] {result_msg}")

        # Send UNDO to clean up the test bets
        try:
            undo_frame = BaccaratManager._build_raw_bet_frame(0.0, config.ANDAR_BET_TYPE)
            undo_payload = json.dumps({
                "arguments": [{"type": 1, "data": json.dumps({
                    "areBetsInZeroCommMode": False,
                    "bets": [],
                    "gameplayMessageType": 1
                })}],
                "target": "Message", "type": 1
            }) + '\x1e'
            if self.account1.loop:
                asyncio.run_coroutine_threadsafe(ws.send(undo_payload), self.account1.loop)
                logger.info(f"🧪 [STACK TEST] UNDO sent to clean up test bets")
        except Exception as e:
            logger.warning(f"🧪 [STACK TEST] UNDO cleanup failed: {e}")

        return {
            "success": True,
            "stacking_works": stacking_works,
            "message": result_msg,
            "details": {
                "table": target_name,
                "fire_ms": round(fire_elapsed, 2),
                "bal_before": bal_before,
                "bal_after": bal_after,
                "deducted": round(bal_diff, 2),
                "ack_statuses": results,
                "verdict": verdict
            }
        }



# We no longer instantiate GlobalCoordinator here.
# It will be instantiated per-session in app.py.
