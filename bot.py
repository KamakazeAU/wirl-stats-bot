# bot.py
# =========================
# Wallaby iRacing League — per-guild slash bot
# =========================
# Quick start:
# 1) pip install -U discord.py
# 2) Create config.json (or let the bot create a template on first run):
# {
#   "token": "YOUR_DISCORD_BOT_TOKEN",
#   "guild_id": "1399631207188271236",
#   "current_season": null,
#   "channels": {
#     "leaderboard": null,
#     "uploads": null,
#     "backups": null,
#     "logs": null
#   },
#   "roles": {
#     "admin": "Admin",
#     "stats": null,
#     "viewer": null
#   }
# }
# 3) python bot.py

import os
import io
import json
import zipfile
import datetime
import asyncio
from typing import Dict, List, Tuple, Optional

import discord
from discord.ext import commands, tasks
from discord import app_commands
import sys

# Google Drive imports (optional)
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from google.oauth2.service_account import Credentials
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False

# ========= Constants / Paths =========
CONFIG_FILE = "config.json"
DATA_ROOT = "data"
SEASONS_DIR = os.path.join(DATA_ROOT, "seasons")
BACKUPS_DIR = "backups"
BACKUP_STATE = os.path.join(BACKUPS_DIR, ".last_backup.txt")
RETENTION_DAYS = 7              # fixed
BACKUP_INTERVAL_DAYS = 7        # auto backup cadence
PAGE_SIZE = 20                  # drivers list pagination size
UPLOADS_STORE_DIR = os.path.join(DATA_ROOT, "uploads")

os.makedirs(DATA_ROOT, exist_ok=True)
os.makedirs(SEASONS_DIR, exist_ok=True)
os.makedirs(BACKUPS_DIR, exist_ok=True)
os.makedirs(UPLOADS_STORE_DIR, exist_ok=True)

# ========= Config I/O =========
def load_config() -> Dict:
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "token": "YOUR_DISCORD_BOT_TOKEN",
                "guild_id": "1399631207188271236",
                "current_season": None,
                "channels": {"leaderboard": None, "uploads": None, "backups": None, "logs": None},
                "roles": {"admin": "Admin", "stats": None, "viewer": None},
                "google_drive": {
                    "enabled": False,
                    "service_account_file": "service-account-key.json",
                    "folder_id": None,
                    "auto_backup": True
                },
                "discord_links": {}
            }, f, indent=2)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(cfg: Dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

# ========= Discord-iRacing Link Management =========
def link_discord_to_iracing(discord_id: int, iracing_name: str) -> bool:
    """Link a Discord user ID to an iRacing driver name"""
    try:
        config.setdefault("discord_links", {})[str(discord_id)] = iracing_name
        save_config(config)
        return True
    except Exception:
        return False

def unlink_discord(discord_id: int) -> bool:
    """Remove Discord-to-iRacing link"""
    try:
        if "discord_links" in config and str(discord_id) in config["discord_links"]:
            del config["discord_links"][str(discord_id)]
            save_config(config)
            return True
        return False
    except Exception:
        return False

def get_iracing_name(discord_id: int) -> Optional[str]:
    """Get iRacing name for a Discord user ID"""
    return config.get("discord_links", {}).get(str(discord_id))

def get_discord_id(iracing_name: str) -> Optional[int]:
    """Get Discord ID for an iRacing driver name"""
    iracing_name_clean = iracing_name.strip().lower()
    for discord_id, name in config.get("discord_links", {}).items():
        name_clean = name.strip().lower()
        # Enhanced matching for discord links
        if (name_clean == iracing_name_clean or
            name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
            iracing_name_clean in name_clean or name_clean in iracing_name_clean):
            return int(discord_id)
    return None

config = load_config()
# Try environment variable first, then fall back to config.json
TOKEN = os.getenv('DISCORD_TOKEN') or config.get("token") or ""
GUILD_ID_RAW = str(config.get("guild_id") or "").strip()
GUILD_ID = int(GUILD_ID_RAW) if GUILD_ID_RAW.isdigit() else None
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None

def tz_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)

def console_safe(text: str) -> str:
    """Return text encodable by current stdout encoding, dropping unsupported chars (e.g., emojis on cp1252)."""
    try:
        enc = sys.stdout.encoding or "utf-8"
        return text.encode(enc, errors="ignore").decode(enc, errors="ignore")
    except Exception:
        return text.encode("ascii", errors="ignore").decode("ascii", errors="ignore")

# ========= Google Drive Backup Functions =========
def get_google_drive_service():
    """Initialize Google Drive service with service account credentials"""
    if not GOOGLE_DRIVE_AVAILABLE:
        return None
    
    gdrive_config = config.get("google_drive", {})
    if not gdrive_config.get("enabled", False):
        return None
    
    service_account_file = gdrive_config.get("service_account_file", "service-account-key.json")
    if not os.path.exists(service_account_file):
        print(console_safe(f"❌ Google Drive service account file not found: {service_account_file}"))
        return None
    
    try:
        credentials = Credentials.from_service_account_file(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=credentials)
        return service
    except Exception as e:
        print(console_safe(f"❌ Failed to initialize Google Drive service: {e}"))
        return None

def upload_to_google_drive(file_path: str, folder_id: str = None) -> bool:
    """Upload a file to Google Drive"""
    if not GOOGLE_DRIVE_AVAILABLE:
        print(console_safe("❌ Google Drive libraries not installed. Run: pip install google-api-python-client google-auth"))
        return False
    
    service = get_google_drive_service()
    if not service:
        return False
    
    try:
        file_name = os.path.basename(file_path)
        
        # File metadata
        file_metadata = {'name': file_name}
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Upload file
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        print(console_safe(f"✅ Uploaded {file_name} to Google Drive (ID: {file.get('id')})"))
        return True
        
    except Exception as e:
        print(console_safe(f"❌ Failed to upload to Google Drive: {e}"))
        return False

async def backup_to_google_drive(backup_path: str) -> bool:
    """Backup a file to Google Drive asynchronously"""
    gdrive_config = config.get("google_drive", {})
    if not gdrive_config.get("enabled", False):
        return False
    
    folder_id = gdrive_config.get("folder_id")
    
    # Run the upload in a thread to avoid blocking the event loop
    import threading
    import asyncio
    
    def upload_task():
        return upload_to_google_drive(backup_path, folder_id)
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, upload_task)
    return result

async def auto_sync_stats_after_change(affected_seasons: list[str]):
    """
    Automatically sync stats and leaderboards after data changes.
    This function runs asynchronously to avoid blocking the main operations.
    """
    try:
        # Small delay to ensure file operations are complete
        await asyncio.sleep(0.1)
        
        # Log the auto-sync operation
        if isinstance(affected_seasons, str):
            # Single season
            print(console_safe(f"🔄 Auto-syncing stats for season: {affected_seasons}"))
        else:
            # Multiple seasons
            print(console_safe(f"🔄 Auto-syncing stats for {len(affected_seasons)} seasons: {', '.join(affected_seasons)}"))
        
        # The actual sync happens automatically when users view leaderboards/stats
        # since they read from the updated data files
        
    except Exception as e:
        print(console_safe(f"❌ Auto-sync error: {e}"))

# ========= Discord Bot Setup =========
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True  # not required for slash cmds but fine

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# Helper to mark commands as guild-only at definition time
GDEC = app_commands.guilds(GUILD) if GUILD else (lambda f: f)

# ========= Utilities: seasons & store =========
def list_seasons() -> List[str]:
    if not os.path.exists(SEASONS_DIR):
        return []
    return sorted([d for d in os.listdir(SEASONS_DIR) if os.path.isdir(os.path.join(SEASONS_DIR, d))])

def ensure_season_dir(season: str) -> str:
    p = os.path.join(SEASONS_DIR, season)
    os.makedirs(p, exist_ok=True)
    return p

def season_drivers_path(season: str) -> str:
    return os.path.join(ensure_season_dir(season), "drivers.json")

def load_season_drivers(season: str) -> Dict[str, Dict]:
    p = season_drivers_path(season)
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save_season_drivers(season: str, data: Dict[str, Dict]) -> None:
    p = season_drivers_path(season)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def flag_shortcode(cc: str) -> str:
    cc = (cc or "").strip().lower()
    return f":flag_{cc}:" if cc else ":checkered_flag:"

def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def current_season_name() -> Optional[str]:
    return config.get("current_season")

def set_current_season(name: Optional[str]):
    config["current_season"] = name
    save_config(config)

# ========= Ingest: iRacing JSON (event_result) =========
def ingest_iracing_event(payload: Dict, season: str) -> Tuple[int, int]:
    """
    Parse iRacing event_result JSON for the RACE session and update season stats.
    Returns (drivers_updated, rows_processed).
    """
    data = payload.get("data", {})
    sessions = data.get("session_results", [])
    race_sessions = [s for s in sessions if str(s.get("simsession_name", "")).upper() == "RACE"]
    if not race_sessions:
        raise ValueError("No RACE session found in payload['data']['session_results']")

    results = race_sessions[0].get("results", []) or []
    drivers_map = load_season_drivers(season)

    updated = 0
    processed = 0
    for row in results:
        name = str(row.get("display_name") or "").strip()
        if not name:
            continue

        processed += 1
        country = (row.get("country_code") or "").lower()

        # positions: 0-based in iRacing → convert to 1-based
        fin_1 = None
        if (fp := row.get("finish_position")) is not None and isinstance(fp, int) and fp >= 0:
            fin_1 = fp + 1

        start_1 = None
        sp = row.get("starting_position")
        if sp is not None and isinstance(sp, int) and sp >= 0:
            start_1 = sp + 1

        inc = float(row.get("incidents", 0) or 0)
        pts = float(row.get("champ_points", 0) or 0)

        d = drivers_map.setdefault(name, {
            "country": country, "races": 0, "wins": 0, "podiums": 0, "top10s": 0, "poles": 0,
            "points": 0.0, "avg_incidents": 0.0, "avg_start": 0.0, "avg_finish": 0.0, "_rp": 0
        })
        if country and not d.get("country"):
            d["country"] = country

        # increment races
        d["races"] = int(d.get("races", 0)) + 1

        # wins/podiums/top10
        if fin_1 is not None:
            if fin_1 == 1:
                d["wins"] = int(d.get("wins", 0)) + 1
            if fin_1 <= 3:
                d["podiums"] = int(d.get("podiums", 0)) + 1
            if fin_1 <= 10:
                # migrate from legacy top5s if present
                legacy = int(d.get("top10s", d.get("top5s", 0)))
                d["top10s"] = legacy + 1

        # poles
        if start_1 is not None and start_1 == 1:
            d["poles"] = int(d.get("poles", 0)) + 1

        # running weighted averages by race count
        rp = int(d.get("_rp", 0))
        ra = 1

        def wavg(prev, add, rp_, ra_):
            prev = safe_float(prev)
            add = safe_float(add)
            tot = rp_ + ra_
            return (prev * rp_ + add * ra_) / tot if tot > 0 else 0.0

        if fin_1 is not None:
            d["avg_finish"] = wavg(d.get("avg_finish", 0), fin_1, rp, ra)
        if start_1 is not None:
            d["avg_start"] = wavg(d.get("avg_start", 0), start_1, rp, ra)
        d["avg_incidents"] = wavg(d.get("avg_incidents", 0), inc, rp, ra)

        d["_rp"] = rp + ra
        d["points"] = safe_float(d.get("points", 0)) + pts

        updated += 1

    # round avgs
    for d in drivers_map.values():
        d["avg_incidents"] = round(safe_float(d.get("avg_incidents")), 3)
        d["avg_start"]     = round(safe_float(d.get("avg_start")), 3)
        d["avg_finish"]    = round(safe_float(d.get("avg_finish")), 3)

    save_season_drivers(season, drivers_map)
    
    # Auto-sync stats after data changes
    asyncio.create_task(auto_sync_stats_after_change(season))
    
    return updated, processed

# ========= Uploads store helpers =========
import hashlib

def _generate_content_hash(content: bytes) -> str:
    """Generate a SHA-256 hash of the JSON content for duplicate detection."""
    return hashlib.sha256(content).hexdigest()[:16]  # Use first 16 chars for readability

def _is_duplicate_json(content: bytes) -> tuple[bool, str]:
    """
    Check if JSON content is a duplicate of an existing file.
    Returns (is_duplicate, existing_filename).
    """
    content_hash = _generate_content_hash(content)
    
    # Check existing files for matching content
    for filename in os.listdir(UPLOADS_STORE_DIR):
        if not filename.lower().endswith(".json"):
            continue
        
        file_path = os.path.join(UPLOADS_STORE_DIR, filename)
        try:
            with open(file_path, "rb") as f:
                existing_content = f.read()
                if _generate_content_hash(existing_content) == content_hash:
                    return True, filename
        except Exception:
            continue
    
    return False, ""

async def _remove_ingested_data(file_content: bytes) -> list[str]:
    """
    Remove ingested data from seasons when a JSON file is deleted.
    Returns list of season names that were affected.
    """
    try:
        # Parse the JSON content to extract race data
        data = json.loads(file_content.decode("utf-8"))
        race_data = data.get("data", {})
        sessions = race_data.get("session_results", [])
        
        # Find RACE session
        race_sessions = [s for s in sessions if str(s.get("simsession_name", "")).upper() == "RACE"]
        if not race_sessions:
            return []
        
        results = race_sessions[0].get("results", []) or []
        if not results:
            return []
        
        # Extract driver names and race details from the deleted file
        deleted_races = {}
        for row in results:
            name = str(row.get("display_name") or "").strip()
            if not name:
                continue
            
            # Get race details for reversal
            fin_1 = None
            if (fp := row.get("finish_position")) is not None and isinstance(fp, int) and fp >= 0:
                fin_1 = fp + 1
            
            start_1 = None
            sp = row.get("starting_position")
            if sp is not None and isinstance(sp, int) and sp >= 0:
                start_1 = sp + 1
            
            inc = float(row.get("incidents", 0) or 0)
            pts = float(row.get("champ_points", 0) or 0)
            
            deleted_races[name] = {
                "finish": fin_1,
                "start": start_1,
                "incidents": inc,
                "points": pts
            }
        
        if not deleted_races:
            return []
        
        # Check all seasons for this data and remove it
        affected_seasons = []
        
        for season in list_seasons():
            season_path = season_drivers_path(season)
            if not os.path.exists(season_path):
                continue
            
            try:
                drivers_data = load_season_drivers(season)
                season_modified = False
                
                for driver_name, race_info in deleted_races.items():
                    if driver_name in drivers_data:
                        driver = drivers_data[driver_name]
                        
                        # Reverse the race data
                        if driver.get("races", 0) > 0:
                            driver["races"] = max(0, driver.get("races", 0) - 1)
                            season_modified = True
                        
                        # Reverse wins
                        if race_info["finish"] == 1 and driver.get("wins", 0) > 0:
                            driver["wins"] = max(0, driver.get("wins", 0) - 1)
                            season_modified = True
                        
                        # Reverse podiums
                        if race_info["finish"] is not None and race_info["finish"] <= 3 and driver.get("podiums", 0) > 0:
                            driver["podiums"] = max(0, driver.get("podiums", 0) - 1)
                            season_modified = True
                        
                        # Reverse top10s
                        if race_info["finish"] is not None and race_info["finish"] <= 10 and driver.get("top10s", 0) > 0:
                            driver["top10s"] = max(0, driver.get("top10s", 0) - 1)
                            season_modified = True
                        
                        # Reverse poles
                        if race_info["start"] == 1 and driver.get("poles", 0) > 0:
                            driver["poles"] = max(0, driver.get("poles", 0) - 1)
                            season_modified = True
                        
                        # Reverse points
                        if race_info["points"] > 0:
                            driver["points"] = max(0, driver.get("points", 0) - race_info["points"])
                            season_modified = True
                        
                        # Recalculate weighted averages
                        if driver.get("races", 0) > 0:
                            # For now, we'll set averages to 0 and let them be recalculated on next upload
                            # This is a simplified approach - in a full implementation you might want to
                            # store the original averages before each race and restore them
                            driver["avg_incidents"] = 0.0
                            driver["avg_start"] = 0.0
                            driver["avg_finish"] = 0.0
                            driver["_rp"] = 0
                        else:
                            # No races left, reset all stats
                            driver["avg_incidents"] = 0.0
                            driver["avg_start"] = 0.0
                            driver["avg_finish"] = 0.0
                            driver["_rp"] = 0
                            driver["wins"] = 0
                            driver["podiums"] = 0
                            driver["top10s"] = 0
                            driver["poles"] = 0
                            driver["points"] = 0.0
                        
                        # Remove driver if no races left
                        if driver.get("races", 0) <= 0:
                            del drivers_data[driver_name]
                            season_modified = True
                
                # Save the modified season data
                if season_modified:
                    save_season_drivers(season, drivers_data)
                    affected_seasons.append(season)
                    
            except Exception as e:
                # Log error but continue with other seasons
                print(f"Error processing season {season}: {e}")
                continue
        
        # Auto-sync stats after data removal
        if affected_seasons:
            asyncio.create_task(auto_sync_stats_after_change(affected_seasons))
        
        return affected_seasons
        
    except Exception as e:
        print(f"Error removing ingested data: {e}")
        return []

def _sanitize_filename(name: str) -> str:
    base = os.path.basename(name).strip().replace(" ", "_")
    # keep simple safe chars only
    safe = []
    for ch in base:
        if ch.isalnum() or ch in ("_", "-", "."):
            safe.append(ch)
    out = "".join(safe) or "file.json"
    if not out.lower().endswith(".json"):
        out += ".json"
    return out

def list_uploaded_jsons() -> list[str]:
    files = []
    for fn in os.listdir(UPLOADS_STORE_DIR):
        if fn.lower().endswith(".json"):
            files.append(fn)
    # sort by mtime desc
    files.sort(key=lambda fn: os.path.getmtime(os.path.join(UPLOADS_STORE_DIR, fn)), reverse=True)
    return files

# ========= Backups =========
def create_backup_zip() -> Tuple[discord.File, str]:
    stamp = tz_now().strftime("%Y-%m-%d_%H-%M")
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        if os.path.exists(CONFIG_FILE):
            z.write(CONFIG_FILE, arcname="config.json")
        for root, _, files in os.walk(DATA_ROOT):
            for file in files:
                fpath = os.path.join(root, file)
                arc = os.path.relpath(fpath, DATA_ROOT)
                z.write(fpath, arcname=os.path.join("data", arc))
    mem.seek(0)
    return discord.File(mem, filename=f"backup_{stamp}.zip"), stamp

def save_backup_to_disk() -> str:
    stamp = tz_now().strftime("%Y-%m-%d_%H-%M")
    out_path = os.path.join(BACKUPS_DIR, f"backup_{stamp}.zip")
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        if os.path.exists(CONFIG_FILE):
            z.write(CONFIG_FILE, arcname="config.json")
        for root, _, files in os.walk(DATA_ROOT):
            for file in files:
                fpath = os.path.join(root, file)
                arc = os.path.relpath(fpath, DATA_ROOT)
                z.write(fpath, arcname=os.path.join("data", arc))
    # retention cleanup
    cutoff = tz_now() - datetime.timedelta(days=RETENTION_DAYS)
    for fname in os.listdir(BACKUPS_DIR):
        if not (fname.startswith("backup_") and fname.endswith(".zip")):
            continue
        fpath = os.path.join(BACKUPS_DIR, fname)
        ts = datetime.datetime.fromtimestamp(os.path.getmtime(fpath), tz=datetime.UTC)
        if ts < cutoff:
            try:
                os.remove(fpath)
            except Exception:
                pass
    # store last backup timestamp
    try:
        with open(BACKUP_STATE, "w", encoding="utf-8") as f:
            f.write(tz_now().isoformat())
    except Exception:
        pass
    return out_path

def backup_due() -> bool:
    try:
        with open(BACKUP_STATE, "r", encoding="utf-8") as f:
            last = datetime.datetime.fromisoformat(f.read().strip())
        diff = tz_now() - last
        return diff.days >= BACKUP_INTERVAL_DAYS
    except Exception:
        # no state file → treat as not due to avoid immediate backup on boot
        return False

async def send_to_logs(embed: discord.Embed):
    logs_ch_id = (config.get("channels") or {}).get("logs")
    if logs_ch_id:
        ch = bot.get_channel(logs_ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(embed=embed)
            except Exception:
                pass

@tasks.loop(hours=24)
async def auto_backup():
    # Run daily, but only perform actual backup when >= 7 days since last
    if not backup_due():
        return
    path = save_backup_to_disk()
    emb = discord.Embed(
        title="Automatic Backup",
        description=f"Saved `{os.path.basename(path)}`",
        color=discord.Color.blurple(),
        timestamp=tz_now()
    )
    await send_to_logs(emb)

# ========= Formatting helpers =========
FILTERS = [
    ("💯 Points", "points"),
    ("🏆 Wins", "wins"),
    ("🚀 Poles", "poles"),
    ("🥈 Podiums", "podiums"),
    ("🔟 Top 10s", "top10s"),
    ("⚠️ Avg Incidents", "avg_incidents"),
    ("🚦 Avg Start", "avg_start"),
    ("🏁 Avg Finish", "avg_finish"),
]
FILTER_LABEL = {k: lbl for (lbl, k) in FILTERS}

def _sort_key(metric: str, row: dict) -> float:
    v = safe_float(row.get(metric, 0))
    if metric in ("avg_start", "avg_finish", "avg_incidents"):  # lower is better
        return -v if v != 0 else float("-inf")
    return v

def _aggregate_career() -> dict:
    out: dict = {}
    for s in list_seasons():
        sd = load_season_drivers(s)
        for name, d in sd.items():
            o = out.setdefault(name, {"country": d.get("country"), "races": 0, "wins": 0, "poles": 0,
                                      "podiums": 0, "top10s": 0, "points": 0.0,
                                      "avg_incidents": 0.0, "avg_start": 0.0, "avg_finish": 0.0, "_rp": 0})
            races_new = int(d.get("races", 0))
            o["races"] += races_new
            o["wins"]  += int(d.get("wins", 0))
            o["poles"] += int(d.get("poles", 0))
            o["podiums"] += int(d.get("podiums", 0))
            o["top10s"] += int(d.get("top10s", d.get("top5s", d.get("top5", 0))))
            o["points"] += safe_float(d.get("points", 0))
            # weighted avgs
            def wavg(prev, add, rp, ra):
                prev = safe_float(prev); add = safe_float(add); tot = rp + ra
                return (prev * rp + add * ra) / tot if tot > 0 else 0.0
            rp = int(o.get("_rp", 0)); ra = races_new
            o["avg_incidents"] = wavg(o["avg_incidents"], d.get("avg_incidents", 0), rp, ra)
            o["avg_start"]     = wavg(o["avg_start"],     d.get("avg_start", 0),     rp, ra)
            o["avg_finish"]    = wavg(o["avg_finish"],    d.get("avg_finish", 0),    rp, ra)
            o["_rp"] = rp + ra
    for d in out.values():
        d["avg_incidents"] = round(safe_float(d["avg_incidents"]), 2)
        d["avg_start"]     = round(safe_float(d["avg_start"]), 2)
        d["avg_finish"]    = round(safe_float(d["avg_finish"]), 2)
    return out

def _rows_from_dataset(dataset: dict, metric: str, limit: int = None) -> list[dict]:
    rows = []
    for name, d in dataset.items():
        rows.append({
            "name": name,
            "flag": flag_shortcode(d.get("country") or ""),
            "points": safe_float(d.get("points")),
            "races": int(d.get("races") or 0),
            "wins": int(d.get("wins") or 0),
            "poles": int(d.get("poles") or 0),
            "podiums": int(d.get("podiums") or 0),
            "top10s": int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0),
            "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
            "avg_start": round(safe_float(d.get("avg_start")), 2),
            "avg_finish": round(safe_float(d.get("avg_finish")), 2),
        })
    rows.sort(key=lambda r: _sort_key(metric, r), reverse=True)
    if limit:
        return rows[:limit]  # Return limited results if specified
    return rows  # Return all results if no limit specified

def get_drivers_around_position(dataset: dict, metric: str, target_name: str, context: int = 2) -> list[dict]:
    """
    Get drivers around a specific position in the leaderboard.
    Returns context drivers above and below the target driver.
    If user is in top positions, shows top 5 instead.
    """
    rows = []
    for name, d in dataset.items():
        rows.append({
            "name": name,
            "flag": flag_shortcode(d.get("country") or ""),
            "points": safe_float(d.get("points")),
            "races": int(d.get("races") or 0),
            "wins": int(d.get("wins") or 0),
            "poles": int(d.get("poles") or 0),
            "podiums": int(d.get("podiums") or 0),
            "top10s": int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0),
            "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
            "avg_start": round(safe_float(d.get("avg_start")), 2),
            "avg_finish": round(safe_float(d.get("avg_finish")), 2),
        })
    
    rows.sort(key=lambda r: _sort_key(metric, r), reverse=True)
    
    # Find target driver position with improved matching
    target_index = None
    target_name_clean = target_name.strip().lower()
    
    # Debug: Log the search attempt
    print(console_safe(f"🔍 get_drivers_around_position: Looking for '{target_name}' (cleaned: '{target_name_clean}')"))
    print(console_safe(f"🔍 Available names: {[row['name'] for row in rows[:5]]}..."))
    
    # Log all names for debugging (only if there are fewer than 50 names to avoid spam)
    if len(rows) <= 50:
        print(console_safe(f"🔍 All available names: {[row['name'] for row in rows]}"))
    else:
        print(console_safe(f"🔍 Total names: {len(rows)} (showing first 10): {[row['name'] for row in rows[:10]]}"))
    
    # First try exact match
    for i, row in enumerate(rows):
        if row["name"].strip().lower() == target_name_clean:
            target_index = i
            print(console_safe(f"✅ Exact match found at position {i}: '{row['name']}'"))
            break
    
    # If no exact match, try various fuzzy matching strategies
    if target_index is None:
        print(console_safe("🔍 No exact match, trying fuzzy matching..."))
        
        # Strategy 1: Remove all spaces and compare
        target_no_spaces = target_name_clean.replace(" ", "")
        for i, row in enumerate(rows):
            row_name_clean = row["name"].strip().lower()
            row_no_spaces = row_name_clean.replace(" ", "")
            if target_no_spaces == row_no_spaces:
                target_index = i
                print(console_safe(f"✅ Space-removed match found at position {i}: '{row['name']}'"))
                break
        
        # Strategy 2: Check if one name contains the other
        if target_index is None:
            for i, row in enumerate(rows):
                row_name_clean = row["name"].strip().lower()
                if (target_name_clean in row_name_clean or row_name_clean in target_name_clean):
                    target_index = i
                    print(console_safe(f"✅ Contains match found at position {i}: '{row['name']}'"))
                    break
        
        # Strategy 3: Check for common variations (numbers, special characters)
        if target_index is None:
            # Remove numbers and special characters for comparison
            target_clean = ''.join(c for c in target_name_clean if c.isalpha() or c.isspace())
            for i, row in enumerate(rows):
                row_name_clean = row["name"].strip().lower()
                row_clean = ''.join(c for c in row_name_clean if c.isalpha() or c.isspace())
                if target_clean == row_clean:
                    target_index = i
                    print(console_safe(f"✅ Cleaned match found at position {i}: '{row['name']}'"))
                    break
        
        # Strategy 4: Check for reversed first/last names
        if target_index is None:
            target_parts = target_name_clean.split()
            if len(target_parts) >= 2:
                target_reversed = f"{target_parts[-1]} {' '.join(target_parts[:-1])}"
                for i, row in enumerate(rows):
                    row_name_clean = row["name"].strip().lower()
                    if row_name_clean == target_reversed:
                        target_index = i
                        print(console_safe(f"✅ Reversed name match found at position {i}: '{row['name']}'"))
                        break
    
    if target_index is None:
        return []
    
    # Special handling for top positions
    if target_index < context:
        # User is in top positions (1st, 2nd, etc.), show top 5
        start = 0
        end = min(5, len(rows))
    else:
        # User is in middle/bottom, show context around them
        start = max(0, target_index - context)
        end = min(len(rows), target_index + context + 1)
    
    # Add global position to each row
    for i, row in enumerate(rows[start:end]):
        row["global_position"] = start + i + 1  # +1 because positions are 1-based
    
    return rows[start:end]

def render_driver_block(title_flag_name: str, d: Dict) -> str:
    title_line = f"**{title_flag_name}**\n" if title_flag_name else ""
    return (
        f"{title_line}"
        f"🏎️ Races: **{int(d.get('races', 0))}**\n"
        f"🏆 Wins: **{int(d.get('wins', 0))}**\n"
        f"🚀 Poles: **{int(d.get('poles', 0))}**\n"
        f"🥈 Podiums: **{int(d.get('podiums', 0))}**\n"
        f"🔟 Top 10s: **{int(d.get('top10s', d.get('top5s', d.get('top5', 0))))}**\n"
        f"⚠️ Avg Incidents: **{round(safe_float(d.get('avg_incidents')), 2)}**\n"
        f"🚦 Avg Start: **{round(safe_float(d.get('avg_start')), 2)}**\n"
        f"🏁 Avg Finish: **{round(safe_float(d.get('avg_finish')), 2)}**\n"
        f"💯 Points: **{int(safe_float(d.get('points')))}**"
    )

def render_leaderboard_embed(season_label: str, rows: list[dict], metric: str) -> discord.Embed:
    title = f"📊 {season_label} — {FILTER_LABEL[metric]}"
    emb = discord.Embed(title=title, color=discord.Color.gold())
    if not rows:
        emb.description = "_No drivers found._"
        return emb
    blocks = []
    for i, r in enumerate(rows, 1):
        blocks.append(render_driver_block(f"{i}. {r['flag']} {r['name']}", r))
    emb.description = "\n\n".join(blocks)
    emb.set_footer(text=f"Sorted by: {FILTER_LABEL[metric]}")
    return emb

def render_stats_embed(driver_name: str, season: str, season_map: Dict[str, Dict], career_map: Dict[str, Dict]) -> Optional[discord.Embed]:
    sd = season_map.get(driver_name)
    cd = career_map.get(driver_name)
    if not sd and not cd:
        return None
    flag = flag_shortcode((sd or {}).get("country") or (cd or {}).get("country") or "")
    emb = discord.Embed(title=f"{flag} {driver_name}", color=discord.Color.blurple())
    if sd:
        emb.add_field(name=f"📅 {season}", value=render_driver_block("", sd), inline=False)
    if cd:
        emb.add_field(name="🏁 Career", value=render_driver_block("", cd), inline=False)
    return emb

# ========= Views: Leaderboard (Season dropdown above Metric dropdown) =========
class SeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str]):
        options = [discord.SelectOption(label="All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=s, value=s, default=(current == s)))
        super().__init__(placeholder="Season", options=options, min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # If we're on a paginated view, return to main leaderboard (page 1)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Create new LeaderboardView with selected season
            new_view = LeaderboardView(self.values[0], view.metric)
            label, data = new_view._dataset()
            rows = _rows_from_dataset(data, new_view.metric, limit=5)
            emb = render_leaderboard_embed(label, rows, new_view.metric)
            await interaction.response.edit_message(embed=emb, view=new_view)
        else:
            # Normal refresh for LeaderboardView
            view.season_choice = self.values[0]
            # keep selected option highlighted
            for opt in self.options:
                opt.default = (opt.value == view.season_choice)
            await view.refresh(interaction)

class MetricDropdown(discord.ui.Select):
    def __init__(self, metric: str):
        options = [discord.SelectOption(label=lbl, value=key, default=(key == metric)) for (lbl, key) in FILTERS]
        super().__init__(placeholder="Metric", options=options, min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # If we're on a paginated view, return to main leaderboard (page 1)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Create new LeaderboardView with selected metric
            new_view = LeaderboardView(view.season_choice, self.values[0])
            label, data = new_view._dataset()
            rows = _rows_from_dataset(data, new_view.metric, limit=5)
            emb = render_leaderboard_embed(label, rows, new_view.metric)
            await interaction.response.edit_message(embed=emb, view=new_view)
        else:
            # Normal refresh for LeaderboardView
            view.metric = self.values[0]
            # keep selected option highlighted
            for opt in self.options:
                opt.default = (opt.value == view.metric)
            await view.refresh(interaction)

class FindMeButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Find Me", row=2)

    async def callback(self, interaction: discord.Interaction):
        view: "LeaderboardView" = self.view  # type: ignore
        
        # Check if user has linked their Discord to iRacing
        iracing_name = get_iracing_name(interaction.user.id)
        if not iracing_name:
            await interaction.response.send_message(
                "🔗 **Link Required**\n\nYou need to link your Discord account to your iRacing name first.\n\n"
                "Use `/link_account <iRacing_name>` to create the link.",
                ephemeral=True
            )
            return
        
        # Debug: Log the search attempt
        print(console_safe(f"🔍 Find Me: User {interaction.user.name} ({interaction.user.id}) searching for '{iracing_name}'"))
        
        # Get dataset and find user's position
        label, data = view._dataset()
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Debug: Log dataset info
        print(console_safe(f"🔍 Find Me: Dataset '{label}' has {len(data)} drivers"))
        if len(data) > 0:
            sample_names = list(data.keys())[:3]
            print(console_safe(f"🔍 Find Me: Sample names: {', '.join(sample_names)}"))
        
        # Check if user exists in the data with enhanced matching
        iracing_name_clean = iracing_name.strip().lower()
        user_found = False
        
        # Debug: Log the search attempt
        print(console_safe(f"🔍 Find Me: Looking for '{iracing_name}' (cleaned: '{iracing_name_clean}') in {len(data)} drivers"))
        
        for name in data.keys():
            name_clean = name.strip().lower()
            # Try multiple matching strategies
            if (name_clean == iracing_name_clean or
                name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                iracing_name_clean in name_clean or name_clean in iracing_name_clean):
                user_found = True
                print(console_safe(f"✅ Find Me: Found match: '{name}'"))
                break
        
        if not user_found:
            # Provide more helpful debugging information
            available_names = list(data.keys())[:10]  # Show first 10 names for debugging
            
            # Check for potential name variations
            potential_matches = []
            iracing_clean = iracing_name.lower().replace(" ", "")
            for name in available_names:
                name_clean = name.lower().replace(" ", "")
                if (iracing_clean in name_clean or name_clean in iracing_clean or 
                    iracing_clean.replace(" ", "") == name_clean.replace(" ", "")):
                    potential_matches.append(name)
            
            error_msg = f"❌ **Could not find your iRacing name**\n\n"
            error_msg += f"**Looking for:** `{iracing_name}`\n"
            error_msg += f"**Available names in data:** {', '.join(available_names[:5])}{'...' if len(available_names) > 5 else ''}\n\n"
            
            if potential_matches:
                error_msg += f"**🔍 Potential matches found:** {', '.join(potential_matches[:3])}\n\n"
            
            error_msg += "**Possible issues:**\n"
            error_msg += "• Name spelling/formatting differences\n"
            error_msg += "• You haven't raced in this season yet\n"
            error_msg += "• Season data needs to be updated\n\n"
            error_msg += "**Try:**\n"
            error_msg += "• Check your exact iRacing name spelling\n"
            error_msg += "• Use `/my_link` to verify your linked name\n"
            error_msg += "• Try a different season or 'All Time' view\n"
            if potential_matches:
                error_msg += f"• Check if your name might be: {', '.join(potential_matches[:2])}"
            
            await interaction.response.send_message(error_msg, ephemeral=True)
            return
        
        # Get all drivers sorted by metric for pagination
        rows = _rows_from_dataset(data, view.metric)
        if not rows:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Find user's position in the full list with enhanced matching
        user_position = None
        iracing_name_clean = iracing_name.strip().lower()
        
        # Debug: Log the search in pagination
        print(console_safe(f"🔍 FindMe pagination: Looking for '{iracing_name}' (cleaned: '{iracing_name_clean}') in {len(rows)} rows"))
        
        for i, row in enumerate(rows):
            row_name_clean = row["name"].strip().lower()
            
            # Try multiple matching strategies
            if (row_name_clean == iracing_name_clean or
                row_name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                iracing_name_clean in row_name_clean or row_name_clean in iracing_name_clean):
                user_position = i
                print(console_safe(f"✅ FindMe pagination: Found match at position {i}: '{row['name']}'"))
                break
        
        if user_position is None:
            available_names = list(data.keys())[:10]
            
            # Check for potential name variations
            potential_matches = []
            iracing_clean = iracing_name.lower().replace(" ", "")
            for name in available_names:
                name_clean = name.lower().replace(" ", "")
                if (iracing_clean in name_clean or name_clean in iracing_clean or 
                    iracing_clean.replace(" ", "") == name_clean.replace(" ", "")):
                    potential_matches.append(name)
            
            error_msg = f"❌ **Could not find your iRacing name**\n\n"
            error_msg += f"**Looking for:** `{iracing_name}`\n"
            error_msg += f"**Available names in data:** {', '.join(available_names[:5])}{'...' if len(available_names) > 5 else ''}\n\n"
            
            if potential_matches:
                error_msg += f"**🔍 Potential matches found:** {', '.join(potential_matches[:3])}\n\n"
            
            error_msg += "**Possible issues:**\n"
            error_msg += "• Name spelling/formatting differences\n"
            error_msg += "• You haven't raced in this season yet\n"
            error_msg += "• Season data needs to be updated\n\n"
            error_msg += "**Try:**\n"
            error_msg += "• Check your exact iRacing name spelling\n"
            error_msg += "• Use `/my_link` to verify your linked name\n"
            error_msg += "• Try a different season or 'All Time' view\n"
            if potential_matches:
                error_msg += f"• Check if your name might be: {', '.join(potential_matches[:2])}"
            
            await interaction.response.send_message(error_msg, ephemeral=True)
            return
        
        # Calculate pagination to center the user's driver on the page
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        # Calculate starting position to center the user (2 drivers above, 2 below when possible)
        # Target: user should be in position 2-4 of the 5 visible drivers (0-indexed: 1-3)
        target_user_page_pos = 2  # Aim for middle position (0-indexed: 2)
        
        # Calculate the page that centers the user
        start_pos = max(0, user_position - target_user_page_pos)
        end_pos = min(start_pos + 5, total_drivers)
        
        # Adjust if we're near the end of the list
        if end_pos == total_drivers and total_drivers > 5:
            start_pos = max(0, total_drivers - 5)
        
        # Calculate which page this represents
        user_page = start_pos // 5
        
        # Get drivers for the centered view
        centered_drivers = rows[start_pos:end_pos]
        
        # Create embed showing centered view around user
        emb = discord.Embed(
            title=f"🎯 {label} — {iracing_name} (Page {user_page + 1}/{total_pages})",
            description=f"Showing drivers in positions {start_pos + 1}-{end_pos} of {total_drivers}",
            color=discord.Color.green()
        )
        
        # Format the display for centered view
        blocks = []
        for i, row in enumerate(centered_drivers):
            global_pos = start_pos + i + 1
            
            # Check if this is the linked driver with enhanced matching
            row_name_clean = row["name"].strip().lower()
            iracing_name_clean = iracing_name.strip().lower()
            is_linked_driver = (row_name_clean == iracing_name_clean or
                               row_name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                               iracing_name_clean in row_name_clean or row_name_clean in iracing_name_clean)
            
            if is_linked_driver:
                display_name = f"👤 {global_pos} {row['flag']} {row['name']}"
            else:
                display_name = f"{global_pos} {row['flag']} {row['name']}"
            
            blocks.append(render_driver_block(display_name, row))
        
        emb.description = "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {FILTER_LABEL[view.metric]}")
        
        # Create a view with dropdowns and pagination for the Find Me results
        find_me_view = FindMeResultsView(view.season_choice, view.metric, iracing_name, user_page)
        await interaction.response.send_message(embed=emb, view=find_me_view, ephemeral=True)

class NextPageButton(discord.ui.Button):
    def __init__(self, current_page: int = 1):
        super().__init__(style=discord.ButtonStyle.primary, label="Next Page", row=2)
        self.current_page = current_page

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Get dataset
        if hasattr(view, '_dataset'):
            label, data = view._dataset()
        else:
            # Handle different view types
            if hasattr(view, 'season_choice') and hasattr(view, 'metric'):
                if view.season_choice == "__CAREER__":
                    label, data = ("All Time", _aggregate_career())
                else:
                    label, data = (view.season_choice, load_season_drivers(view.season_choice))
            else:
                await interaction.response.send_message("❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Calculate next page details
        next_page = self.current_page + 1
        start_pos = (next_page - 1) * 5
        end_pos = start_pos + 5
        
        # Get next 5 drivers
        rows = []
        for name, d in data.items():
            rows.append({
                "name": name,
                "flag": flag_shortcode(d.get("country") or ""),
                "points": safe_float(d.get("points")),
                "races": int(d.get("races") or 0),
                "wins": int(d.get("wins") or 0),
                "poles": int(d.get("poles") or 0),
                "podiums": int(d.get("podiums") or 0),
                "top10s": int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0),
                "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
                "avg_start": round(safe_float(d.get("avg_start")), 2),
                "avg_finish": round(safe_float(d.get("avg_finish")), 2),
            })
        
        rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
        
        # Get next 5 drivers
        next_rows = rows[start_pos:end_pos] if len(rows) > start_pos else []
        
        if not next_rows:
            await interaction.response.send_message("❌ No more drivers to show.", ephemeral=True)
            return
        
        # Add global positions
        for i, row in enumerate(next_rows):
            row["global_position"] = start_pos + i + 1
        
        # Create embed for next page
        emb = discord.Embed(
            title=f"📊 {label} — {FILTER_LABEL[view.metric]} (Page {next_page})",
            description=f"Showing drivers in positions {start_pos + 1}-{end_pos}",
            color=discord.Color.gold()
        )
        
        # Format the display
        blocks = []
        for row in next_rows:
            # Check if this is the linked driver
            linked_driver_name = get_iracing_name(interaction.user.id)
            if linked_driver_name and row['name'].lower() == linked_driver_name.lower():
                display_name = f"👤 {row['global_position']} {row['flag']} {row['name']}"
            else:
                display_name = f"{row['global_position']} {row['flag']} {row['name']}"
            blocks.append(render_driver_block(display_name, row))
        
        emb.description = "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {FILTER_LABEL[view.metric]}")
        
        # Check if there are more pages after this
        has_more_pages = len(rows) > end_pos
        
        # Create view with Previous Page button and Next Page if more available
        next_page_view = NextPageView(view.season_choice, view.metric, has_more_pages, next_page)
        await interaction.response.edit_message(embed=emb, view=next_page_view)

class PreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Previous Page", row=2)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Check if we're in a NextPageView (pagination context)
        if hasattr(view, 'current_page') and view.current_page > 2:
            # We're on page 3 or higher, go to previous page
            previous_page = view.current_page - 1
            start_pos = (previous_page - 1) * 5
            end_pos = start_pos + 5
            
            # Get dataset
            if hasattr(view, '_dataset'):
                label, data = view._dataset()
            else:
                # Handle different view types
                if hasattr(view, 'season_choice') and hasattr(view, 'metric'):
                    if view.season_choice == "__CAREER__":
                        label, data = ("All Time", _aggregate_career())
                    else:
                        label, data = (view.season_choice, load_season_drivers(view.season_choice))
                else:
                    await interaction.response.send_message("❌ Error: Cannot determine dataset.", ephemeral=True)
                    return
            
            if not data:
                await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
                return
            
            # Get all drivers and calculate previous page
            rows = []
            for name, d in data.items():
                rows.append({
                    "name": name,
                    "flag": flag_shortcode(d.get("country") or ""),
                    "points": safe_float(d.get("points")),
                    "races": int(d.get("races") or 0),
                    "wins": int(d.get("wins") or 0),
                    "poles": int(d.get("poles") or 0),
                    "podiums": int(d.get("podiums") or 0),
                    "top10s": int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0),
                    "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
                    "avg_start": round(safe_float(d.get("avg_start")), 2),
                    "avg_finish": round(safe_float(d.get("avg_finish")), 2),
                })
            
            rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
            
            # Get previous page drivers
            previous_page_drivers = rows[start_pos:end_pos]
            
            # Create embed for previous page
            emb = discord.Embed(
                title=f"📊 {label} — {FILTER_LABEL[view.metric]} (Page {previous_page})",
                description=f"Showing drivers in positions {start_pos + 1}-{end_pos}",
                color=discord.Color.gold()
            )
            
            # Format the display
            blocks = []
            for i, row in enumerate(previous_page_drivers):
                global_pos = start_pos + i + 1
                
                # Check if this is the linked driver
                linked_driver_name = get_iracing_name(interaction.user.id)
                if linked_driver_name and row['name'].lower() == linked_driver_name.lower():
                    display_name = f"👤 {global_pos} {row['flag']} {row['name']}"
                else:
                    display_name = f"{global_pos} {row['flag']} {row['name']}"
                
                blocks.append(render_driver_block(display_name, row))
            
            emb.description = "\n\n".join(blocks)
            emb.set_footer(text=f"Sorted by: {FILTER_LABEL[view.metric]}")
            
            # Check if there are more pages after this
            has_more_pages = len(rows) > end_pos
            
            # Create view with Previous Page button and Next Page if more available
            previous_page_view = NextPageView(view.season_choice, view.metric, has_more_pages, previous_page)
            await interaction.response.edit_message(embed=emb, view=previous_page_view)
            
        else:
            # We're on page 2, go back to top 5 (page 1)
            label, data = view._dataset()
            if not data:
                await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
                return
            
            # Get top 5 drivers
            rows = _rows_from_dataset(data, view.metric, limit=5)
            emb = render_leaderboard_embed(label, rows, view.metric)
            
            # Create view with Next Page button
            main_view = LeaderboardView(view.season_choice, view.metric)
            await interaction.response.edit_message(embed=emb, view=main_view)

class GoToTopButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Go To Top", row=3)
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Get dataset and show top 5 drivers
        if hasattr(view, '_dataset'):
            label, data = view._dataset()
        else:
            # Handle different view types
            if hasattr(view, 'season_choice') and hasattr(view, 'metric'):
                if view.season_choice == "__CAREER__":
                    label, data = ("All Time", _aggregate_career())
                else:
                    label, data = (view.season_choice, load_season_drivers(view.season_choice))
            else:
                await interaction.response.send_message("❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Get top 5 drivers
        rows = _rows_from_dataset(data, view.metric, limit=5)
        emb = render_leaderboard_embed(label, rows, view.metric)
        
        # Create new main view
        main_view = LeaderboardView(view.season_choice, view.metric)
        await interaction.response.edit_message(embed=emb, view=main_view)

class GoToBottomButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Go To Bottom", row=3)
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Get dataset
        if hasattr(view, '_dataset'):
            label, data = view._dataset()
        else:
            # Handle different view types
            if hasattr(view, 'season_choice') and hasattr(view, 'metric'):
                if view.season_choice == "__CAREER__":
                    label, data = ("All Time", _aggregate_career())
                else:
                    label, data = (view.season_choice, load_season_drivers(view.season_choice))
            else:
                await interaction.response.send_message("❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Get all drivers and calculate last page
        rows = []
        for name, d in data.items():
            rows.append({
                "name": name,
                "flag": flag_shortcode(d.get("country") or ""),
                "points": safe_float(d.get("points")),
                "races": int(d.get("races") or 0),
                "wins": int(d.get("wins") or 0),
                "poles": int(d.get("poles") or 0),
                "podiums": int(d.get("podiums") or 0),
                "top10s": int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0),
                "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
                "avg_start": round(safe_float(d.get("avg_start")), 2),
                "avg_finish": round(safe_float(d.get("avg_finish")), 2),
            })
        
        rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
        
        # Calculate last page
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        start_pos = (total_pages - 1) * 5  # Last page start position
        end_pos = total_drivers
        
        # Get last page drivers
        last_page_drivers = rows[start_pos:end_pos]
        
        # Create embed for last page
        emb = discord.Embed(
            title=f"📊 {label} — {FILTER_LABEL[view.metric]} (Page {total_pages}/{total_pages})",
            description=f"Showing drivers in positions {start_pos + 1}-{end_pos} of {total_drivers}",
            color=discord.Color.gold()
        )
        
        # Format the display
        blocks = []
        for i, row in enumerate(last_page_drivers):
            global_pos = start_pos + i + 1
            
            # Check if this is the linked driver
            linked_driver_name = get_iracing_name(interaction.user.id)
            if linked_driver_name and row['name'].lower() == linked_driver_name.lower():
                display_name = f"👤 {global_pos} {row['flag']} {row['name']}"
            else:
                display_name = f"{global_pos} {row['flag']} {row['name']}"
            
            blocks.append(render_driver_block(display_name, row))
        
        emb.description = "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {FILTER_LABEL[view.metric]}")
        
        # Create view with Previous Page button (no Next Page since we're at the end)
        last_page_view = NextPageView(view.season_choice, view.metric, has_more_pages=False, current_page=total_pages)
        await interaction.response.edit_message(embed=emb, view=last_page_view)

class NextPageView(discord.ui.View):
    def __init__(self, season_choice: Optional[str], metric: str, has_more_pages: bool = False, current_page: int = 2):
        super().__init__(timeout=900)
        self.season_choice = season_choice
        self.metric = metric
        self.has_more_pages = has_more_pages
        self.current_page = current_page
        self.add_item(SeasonDropdown(self.season_choice))
        self.add_item(MetricDropdown(self.metric))
        self.add_item(FindMeButton())
        self.add_item(PreviousPageButton())
        self.add_item(GoToTopButton())
        self.add_item(GoToBottomButton())
        
        # Add Next Page button if there are more pages
        if has_more_pages:
            self.add_item(NextPageButton(current_page=self.current_page))

    def _dataset(self) -> tuple[str, dict]:
        # NextPageView dataset method
        if self.season_choice == "__CAREER__":
            return ("All Time", _aggregate_career())
        if not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
        if not self.season_choice:
            return ("All Time", {})
        
        return (self.season_choice, load_season_drivers(self.season_choice))

class BackToTopButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Back To Top", row=2)

    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        
        # Create a new main leaderboard view
        main_view = LeaderboardView(view.season_choice, view.metric)
        
        # Get dataset and show top 5 leaderboard
        label, data = main_view._dataset()
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Get top 5 drivers
        rows = _rows_from_dataset(data, view.metric, limit=5)
        emb = render_leaderboard_embed(label, rows, view.metric)
        
        # Update the message to show the normal leaderboard
        try:
            await interaction.response.edit_message(embed=emb, view=main_view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=main_view)

class FindMeResultsView(discord.ui.View):
    def __init__(self, season_choice: Optional[str], metric: str, iracing_name: str, start_page: int = 0):
        super().__init__(timeout=900)
        self.season_choice = season_choice
        self.metric = metric
        self.iracing_name = iracing_name
        self.page = start_page  # Start at the user's actual page
        self.add_item(FindMeSeasonDropdown(self.season_choice, self.iracing_name))
        self.add_item(FindMeMetricDropdown(self.metric, self.iracing_name))
        self.add_item(FindMePreviousPageButton())
        self.add_item(FindMeNextPageButton())
        self.add_item(BackToTopButton())
        self.add_item(GoToBottomButton())
        


    async def refresh_find_me(self, interaction: discord.Interaction):
        """Refresh the Find Me results with new season/metric selection"""
        
        # Get dataset and find user's position
        if self.season_choice == "__CAREER__":
            label, data = ("All Time", _aggregate_career())
        elif not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
            if not self.season_choice:
                label, data = ("All Time", {})
            else:
                label = self.season_choice
                data = load_season_drivers(self.season_choice)
        else:
            label = self.season_choice
            data = load_season_drivers(self.season_choice)
        
        if not data:
            await interaction.response.edit_message(content="❌ No data found for the selected season.", embed=None, view=None)
            return
        
        # Get all drivers sorted by metric
        rows = _rows_from_dataset(data, self.metric)
        if not rows:
            available_names = list(data.keys())[:10]
            await interaction.response.edit_message(
                content=f"❌ **Could not find your iRacing name**\n\n"
                f"**Looking for:** `{self.iracing_name}`\n"
                "**Available names in data:** {', '.join(available_names[:5])}{'...' if len(available_names) > 5 else ''}\n\n"
                "**Possible issues:**\n"
                "• Name spelling/formatting differences\n"
                "• You haven't raced in this season yet\n"
                "• Season data needs to be updated\n\n"
                "**Try:**\n"
                "• Check your exact iRacing name spelling\n"
                "• Use `/my_link` to verify your linked name\n"
                "• Try a different season or 'All Time' view",
                embed=None,
                view=None
            )
            return
        
        # Find user's position in the full list with enhanced matching
        user_position = None
        iracing_name_clean = self.iracing_name.strip().lower()
        for i, row in enumerate(rows):
            row_name_clean = row["name"].strip().lower()
            # Enhanced matching for FindMeResultsView
            if (row_name_clean == iracing_name_clean or
                row_name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                iracing_name_clean in row_name_clean or row_name_clean in iracing_name_clean):
                user_position = i
                break
        
        if user_position is None:
            available_names = list(data.keys())[:10]
            await interaction.response.edit_message(
                content=f"❌ **Could not find your iRacing name**\n\n"
                f"**Looking for:** `{self.iracing_name}`\n"
                f"**Available names in data:** {', '.join(available_names[:5])}{'...' if len(available_names) > 5 else ''}\n\n"
                "**Possible issues:**\n"
                "• Name spelling/formatting differences\n"
                "• You haven't raced in this season yet\n"
                "• Season data needs to be updated\n\n"
                "**Try:**\n"
                "• Check your exact iRacing name spelling\n"
                "• Use `/my_link` to verify your linked name\n"
                "• Try a different season or 'All Time' view",
                embed=None,
                view=None
            )
            return
        
        # Calculate pagination to center the user's driver on the page
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        # Calculate starting position to center the user (2 drivers above, 2 below when possible)
        target_user_page_pos = 2  # Aim for middle position (0-indexed: 2)
        
        # Calculate the page that centers the user
        start_pos = max(0, user_position - target_user_page_pos)
        end_pos = min(start_pos + 5, total_drivers)
        
        # Adjust if we're near the end of the list
        if end_pos == total_drivers and total_drivers > 5:
            start_pos = max(0, total_drivers - 5)
        
        # Update the page number for pagination buttons
        self.page = start_pos // 5
        
        # Get drivers for the centered view
        centered_drivers = rows[start_pos:end_pos]
        
        # Create embed showing current page
        emb = discord.Embed(
            title=f"🎯 {label} — {self.iracing_name} (Page {self.page + 1}/{total_pages})",
            description=f"Showing drivers in positions {start_pos + 1}-{end_pos} of {total_drivers}",
            color=discord.Color.green()
        )
        
        # Format the display for centered view
        blocks = []
        for i, row in enumerate(centered_drivers):
            global_pos = start_pos + i + 1
            
            # Check if this is the linked driver with enhanced matching
            row_name_clean = row["name"].strip().lower()
            iracing_name_clean = self.iracing_name.strip().lower()
            is_linked_driver = (row_name_clean == iracing_name_clean or
                               row_name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                               iracing_name_clean in row_name_clean or row_name_clean in iracing_name_clean)
            
            if is_linked_driver:
                display_name = f"👤 {global_pos} {row['flag']} {row['name']}"
            else:
                display_name = f"{global_pos} {row['flag']} {row['name']}"
            
            blocks.append(render_driver_block(display_name, row))
        
        emb.description = "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {FILTER_LABEL[self.metric]}")
        
        # Update button states
        for item in self.children:
            if isinstance(item, FindMePreviousPageButton):
                item.disabled = (self.page <= 0)
            elif isinstance(item, FindMeNextPageButton):
                item.disabled = (self.page >= total_pages - 1)
        
        await interaction.response.edit_message(embed=emb, view=self)

class FindMePreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Previous Page", row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        if view.page > 0:
            view.page -= 1
        await view.refresh_find_me(interaction)

class FindMeNextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Next Page", row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        
        # Get dataset to check if there are more pages
        if view.season_choice == "__CAREER__":
            label, data = ("All Time", _aggregate_career())
        elif not view.season_choice:
            ss = list_seasons()
            view.season_choice = ss[-1] if ss else None
            if not view.season_choice:
                label, data = ("All Time", {})
            else:
                label, data = (view.season_choice, load_season_drivers(view.season_choice))
        else:
            label, data = (view.season_choice, load_season_drivers(view.season_choice))
        
        if not data:
            await interaction.response.send_message("❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Check if there are more pages
        total_drivers = len(data)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        if view.page < total_pages - 1:
            view.page += 1
            await view.refresh_find_me(interaction)
        else:
            await interaction.response.send_message("❌ No more pages to show.", ephemeral=True)

class FindMeSeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str], iracing_name: str):
        self.iracing_name = iracing_name
        options = [discord.SelectOption(label="All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=s, value=s, default=(current == s)))
        super().__init__(placeholder="Season", options=options, min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        view.season_choice = self.values[0]
        
        # Update selected option highlighting
        for opt in self.options:
            opt.default = (opt.value == view.season_choice)
        
        # Refresh the Find Me results
        await view.refresh_find_me(interaction)

class FindMeMetricDropdown(discord.ui.Select):
    def __init__(self, metric: str, iracing_name: str):
        self.iracing_name = iracing_name
        options = [discord.SelectOption(label=lbl, value=key, default=(key == metric)) for (lbl, key) in FILTERS]
        super().__init__(placeholder="Metric", options=options, min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        view.metric = self.values[0]
        
        # Update selected option highlighting
        for opt in self.options:
            opt.default = (opt.value == view.metric)
        
        # Refresh the Find Me results
        await view.refresh_find_me(interaction)
        label, data = (view.season_choice, load_season_drivers(view.season_choice))
        
        if not data:
            await interaction.response.edit_message(content="❌ No data found for the selected season.", embed=None, view=None)
            return
        
        # Get drivers around user's position
        drivers_around = get_drivers_around_position(data, view.metric, view.iracing_name, context=2)
        if not drivers_around:
            available_names = list(data.keys())[:10]
            await interaction.response.edit_message(
                content=f"❌ **Could not find your iRacing name**\n\n"
                f"**Looking for:** `{view.iracing_name}`\n"
                f"**Available names in data:** {', '.join(available_names[:5])}{'...' if len(available_names) > 5 else ''}\n\n"
                "**Possible issues:**\n"
                "• Name spelling/formatting differences\n"
                "• You haven't raced in this season yet\n"
                "• Season data needs to be updated\n\n"
                "**Try:**\n"
                "• Check your exact iRacing name spelling\n"
                "• Use `/my_link` to verify your linked name\n"
                "• Try a different season or 'All Time' view",
                embed=None,
                view=None
            )
            return
        
        # Create embed showing drivers around user
        emb = discord.Embed(
            title=f"🎯 {label} — Around {view.iracing_name}",
            description=f"Showing drivers around your position in **{FILTER_LABEL[view.metric]}**",
            color=discord.Color.green()
        )
        
        # Find user's position in the list with enhanced matching
        user_position = None
        iracing_name_clean = view.iracing_name.strip().lower()
        for i, driver in enumerate(drivers_around):
            driver_name_clean = driver["name"].strip().lower()
            # Enhanced matching for legacy code
            if (driver_name_clean == iracing_name_clean or
                driver_name_clean.replace(" ", "") == iracing_name_clean.replace(" ", "") or
                iracing_name_clean in driver_name_clean or driver_name_clean in iracing_name_clean):
                user_position = i
                break
        
        # Format the display for refresh_find_me
        blocks = []
        for i, driver in enumerate(drivers_around):
            global_pos = driver.get("global_position", "?")
            
            # Format: Position + Flag + Name (with person emoji for current user)
            if i == user_position:
                display_name = f"👤 {global_pos} {driver['flag']} {driver['name']}"
            else:
                display_name = f"{global_pos} {driver['flag']} {driver['name']}"
            
            blocks.append(render_driver_block(display_name, driver))
        
        emb.description = "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {FILTER_LABEL[view.metric]}")
        
        await interaction.response.edit_message(embed=emb, view=view)

class LeaderboardView(discord.ui.View):
    def __init__(self, initial_season: Optional[str], metric: str = "points"):
        super().__init__(timeout=900)
        self.season_choice = initial_season  # "__CAREER__" or season name
        self.metric = metric
        self.add_item(SeasonDropdown(self.season_choice))
        self.add_item(MetricDropdown(self.metric))
        self.add_item(FindMeButton())
        self.add_item(NextPageButton())
        self.add_item(GoToTopButton())
        self.add_item(GoToBottomButton())

    def _dataset(self) -> tuple[str, dict]:
        if self.season_choice == "__CAREER__":
            return ("All Time", _aggregate_career())
        if not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
        if not self.season_choice:
            return ("All Time", {})
        
        return (self.season_choice, load_season_drivers(self.season_choice))

    async def refresh(self, interaction: discord.Interaction):
        label, data = self._dataset()
        rows = _rows_from_dataset(data, self.metric, limit=5)
        emb = render_leaderboard_embed(label, rows, self.metric)
        try:
            await interaction.response.edit_message(embed=emb, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self)

    async def show(self, interaction: discord.Interaction):
        label, data = self._dataset()
        rows = _rows_from_dataset(data, self.metric, limit=5)
        emb = render_leaderboard_embed(label, rows, self.metric)
        await interaction.response.send_message(embed=emb, view=self, ephemeral=True)

# ========= Views: Stats season selector =========
class StatsSeasonDropdown(discord.ui.Select):
    def __init__(self, driver_name: str, current: Optional[str]):
        self.driver_name = driver_name
        options = [discord.SelectOption(label=s, value=s, default=(current == s)) for s in list_seasons()]
        super().__init__(placeholder="Select Season", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        season = self.values[0]
        # keep selected option highlighted
        for opt in self.options:
            opt.default = (opt.value == season)
        season_map = load_season_drivers(season)
        career_map = _aggregate_career()
        emb = render_stats_embed(self.driver_name, season, season_map, career_map)
        if not emb:
            try:
                await interaction.response.edit_message(content="No stats found.", embed=None, view=None)
            except discord.InteractionResponded:
                await interaction.edit_original_response(content="No stats found.", embed=None, view=None)
            return
        try:
            await interaction.response.edit_message(embed=emb, view=self.view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self.view)

class StatsView(discord.ui.View):
    def __init__(self, driver_name: str, initial_season: Optional[str]):
        super().__init__(timeout=600)
        self.driver_name = driver_name
        self.season_choice = initial_season
        self.metric = "points"  # Default metric for stats view
        self.add_item(StatsSeasonDropdown(driver_name, initial_season))
        self.add_item(FindMeButton())
        self.add_item(NextPageButton())
        self.add_item(GoToTopButton())
        self.add_item(GoToBottomButton())

# ========= Views: Drivers list (season dropdown only) =========
class DriversSeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str]):
        options = [discord.SelectOption(label="All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=s, value=s, default=(current == s)))
        super().__init__(placeholder="Season", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        season = self.values[0]
        # keep selected option highlighted
        for opt in self.options:
            opt.default = (opt.value == season)
        if season == "__CAREER__":
            dmap = _aggregate_career()
            title = "All Time"
        else:
            dmap = load_season_drivers(season)
            title = season
        names = sorted([f"{flag_shortcode(d.get('country') or '')} {name}" for name, d in dmap.items()], key=lambda x: x.lower())
        # update parent view with dataset for pagination
        view: "DriversView" = self.view  # type: ignore
        if isinstance(view, DriversView):
            view.names = names
            view.title = title
        desc = "\n".join(f"{i+1}. {n}" for i, n in enumerate(names[:PAGE_SIZE])) or "_No drivers found._"
        emb = discord.Embed(title=f"👥 {title}", description=desc, color=discord.Color.teal())
        try:
            await interaction.response.edit_message(embed=emb, view=self.view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self.view)

class DriversView(discord.ui.View):
    def __init__(self, initial_season: Optional[str]):
        super().__init__(timeout=600)
        self.page = 0
        self.names: list[str] = []
        self.title: str = initial_season or ""
        self.season_choice = initial_season
        self.metric = "points"  # Default metric for drivers view
        self.add_item(DriversSeasonDropdown(initial_season))
        self.add_item(DriversPrevButton())
        self.add_item(DriversNextButton())

    def render_description(self) -> str:
        if not self.names:
            return "_No drivers found._"
        start = self.page * PAGE_SIZE
        end = start + PAGE_SIZE
        sliced = self.names[start:end]
        return "\n".join(f"{i+1+start}. {n}" for i, n in enumerate(sliced))

    async def rerender(self, interaction: discord.Interaction):
        emb = discord.Embed(title=f"👥 {self.title}", description=self.render_description(), color=discord.Color.teal())
        # enable/disable buttons based on page
        for item in self.children:
            if isinstance(item, DriversPrevButton):
                item.disabled = (self.page <= 0)
            if isinstance(item, DriversNextButton):
                total_pages = max(1, (len(self.names) + PAGE_SIZE - 1) // PAGE_SIZE)
                item.disabled = (self.page >= total_pages - 1)
        try:
            await interaction.response.edit_message(embed=emb, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self)

class DriversPrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Previous", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: "DriversView" = self.view  # type: ignore
        if not isinstance(view, DriversView):
            return
        if view.page > 0:
            view.page -= 1
        await view.rerender(interaction)

class DriversNextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Next", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: "DriversView" = self.view  # type: ignore
        if not isinstance(view, DriversView):
            return
        total_pages = max(1, (len(view.names) + PAGE_SIZE - 1) // PAGE_SIZE)
        if view.page < total_pages - 1:
            view.page += 1
        await view.rerender(interaction)

# ========= Permissions helper =========
def is_admin(member: discord.Member) -> bool:
    want = (config.get("roles") or {}).get("admin") or "Admin"
    # accept role id stored as int too
    if isinstance(want, int):
        return any(r.id == want for r in member.roles) or member.guild_permissions.administrator
    return any(r.name == want for r in member.roles) or member.guild_permissions.administrator

# ========= Events =========
@bot.event
async def on_ready():
    print(console_safe(f"✅ Logged in as {bot.user} | Guilds: {[ (g.name, g.id) for g in bot.guilds ]}"))
    print(console_safe("🚀 Bot is ready to receive commands!"))

    # starts the daily check; will only back up when 7 days elapsed since last run
    auto_backup.start()

# ========= Setup =========
class ChannelPicker(discord.ui.ChannelSelect):
    def __init__(self, key: str, placeholder: str):
        super().__init__(placeholder=placeholder, channel_types=[discord.ChannelType.text])
        self.key = key
    async def callback(self, i: discord.Interaction):
        ch = self.values[0]
        config.setdefault("channels", {})[self.key] = ch.id
        save_config(config)
        await i.response.send_message(f"✅ Set **{self.placeholder}** to {ch.mention}", ephemeral=True)

class RolePicker(discord.ui.RoleSelect):
    def __init__(self, key: str, placeholder: str):
        super().__init__(placeholder=placeholder)
        self.key = key
    async def callback(self, i: discord.Interaction):
        role = self.values[0]
        config.setdefault("roles", {})[self.key] = role.id if isinstance(role, discord.Role) else role
        save_config(config)
        await i.response.send_message(f"✅ Set **{self.placeholder}** to {role.mention}", ephemeral=True)

class SetupChannelsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(ChannelPicker("leaderboard", "Choose Leaderboard Channel"))
        self.add_item(ChannelPicker("uploads", "Choose Uploads Channel"))
        self.add_item(ChannelPicker("backups", "Choose Backups Log Channel"))
        self.add_item(ChannelPicker("logs", "Choose Admin Logs Channel"))
        next_btn = discord.ui.Button(label="➡ Next: Roles", style=discord.ButtonStyle.primary)
        next_btn.callback = self.go_roles
        self.add_item(next_btn)
    async def go_roles(self, i: discord.Interaction):
        await i.response.edit_message(content="👤 **Setup — Roles**\nPick your Admin/Stats/Viewer roles.", view=SetupRolesView())

class SetupRolesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(RolePicker("admin", "Pick Admin Role"))
        self.add_item(RolePicker("stats", "Pick Stats Role (optional)"))
        self.add_item(RolePicker("viewer", "Pick Viewer Role (optional)"))
        back_btn = discord.ui.Button(label="⬅ Back: Channels", style=discord.ButtonStyle.secondary)
        back_btn.callback = self.go_channels
        self.add_item(back_btn)
    async def go_channels(self, i: discord.Interaction):
        await i.response.edit_message(content="⚙️ **Setup — Channels**\nSelect the target channels for bot features.", view=SetupChannelsView())

@tree.command(name="setup", description="(Admin) Configure channels and roles")
@GDEC
async def setup_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.send_message("⚙️ **Setup — Channels**\nSelect the target channels for bot features.", view=SetupChannelsView(), ephemeral=True)

# ========= Upload (Admin) — to current season only =========
@tree.command(name="upload", description="(Admin) Upload an iRacing event_result JSON file into the CURRENT season.")
@GDEC
@app_commands.describe(file="JSON file to upload")
async def upload_cmd(interaction: discord.Interaction, file: discord.Attachment):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    season = current_season_name()
    if not season:
        await interaction.response.send_message("⚠️ No **current season** set. Create one with `/season_create` and set it with `/season_set_current`.", ephemeral=True); return
    if not file.filename.lower().endswith(".json"):
        await interaction.response.send_message("⚠️ Please upload a `.json` iRacing event_result.", ephemeral=True); return

    await interaction.response.defer(ephemeral=True)
    try:
        raw = await file.read()
        
        # Check for duplicate content
        is_duplicate, existing_file = _is_duplicate_json(raw)
        if is_duplicate:
            await interaction.followup.send(f"⚠️ **Duplicate detected!** This JSON content already exists in `{existing_file}`. Upload cancelled to prevent duplicate data.", ephemeral=True)
            return
        
        data = json.loads(raw.decode("utf-8"))
        ensure_season_dir(season)
        updated, processed = ingest_iracing_event(data, season)
        
        # store a copy to disk for management
        fname = _sanitize_filename(file.filename)
        stamp = tz_now().strftime("%Y%m%d_%H%M%S")
        out_name = f"{stamp}_{fname}"
        with open(os.path.join(UPLOADS_STORE_DIR, out_name), "wb") as f:
            f.write(raw)
        
        await interaction.followup.send(f"✅ Ingested **{processed}** rows, updated **{updated}** drivers into **{season}**.\n🔄 Stats automatically synced!", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to ingest: `{e}`", ephemeral=True)

# ========= Refresh Commands (Admin) =========
@tree.command(name="refresh_commands", description="(Admin) Refresh/sync all slash commands.")
@GDEC
async def refresh_commands_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    try:
        # Get command count before sync
        commands_before = tree.get_commands()
        before_count = len(commands_before)
        
        # Sync commands to the guild
        synced_commands = await tree.sync(guild=interaction.guild)
        after_count = len(synced_commands)
        
        # Detailed response with command info
        response = f"✅ **Successfully refreshed all slash commands!**\n\n"
        response += f"📊 **Command Summary:**\n"
        response += f"• Commands registered: {before_count}\n"
        response += f"• Commands synced to Discord: {after_count}\n"
        
        if before_count != after_count:
            response += f"⚠️ **Note:** Command count mismatch detected. Some commands may not have synced properly.\n"
        
        response += f"\n💡 **Use `/help` to see the updated command list** (auto-generated from current commands)"
        response += f"\n🔍 **Use `/commands_info` for detailed command analysis**"
        
        await interaction.followup.send(response, ephemeral=True)
        
        # Log to console
        print(console_safe(f"🔄 Commands refreshed: {before_count} registered → {after_count} synced"))
        
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to refresh commands: `{e}`", ephemeral=True)

# ========= Upload New Files (Admin) =========











# ========= Leaderboard =========
@tree.command(name="leaderboard", description="Show Top-5 leaderboard with season & metric dropdowns (Career included).")
@GDEC
async def leaderboard_cmd(interaction: discord.Interaction):
    seasons = list_seasons()
    default_choice = "__CAREER__" if not seasons else (current_season_name() or seasons[-1])
    view = LeaderboardView(initial_season=default_choice, metric="points")
    await view.show(interaction)

# ========= Stats =========
@tree.command(name="driver_stats", description="Show a driver's Season & Career stats.")
@GDEC
@app_commands.describe(driver="Driver name (as stored)")
async def driver_stats_cmd(interaction: discord.Interaction, driver: str):
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons yet.", ephemeral=True); return
    season = current_season_name() or seasons[-1]
    season_map = load_season_drivers(season)
    career_map = _aggregate_career()
    emb = render_stats_embed(driver, season, season_map, career_map)
    if not emb:
        await interaction.response.send_message(f"❌ No stats found for `{driver}`.", ephemeral=True); return
    await interaction.response.send_message(embed=emb, view=StatsView(driver, season), ephemeral=True)

# ========= Drivers =========
@tree.command(name="driver_list", description="List drivers with a season dropdown (no text input).")
@GDEC
async def drivers_cmd(interaction: discord.Interaction):
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons found.", ephemeral=True); return
    season = current_season_name() or seasons[-1]
    dmap = load_season_drivers(season)
    names = sorted([f"{flag_shortcode(d.get('country') or '')} {name}" for name, d in dmap.items()], key=lambda x: x.lower())
    view = DriversView(season)
    view.names = names
    
    # Set title
    if season == "__CAREER__":
        view.title = "All Time"
    else:
        view.title = season
    
    desc = view.render_description()
    emb = discord.Embed(title=f"👥 {view.title}", description=desc, color=discord.Color.teal())
    await interaction.response.send_message(embed=emb, view=view, ephemeral=True)

 

# ========= Season management =========
@tree.command(name="season_create", description="(Admin) Create a new season")
@GDEC
@app_commands.describe(name="Season name (folder)")
async def season_create_cmd(interaction: discord.Interaction, name: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    ensure_season_dir(name)
    await interaction.response.send_message(f"✅ Created season **{name}**.", ephemeral=True)

class SeasonDeleteDropdown(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=s, value=s) for s in list_seasons()]
        super().__init__(placeholder="Pick a season to delete…", options=options, min_values=1, max_values=1)
    async def callback(self, i: discord.Interaction):
        if not is_admin(i.user):
            await i.response.send_message("🚫 Admins only.", ephemeral=True); return
        season = self.values[0]
        # refuse to delete if it's current season
        if current_season_name() == season:
            await i.response.send_message("⚠️ Unset current season first with `/season_set_current`.", ephemeral=True); return
        import shutil
        try:
            shutil.rmtree(ensure_season_dir(season))
            await i.response.send_message(f"🗑 Deleted season **{season}**.", ephemeral=True)
        except Exception as e:
            await i.response.send_message(f"❌ Delete failed: `{e}`", ephemeral=True)

class SeasonSetDropdown(discord.ui.Select):
    def __init__(self):
        cur = current_season_name()
        options = [discord.SelectOption(label="No Season", value="__NONE__", default=(cur in (None, "",)))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=s, value=s, default=(s == cur)))
        super().__init__(placeholder="Select current season…", options=options, min_values=1, max_values=1)
    async def callback(self, i: discord.Interaction):
        if not is_admin(i.user):
            await i.response.send_message("🚫 Admins only.", ephemeral=True); return
        choice = self.values[0]
        if choice == "__NONE__":
            set_current_season(None)
            await i.response.send_message("✅ Current season unset.", ephemeral=True)
        else:
            set_current_season(choice)
            await i.response.send_message(f"✅ Current season set to **{choice}**.", ephemeral=True)

@tree.command(name="season_delete", description="(Admin) Delete a season via dropdown")
@GDEC
async def season_delete_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    if not list_seasons():
        await interaction.response.send_message("⚠️ No seasons to delete.", ephemeral=True); return
    view = discord.ui.View(timeout=180)
    view.add_item(SeasonDeleteDropdown())
    await interaction.response.send_message("Pick a season to delete:", view=view, ephemeral=True)

@tree.command(name="season_set_current", description="(Admin) Set the current season via dropdown")
@GDEC
async def season_set_current_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    if not list_seasons():
        await interaction.response.send_message("⚠️ No seasons found. Create one with `/season_create`.", ephemeral=True); return
    view = discord.ui.View(timeout=180)
    view.add_item(SeasonSetDropdown())
    await interaction.response.send_message("Pick the current season:", view=view, ephemeral=True)

# ========= Season rename =========
class SeasonRenameModal(discord.ui.Modal, title="Rename Season"):
    def __init__(self, old_name: str):
        super().__init__()
        self.old_name = old_name
        self.new_name: discord.ui.TextInput = discord.ui.TextInput(
            label="New season name",
            placeholder="e.g. Schitt Kickers GT3 Cup - Season 2",
            required=True,
            max_length=100
        )
        self.add_item(self.new_name)

    async def on_submit(self, interaction: discord.Interaction):
        old_name = self.old_name
        new_name = str(self.new_name.value).strip()
        if not new_name:
            await interaction.response.send_message("⚠️ Name cannot be empty.", ephemeral=True)
            return
        if new_name == old_name:
            await interaction.response.send_message("ℹ️ Same name provided; nothing changed.", ephemeral=True)
            return
        import shutil
        src = os.path.join(SEASONS_DIR, old_name)
        dst = os.path.join(SEASONS_DIR, new_name)
        if not os.path.exists(src):
            await interaction.response.send_message("❌ Source season not found.", ephemeral=True)
            return
        if os.path.exists(dst):
            await interaction.response.send_message("❌ A season with that name already exists.", ephemeral=True)
            return
        try:
            shutil.move(src, dst)
            if current_season_name() == old_name:
                set_current_season(new_name)
            await interaction.response.send_message(f"✅ Renamed **{old_name}** → **{new_name}**.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Rename failed: `{e}`", ephemeral=True)

class SeasonRenameDropdown(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=s, value=s) for s in list_seasons()]
        super().__init__(placeholder="Pick a season to rename…", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.send_modal(SeasonRenameModal(target))

@tree.command(name="season_rename", description="(Admin) Rename a season via dropdown + modal")
@GDEC
async def season_rename_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    if not list_seasons():
        await interaction.response.send_message("⚠️ No seasons found.", ephemeral=True); return
    view = discord.ui.View(timeout=180)
    view.add_item(SeasonRenameDropdown())
    await interaction.response.send_message("Pick a season to rename:", view=view, ephemeral=True)

# ========= Career wipes =========

@tree.command(name="career_wipe_driver", description="(Admin) Wipe a single driver's stats from ALL seasons")
@GDEC
@app_commands.describe(driver="Driver name (exact match)")
async def career_wipe_driver(interaction: discord.Interaction, driver: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    removed_any = False
    try:
        for s in list_seasons():
            m = load_season_drivers(s)
            if driver in m:
                del m[driver]
                save_season_drivers(s, m)
                removed_any = True
        if removed_any:
            await interaction.response.send_message(f"🧨 Removed **{driver}** from all seasons.", ephemeral=True)
        else:
            await interaction.response.send_message(f"ℹ️ `{driver}` not found in any season.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed: `{e}`", ephemeral=True)

# ========= Manual backups =========
@tree.command(name="backup_now", description="(Admin) Create a backup and store on disk")
@GDEC
async def backup_now(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    path = save_backup_to_disk()
    await interaction.followup.send(f"✅ Backup saved: `{os.path.basename(path)}` (retention {RETENTION_DAYS}d).", ephemeral=True)

@tree.command(name="backup_info", description="(Admin) Show backup status (last run & retention)")
@GDEC
async def backup_info(interaction: discord.Interaction):
    try:
        with open(BACKUP_STATE, "r", encoding="utf-8") as f:
            last = f.read().strip()
    except Exception:
        last = "none"
    desc = f"Auto backup interval: **{BACKUP_INTERVAL_DAYS} days**\nRetention: **{RETENTION_DAYS} days**\nLast backup: **{last}**"
    await interaction.response.send_message(desc, ephemeral=True)

# ========= Uploads management =========
class UploadsDropdown(discord.ui.Select):
    def __init__(self):
        files = list_uploaded_jsons()
        options = [discord.SelectOption(label=fn, value=fn) for fn in files[:25]]  # Discord limit
        if not options:
            options = [discord.SelectOption(label="<no uploads>", value="__NONE__", default=True)]
        super().__init__(placeholder="Select JSON files to manage…", options=options, min_values=1, max_values=min(25, len(options)))

    async def callback(self, interaction: discord.Interaction):
        view: "UploadsManageView" = self.view  # type: ignore
        choices = self.values
        if "__NONE__" in choices:
            await interaction.response.defer(ephemeral=True)
            return
        view.selected_files = choices
        await interaction.response.defer(ephemeral=True)

class UploadsMultiDropdown(discord.ui.Select):
    def __init__(self):
        files = list_uploaded_jsons()
        options = [discord.SelectOption(label=fn, value=fn) for fn in files[:25]]  # Discord limit
        if not options:
            options = [discord.SelectOption(label="<no uploads>", value="__NONE__", default=True)]
        super().__init__(placeholder="Select multiple JSON files…", options=options, min_values=1, max_values=min(10, len(options)))

    async def callback(self, interaction: discord.Interaction):
        view: "UploadsMultiManageView" = self.view  # type: ignore
        choices = self.values
        if "__NONE__" in choices:
            await interaction.response.defer(ephemeral=True)
            return
        view.selected_files = choices
        await interaction.response.defer(ephemeral=True)

class UploadsDeleteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.danger, label="Delete Selected")

    async def callback(self, interaction: discord.Interaction):
        view: "UploadsManageView" = self.view  # type: ignore
        if not view.selected_files:
            await interaction.response.send_message("⚠️ Pick files first.", ephemeral=True)
            return
        
        filenames = view.selected_files
        if len(filenames) == 1:
            # Single file deletion with confirmation
            filename = filenames[0]
            await interaction.response.send_message(
                f"⚠️ **Confirm Deletion**\n\nAre you sure you want to delete `{filename}`?\n\nThis action cannot be undone.",
                view=DeleteConfirmView(filename, view.season),
                ephemeral=True
            )
        else:
            # Multiple file deletion with confirmation
            await interaction.response.send_message(
                f"⚠️ **Confirm Deletion**\n\nAre you sure you want to delete **{len(filenames)}** files?\n\nThis action cannot be undone.",
                view=DeleteConfirmView(filenames, view.season),
                ephemeral=True
            )

class UploadsMultiDeleteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.danger, label="Delete Selected")

    async def callback(self, interaction: discord.Interaction):
        view: "UploadsMultiManageView" = self.view  # type: ignore
        if not view.selected_files:
            await interaction.response.send_message("⚠️ Pick files first.", ephemeral=True)
            return
        
        filenames = view.selected_files
        if len(filenames) == 1:
            # Single file deletion
            filename = filenames[0]
            path = os.path.join(UPLOADS_STORE_DIR, filename)
            
            try:
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        file_content = f.read()
                    os.remove(path)
                    seasons_affected = await _remove_ingested_data(file_content)
                    
                    nv = UploadsMultiManageView()
                    if seasons_affected:
                        await interaction.response.edit_message(
                            content=f"🗑 Deleted `{filename}` and removed ingested data from {len(seasons_affected)} season(s): {', '.join(seasons_affected)}", 
                            view=nv
                        )
                    else:
                        await interaction.response.edit_message(
                            content=f"🗑 Deleted `{filename}` (no ingested data found)", 
                            view=nv
                        )
                else:
                    nv = UploadsMultiManageView()
                    await interaction.response.edit_message(content=f"ℹ️ File `{filename}` was already deleted.", view=nv)
                    
            except Exception as e:
                await interaction.response.send_message(f"❌ Delete failed: `{e}`", ephemeral=True)
        else:
            # Multiple file deletion
            await interaction.response.defer(ephemeral=True)
            
            try:
                total_deleted = 0
                total_seasons_affected = set()
                results = []
                
                for filename in filenames:
                    path = os.path.join(UPLOADS_STORE_DIR, filename)
                    if os.path.exists(path):
                        try:
                            with open(path, "rb") as f:
                                file_content = f.read()
                            os.remove(path)
                            
                            seasons_affected = await _remove_ingested_data(file_content)
                            total_seasons_affected.update(seasons_affected)
                            total_deleted += 1
                            
                            if seasons_affected:
                                results.append(f"✅ `{filename}`: Deleted, data removed from {len(seasons_affected)} season(s)")
                            else:
                                results.append(f"✅ `{filename}`: Deleted (no ingested data)")
                                
                        except Exception as e:
                            results.append(f"❌ `{filename}`: Failed - {e}")
                    else:
                        results.append(f"⚠️ `{filename}`: Already deleted")
                
                # Create summary message
                summary = f"🗑 **Multiple Delete Summary**\n\n"
                summary += f"📁 **Files processed:** {len(filenames)}\n"
                summary += f"🗑 **Successfully deleted:** {total_deleted}\n"
                if total_seasons_affected:
                    summary += f"📊 **Seasons affected:** {', '.join(sorted(total_seasons_affected))}\n"
                summary += f"\n**Results:**\n" + "\n".join(results)
                
                # Send updated response
                await interaction.followup.send(
                    content=summary,
                    ephemeral=True
                )
                
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to process multiple deletions: `{e}`", ephemeral=True)

class UploadsManageView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.selected_files: list[str] = []
        self.season = current_season_name()
        self.add_item(UploadsDropdown())
        self.add_item(UploadsDeleteButton())
        self.add_item(DeleteAllButton())

class DeleteAllButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.danger, label="Delete All Files")

    async def callback(self, interaction: discord.Interaction):
        files = list_uploaded_jsons()
        if not files:
            await interaction.response.send_message("ℹ️ No files to delete.", ephemeral=True)
            return
        
        await interaction.response.send_message(
            f"⚠️ **Confirm Delete All**\n\nAre you sure you want to delete **ALL {len(files)}** uploaded JSON files?\n\nThis action cannot be undone and will affect all seasons!",
            view=DeleteConfirmView(files, current_season_name()),
            ephemeral=True
        )

class DeleteConfirmView(discord.ui.View):
    def __init__(self, files_to_delete, season: str):
        super().__init__(timeout=60)
        self.files_to_delete = files_to_delete
        self.season = season

    @discord.ui.button(label="✅ Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        try:
            if isinstance(self.files_to_delete, str):
                # Single file deletion
                filenames = [self.files_to_delete]
            else:
                # Multiple file deletion
                filenames = self.files_to_delete
            
            total_deleted = 0
            total_seasons_affected = set()
            results = []
            
            for filename in filenames:
                path = os.path.join(UPLOADS_STORE_DIR, filename)
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as f:
                            file_content = f.read()
                        os.remove(path)
                        
                        seasons_affected = await _remove_ingested_data(file_content)
                        total_seasons_affected.update(seasons_affected)
                        total_deleted += 1
                        
                        if seasons_affected:
                            results.append(f"✅ `{filename}`: Deleted, data removed from {len(seasons_affected)} season(s)")
                        else:
                            results.append(f"✅ `{filename}`: Deleted (no ingested data)")
                            
                    except Exception as e:
                        results.append(f"❌ `{filename}`: Failed - {e}")
                else:
                    results.append(f"⚠️ `{filename}`: Already deleted")
            
            # Create summary message
            if len(filenames) == 1:
                summary = f"🗑 **File Deleted Successfully**\n\n"
            else:
                summary = f"🗑 **Delete Summary**\n\n"
            
            summary += f"📁 **Files processed:** {len(filenames)}\n"
            summary += f"🗑 **Successfully deleted:** {total_deleted}\n"
            if total_seasons_affected:
                summary += f"📊 **Seasons affected:** {', '.join(sorted(total_seasons_affected))}\n"
            summary += f"\n**Results:**\n" + "\n".join(results)
            
            # Send updated response
            await interaction.followup.send(
                content=summary,
                ephemeral=True
            )
            
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to process deletions: `{e}`", ephemeral=True)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Deletion cancelled.", view=None)

class UploadsMultiManageView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.selected_files: list[str] = []
        self.add_item(UploadsMultiDropdown())
        self.add_item(UploadsMultiDeleteButton())

# ========= Upload View =========
class UploadDropdown(discord.ui.Select):
    def __init__(self):
        files = list_uploaded_jsons()
        options = [discord.SelectOption(label=fn, value=fn) for fn in files[:25]]  # Discord limit
        if not options:
            options = [discord.SelectOption(label="<no uploads>", value="__NONE__", default=True)]
        super().__init__(placeholder="Select JSON files to upload…", options=options, min_values=1, max_values=min(25, len(options)))

    async def callback(self, interaction: discord.Interaction):
        view: "UploadView" = self.view  # type: ignore
        choices = self.values
        if "__NONE__" in choices:
            await interaction.response.defer(ephemeral=True)
            return
        view.selected_files = choices
        await interaction.response.defer(ephemeral=True)

class UploadButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Upload Selected Files")

    async def callback(self, interaction: discord.Interaction):
        view: "UploadView" = self.view  # type: ignore
        if not view.selected_files:
            await interaction.response.send_message("⚠️ Pick files first.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            total_processed = 0
            total_updated = 0
            results = []
            
            for filename in view.selected_files:
                try:
                    file_path = os.path.join(UPLOADS_STORE_DIR, filename)
                    with open(file_path, "rb") as f:
                        raw = f.read()
                    
                    # Check for duplicate content
                    is_duplicate, existing_file = _is_duplicate_json(raw)
                    if is_duplicate:
                        results.append(f"⚠️ `{filename}`: Duplicate of `{existing_file}` (skipped)")
                        continue
                    
                    data = json.loads(raw.decode("utf-8"))
                    ensure_season_dir(view.season)
                    updated, processed = ingest_iracing_event(data, view.season)
                    
                    total_processed += processed
                    total_updated += updated
                    results.append(f"✅ `{filename}`: {processed} rows, {updated} drivers")
                    
                except Exception as e:
                    results.append(f"❌ `{filename}`: Failed - {e}")
            
            # Create summary message
            summary = f"📊 **Upload Summary**\n\n"
            summary += f"📁 **Files processed:** {len(view.selected_files)}\n"
            summary += f"📊 **Total rows:** {total_processed}\n"
            summary += f"👥 **Total drivers updated:** {total_updated}\n\n"
            summary += "**Results:**\n" + "\n".join(results)
            
            await interaction.followup.send(summary, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to process files: `{e}`", ephemeral=True)

class UploadView(discord.ui.View):
    def __init__(self, season: str):
        super().__init__(timeout=300)
        self.season = season
        self.selected_files: list[str] = []
        self.add_item(UploadDropdown())
        self.add_item(UploadButton())

@tree.command(name="uploads_manage", description="(Admin) Manage uploaded JSON files: select and delete.")
@GDEC
async def uploads_manage(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.send_message("Manage uploaded JSON files:", view=UploadsManageView(), ephemeral=True)



@tree.command(name="uploads_check_duplicates", description="(Admin) Check for duplicate JSON files in uploads directory.")
@GDEC
async def uploads_check_duplicates_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        files = list_uploaded_jsons()
        if len(files) < 2:
            await interaction.followup.send("ℹ️ Not enough files to check for duplicates (need at least 2).", ephemeral=True)
            return
        
        # Group files by content hash
        hash_groups = {}
        duplicate_groups = []
        
        for filename in files:
            file_path = os.path.join(UPLOADS_STORE_DIR, filename)
            try:
                with open(file_path, "rb") as f:
                    content = f.read()
                    content_hash = _generate_content_hash(content)
                    
                    if content_hash not in hash_groups:
                        hash_groups[content_hash] = []
                    hash_groups[content_hash].append(filename)
                    
                    # If we have multiple files with same hash, it's a duplicate group
                    if len(hash_groups[content_hash]) > 1:
                        duplicate_groups.append(hash_groups[content_hash])
            except Exception as e:
                continue
        
        if not duplicate_groups:
            await interaction.followup.send("✅ No duplicate files found in uploads directory.", ephemeral=True)
            return
        
        # Format duplicate report
        report_lines = ["🔍 **Duplicate Files Found:**"]
        total_duplicates = 0
        
        for group in duplicate_groups:
            report_lines.append(f"\n**Group {len(group)} files:**")
            for filename in group:
                report_lines.append(f"  • `{filename}`")
                total_duplicates += 1
        
        report_lines.append(f"\n📊 **Total duplicate files:** {total_duplicates}")
        report_lines.append(f"📁 **Total files scanned:** {len(files)}")
        
        report = "\n".join(report_lines)
        
        # Split long reports if needed
        if len(report) > 2000:
            chunks = [report[i:i+1900] for i in range(0, len(report), 1900)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await interaction.followup.send(chunk, ephemeral=True)
                else:
                    await interaction.followup.send(f"**Report Part {i+1}:**\n{chunk}", ephemeral=True)
        else:
            await interaction.followup.send(report, ephemeral=True)
            
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to check for duplicates: `{e}`", ephemeral=True)

@tree.command(name="link_account", description="Link your Discord account to your iRacing name")
@GDEC
@app_commands.describe(iracing_name="Your exact iRacing driver name")
async def link_account_cmd(interaction: discord.Interaction, iracing_name: str):
    iracing_name = iracing_name.strip()
    if not iracing_name:
        await interaction.response.send_message("⚠️ Please provide your iRacing name.", ephemeral=True)
        return
    
    # Check if this iRacing name is already linked to another Discord user
    existing_discord_id = get_discord_id(iracing_name)
    if existing_discord_id and existing_discord_id != interaction.user.id:
        await interaction.response.send_message(
            f"⚠️ **Name Already Linked**\n\nThe iRacing name '{iracing_name}' is already linked to another Discord user.\n\n"
            "If this is your name, ask an admin to unlink it first.",
            ephemeral=True
        )
        return
    
    # Check if user already has a link
    existing_link = get_iracing_name(interaction.user.id)
    if existing_link:
        # Update existing link
        if link_discord_to_iracing(interaction.user.id, iracing_name):
            await interaction.response.send_message(
                f"✅ **Link Updated!**\n\nYour Discord account is now linked to: **{iracing_name}**\n\n"
                f"Previous link: **{existing_link}**",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ Failed to update link. Please try again.", ephemeral=True)
    else:
        # Create new link
        if link_discord_to_iracing(interaction.user.id, iracing_name):
            await interaction.response.send_message(
                f"✅ **Account Linked!**\n\nYour Discord account is now linked to: **{iracing_name}**\n\n"
                "You can now use the 'Find Me' button in leaderboards to see your position!",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ Failed to create link. Please try again.", ephemeral=True)

@tree.command(name="unlink_account", description="Remove the link between your Discord account and iRacing name")
@GDEC
async def unlink_account_cmd(interaction: discord.Interaction):
    if unlink_discord(interaction.user.id):
        await interaction.response.send_message(
            "✅ **Account Unlinked!**\n\nYour Discord account is no longer linked to any iRacing name.\n\n"
            "You can link to a new name anytime with `/link_account <iRacing_name>`.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            "ℹ️ **No Link Found**\n\nYour Discord account is not currently linked to any iRacing name.",
            ephemeral=True
        )

@tree.command(name="my_link", description="Show your current Discord-to-iRacing link")
@GDEC
async def my_link_cmd(interaction: discord.Interaction):
    iracing_name = get_iracing_name(interaction.user.id)
    if iracing_name:
        await interaction.response.send_message(
            f"🔗 **Your Link**\n\nYour Discord account is linked to: **{iracing_name}**\n\n"
            "Use `/unlink_account` to remove this link.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            "🔗 **No Link Found**\n\nYour Discord account is not linked to any iRacing name.\n\n"
            "Use `/link_account <iRacing_name>` to create a link.",
            ephemeral=True
        )

@tree.command(name="admin_unlink", description="(Admin) Remove a Discord-to-iRacing link")
@GDEC
@app_commands.describe(discord_user="Discord user to unlink")
async def admin_unlink_cmd(interaction: discord.Interaction, discord_user: discord.Member):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True)
        return
    
    iracing_name = get_iracing_name(discord_user.id)
    if not iracing_name:
        await interaction.response.send_message(
            f"ℹ️ **No Link Found**\n\n{discord_user.mention} is not linked to any iRacing name.",
            ephemeral=True
        )
        return
    
    if unlink_discord(discord_user.id):
        await interaction.response.send_message(
            f"✅ **Admin Unlink Complete**\n\nRemoved link between {discord_user.mention} and **{iracing_name}**.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message("❌ Failed to unlink. Please try again.", ephemeral=True)

@tree.command(name="find_similar_names", description="Find similar names in the current season data")
@GDEC
@app_commands.describe(search_name="Name to search for (partial matches supported)")
async def find_similar_names_cmd(interaction: discord.Interaction, search_name: str):
    """Find similar names in the current season data to help with name matching issues"""
    search_name = search_name.strip()
    if not search_name:
        await interaction.response.send_message("⚠️ Please provide a name to search for.", ephemeral=True)
        return
    
    # Get current season data
    current_season = current_season_name()
    if not current_season:
        await interaction.response.send_message("⚠️ No current season set. Use `/season_set_current` first.", ephemeral=True)
        return
    
    season_data = load_season_drivers(current_season)
    if not season_data:
        await interaction.response.send_message(f"⚠️ No data found for season '{current_season}'.", ephemeral=True)
        return
    
    # Search for similar names
    search_clean = search_name.lower()
    similar_names = []
    exact_matches = []
    
    for name in season_data.keys():
        name_clean = name.lower()
        
        # Check for exact match
        if name_clean == search_clean:
            exact_matches.append(name)
        # Check for contains
        elif search_clean in name_clean or name_clean in search_clean:
            similar_names.append(name)
        # Check for space-removed match
        elif search_clean.replace(" ", "") == name_clean.replace(" ", ""):
            similar_names.append(name)
        # Check for reversed names
        elif len(search_clean.split()) >= 2:
            search_parts = search_clean.split()
            search_reversed = f"{search_parts[-1]} {' '.join(search_parts[:-1])}"
            if name_clean == search_reversed:
                similar_names.append(name)
    
    # Build response message
    response = f"🔍 **Name Search Results for '{search_name}'**\n\n"
    response += f"**Season:** {current_season}\n"
    response += f"**Total drivers in season:** {len(season_data)}\n\n"
    
    if exact_matches:
        response += f"✅ **Exact matches:** {', '.join(exact_matches)}\n\n"
    
    if similar_names:
        response += f"🔍 **Similar names:** {', '.join(similar_names[:10])}{'...' if len(similar_names) > 10 else ''}\n\n"
    
    if not exact_matches and not similar_names:
        response += "❌ **No matches found**\n\n"
        response += "**Try:**\n"
        response += "• Check spelling and capitalization\n"
        response += "• Try partial names (e.g., 'Lorenzo' instead of 'Lorenzo Schoovaerts')\n"
        response += "• Check if you're in the right season\n"
        response += "• Use `/drivers` to see all available names"
    else:
        response += "**💡 Tip:** Use `/link_account <exact_name>` with one of the matches above"
    
    await interaction.response.send_message(response, ephemeral=True)

def generate_dynamic_help() -> str:
    """Generate help text dynamically from registered commands"""
    
    # Get all registered commands from the command tree
    commands = tree.get_commands()
    
    # Command categories with their emoji prefixes and descriptions
    categories = {
        "general": {
            "emoji": "📊",
            "title": "General Commands",
            "commands": [],
            "keywords": ["leaderboard", "driver_stats", "driver_list", "help", "find_similar_names"]
        },
        "linking": {
            "emoji": "🔗", 
            "title": "Account Linking",
            "commands": [],
            "keywords": ["link_account", "unlink_account", "my_link"]
        },
        "admin": {
            "emoji": "⚙️",
            "title": "Admin Commands", 
            "commands": [],
            "keywords": ["setup", "refresh_commands", "season_", "admin_"]
        },
        "files": {
            "emoji": "📁",
            "title": "File Management",
            "commands": [],
            "keywords": ["upload", "uploads_"]
        },
        "data": {
            "emoji": "🧹",
            "title": "Data Management", 
            "commands": [],
            "keywords": ["career_wipe"]
        },
        "backup": {
            "emoji": "💾",
            "title": "Backup Commands",
            "commands": [],
            "keywords": ["backup_"]
        }
    }
    
    # Categorize commands
    for cmd in commands:
        categorized = False
        
        for category_key, category_info in categories.items():
            for keyword in category_info["keywords"]:
                if keyword in cmd.name:
                    # Format command with parameters
                    cmd_params = ""
                    if hasattr(cmd, 'parameters') and cmd.parameters:
                        param_names = []
                        for param in cmd.parameters:
                            if param.required:
                                param_names.append(f"<{param.name}>")
                            else:
                                param_names.append(f"[{param.name}]")
                        if param_names:
                            cmd_params = " " + " ".join(param_names)
                    
                    # Add admin prefix to description if it's an admin command
                    description = cmd.description
                    if category_key == "admin" and not description.startswith("(Admin)"):
                        description = f"(Admin) {description}"
                    
                    category_info["commands"].append(f"• `/{cmd.name}{cmd_params}` - {description}")
                    categorized = True
                    break
            if categorized:
                break
        
        # If not categorized, add to general
        if not categorized:
            cmd_params = ""
            if hasattr(cmd, 'parameters') and cmd.parameters:
                param_names = []
                for param in cmd.parameters:
                    if param.required:
                        param_names.append(f"<{param.name}>")
                    else:
                        param_names.append(f"[{param.name}]")
                if param_names:
                    cmd_params = " " + " ".join(param_names)
            
            categories["general"]["commands"].append(f"• `/{cmd.name}{cmd_params}` - {cmd.description}")
    
    # Build help text
    help_text = "🤖 **WiRL Bot Commands**\n\n"
    
    for category_info in categories.values():
        if category_info["commands"]:  # Only show categories that have commands
            help_text += f"**{category_info['emoji']} {category_info['title']}:**\n"
            help_text += "\n".join(sorted(category_info["commands"])) + "\n\n"
    
    # Add usage tips
            help_text += """**💡 Usage Tips:**
• Use `/upload` to upload JSON files to the current season
• Use `/uploads_manage` to select multiple files, delete all, or manage individual files
• **🔄 Auto-sync enabled:** Leaderboards and stats automatically update after data changes
• All file deletions automatically update stats and leaderboards
• Duplicate detection prevents accidental duplicate data ingestion
• Set a current season with `/season_set_current` before uploading files
• Use `/driver_stats <driver>` to view individual driver statistics
• Use `/refresh_commands` to manually sync commands after code changes
• **🔗 Link your Discord:** Use `/link_account <iRacing_name>` to enable the "Find Me" button in leaderboards
• Commands are automatically updated when new features are added
• Leaderboard titles are simplified (e.g., 'All Time' instead of '🏁 All Time Driver List')"""
    
    return help_text

@tree.command(name="help", description="Show all available commands and their descriptions.")
@GDEC
async def help_cmd(interaction: discord.Interaction):
    help_text = generate_dynamic_help()
    await interaction.response.send_message(help_text, ephemeral=True)

@tree.command(name="commands_info", description="(Admin) Show detailed information about registered commands")
@GDEC
async def commands_info_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True)
        return
    
    commands = tree.get_commands()
    
    # Count commands by category
    categories = {
        "General": ["leaderboard", "driver_stats", "driver_list", "help", "find_similar_names"],
        "Account Linking": ["link_account", "unlink_account", "my_link"],
        "Admin": ["setup", "refresh_commands", "season_", "admin_", "commands_info"],
        "File Management": ["upload", "uploads_"],
        "Data Management": ["career_wipe"],
        "Backup": ["backup_"]
    }
    
    category_counts = {cat: 0 for cat in categories.keys()}
    uncategorized = []
    
    for cmd in commands:
        categorized = False
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in cmd.name:
                    category_counts[category] += 1
                    categorized = True
                    break
            if categorized:
                break
        if not categorized:
            uncategorized.append(cmd.name)
    
    info_text = f"🤖 **Command Registration Info**\n\n"
    info_text += f"**📊 Total Commands:** {len(commands)}\n\n"
    
    info_text += "**📂 Commands by Category:**\n"
    for category, count in category_counts.items():
        info_text += f"• {category}: {count} commands\n"
    
    if uncategorized:
        info_text += f"• Uncategorized: {len(uncategorized)} commands\n"
        info_text += f"  └─ {', '.join(uncategorized)}\n"
    
    info_text += f"\n**🔍 All Registered Commands:**\n"
    cmd_list = []
    for cmd in sorted(commands, key=lambda x: x.name):
        cmd_list.append(f"• `/{cmd.name}` - {cmd.description}")
    
    # Split into chunks if too long
    if len("\n".join(cmd_list)) > 1500:
        info_text += f"```\n{', '.join([cmd.name for cmd in sorted(commands, key=lambda x: x.name)])}\n```"
    else:
        info_text += "\n".join(cmd_list)
    
    info_text += f"\n\n**💡 Note:** Help command is automatically generated from these registered commands."
    
    await interaction.response.send_message(info_text, ephemeral=True)

# ========= Bot Events =========
@bot.event
async def on_ready():
    print(console_safe(f"✅ {bot.user} is ready and online!"))
    print(console_safe(f"🏠 Connected to {len(bot.guilds)} guild(s)"))
    print(console_safe("🚀 Bot is ready to receive commands!"))

# ========= Run =========
if not TOKEN or TOKEN == "YOUR_DISCORD_BOT_TOKEN":
    print(console_safe("❌ No valid Discord bot token found!"))
    print(console_safe("   Please set DISCORD_TOKEN environment variable or add token to config.json"))
    print(console_safe(f"   Environment variable: {os.getenv('DISCORD_TOKEN', 'Not set')}"))
    print(console_safe(f"   Config file: {config.get('token', 'Not set')}"))
else:
    print(console_safe("🚀 Starting WiRL Stats Bot with 24/7 capabilities..."))
    print(console_safe(f"🔐 Using token: {TOKEN[:10]}...{TOKEN[-4:] if len(TOKEN) > 14 else 'short'}"))
    bot.run(TOKEN)
