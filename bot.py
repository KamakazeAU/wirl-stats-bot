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
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

# ========= Formatting helpers =========
FILTERS = [
    ("🏎️ Races", "races"),
    ("🏆 Wins", "wins"),
    ("🏆 Win %", "wins_pct"),
    ("🥈 Podiums", "podiums"),
    ("🥈 Podium %", "podiums_pct"),
    ("🔟 Top 10s", "top10s"),
    ("🔟 Top 10s %", "top10s_pct"),
    ("💯 Points", "points"),
    ("1️⃣ Poles", "poles"),
    ("1️⃣ Poles %", "poles_pct"),
    ("⚡ Fastest Laps", "fastest_laps"),
    ("✅ Laps Completed", "laps_complete"),
    ("✅ Laps Completed %", "laps_complete_pct"),
    ("🚀 Laps Led", "laps_lead"),
    ("🚀 Laps Led %", "laps_lead_pct"),
    ("🚦 Avg Start", "avg_start"),
    ("🏁 Avg Finish", "avg_finish"),
    ("⚠️ Avg Incidents", "avg_incidents"),
    ("📊 Pos Gain/Loss", "position_change"),
]

FILTER_LABEL = {k: lbl for (lbl, k) in FILTERS}

def format_position_change(value: float) -> str:
    """Format position gain/loss as whole number with + or - sign"""
    if value == 0:
        return "0"
    elif value > 0:
        return f"+{int(value)}"
    else:
        return f"{int(value)}"

def _sort_key(metric: str, row: dict) -> float:
    v = safe_float(row.get(metric, 0))
    if metric in ("avg_start", "avg_finish", "avg_incidents"):  # lower is better
        return -v  # Simply negate the value, treat 0.0 as 0.0
    # Percentage metrics (higher is better) - sort normally
    elif metric in ("wins_pct", "podiums_pct", "top10s_pct", "poles_pct", "laps_complete_pct", "laps_lead_pct"):
        return v
    return v

def _aggregate_career() -> dict:
    """Aggregate driver stats across all seasons into career totals"""
    out: dict = {}
    for s in list_seasons():
        sd = load_season_drivers(s)
        for name, d in sd.items():
            o = out.setdefault(name, {
                "country": d.get("country"), 
                "races": 0, 
                "wins": 0, 
                "poles": 0,
                "podiums": 0, 
                "top10s": 0, 
                "points": 0.0,
                "avg_incidents": 0.0, 
                "avg_start": 0.0, 
                "avg_finish": 0.0, 
                "_rp": 0,
                "laps_complete": 0,
                "laps_lead": 0,
                "fastest_laps": 0,
                "race_distances": [],
                "position_change": 0.0,
                # Percentage fields
                "wins_pct": 0.0,
                "podiums_pct": 0.0,
                "top10s_pct": 0.0,
                "poles_pct": 0.0,
                "laps_complete_pct": 0.0,
                "laps_lead_pct": 0.0
            })
            
            races_new = int(d.get("races", 0))
            o["races"] += races_new
            o["wins"] += int(d.get("wins", 0))
            o["poles"] += int(d.get("poles", 0))
            o["podiums"] += int(d.get("podiums", 0))
            o["top10s"] += int(d.get("top10s", d.get("top5s", d.get("top5", 0))))
            o["points"] += safe_float(d.get("points", 0))
            o["laps_complete"] += int(d.get("laps_complete", 0))
            o["laps_lead"] += int(d.get("laps_lead", 0))
            o["fastest_laps"] += int(d.get("fastest_laps", 0))
            
            # Combine race distances from all seasons
            if "race_distances" in d:
                o["race_distances"].extend(d.get("race_distances", []))
            
            # weighted avgs
            def wavg(prev, add, rp, ra):
                prev = safe_float(prev); add = safe_float(add); tot = rp + ra
                return (prev * rp + add * ra) / tot if tot > 0 else 0.0
            
            rp = int(o.get("_rp", 0)); ra = races_new
            o["avg_incidents"] = wavg(o["avg_incidents"], d.get("avg_incidents", 0), rp, ra)
            o["avg_start"] = wavg(o["avg_start"], d.get("avg_start", 0), rp, ra)
            o["avg_finish"] = wavg(o["avg_finish"], d.get("avg_finish", 0), rp, ra)
            

            
            # Position change should be cumulative, not averaged
            o["position_change"] = safe_float(o.get("position_change", 0)) + safe_float(d.get("position_change", 0))
            
            o["_rp"] = rp + ra
    
    # Calculate percentages and round averages
    for driver_name, d in out.items():
        if d["races"] > 0:
            d["wins_pct"] = round((d["wins"] / d["races"]) * 100, 1)
            d["podiums_pct"] = round((d["podiums"] / d["races"]) * 100, 1)
            d["top10s_pct"] = round((d["top10s"] / d["races"]) * 100, 1)
            d["poles_pct"] = round((d["poles"] / d["races"]) * 100, 1)
        else:
            d["wins_pct"] = 0.0
            d["podiums_pct"] = 0.0
            d["top10s_pct"] = 0.0
            d["poles_pct"] = 0.0
        
        # Calculate laps percentages correctly (based on total laps available, not races)
        total_laps_available = sum(d.get("race_distances", []))
        if total_laps_available > 0:
            d["laps_complete_pct"] = round((d["laps_complete"] / total_laps_available) * 100, 1)
            d["laps_lead_pct"] = round((d["laps_lead"] / total_laps_available) * 100, 1)
        else:
            d["laps_complete_pct"] = 0.0
            d["laps_lead_pct"] = 0.0
        
        # Validate calculations for debugging (only for first few drivers to avoid spam)
        if len([name for name in out.keys() if name <= driver_name]) <= 3:
            validate_percentage_calculations(d, driver_name)
        
        d["avg_incidents"] = round(safe_float(d["avg_incidents"]), 2)
        d["avg_start"] = round(safe_float(d["avg_start"]), 2)
        d["avg_finish"] = round(safe_float(d["avg_finish"]), 2)
    
    return out

def _rows_from_dataset(dataset: dict, metric: str, limit: int = None) -> list[dict]:
    """Convert dataset to rows for display, with optional limit"""
    rows = []
    for name, d in dataset.items():
        races = int(d.get("races") or 0)
        wins = int(d.get("wins") or 0)
        poles = int(d.get("poles") or 0)
        podiums = int(d.get("podiums") or 0)
        top10s = int(d.get("top10s") or d.get("top5s") or d.get("top5") or 0)
        laps_complete = int(d.get("laps_complete") or 0)
        laps_lead = int(d.get("laps_lead") or 0)
        race_distances = d.get("race_distances", [])
        
        # Calculate percentages if not already present
        wins_pct = safe_float(d.get("wins_pct"))
        if wins_pct == 0 and races > 0:
            wins_pct = round((wins / races) * 100, 1)
        
        poles_pct = safe_float(d.get("poles_pct"))
        if poles_pct == 0 and races > 0:
            poles_pct = round((poles / races) * 100, 1)
        
        podiums_pct = safe_float(d.get("podiums_pct"))
        if podiums_pct == 0 and races > 0:
            podiums_pct = round((podiums / races) * 100, 1)
        
        top10s_pct = safe_float(d.get("top10s_pct"))
        if top10s_pct == 0 and races > 0:
            top10s_pct = round((top10s / races) * 100, 1)
        
        # Calculate laps percentages
        total_laps_available = sum(race_distances)
        laps_complete_pct = safe_float(d.get("laps_complete_pct"))
        if laps_complete_pct == 0 and total_laps_available > 0:
            laps_complete_pct = round((laps_complete / total_laps_available) * 100, 1)
        
        laps_lead_pct = safe_float(d.get("laps_lead_pct"))
        if laps_lead_pct == 0 and total_laps_available > 0:
            laps_lead_pct = round((laps_lead / total_laps_available) * 100, 1)
        
        rows.append({
            "name": name,
            "flag": flag_shortcode(d.get("country") or ""),
            "points": safe_float(d.get("points")),
            "races": races,
            "wins": wins,
            "wins_pct": wins_pct,
            "poles": poles,
            "poles_pct": poles_pct,
            "podiums": podiums,
            "podiums_pct": podiums_pct,
            "top10s": top10s,
            "top10s_pct": top10s_pct,
            "avg_incidents": round(safe_float(d.get("avg_incidents")), 2),
            "avg_start": round(safe_float(d.get("avg_start")), 2),
            "avg_finish": round(safe_float(d.get("avg_finish")), 2),
            "laps_complete": laps_complete,
            "laps_complete_pct": laps_complete_pct,
            "laps_lead": laps_lead,
            "laps_lead_pct": laps_lead_pct,
            "fastest_laps": int(d.get("fastest_laps") or 0),
            "race_distances": race_distances,
            "position_change": safe_float(d.get("position_change")),
        })
    
    rows.sort(key=lambda r: _sort_key(metric, r), reverse=True)
    if limit:
        return rows[:limit]  # Return limited results if specified
    return rows  # Return all results if no limit specified

DATA_ROOT = os.path.join(os.path.dirname(__file__), "data")
SEASONS_DIR = os.path.join(DATA_ROOT, "seasons")
BACKUPS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "backups")
BACKUP_STATE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "backups", ".last_backup.txt")
MAX_BACKUPS = 5                 # maximum number of backups to keep
BACKUP_INTERVAL_HOURS = 24      # auto backup cadence in hours
BACKUP_INTERVAL_DAYS = 7        # days between backups
RETENTION_DAYS = 30             # days to keep old backups
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
                    "service_account_file": os.path.join(os.path.dirname(__file__), "service-account-key.json"),
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
TOKEN = config.get("token") or ""
GUILD_IDS_RAW = config.get("guild_ids", [])
GUILD_IDS = [int(gid) for gid in GUILD_IDS_RAW if str(gid).isdigit()]
GUILD_OBJECTS = [discord.Object(id=gid) for gid in GUILD_IDS]

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
    
    service_account_file = gdrive_config.get("service_account_file", os.path.join(os.path.dirname(__file__), "service-account-key.json"))
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

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
tree = bot.tree

# Helper to mark commands as guild-only at definition time
# For now, allow all guild members to access non-admin commands
GDEC = app_commands.guilds(*GUILD_OBJECTS) if GUILD_OBJECTS else (lambda f: f)

# Channel restriction - bot only works in specific channel
RESTRICTED_CHANNEL_ID = 1408434832580808827

def check_channel_restriction(interaction: discord.Interaction) -> bool:
    """Check if the command is being used in the allowed channel"""
    return interaction.channel_id == RESTRICTED_CHANNEL_ID

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
    """Convert country code to Unicode flag emoji"""
    cc = (cc or "").strip().lower()
    if not cc or len(cc) != 2:
        return "🏁"  # Checkered flag for unknown/invalid country
    
    # Convert country code to Unicode flag emoji
    # Country codes are 2 letters, convert to regional indicator symbols
    try:
        flag = ""
        for char in cc:
            if 'a' <= char <= 'z':
                # Convert 'a' to '🇦' (U+1F1E6), 'b' to '🇧' (U+1F1E7), etc.
                flag += chr(ord('🇦') + ord(char) - ord('a'))
            else:
                # If character is not a-z, use a fallback
                flag += "🏁"
        return flag if flag and len(flag) == 2 else "🏁"
    except Exception:
        return "🏁"  # Fallback to checkered flag on any error

def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def validate_percentage_calculations(driver_data: dict, driver_name: str) -> None:
    """Validate percentage calculations for debugging"""
    races = driver_data.get("races", 0)
    wins = driver_data.get("wins", 0)
    podiums = driver_data.get("podiums", 0)
    top10s = driver_data.get("top10s", 0)
    poles = driver_data.get("poles", 0)
    
    # Validate basic logic
    if wins > races or podiums > races or top10s > races or poles > races:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Invalid counts (races: {races}, wins: {wins}, podiums: {podiums}, top10s: {top10s}, poles: {poles})"))
    
    if podiums < wins:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Podiums ({podiums}) less than wins ({wins})"))
    
    if top10s < podiums:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Top10s ({top10s}) less than podiums ({podiums})"))
    
    # Validate race distances
    race_distances = driver_data.get("race_distances", [])
    if len(race_distances) != races:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Race distances count ({len(race_distances)}) doesn't match races ({races})"))
    
    # Validate laps logic
    laps_complete = driver_data.get("laps_complete", 0)
    laps_lead = driver_data.get("laps_lead", 0)
    total_laps_available = sum(race_distances)
    
    if laps_lead > laps_complete:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Laps lead ({laps_lead}) > laps complete ({laps_complete})"))
    
    if laps_complete > total_laps_available and total_laps_available > 0:
        print(console_safe(f"⚠️ Validation Warning for {driver_name}: Laps complete ({laps_complete}) > total available ({total_laps_available})"))
    
    # Log calculated percentages for verification
    wins_pct = driver_data.get("wins_pct", 0)
    podiums_pct = driver_data.get("podiums_pct", 0)
    top10s_pct = driver_data.get("top10s_pct", 0)
    poles_pct = driver_data.get("poles_pct", 0)
    laps_complete_pct = driver_data.get("laps_complete_pct", 0)
    laps_lead_pct = driver_data.get("laps_lead_pct", 0)

async def send_to_logs(embed: discord.Embed) -> None:
    """Send an embed to the logs channel if configured"""
    try:
        logs_channel_id = config.get("channels", {}).get("logs")
        if logs_channel_id:
            logs_channel = bot.get_channel(int(logs_channel_id))
            if logs_channel:
                await logs_channel.send(embed=embed)
    except Exception as e:
        print(console_safe(f"⚠️ Could not send to logs channel: {e}"))

def list_uploaded_jsons() -> list[str]:
    """List all uploaded JSON files in the uploads directory"""
    files = []
    for fn in os.listdir(UPLOADS_STORE_DIR):
        if fn.lower().endswith(".json"):
            files.append(fn)
    # sort by mtime desc
    files.sort(key=lambda fn: os.path.getmtime(os.path.join(UPLOADS_STORE_DIR, fn)), reverse=True)
    return files

def _generate_content_hash(content: bytes) -> str:
    """Generate a SHA-256 hash of the JSON content for duplicate detection."""
    import hashlib
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
                        
                        # Reverse poles
                        if race_info["start"] == 1 and driver.get("poles", 0) > 0:
                            driver["poles"] = max(0, driver.get("poles", 0) - 1)
                            season_modified = True
                        
                        # Reverse other stats
                        if driver.get("podiums", 0) > 0 and race_info["finish"] <= 3:
                            driver["podiums"] = max(0, driver.get("podiums", 0) - 1)
                            season_modified = True
                        
                        if driver.get("top10s", 0) > 0 and race_info["finish"] <= 10:
                            driver["top10s"] = max(0, driver.get("top10s", 0) - 1)
                            season_modified = True
                        
                        # Reverse points and incidents
                        if driver.get("points", 0) > 0:
                            driver["points"] = max(0, driver.get("points", 0) - race_info["points"])
                            season_modified = True
                        
                        if driver.get("incidents", 0) > 0:
                            driver["incidents"] = max(0, driver.get("incidents", 0) - race_info["incidents"])
                            season_modified = True
                
                if season_modified:
                    save_season_drivers(season, drivers_data)
                    affected_seasons.append(season)
                    
            except Exception as e:
                print(console_safe(f"⚠️ Error processing season {season}: {e}"))
                continue
        
        return affected_seasons
        
    except Exception as e:
        print(console_safe(f"❌ Error removing ingested data: {e}"))
        return []

def _sanitize_filename(name: str) -> str:
    """Sanitize a filename to be safe for filesystem operations"""
    # Remove or replace unsafe characters
    unsafe_chars = '<>:"/\\|?*'
    for char in unsafe_chars:
        name = name.replace(char, '_')
    
    # Limit length
    if len(name) > 100:
        name = name[:100]
    
    return name.strip()

def current_season_name() -> Optional[str]:
    return config.get("current_season")

def set_current_season(name: Optional[str]):
    config["current_season"] = name
    save_config(config)

# ========= Ingest: iRacing JSON (event_result) =========
def process_json_into_season(file_content: str, season: str) -> bool:
    """Process a JSON file content into a season and return success status"""
    try:
        # Parse the JSON content
        data = json.loads(file_content)
        
        # Ensure the season directory exists
        ensure_season_dir(season)
        
        # Ingest the data into the season
        updated, processed = ingest_iracing_event(data, season)
        
        # Return True if processing was successful
        return processed > 0
    except Exception as e:
        print(f"Error processing JSON into season {season}: {e}")
        return False

def ingest_iracing_event(payload: Dict, season: str) -> Tuple[int, int]:
    """
    Parse iRacing event_result JSON for the RACE session and update season stats.
    Returns (drivers_updated, rows_processed).
    """
    data = payload.get("data", {})
    sessions = data.get("session_results", [])
    
    # Find qualifying and race sessions
    qual_sessions = [s for s in sessions if str(s.get("simsession_name", "")).upper() == "QUALIFY"]
    race_sessions = [s for s in sessions if str(s.get("simsession_name", "")).upper() == "RACE"]
    
    if not race_sessions:
        raise ValueError("No RACE session found in payload['data']['session_results']")

    race_results = race_sessions[0].get("results", []) or []
    qual_results = qual_sessions[0].get("results", []) if qual_sessions else []
    
    # Create qualifying lookup by driver name
    qual_lookup = {}
    for qual_row in qual_results:
        name = str(qual_row.get("display_name") or "").strip()
        if name:
            qual_lookup[name] = qual_row
    
    drivers_map = load_season_drivers(season)

    updated = 0
    processed = 0
    for row in race_results:
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
        else:
            # If no starting position, try to use qualifying position
            if name in qual_lookup:
                qual_row = qual_lookup[name]
                qual_pos = qual_row.get("finish_position")
                if qual_pos is not None and isinstance(qual_pos, int) and qual_pos >= 0:
                    start_1 = qual_pos + 1
            # If still no starting position, use a reasonable default (like last place)
            if start_1 is None and fin_1 is not None:
                # Assume they started from the back if no qualifying data
                start_1 = fin_1 + 5  # Start 5 positions behind finish as a reasonable default

        inc = float(row.get("incidents", 0) or 0)
        pts = float(row.get("champ_points", 0) or 0)

        d = drivers_map.setdefault(name, {
            "country": country, "races": 0, "wins": 0, "podiums": 0, "top10s": 0, "poles": 0,
            "points": 0.0, "avg_incidents": 0.0, "avg_start": 0.0, "avg_finish": 0.0, "_rp": 0,
            "laps_complete": 0, "laps_lead": 0, "fastest_laps": 0,
            "position_change": 0.0,
            # Data tracking for calculations
            "lap_times": [], "position_changes": [], "weather_conditions": []
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
                # Ensure consistent top10s counting (migrate from legacy if needed)
                current_top10s = int(d.get("top10s", d.get("top5s", d.get("top5", 0))))
                d["top10s"] = current_top10s + 1

        # poles (starting position 1 = pole position)
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

        # Extract lap-related data
        laps_complete = int(row.get("laps_complete", 0) or 0)
        laps_lead = int(row.get("laps_lead", 0) or 0)
        
        # Update lap stats
        d["laps_complete"] = int(d.get("laps_complete", 0)) + laps_complete
        d["laps_lead"] = int(d.get("laps_lead", 0)) + laps_lead
        
        # Track race distances for each individual race this driver participated in
        if "race_distances" not in d:
            d["race_distances"] = []
        
        # Find the winner's lap count for this specific race to determine race distance
        race_distance = 0
        for other_row in race_results:
            other_finish = other_row.get("finish_position", -1)
            if other_finish == 0:  # Winner (0-indexed in iRacing)
                race_distance = int(other_row.get("laps_complete", 0) or 0)
                break
        
        # If no winner found, use max laps completed as fallback
        if race_distance == 0:
            race_distance = max((int(r.get("laps_complete", 0) or 0) for r in race_results), default=0)
        
        # Add this race's distance to the driver's list
        if race_distance > 0:
            d["race_distances"].append(race_distance)
        
        # Check if this driver had the fastest lap in this race
        # We'll need to compare with other drivers' best lap times
        best_lap_time = row.get("best_lap_time", -1)
        if best_lap_time > 0:  # Valid lap time
            # Find the fastest lap time across all drivers in this race
            fastest_lap_in_race = float('inf')
            for other_row in race_results:
                other_best = other_row.get("best_lap_time", -1)
                if other_best > 0:
                    fastest_lap_in_race = min(fastest_lap_in_race, other_best)
            
            # If this driver had the fastest lap, increment their count
            if best_lap_time == fastest_lap_in_race:
                d["fastest_laps"] = int(d.get("fastest_laps", 0)) + 1
        
        d["_rp"] = rp + ra
        d["points"] = safe_float(d.get("points", 0)) + pts
        
        # Calculate specialist metrics
        # Position change (gained/lost positions)
        if start_1 is not None and fin_1 is not None:
            pos_change = start_1 - fin_1  # Positive = gained positions, Negative = lost positions
            d["position_change"] = safe_float(d.get("position_change", 0)) + pos_change
        
        # Store lap time data for consistency calculation
        if "lap_times" not in d:
            d["lap_times"] = []
        avg_lap = row.get("average_lap", 0)
        if avg_lap > 0:
            d["lap_times"].append(avg_lap)
        
        # Store position change data
        if "position_changes" not in d:
            d["position_changes"] = []
        if start_1 is not None and fin_1 is not None:
            d["position_changes"].append(start_1 - fin_1)
        
        # Store weather conditions
        if "weather_conditions" not in d:
            d["weather_conditions"] = []
        weather_data = race_sessions[0].get("weather_result", {})
        if weather_data:
            d["weather_conditions"].append({
                "temp": weather_data.get("avg_temp", 0),
                "humidity": weather_data.get("avg_rel_humidity", 0),
                "wind": weather_data.get("avg_wind_speed", 0),
                "clouds": weather_data.get("avg_cloud_cover_pct", 0)
            })
        
        # Qualifying vs Race performance
        if name in qual_lookup:
            qual_row = qual_lookup[name]
            qual_time = qual_row.get("best_qual_lap_time", -1)
            race_time = row.get("best_lap_time", -1)
            if qual_time > 0 and race_time > 0:
                # Calculate percentage difference (positive = race faster than qual)
                qual_vs_race = ((qual_time - race_time) / qual_time) * 100
                d["qual_vs_race"] = safe_float(d.get("qual_vs_race", 0)) + qual_vs_race

        updated += 1

    # round avgs
    for d in drivers_map.values():
        d["avg_incidents"] = round(safe_float(d.get("avg_incidents")), 3)
        d["avg_start"]     = round(safe_float(d.get("avg_start")), 3)
        d["avg_finish"]    = round(safe_float(d.get("avg_finish")), 3)
        
        # Calculate final specialist metrics
        races = d.get("races", 0)
        if races > 0:
            # Consistency Rating: Based on lap time variance (lower variance = higher rating)
            if len(d.get("lap_times", [])) > 1:
                lap_times = d["lap_times"]
                mean_time = sum(lap_times) / len(lap_times)
                variance = sum((t - mean_time) ** 2 for t in lap_times) / len(lap_times)
                # Convert to 0-100 scale (lower variance = higher rating)
                consistency = max(0, 100 - (variance / 1000000))  # Scale factor for milliseconds
                d["consistency_rating"] = round(consistency, 1)
            
            # Recovery Rate: How often drivers improve position after incidents
            position_changes = d.get("position_changes", [])
            if position_changes:
                positive_changes = sum(1 for pc in position_changes if pc > 0)
                recovery_rate = (positive_changes / len(position_changes)) * 100
                d["recovery_rate"] = round(recovery_rate, 1)
            
            # Qual vs Race: Average performance difference
            if d.get("qual_vs_race", 0) != 0:
                d["qual_vs_race"] = round(d["qual_vs_race"] / races, 1)
            

            
            # Total position change (not average) - keep as cumulative total
            d["position_change"] = safe_float(d.get("position_change", 0))

    # Save the updated season data
    save_season_drivers(season, drivers_map)
    
    return updated, processed

# Removed duplicate _rows_from_dataset function - using the correct one from line 214

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
            "laps_complete": int(d.get("laps_complete") or 0),
            "laps_lead": int(d.get("laps_lead") or 0),
            "fastest_laps": int(d.get("fastest_laps") or 0),
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

def render_driver_block(title_flag_name: str, d: Dict, show_person_emoji: bool = False) -> str:
    # Handle the title line and person emoji placement
    if show_person_emoji:
        # If this is the linked driver (Find Me), use Italic Bold formatting without emoji
        title_line = f"***{title_flag_name}***"
    else:
        # Normal title without emoji
        title_line = f"**{title_flag_name}**" if title_flag_name else ""
    
    # Calculate completion percentage based on races the driver actually participated in
    races = int(d.get('races', 0))
    laps_complete = int(d.get('laps_complete', 0))
    laps_lead = int(d.get('laps_lead', 0))
    wins = int(d.get('wins', 0))
    podiums = int(d.get('podiums', 0))
    top10s = int(d.get('top10s', d.get('top5s', d.get('top5', 0))))
    poles = int(d.get('poles', 0))
    race_distances = d.get('race_distances', [])
    
    # Get percentage values from data (already calculated in _aggregate_career)
    wins_pct = safe_float(d.get('wins_pct', 0))
    podiums_pct = safe_float(d.get('podiums_pct', 0))
    top10s_pct = safe_float(d.get('top10s_pct', 0))
    poles_pct = safe_float(d.get('poles_pct', 0))
    laps_complete_pct = safe_float(d.get('laps_complete_pct', 0))
    laps_lead_pct = safe_float(d.get('laps_lead_pct', 0))
    
    # Calculate completion percentage based on races the driver actually participated in
    if races > 0 and race_distances:
        # Calculate percentage based on total laps available from races this driver participated in
        total_laps_available = sum(race_distances)
        completion_pct = round((laps_complete / total_laps_available) * 100, 1) if total_laps_available > 0 else 0
    elif races > 0:
        # Fallback: Estimate total possible laps (assuming average race length)
        estimated_total_laps = races * 20  # Assume average 20 laps per race
        completion_pct = round((laps_complete / estimated_total_laps) * 100, 1) if estimated_total_laps > 0 else 0
    else:
        completion_pct = 0
    
    # Use the calculated percentages from _aggregate_career() instead of hardcoded values
    # These percentages are already calculated as: (metric / races) * 100
    
    return (
        f"{title_line}\n"
        f"🏎️ Races: **{races}**\n"
        f"🏆 Wins: **{wins}** - *{wins_pct}%*\n"
        f"🥈 Podiums: **{podiums}** - *{podiums_pct}%*\n"
        f"🔟 Top 10s: **{top10s}** - *{top10s_pct}%*\n"
        f"💯 Points: **{int(safe_float(d.get('points')))}**\n"
        f"🎯 Poles: **{poles}** - *{poles_pct}%*\n"
        f"⚡ Fastest Laps: **{int(d.get('fastest_laps', 0))}**\n"
        f"✅ Laps Completed: **{laps_complete}** - *{laps_complete_pct}%*\n"
        f"🚀 Laps Led: **{laps_lead}** - *{laps_lead_pct}%*\n"
        f"🚦 Avg Start: **{round(safe_float(d.get('avg_start')), 2)}**\n"
        f"🏁 Avg Finish: **{round(safe_float(d.get('avg_finish')), 2)}**\n"
        f"⚠️ Avg Incidents: **{round(safe_float(d.get('avg_incidents')), 2)}**\n"
        f"📊 Pos Gain/Loss: **{format_position_change(safe_float(d.get('position_change', 0)))}**"
    )



def render_leaderboard_embed(season_label: str, rows: list[dict], metric: str, current_page: int = 1, total_pages: int = 1, linked_driver_name: str = None, total_drivers: int = None, interaction: discord.Interaction = None) -> discord.Embed:
    # Format title: "WiRL Leaderboard - (Season Name)" or "WiRL Leaderboard - (All Time)"
    if season_label == "All Time":
        title = "WiRL Leaderboard - All Time"
    else:
        title = f"WiRL Leaderboard - {season_label}"
    
    emb = discord.Embed(title=title, color=discord.Color.gold())
    if not rows:
        emb.description = "_No drivers found._"
        return emb
    
    # Add sorting info and pagination details
    sorting_info = f"Sorted by: {FILTER_LABEL[metric].split(' ', 1)[1]}"  # Remove emoji from filter label
    
    # Calculate total drivers if not provided
    if total_drivers is None:
        total_drivers = len(rows) + ((current_page - 1) * 5)
    
    # Calculate current page range
    start_pos = ((current_page - 1) * 5) + 1
    end_pos = min(current_page * 5, total_drivers)
    
    blocks = []
    for i, r in enumerate(rows):
        # Calculate the actual global position based on current page
        global_position = start_pos + i
        
        # Check if this is the linked driver
        is_linked_driver = linked_driver_name and r['name'].strip().lower() == linked_driver_name.strip().lower()
        
        # Use render_driver_block for consistency with other pages
        blocks.append(render_driver_block(f"{global_position}. {r['flag']} {r['name']}", r, show_person_emoji=is_linked_driver))
    
    # Check if this is a Find Me view (custom title indicates Find Me was used)
    if interaction and hasattr(interaction, 'message') and interaction.message and interaction.message.embeds:
        embed_title = interaction.message.embeds[0].title if interaction.message.embeds else ""
        if "Rank" in embed_title:
            # This is a Find Me view, show "Your Rank: X of Y"
            description_text = f"{sorting_info}\n\nYour Rank: {start_pos} of {total_drivers}\n\n"
        else:
            # Regular view, show "Driver ranks: X-Y of Z"
            description_text = f"{sorting_info}\n\nDriver ranks: {start_pos}-{end_pos} of {total_drivers}\n\n"
    else:
        # Default to regular view
        description_text = f"{sorting_info}\n\nDriver ranks: {start_pos}-{end_pos} of {total_drivers}\n\n"
    
    emb.description = description_text + "\n\n".join(blocks)
    emb.set_footer(text=f"Sorted by: {FILTER_LABEL[metric].split(' ', 1)[1]} • Page {current_page} of {total_pages}")
    return emb



def render_stats_embed(driver_name: str, season: str, season_map: Dict[str, Dict], career_map: Dict[str, Dict]) -> Optional[discord.Embed]:
    sd = season_map.get(driver_name)
    cd = career_map.get(driver_name)
    if not sd and not cd:
        return None
    flag = flag_shortcode((sd or {}).get("country") or (cd or {}).get("country") or "")
    emb = discord.Embed(title=f"{flag} {driver_name}", color=discord.Color.blurple())
    emb.description = " "  # Invisible separator (single space)
    if sd:
        emb.add_field(name=f"📅 **{season}**", value=render_driver_block("", sd), inline=False)
    if cd:
        emb.add_field(name=" ", value=" ", inline=False)  # Invisible separator
        emb.add_field(name="🏁 **Career**", value=render_driver_block("", cd), inline=False)
        emb.add_field(name=" ", value=" ", inline=False)  # Invisible separator
    return emb



# ========= Views: Leaderboard (Season dropdown above Metric dropdown) =========
class SeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str]):
        options = [discord.SelectOption(label="♾️ All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)))
        super().__init__(placeholder="Season", options=options, min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # If we're on a paginated view, return to main leaderboard (page 1)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Create new LeaderboardView with selected season
            new_view = LeaderboardView(self.values[0], view.metric)
            label, data = new_view._dataset()
            rows = _rows_from_dataset(data, new_view.metric, limit=5)
            
            # Get linked driver name for this user
            linked_driver_name = get_iracing_name(interaction.user.id)
            
            emb = render_leaderboard_embed(label, rows, new_view.metric, 1, 1, linked_driver_name, len(data))
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
            
            # Get linked driver name for this user
            linked_driver_name = get_iracing_name(interaction.user.id)
            
            emb = render_leaderboard_embed(label, rows, new_view.metric, 1, 1, linked_driver_name, len(data))
            await interaction.response.edit_message(embed=emb, view=new_view)
        else:
            # Normal refresh for LeaderboardView
            view.metric = self.values[0]
            # keep selected option highlighted
            for opt in self.options:
                opt.default = (opt.value == view.metric)
            await view.refresh(interaction)

# ========= Views: Specialist Leaderboard Dropdowns =========
class SpecialistSeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str]):
        options = [discord.SelectOption(label="♾️ All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)))
        super().__init__(placeholder="Season", options=options, min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # If we're on a paginated view, return to main specialist leaderboard (page 1)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Create new SpecialistLeaderboardView with selected season
            new_view = SpecialistLeaderboardView(self.values[0], view.metric)
            label, data = new_view._dataset()
            rows = _rows_from_dataset(data, new_view.metric, limit=5)
            
            # Get linked driver name for this user
            linked_driver_name = get_iracing_name(interaction.user.id)
            
            emb = render_leaderboard_embed(label, rows, new_view.metric, 1, 1, linked_driver_name, len(data))
            await interaction.response.edit_message(embed=emb, view=new_view)
        else:
            # Normal refresh for LeaderboardView
            view.season_choice = self.values[0]
            # keep selected option highlighted
            for opt in self.options:
                opt.default = (opt.value == view.season_choice)
            await view.refresh(interaction)

class SpecialistMetricDropdown(discord.ui.Select):
    def __init__(self, metric: str):
        options = [discord.SelectOption(label=lbl, value=key, description=desc, default=(key == metric)) for (lbl, key, desc) in SPECIALIST_FILTERS]
        super().__init__(placeholder="Specialist Metric", options=options, min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # If we're on a paginated view, return to main specialist leaderboard (page 1)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Create new SpecialistLeaderboardView with selected metric
            new_view = SpecialistLeaderboardView(view.season_choice, self.values[0])
            label, data = new_view._dataset()
            rows = _rows_from_dataset(data, new_view.metric, limit=5)
            
            # Get linked driver name for this user
            linked_driver_name = get_iracing_name(interaction.user.id)
            
            emb = render_leaderboard_embed(label, rows, new_view.metric, 1, 1, linked_driver_name, len(data))
            await interaction.response.edit_message(embed=emb, view=new_view)
        else:
            # Normal refresh for LeaderboardView
            view.metric = self.values[0]
            # keep selected option highlighted
            for opt in self.options:
                opt.default = (opt.value == view.metric)
            await view.refresh(interaction)

class FindMeButton(discord.ui.Button):
    def __init__(self, season_choice: Optional[str] = None, metric: str = "points", iracing_name: Optional[str] = None, is_specialist: bool = False):
        super().__init__(style=discord.ButtonStyle.secondary, label="Find Me", emoji="👤", row=2)
        self.season_choice = season_choice
        self.metric = metric
        self.iracing_name = iracing_name
        self.is_specialist = is_specialist

    async def callback(self, interaction: discord.Interaction):
        # Check if user has linked their Discord to iRacing
        if not self.iracing_name:
            iracing_name = get_iracing_name(interaction.user.id)
            if not iracing_name:
                await interaction.response.send_message(
                    content="🔗 **Link Required**\n\nYou need to link your Discord account to your iRacing name first.\n\n"
                    "Use `/link_account <iRacing_name>` to create the link.",
                    ephemeral=True
                )
                return
        else:
            iracing_name = self.iracing_name
        
        # Debug: Log the search attempt
        print(console_safe(f"🔍 Find Me: User {interaction.user.name} ({interaction.user.id}) searching for '{iracing_name}'"))
        
        # Get current season and metric from view if available (for dropdown updates)
        current_season = getattr(self.view, 'season_choice', self.season_choice)
        current_metric = getattr(self.view, 'metric', self.metric)
        current_is_specialist = getattr(self.view, 'is_specialist', self.is_specialist)
        
        # Get dataset and find user's position
        if hasattr(self.view, '_dataset'):
            # Called from LeaderboardView or SpecialistLeaderboardView
            label, data = self.view._dataset()
            # Check if this is a specialist view by looking at the view type or existing attribute
            if hasattr(self.view, '__class__') and 'Specialist' in self.view.__class__.__name__:
                current_is_specialist = True
            elif hasattr(self.view, 'is_specialist'):
                current_is_specialist = self.view.is_specialist
        elif hasattr(self.view, 'season_choice') and hasattr(self.view, 'metric'):
            # Called from FindMeResultsView - use current values from view
            if current_season == "__CAREER__":
                label, data = ("All Time", _aggregate_career())
            elif not current_season:
                ss = list_seasons()
                current_season = ss[-1] if ss else None
                if not current_season:
                    label, data = ("All Time", {})
                else:
                    label, data = (current_season, load_season_drivers(current_season))
            else:
                label, data = (current_season, load_season_drivers(current_season))
        else:
            await interaction.response.send_message(content="❌ Error: Cannot determine dataset.", ephemeral=True)
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
            
            await interaction.response.send_message(content=error_msg, ephemeral=True)
            return
        
        # Get all drivers sorted by metric for pagination
        rows = _rows_from_dataset(data, current_metric)
        if not rows:
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
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
            error_msg += f"**Available names in data:** {', '.join(potential_matches[:3])}\n\n"
            
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
            
            await interaction.response.send_message(content=error_msg, ephemeral=True)
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
        
        # Navigate the leaderboard to show the user's position
        # Calculate which page contains the user's rank
        user_rank = user_position + 1  # Convert to 1-indexed
        page_containing_user = (user_rank - 1) // 5  # 5 drivers per page
        
        # If this was called from a leaderboard view, navigate to the user's page
        if hasattr(self.view, 'current_page') and hasattr(self.view, 'refresh'):
            # Update the view to show the page containing the user
            self.view.current_page = page_containing_user
            
            # Update the leaderboard title to show the linked driver name with country flag
            if hasattr(self.view, 'season_choice') and hasattr(self.view, 'metric'):
                # Get the driver's country flag from the data
                driver_flag = ""
                for row in rows:
                    if row['name'].strip().lower() == iracing_name.strip().lower():
                        driver_flag = row['flag']
                        break
                
                # Create custom title showing linked driver name with flag and rank
                if hasattr(self.view, 'is_specialist') and self.view.is_specialist:
                    custom_title = f"WiRL Specialist Leaderboard - {driver_flag} {iracing_name} (Rank {user_rank})"
                else:
                    custom_title = f"WiRL Leaderboard - {driver_flag} {iracing_name} (Rank {user_rank})"
                
                # Refresh the leaderboard to show the user's page with updated title
                await self.view.refresh(interaction, custom_title)
            else:
                # Fallback if view doesn't have expected attributes
                await self.view.refresh(interaction)
        else:
            # If not called from a leaderboard view, just show the position
            await interaction.response.send_message(
                f"🎯 **Find Me Results**\n\n"
                f"**Your Driver Position: {user_rank} of {total_drivers}**\n\n"
                f"Use `/leaderboard` to see the full leaderboard.",
                ephemeral=True
            )

class NextPageButton(discord.ui.Button):
    def __init__(self, current_page: int = 1):
        super().__init__(style=discord.ButtonStyle.primary, label="Next", emoji="➡️", row=2)
        self.current_page = current_page

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Get dataset
        if hasattr(view, 'current_page') and hasattr(view, '_dataset'):
            label, data = view._dataset()
        else:
            # Handle different view types
            if hasattr(view, 'season_choice') and hasattr(view, 'metric'):
                if view.season_choice == "__CAREER__":
                    label, data = ("All Time", _aggregate_career())
                else:
                    label, data = (view.season_choice, load_season_drivers(view.season_choice))
            else:
                await interaction.response.send_message(content="❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Calculate next page details
        next_page = self.current_page + 1
        start_pos = (next_page - 1) * 5
        end_pos = start_pos + 5
        
        # Get all drivers and sort them
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
                "laps_complete": int(d.get("laps_complete") or 0),
                "laps_lead": int(d.get("laps_lead") or 0),
                "fastest_laps": int(d.get("fastest_laps") or 0),
                "race_distances": d.get("race_distances", []),
                "position_change": safe_float(d.get("position_change")),
            })
        
        rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
        
        # Get next 5 drivers
        next_rows = rows[start_pos:end_pos] if len(rows) > start_pos else []
        
        if not next_rows:
            await interaction.response.send_message(content="❌ No more drivers to show.", ephemeral=True)
            return
        
        # Add global positions
        for i, row in enumerate(next_rows):
            row["global_position"] = start_pos + i + 1
        
        # Calculate total pages
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        # Use standardized render function for consistent width and formatting
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, next_rows, view.metric, next_page, total_pages, linked_driver_name, total_drivers)
        
        # Check if there are more pages after this
        has_more_pages = len(rows) > end_pos
        
        # Create view with Previous Page button and Next Page if more available
        next_page_view = NextPageView(view.season_choice, view.metric, has_more_pages, next_page)
        await interaction.response.edit_message(embed=emb, view=next_page_view)

class PreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Previous", emoji="⬅️", row=2)

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        
        # Check if we're in a NextPageView (pagination context)
        if hasattr(view, 'current_page') and view.current_page > 1:
            # Go to previous page
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
                    await interaction.response.send_message(content="❌ Error: Cannot determine dataset.", ephemeral=True)
                    return
            
            if not data:
                await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
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
                    "laps_complete": int(d.get("laps_complete") or 0),
                    "laps_lead": int(d.get("laps_lead") or 0),
                    "fastest_laps": int(d.get("fastest_laps") or 0),
                    "race_distances": d.get("race_distances", []),
                    "position_change": safe_float(d.get("position_change")),
                })
            
            rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
            
            # Get previous page drivers
            previous_page_drivers = rows[start_pos:end_pos]
            
            # Add global positions
            for i, row in enumerate(previous_page_drivers):
                row["global_position"] = start_pos + i + 1
            
            # Calculate total pages
            total_drivers = len(rows)
            total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
            
            # Use standardized render function for consistent width and formatting
            # Get linked driver name for this user
            linked_driver_name = get_iracing_name(interaction.user.id)
            
            emb = render_leaderboard_embed(label, previous_page_drivers, view.metric, previous_page, total_pages, linked_driver_name, total_drivers)
            
            # Check if there are more pages after this
            has_more_pages = len(rows) > end_pos
            
            # Create view with Previous Page button and Next Page if more available
            if previous_page == 1:
                # If going back to page 1, use the main LeaderboardView
                main_view = LeaderboardView(view.season_choice, view.metric)
                await interaction.response.edit_message(embed=emb, view=main_view)
            else:
                # Create NextPageView with Previous Page button
                previous_page_view = NextPageView(view.season_choice, view.metric, has_more_pages, previous_page)
                await interaction.response.edit_message(embed=emb, view=previous_page_view)
        else:
            # We're already on page 1, do nothing
            await interaction.response.send_message(content="ℹ️ Already on the first page.", ephemeral=True)

class GoToTopButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Top", emoji="⬆️", row=3)
    
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
                await interaction.response.send_message(content="❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Get top 5 drivers
        rows = _rows_from_dataset(data, view.metric, limit=5)
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, view.metric, 1, 1, linked_driver_name, len(data))
        
        # Create new main view
        main_view = LeaderboardView(view.season_choice, view.metric)
        await interaction.response.edit_message(embed=emb, view=main_view)

class GoToBottomButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Bottom", emoji="⬇️", row=3)
    
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
                await interaction.response.send_message(content="❌ Error: Cannot determine dataset.", ephemeral=True)
                return
        
        if not data:
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
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
                "laps_complete": int(d.get("laps_complete") or 0),
                "laps_lead": int(d.get("laps_lead") or 0),
                "fastest_laps": int(d.get("fastest_laps") or 0),
                "race_distances": d.get("race_distances", []),
                "position_change": safe_float(d.get("position_change")),
            })
        
        # Use normal sorting to get correct rankings (best to worst)
        rows.sort(key=lambda r: _sort_key(view.metric, r), reverse=True)
        
        # Calculate last page
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        start_pos = (total_pages - 1) * 5  # Last page start position
        end_pos = total_drivers
        
        # Get last page drivers and add global positions
        last_page_drivers = rows[start_pos:end_pos]
        
        # Add global positions to each driver for proper ranking display
        for i, driver in enumerate(last_page_drivers):
            driver['global_position'] = start_pos + i + 1
        
        # Use standardized render function for consistent width and formatting
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, last_page_drivers, view.metric, total_pages, total_pages, linked_driver_name, total_drivers, interaction)
        
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
        self.add_item(FindMeButton(self.season_choice, self.metric, None, False))
        self.add_item(PreviousPageButton())
        self.add_item(GoToTopButton())
        self.add_item(GoToBottomButton())
        
        # Always add Next Page button, but disable if no more pages
        next_button = NextPageButton()
        next_button.disabled = not has_more_pages
        self.add_item(next_button)
        
        # Update all button states after adding all buttons
        self._update_button_states()

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
    
    def _update_button_states(self):
        """Update button states based on current page and data"""
        label, data = self._dataset()
        if not data:
            return
        
        # Calculate total pages
        total_drivers = len(data)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        # Update button states
        for item in self.children:
            if isinstance(item, PreviousPageButton):
                item.disabled = (self.current_page <= 1)  # Disable on page 1
            elif isinstance(item, NextPageButton):
                item.disabled = (self.current_page >= total_pages)  # Disable on last page
            elif isinstance(item, GoToTopButton):
                item.disabled = (self.current_page <= 1)  # Disable on page 1
            elif isinstance(item, GoToBottomButton):
                item.disabled = (self.current_page >= total_pages)  # Disable on last page

class BackToTopButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Top", emoji="⬆️", row=3)

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
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, view.metric, 1, 1, linked_driver_name, len(data))
        
        # Update the message to show the normal leaderboard with Find Me button
        try:
            await interaction.response.edit_message(embed=emb, view=main_view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=main_view)

class FindMeResultsView(discord.ui.View):
    def __init__(self, season_choice: Optional[str], metric: str, iracing_name: str, start_page: int = 0, is_specialist: bool = False):
        super().__init__(timeout=900)
        self.season_choice = season_choice
        self.metric = metric
        self.iracing_name = iracing_name
        self.page = start_page  # Start at the user's actual page
        self.is_specialist = is_specialist
        self.add_item(FindMeSeasonDropdown(self.season_choice, self.iracing_name))
        self.add_item(FindMeMetricDropdown(self.metric, self.iracing_name, self.is_specialist))
        self.add_item(FindMeButton(self.season_choice, self.metric, self.iracing_name, self.is_specialist))
        self.add_item(FindMePreviousPageButton())
        self.add_item(FindMeNextPageButton())
        self.add_item(BackToTopButton())
        self.add_item(GoToBottomButton())
        
        # Store initial data for immediate display
        self._initial_data = None
        self._initial_user_position = None
        self._initial_total_pages = None
        
    async def initialize_with_centered_data(self, interaction: discord.Interaction, rows: list, user_position: int, total_drivers: int):
        """Initialize the view with centered data around the user's driver"""
        # Calculate pagination to center the user's driver on the page
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
        
        # Create embed showing centered view around user
        # Determine if this is a specialist metric or regular metric
        if self.is_specialist or self.metric in SPECIALIST_FILTER_LABEL:
            metric_label = SPECIALIST_FILTER_LABEL.get(self.metric, FILTER_LABEL.get(self.metric, "Unknown")).split(' ', 1)[1]
            title = f"WiRL Specialist Leaderboard - {self.iracing_name}"
            color = discord.Color.purple()
        else:
            metric_label = FILTER_LABEL.get(self.metric, "Unknown").split(' ', 1)[1]
            title = f"WiRL Leaderboard - {self.iracing_name}"
            color = discord.Color.green()
        
        emb = discord.Embed(
            title=title,
            description=f"Sorted by: {metric_label}\n\nYour Driver Position is {user_position + 1} of {total_drivers}\n\n",
            color=color
        )
        
        # Format the display for centered view
        blocks = []
        for i, row in enumerate(centered_drivers):
            global_pos = start_pos + i + 1
            display_name = f"{global_pos}. {row['flag']} {row['name']}"
            
            # Check if this is the linked driver to show person emoji
            row_name_clean = row["name"].strip().lower()
            iracing_name_clean = self.iracing_name.strip().lower()
            is_linked_driver = (row_name_clean == iracing_name_clean)
            
            # Use specialist driver block if this is a specialist view
            if self.is_specialist:
                blocks.append(render_specialist_driver_block(display_name, row, show_person_emoji=is_linked_driver))
            else:
                blocks.append(render_driver_block(display_name, row, show_person_emoji=is_linked_driver))
        
        emb.description += "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {metric_label}")
        
        # Update button states
        self._update_button_states(total_pages)
        
        # Store initial data for future use
        self._initial_data = rows
        self._initial_user_position = user_position
        self._initial_total_pages = total_pages
        
        # Display the centered view
        await interaction.response.edit_message(embed=emb, view=self)
        
    def _dataset(self) -> tuple[str, dict]:
        """Get dataset for this view - required for GoToBottomButton compatibility"""
        if self.season_choice == "__CAREER__":
            return ("All Time", _aggregate_career())
        elif not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
            if not self.season_choice:
                return ("All Time", {})
            else:
                return (self.season_choice, load_season_drivers(self.season_choice))
        else:
            return (self.season_choice, load_season_drivers(self.season_choice))

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
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
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
                iracing_name_clean in row_name_clean or row_name_clean in row_name_clean):
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
        # Determine if this is a specialist metric or regular metric
        if self.is_specialist or self.metric in SPECIALIST_FILTER_LABEL:
            metric_label = SPECIALIST_FILTER_LABEL.get(self.metric, FILTER_LABEL.get(self.metric, "Unknown")).split(' ', 1)[1]
            title = f"WiRL Specialist Leaderboard - {self.iracing_name}"
            color = discord.Color.purple()
        else:
            metric_label = FILTER_LABEL.get(self.metric, "Unknown").split(' ', 1)[1]
            title = f"WiRL Leaderboard - {self.iracing_name}"
            color = discord.Color.green()
        
        emb = discord.Embed(
            title=title,
            description=f"Sorted by: {metric_label}\n\nShowing drivers in positions {start_pos + 1}-{end_pos} of {total_drivers}",
            color=color
        )
        
        # Format the display for centered view
        blocks = []
        for i, row in enumerate(centered_drivers):
            global_pos = start_pos + i + 1
            display_name = f"{global_pos}. {row['flag']} {row['name']}"
            
            # Check if this is the linked driver to show person emoji
            row_name_clean = row["name"].strip().lower()
            iracing_name_clean = self.iracing_name.strip().lower()
            is_linked_driver = (row_name_clean == iracing_name_clean)
            
            # Use specialist driver block if this is a specialist view
            if self.is_specialist:
                blocks.append(render_specialist_driver_block(display_name, row, show_person_emoji=is_linked_driver))
            else:
                blocks.append(render_driver_block(display_name, row, show_person_emoji=is_linked_driver))
        
        # For Find Me view, show "Your Driver Position" instead of "Showing drivers in positions"
        position_info = f"Your Driver Position is {start_pos + 1} of {total_drivers}"
        emb.description = f"Sorted by: {metric_label}\n\n{position_info}\n\n" + "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {metric_label}")
        
        # Update button states - ensure buttons are properly enabled/disabled
        self._update_button_states(total_pages)
        
        await interaction.response.edit_message(embed=emb, view=self)

    def _update_button_states(self, total_pages: int):
        """Update the state of all navigation buttons"""
        for item in self.children:
            if isinstance(item, FindMePreviousPageButton):
                item.disabled = (self.page <= 0)
            elif isinstance(item, FindMeNextPageButton):
                item.disabled = (self.page >= total_pages - 1)
            elif isinstance(item, GoToTopButton):
                item.disabled = (self.page <= 0)
            elif isinstance(item, GoToBottomButton):
                item.disabled = (self.page >= total_pages - 1)

    async def show_page(self, interaction: discord.Interaction, page: int):
        """Show a specific page of the Find Me results"""
        # Get dataset
        if self.season_choice == "__CAREER__":
            label, data = ("All Time", _aggregate_career())
        elif not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
            if not self.season_choice:
                label, data = ("All Time", {})
            else:
                label, data = (self.season_choice, load_season_drivers(self.season_choice))
        else:
            label, data = (self.season_choice, load_season_drivers(self.season_choice))
        
        if not data:
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Get all drivers sorted by metric
        rows = _rows_from_dataset(data, self.metric)
        if not rows:
            await interaction.response.edit_message(content="❌ No data found for the selected season.", view=None)
            return
        
        # Calculate pagination for the requested page
        total_drivers = len(rows)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        # Ensure page is within bounds
        page = max(0, min(page, total_pages - 1))
        self.page = page
        
        # Calculate start and end positions for the page
        start_pos = page * 5
        end_pos = min(start_pos + 5, total_drivers)
        
        # Get drivers for the requested page
        page_drivers = rows[start_pos:end_pos]
        
        # Create embed showing the requested page
        # Determine if this is a specialist metric or regular metric
        if self.is_specialist or self.metric in SPECIALIST_FILTER_LABEL:
            metric_label = SPECIALIST_FILTER_LABEL.get(self.metric, FILTER_LABEL.get(self.metric, "Unknown")).split(' ', 1)[1]
            title = f"WiRL Specialist Leaderboard - {self.iracing_name}"
            color = discord.Color.purple()
        else:
            metric_label = FILTER_LABEL.get(self.metric, "Unknown").split(' ', 1)[1]
            title = f"WiRL Leaderboard - {self.iracing_name}"
            color = discord.Color.green()
        
        emb = discord.Embed(
            title=title,
            color=color
        )
        
        # Format the display for the page
        blocks = []
        for i, row in enumerate(page_drivers):
            global_pos = start_pos + i + 1
            display_name = f"{global_pos}. {row['flag']} {row['name']}"
            
            # Check if this is the linked driver to show person emoji
            row_name_clean = row["name"].strip().lower()
            iracing_name_clean = self.iracing_name.strip().lower()
            is_linked_driver = (row_name_clean == iracing_name_clean)
            
            # Use specialist driver block if this is a specialist view
            if self.is_specialist:
                blocks.append(render_specialist_driver_block(display_name, row, show_person_emoji=is_linked_driver))
            else:
                blocks.append(render_driver_block(display_name, row, show_person_emoji=is_linked_driver))
        
        # For Find Me view, show "Your Driver Position" instead of "Showing drivers in positions"
        # Find the actual ranking of the linked driver
        linked_driver_ranking = None
        for i, row in enumerate(rows):
            if row['name'].strip().lower() == self.iracing_name.strip().lower():
                linked_driver_ranking = i + 1
                break
        
        position_info = f"Your Driver Position is {linked_driver_ranking} of {total_drivers}"
        emb.description = f"Sorted by: {metric_label}\n\n{position_info}\n\n" + "\n\n".join(blocks)
        emb.set_footer(text=f"Sorted by: {metric_label}")
        
        # Update button states using the centralized method
        self._update_button_states(total_pages)
        
        await interaction.response.edit_message(embed=emb, view=self)

class FindMePreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Previous", emoji="⬅️", row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        if view.page > 0:
            view.page -= 1
            await view.show_page(interaction, view.page)
        else:
            await interaction.response.send_message(content="❌ Already on first page.", ephemeral=True)

class FindMeNextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Next", emoji="➡️", row=2)
    
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
            await interaction.response.send_message(content="❌ No data found for the selected season.", ephemeral=True)
            return
        
        # Check if there are more pages
        total_drivers = len(data)
        total_pages = max(1, (total_drivers + 4) // 5)  # 5 drivers per page
        
        if view.page < total_pages - 1:
            view.page += 1
            await view.show_page(interaction, view.page)
        else:
            await interaction.response.send_message(content="❌ No more pages to show.", ephemeral=True)

class FindMeSeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str], iracing_name: str):
        self.iracing_name = iracing_name
        options = [discord.SelectOption(label="♾️ All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)))
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
    def __init__(self, metric: str, iracing_name: str, is_specialist: bool = False):
        self.iracing_name = iracing_name
        self.is_specialist = is_specialist
        
        # Use specialist filters if this is a specialist view, otherwise use regular filters
        if is_specialist:
            filter_options = FILTERS + SPECIALIST_FILTERS
        else:
            filter_options = FILTERS
        
        options = [discord.SelectOption(label=lbl, value=key, default=(key == metric)) for (lbl, key) in filter_options]
        super().__init__(placeholder="Metric", options=options, min_values=1, max_values=1, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: "FindMeResultsView" = self.view  # type: ignore
        view.metric = self.values[0]
        
        # Update selected option highlighting
        for opt in self.options:
            opt.default = (opt.value == view.metric)
        
        # Refresh the Find Me results
        await view.refresh_find_me(interaction)

# ========= Views: Leaderboard (Season dropdown above Metric dropdown) =========
class LeaderboardView(discord.ui.View):
    def __init__(self, initial_season: Optional[str], metric: str = "points"):
        super().__init__(timeout=900)
        self.season_choice = initial_season  # "__CAREER__" or season name
        self.metric = metric
        self.current_page = 0
        self.drivers_per_page = 5  # Show 5 drivers per page
        self.add_item(SeasonDropdown(self.season_choice))
        self.add_item(MetricDropdown(self.metric))
        self.add_item(FindMeButton(self.season_choice, self.metric, None, False))
        self.add_item(PreviousPageButton())
        self.add_item(NextPageButton())
        self.add_item(GoToTopButton())
        self.add_item(GoToBottomButton())
        
        # Initialize button states
        self._update_button_states()

    def _dataset(self) -> tuple[str, dict]:
        if self.season_choice == "__CAREER__":
            return ("All Time", _aggregate_career())
        if not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
        if not self.season_choice:
            return ("All Time", {})
        
        return (f"{self.season_choice}", load_season_drivers(self.season_choice))

    def _get_rows(self) -> list[dict]:
        """Get all rows without limit, then paginate them"""
        label, data = self._dataset()
        # Get ALL rows without limit, then paginate them
        all_rows = _rows_from_dataset(data, self.metric, limit=None)
        return all_rows

    def _get_paginated_rows(self) -> list[dict]:
        """Get the current page of rows"""
        all_rows = self._get_rows()
        start_idx = self.current_page * self.drivers_per_page
        end_idx = start_idx + self.drivers_per_page
        return all_rows[start_idx:end_idx]

    def _update_button_states(self):
        """Update button states based on current page"""
        all_rows = self._get_rows()
        total_pages = max(1, (len(all_rows) + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Find and update all navigation buttons
        for item in self.children:
            if isinstance(item, PreviousPageButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, NextPageButton):
                item.disabled = (self.current_page >= total_pages - 1)
            elif isinstance(item, GoToTopButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, GoToBottomButton):
                item.disabled = (self.current_page >= total_pages - 1)

    async def refresh(self, interaction: discord.Interaction, custom_title: str = None):
        label, data = self._dataset()
        rows = self._get_paginated_rows()
        # Calculate total pages
        total_drivers = len(self._get_rows())
        total_pages = max(1, (total_drivers + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, self.metric, self.current_page + 1, total_pages, linked_driver_name, total_drivers)
        
        # Override title if custom title is provided
        if custom_title:
            emb.title = custom_title
        
        # Update button states
        self._update_button_states()
        
        try:
            await interaction.response.edit_message(embed=emb, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self)

    async def show(self, interaction: discord.Interaction):
        label, data = self._dataset()
        rows = self._get_paginated_rows()
        # Calculate total pages
        total_drivers = len(self._get_rows())
        total_pages = max(1, (total_drivers + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, self.metric, self.current_page + 1, total_pages, linked_driver_name, total_drivers)
        
        # Update button states
        self._update_button_states()
        
        await interaction.response.send_message(embed=emb, view=self, ephemeral=True)

    async def go_to_page(self, page: int, interaction: discord.Interaction):
        """Go to a specific page"""
        all_rows = self._get_rows()
        total_pages = max(1, (len(all_rows) + self.drivers_per_page - 1) // self.drivers_per_page)
        
        if 0 <= page < total_pages:
            self.current_page = page
            await self.refresh(interaction)
        else:
            await interaction.response.send_message(f"❌ Invalid page. Available pages: 1-{total_pages}", ephemeral=True)




    def _dataset(self) -> tuple[str, dict]:
        if self.season_choice == "__CAREER__":
            return ("All Time", _aggregate_career())
        if not self.season_choice:
            ss = list_seasons()
            self.season_choice = ss[-1] if ss else None
        if not self.season_choice:
            return ("All Time", {})
        
        return (f"{self.season_choice}", load_season_drivers(self.season_choice))

    def _get_rows(self) -> list[dict]:
        """Get all rows without limit, then paginate them"""
        label, data = self._dataset()
        # Get ALL rows without limit, then paginate them
        all_rows = _rows_from_dataset(data, self.metric, limit=None)
        return all_rows

    def _get_paginated_rows(self) -> list[dict]:
        """Get the current page of rows"""
        all_rows = self._get_rows()
        start_idx = self.current_page * self.drivers_per_page
        end_idx = start_idx + self.drivers_per_page
        return all_rows[start_idx:end_idx]

    def _update_button_states(self):
        """Update button states based on current page"""
        all_rows = self._get_rows()
        total_pages = max(1, (len(all_rows) + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Find and update all navigation buttons
        for item in self.children:
            if isinstance(item, PreviousPageButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, NextPageButton):
                item.disabled = (self.current_page >= total_pages - 1)
            elif isinstance(item, GoToTopButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, GoToBottomButton):
                item.disabled = (self.current_page >= total_pages - 1)

    async def refresh(self, interaction: discord.Interaction, custom_title: str = None):
        label, data = self._dataset()
        rows = self._get_paginated_rows()
        # Calculate total pages
        total_drivers = len(self._get_rows())
        total_pages = max(1, (total_drivers + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, self.metric, self.current_page + 1, total_pages, linked_driver_name, total_drivers)
        
        # Override title if custom title is provided
        if custom_title:
            emb.title = custom_title
        
        # Update button states
        self._update_button_states()
        
        try:
            await interaction.response.edit_message(embed=emb, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self)

    async def show(self, interaction: discord.Interaction):
        label, data = self._dataset()
        rows = self._get_paginated_rows()
        # Calculate total pages
        total_drivers = len(self._get_rows())
        total_pages = max(1, (total_drivers + self.drivers_per_page - 1) // self.drivers_per_page)
        
        # Get linked driver name for this user
        linked_driver_name = get_iracing_name(interaction.user.id)
        
        emb = render_leaderboard_embed(label, rows, self.metric, self.current_page + 1, total_pages, linked_driver_name, total_drivers)
        
        # Update button states
        self._update_button_states()
        
        await interaction.response.send_message(embed=emb, view=self, ephemeral=True)

    async def go_to_page(self, page: int, interaction: discord.Interaction):
        """Go to a specific page"""
        all_rows = self._get_rows()
        total_pages = max(1, (len(all_rows) + self.drivers_per_page - 1) // self.drivers_per_page)
        
        if 0 <= page < total_pages:
            self.current_page = page
            await self.refresh(interaction)
        else:
            await interaction.response.send_message(f"❌ Invalid page. Available pages: 1-{total_pages}", ephemeral=True)

# ========= Views: Stats season selector =========
class StatsSeasonDropdown(discord.ui.Select):
    def __init__(self, driver_name: str, current: Optional[str]):
        self.driver_name = driver_name
        # Only show actual created seasons, no "All Time" option
        options = [discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)) for s in list_seasons()]
        super().__init__(placeholder="Select Season", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        season = self.values[0]
        # keep selected option highlighted
        for opt in self.options:
            opt.default = (opt.value == season)
        
        # If no driver is selected yet (initial view), just update the selection
        if not self.driver_name:
            await interaction.response.defer()
            return
        
        # Handle season data based on selection
        if season == "__CAREER__":
            # Career mode - use aggregated data for both season and career
            season_map = _aggregate_career()
        else:
            # Specific season mode
            season_map = load_season_drivers(season)
        
        career_map = _aggregate_career()
        emb = render_stats_embed(self.driver_name, season, season_map, career_map)
        if not emb:
            try:
                await interaction.response.send_message(content="❌ No stats found.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.edit_original_response(content="No stats found.", embed=None, view=None)
            return
        try:
            await interaction.response.edit_message(embed=emb, view=self.view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self.view)

class SpecialistStatsSeasonDropdown(discord.ui.Select):
    def __init__(self, driver_name: str, current: Optional[str]):
        self.driver_name = driver_name
        options = [discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)) for s in list_seasons()]
        super().__init__(placeholder="Select Season", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        season = self.values[0]
        # keep selected option highlighted
        for opt in self.options:
            opt.default = (opt.value == season)
        season_map = load_season_drivers(season)
        career_map = _aggregate_career()
        emb = render_specialist_stats_embed(self.driver_name, season, season_map, career_map)
        if not emb:
            try:
                await interaction.response.send_message(content="❌ No stats found.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.edit_original_response(content="No stats found.", embed=None, view=None)
            return
        try:
            await interaction.response.edit_message(embed=emb, view=self.view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self.view)

class StatsView(discord.ui.View):
    def __init__(self, driver_name: str, initial_season: Optional[str], career_map: Dict[str, Dict]):
        super().__init__(timeout=600)
        self.driver_name = driver_name
        self.season_choice = initial_season
        self.career_map = career_map
        self.metric = "points"  # Default metric for stats view
        self.add_item(StatsSeasonDropdown(driver_name, initial_season))
        self.add_item(BackToDriverListButton(career_map, is_specialist=False))

class SpecialistStatsView(discord.ui.View):
    def __init__(self, driver_name: str, initial_season: Optional[str], career_map: Dict[str, Dict]):
        super().__init__(timeout=600)
        self.driver_name = driver_name
        self.season_choice = initial_season
        self.career_map = career_map
        self.metric = "consistency_rating"  # Default metric for specialist stats view
        self.add_item(SpecialistStatsSeasonDropdown(driver_name, initial_season))
        self.add_item(BackToDriverListButton(career_map, is_specialist=True))

class BackToDriverListButton(discord.ui.Button):
    def __init__(self, career_map: Dict[str, Dict], is_specialist: bool = False):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="⬅️ Back to Driver List",
            row=1
        )
        self.career_map = career_map
        self.is_specialist = is_specialist

    async def callback(self, interaction: discord.Interaction):
        # Create a new driver selection view based on context
        if self.is_specialist:
            view = SpecialistDriverStatsView(self.career_map)
        else:
            # Get the current season from the parent view to maintain selection
            current_season = None  # Default to first available season
            if hasattr(self.view, 'season_choice'):
                current_season = self.view.season_choice
            view = DriverStatsView(self.career_map, current_season)
        await interaction.response.edit_message(
            content="",
            view=view
        )

class DriverStatsView(discord.ui.View):
    def __init__(self, career_map: Dict[str, Dict], default_season: Optional[str] = None):
        super().__init__(timeout=600)
        self.career_map = career_map
        self.default_season = default_season
        self.current_page = 0
        self.drivers_per_page = 25  # Discord limit
        
        # Add season selector dropdown (row 0)
        self.add_item(StatsSeasonDropdown("", default_season))  # Empty driver name for initial view
        
        # Create paginated dropdown (row 0)
        self.add_item(PaginatedDriverDropdown(career_map, self.current_page, self.drivers_per_page))
        
        # Add navigation buttons (row 1)
        self.add_item(PreviousPageButton())
        self.add_item(NextPageButton())
        self.add_item(DriverSearchButton())
        
        # Update button states
        self._update_button_states()
    
    def _update_button_states(self):
        """Update button states based on current page"""
        total_pages = (len(self.career_map) + self.drivers_per_page - 1) // self.drivers_per_page
        
        # Find and update Previous button
        for item in self.children:
            if isinstance(item, PreviousPageButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, NextPageButton):
                item.disabled = (self.current_page >= total_pages - 1)
    
    async def refresh_dropdown(self, interaction: discord.Interaction):
        """Refresh the dropdown with current page data"""
        # Remove old dropdown
        for item in self.children[:]:
            if isinstance(item, PaginatedDriverDropdown):
                self.remove_item(item)
                break
        
        # Add new dropdown
        self.add_item(PaginatedDriverDropdown(self.career_map, self.current_page, self.drivers_per_page))
        
        # Update button states
        self._update_button_states()
        
        # Update the message
        await interaction.response.edit_message(view=self)

class SpecialistDriverStatsView(discord.ui.View):
    def __init__(self, career_map: Dict[str, Dict], default_season: Optional[str] = None):
        super().__init__(timeout=600)
        self.career_map = career_map
        self.default_season = default_season
        self.current_page = 0
        self.drivers_per_page = 25  # Discord limit
        
        # Add season selector dropdown (row 0)
        self.add_item(SpecialistStatsSeasonDropdown("", default_season))  # Empty driver name for initial view
        
        # Create paginated dropdown (row 0)
        self.add_item(PaginatedSpecialistDriverDropdown(career_map, self.current_page, self.drivers_per_page))
        
        # Add navigation buttons (row 1)
        self.add_item(PreviousPageButton())
        self.add_item(NextPageButton())
        self.add_item(DriverSearchButton())
        
        # Update button states
        self._update_button_states()
    
    def _update_button_states(self):
        """Update button states based on current page"""
        total_pages = (len(self.career_map) + self.drivers_per_page - 1) // self.drivers_per_page
        
        # Find and update Previous button
        for item in self.children:
            if isinstance(item, PreviousPageButton):
                item.disabled = (self.current_page <= 0)
            elif isinstance(item, NextPageButton):
                item.disabled = (self.current_page >= total_pages - 1)
    
    async def refresh_dropdown(self, interaction: discord.Interaction):
        """Refresh the dropdown with current page data"""
        # Remove old dropdown
        for item in self.children[:]:
            if isinstance(item, PaginatedSpecialistDriverDropdown):
                self.remove_item(item)
                break
        
        # Add new dropdown
        self.add_item(PaginatedSpecialistDriverDropdown(self.career_map, self.current_page, self.drivers_per_page))
        
        # Update button states
        self._update_button_states()
        
        # Update the message
        await interaction.response.edit_message(view=self)

class PaginatedDriverDropdown(discord.ui.Select):
    def __init__(self, career_map: Dict[str, Dict], page: int, drivers_per_page: int):
        # Create options for the current page
        options = []
        
        # Sort all drivers alphabetically
        sorted_drivers = sorted(career_map.items(), key=lambda x: x[0].lower())
        
        # Calculate start and end indices for current page
        start_idx = page * drivers_per_page
        end_idx = min(start_idx + drivers_per_page, len(sorted_drivers))
        
        # Create options for current page
        for name, data in sorted_drivers[start_idx:end_idx]:
            country_code = data.get("country", "").upper()
            
            # Create a clean label with just the driver name
            label = name
            
            # Add full country name in description (e.g., "AU - Australia", "BR - Brazil")
            if country_code:
                # Map common country codes to full names
                country_names = {
                    "AR": "Argentina", "AU": "Australia", "BR": "Brazil", "CA": "Canada",
                    "CL": "Chile", "CN": "China", "CO": "Colombia", "CZ": "Czech Republic",
                    "DK": "Denmark", "FI": "Finland", "FR": "France", "DE": "Germany",
                    "HK": "Hong Kong", "IN": "India", "ID": "Indonesia", "IE": "Ireland",
                    "IT": "Italy", "JP": "Japan", "MY": "Malaysia", "MX": "Mexico",
                    "NL": "Netherlands", "NZ": "New Zealand", "NO": "Norway", "PE": "Peru",
                    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PY": "Paraguay",
                    "RU": "Russia", "SG": "Singapore", "ZA": "South Africa", "ES": "Spain",
                    "SE": "Sweden", "CH": "Switzerland", "TH": "Thailand", "TR": "Turkey",
                    "AE": "United Arab Emirates", "GB": "United Kingdom", "US": "United States",
                    "UY": "Uruguay", "VE": "Venezuela"
                }
                full_name = country_names.get(country_code, country_code)
                description = f"{country_code} - {full_name}"
            else:
                description = "Country: Unknown"
            
            options.append(discord.SelectOption(
                label=label, 
                value=name,
                description=description
            ))
        
        # Create placeholder with page info
        total_pages = (len(career_map) + drivers_per_page - 1) // drivers_per_page
        placeholder = f"Select a driver... (Page {page + 1} of {total_pages})"
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            min_values=1,
            max_values=1
        )
        self.career_map = career_map
        self.page = page
        self.drivers_per_page = drivers_per_page

    async def callback(self, interaction: discord.Interaction):
        """Handle driver selection from dropdown"""
        selected_driver = self.values[0]
        
        # Get the selected season from the parent view
        view = self.view  # type: ignore
        selected_season = None
        
        # Find the season dropdown to get the current selection
        for item in view.children:
            if isinstance(item, StatsSeasonDropdown):
                selected_season = item.values[0] if item.values else view.default_season
                break
        
        # If no season dropdown found, use first available season
        if not selected_season:
            seasons = list_seasons()
            if seasons:
                selected_season = seasons[0]  # Use first available season
            else:
                selected_season = None
        
        # Handle season data based on selection
        if selected_season:
            # Specific season mode
            season = selected_season
            season_map = load_season_drivers(season)
        else:
            # No seasons available
            season = None
            season_map = {}
        
        # Render the stats embed
        emb = render_stats_embed(selected_driver, season, season_map, self.career_map)
        if not emb:
            await interaction.response.send_message(f"❌ No stats found for `{selected_driver}`.", ephemeral=True)
            return
        
        # Create stats view with navigation buttons
        stats_view = StatsView(selected_driver, season, self.career_map)
        await interaction.response.send_message(
            content=f"Stats for **{selected_driver}**:",
            embed=emb,
            view=stats_view,
            ephemeral=True
        )

class PreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Previous",
            emoji="⬅️",
            row=2
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        if hasattr(view, 'current_page') and view.current_page > 0:
            # Ensure we don't skip any ranks by going to the previous page
            view.current_page -= 1
            # Check if this is a leaderboard view or driver stats view
            if hasattr(view, 'refresh'):
                await view.refresh(interaction)
            elif hasattr(view, 'refresh_dropdown'):
                await view.refresh_dropdown(interaction)

class NextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Next",
            emoji="➡️",
            row=2
        )
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore
        if hasattr(view, 'current_page'):
            # Check if this is a leaderboard view or driver stats view
            if hasattr(view, 'refresh'):
                # Leaderboard view - calculate total pages from _get_rows()
                all_rows = view._get_rows()
                total_pages = max(1, (len(all_rows) + view.drivers_per_page - 1) // view.drivers_per_page)
                if view.current_page < total_pages - 1:
                    view.current_page += 1
                    await view.refresh(interaction)
            elif hasattr(view, 'refresh_dropdown'):
                # Driver stats view - calculate total pages from career_map
                total_pages = (len(view.career_map) + view.drivers_per_page - 1) // view.drivers_per_page
                if view.current_page < total_pages - 1:
                    view.current_page += 1
                    await view.refresh_dropdown(interaction)

class PaginatedSpecialistDriverDropdown(discord.ui.Select):
    def __init__(self, career_map: Dict[str, Dict], page: int, drivers_per_page: int):
        # Create options for the current page
        options = []
        
        # Sort all drivers alphabetically
        sorted_drivers = sorted(career_map.items(), key=lambda x: x[0].lower())
        
        # Calculate start and end indices for current page
        start_idx = page * drivers_per_page
        end_idx = min(start_idx + drivers_per_page, len(sorted_drivers))
        
        # Create options for current page
        for name, data in sorted_drivers[start_idx:end_idx]:
            country_code = data.get("country", "").upper()
            
            # Create a clean label with just the driver name
            label = name
            
            # Add full country name in description (e.g., "AU - Australia", "BR - Brazil")
            if country_code:
                # Map common country codes to full names
                country_names = {
                    "AR": "Argentina", "AU": "Australia", "BR": "Brazil", "CA": "Canada",
                    "CL": "Chile", "CN": "China", "CO": "Colombia", "CZ": "Czech Republic",
                    "DK": "Denmark", "FI": "Finland", "FR": "France", "DE": "Germany",
                    "HK": "Hong Kong", "IN": "India", "ID": "Indonesia", "IE": "Ireland",
                    "IT": "Italy", "JP": "Japan", "MY": "Malaysia", "MX": "Mexico",
                    "NL": "Netherlands", "NZ": "New Zealand", "NO": "Norway", "PE": "Peru",
                    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PY": "Paraguay",
                    "RU": "Russia", "SG": "Singapore", "ZA": "South Africa", "ES": "Spain",
                    "SE": "Sweden", "CH": "Switzerland", "TH": "Thailand", "TR": "Turkey",
                    "AE": "United Arab Emirates", "GB": "United Kingdom", "US": "United States",
                    "UY": "Uruguay", "VE": "Venezuela"
                }
                full_name = country_names.get(country_code, country_code)
                description = f"{country_code} - {full_name}"
            else:
                description = "Country: Unknown"
            
            options.append(discord.SelectOption(
                label=label, 
                value=name,
                description=description
            ))
        
        # Create placeholder with page info
        total_pages = (len(career_map) + drivers_per_page - 1) // drivers_per_page
        placeholder = f"Select a driver for specialist stats... (Page {page + 1} of {total_pages})"
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            min_values=1,
            max_values=1
        )
        self.career_map = career_map
        self.page = page
        self.drivers_per_page = drivers_per_page

    async def callback(self, interaction: discord.Interaction):
        """Handle driver selection from dropdown for specialist stats"""
        selected_driver = self.values[0]
        
        # Get available seasons for stats
        seasons = list_seasons()
        current_season = current_season_name()
        
        # If no current season, use the first available season or None
        if current_season:
            season = current_season
            season_map = load_season_drivers(season)
        elif seasons:
            season = seasons[0]  # Use first available season
            season_map = load_season_drivers(season)
        else:
            season = None
            season_map = {}
        
        # Render the specialist stats embed
        emb = render_specialist_stats_embed(selected_driver, season, season_map, self.career_map)
        if not emb:
            await interaction.response.send_message(f"❌ No specialist stats found for `{selected_driver}`.", ephemeral=True)
            return
        
        # Create specialist stats view with navigation buttons
        stats_view = SpecialistStatsView(selected_driver, season, self.career_map)
        await interaction.response.send_message(
            content=f"Specialist Stats for **{selected_driver}**:",
            embed=emb,
            view=stats_view,
            ephemeral=True
        )

class SpecialistDriverStatsDropdown(discord.ui.Select):
    def __init__(self, career_map: Dict[str, Dict]):
        # Create options with driver name and full country name
        # Limit to 25 options maximum (Discord limit)
        options = []
        for name, data in career_map.items():
            country_code = data.get("country", "").upper()
            
            # Create a clean label with just the driver name
            label = name
            
            # Add full country name in description (e.g., "AU - Australia", "BR - Brazil")
            if country_code:
                # Map common country codes to full names
                country_names = {
                    "AR": "Argentina", "AU": "Australia", "BR": "Brazil", "CA": "Canada",
                    "CL": "Chile", "CN": "China", "CO": "Colombia", "CZ": "Czech Republic",
                    "DK": "Denmark", "FI": "Finland", "FR": "France", "DE": "Germany",
                    "HK": "Hong Kong", "IN": "India", "ID": "Indonesia", "IE": "Ireland",
                    "IT": "Italy", "JP": "Japan", "MY": "Malaysia", "MX": "Mexico",
                    "NL": "Netherlands", "NZ": "New Zealand", "NO": "Norway", "PE": "Peru",
                    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PY": "Paraguay",
                    "RU": "Russia", "SG": "Singapore", "ZA": "South Africa", "ES": "Spain",
                    "SE": "Sweden", "CH": "Switzerland", "TH": "Thailand", "TR": "Turkey",
                    "AE": "United Arab Emirates", "GB": "United Kingdom", "US": "United States",
                    "UY": "Uruguay", "VE": "Venezuela"
                }
                full_name = country_names.get(country_code, country_code)
                description = f"{country_code} - {full_name}"
            else:
                description = "Country: Unknown"
            
            options.append(discord.SelectOption(
                label=label, 
                value=name,
                description=description
            ))
            
            # Stop at 25 options to respect Discord's limit
            if len(options) >= 25:
                break
        
        # Sort options alphabetically by driver name
        options.sort(key=lambda x: x.label.lower())
        
        super().__init__(
            placeholder="Select a driver to view specialist stats...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.career_map = career_map

    async def callback(self, interaction: discord.Interaction):
        """Handle driver selection from dropdown for specialist stats"""
        selected_driver = self.values[0]
        
        # Get available seasons for stats
        seasons = list_seasons()
        current_season = current_season_name()
        
        # If no current season, use the first available season or None
        if current_season:
            season = current_season
            season_map = load_season_drivers(season)
        elif seasons:
            season = seasons[0]  # Use first available season
            season_map = load_season_drivers(season)
        else:
            season = None
            season_map = {}
        
        # Render the specialist stats embed
        emb = render_specialist_stats_embed(selected_driver, season, season_map, self.career_map)
        if not emb:
            await interaction.response.send_message(f"❌ No specialist stats found for `{selected_driver}`.", ephemeral=True)
            return
        
        # Create specialist stats view with navigation buttons
        stats_view = SpecialistStatsView(selected_driver, season, self.career_map)
        await interaction.response.send_message(
            content=f"Specialist Stats for **{selected_driver}**:",
            embed=emb,
            view=stats_view,
            ephemeral=True
        )

class DriverSearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="🔍 Search Drivers",
            row=2
        )

    async def callback(self, interaction: discord.Interaction):
        # Get the appropriate data source based on the view type
        if hasattr(self.view, 'career_map'):
            # DriverStatsView - use career_map
            data_source = self.view.career_map
        elif hasattr(self.view, 'names') and hasattr(self.view, 'season_choice'):
            # DriversView - load season data
            if self.view.season_choice == "__CAREER__":
                data_source = _aggregate_career()
            else:
                data_source = load_season_drivers(self.view.season_choice)
        else:
            await interaction.response.send_message("❌ Error: Cannot determine data source.", ephemeral=True)
            return
        
        # Create a modal for search input
        modal = DriverSearchModal(data_source)
        await interaction.response.send_modal(modal)

class DriverSearchModal(discord.ui.Modal, title="Search Drivers"):
    def __init__(self, data_source: Dict[str, Dict]):
        super().__init__()
        self.data_source = data_source
        self.search_input = discord.ui.TextInput(
            label="Search by name or country",
            placeholder="Enter driver name or country code (e.g., 'john' or 'us')",
            min_length=1,
            max_length=50,
            required=True
        )
        self.add_item(self.search_input)

    async def on_submit(self, interaction: discord.Interaction):
        search_term = self.search_input.value.lower().strip()
        
        # Search through drivers
        results = []
        for name, data in self.data_source.items():
            name_lower = name.lower()
            country = (data.get("country") or "").lower()
            
            # Check if search term matches name or country
            name_match = (search_term in name_lower or 
                         search_term in name_lower.replace(" ", ""))
            country_match = search_term in country
            
            if name_match or country_match:
                results.append((name, data))
        
        # Sort results alphabetically
        results.sort(key=lambda x: x[0].lower())
        
        if not results:
            # Get some sample data to show what's available
            sample_drivers = list(self.data_source.items())[:3]
            sample_text = ""
            if sample_drivers:
                sample_text = f"\n\n**Sample available drivers:**\n"
                for name, data in sample_drivers:
                    country = data.get("country", "Unknown")
                    sample_text += f"• {name} ({country.upper()})\n"
            
            await interaction.response.send_message(
                f"🔍 **No drivers found** matching '{search_term}'{sample_text}",
                ephemeral=True
            )
            return
        
        # If only one result, show stats directly
        if len(results) == 1:
            driver_name = results[0][0]
            await self.show_driver_stats(interaction, driver_name)
            return
        
        # If multiple results, show selection dropdown
        if len(results) <= 25:  # Discord dropdown limit
            view = DriverSearchResultsView(results, self.data_source)
            await interaction.response.send_message(
                f"🔍 **Search Results** for '{search_term}':\nFound {len(results)} drivers. Select one to view their stats:",
                view=view,
                ephemeral=True
            )
        else:
            # Too many results, show first 25 with note
            view = DriverSearchResultsView(results[:25], self.data_source)
            await interaction.response.send_message(
                f"🔍 **Search Results** for '{search_term}' (showing first 25 of {len(results)}):\nSelect one to view their stats:",
                view=view,
                ephemeral=True
            )
    
    async def show_driver_stats(self, interaction: discord.Interaction, driver_name: str):
        """Show stats for a specific driver"""
        # Get available seasons for stats
        seasons = list_seasons()
        current_season = current_season_name()
        
        # If no current season, use the first available season or None
        if current_season:
            season = current_season
            season_map = load_season_drivers(season)
        elif seasons:
            season = seasons[0]  # Use first available season
            season_map = load_season_drivers(season)
        else:
            season = None
            season_map = {}
        
        # Render the stats embed
        emb = render_stats_embed(driver_name, season, season_map, self.data_source)
        if not emb:
            await interaction.response.send_message(f"❌ No stats found for `{driver_name}`.", ephemeral=True)
            return
        
        # Create stats view with navigation buttons
        stats_view = StatsView(driver_name, season, self.data_source)
        await interaction.response.send_message(
            content=f"Stats for **{driver_name}**:",
            embed=emb,
            view=stats_view,
            ephemeral=True
        )

class DriverSearchResultsView(discord.ui.View):
    def __init__(self, search_results: list, data_source: Dict[str, Dict]):
        super().__init__(timeout=300)
        self.search_results = search_results
        self.data_source = data_source
        self.add_item(DriverSearchResultsDropdown(search_results, data_source))

class DriverSearchResultsDropdown(discord.ui.Select):
    def __init__(self, search_results: list, data_source: Dict[str, Dict]):
        # Create options for each search result
        options = []
        for name, data in search_results:
            country_code = data.get("country", "").upper()
            
            # Create a clean label with just the driver name
            label = name
            
            # Add full country name in description (e.g., "AU - Australia", "BR - Brazil")
            if country_code:
                # Map common country codes to full names
                country_names = {
                    "AR": "Argentina", "AU": "Australia", "BR": "Brazil", "CA": "Canada",
                    "CL": "Chile", "CN": "China", "CO": "Colombia", "CZ": "Czech Republic",
                    "DK": "Denmark", "FI": "Finland", "FR": "France", "DE": "Germany",
                    "HK": "Hong Kong", "IN": "India", "ID": "Indonesia", "IE": "Ireland",
                    "IT": "Italy", "JP": "Japan", "MY": "Malaysia", "MX": "Mexico",
                    "NL": "Netherlands", "NZ": "New Zealand", "NO": "Norway", "PE": "Peru",
                    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PY": "Paraguay",
                    "RU": "Russia", "SG": "Singapore", "ZA": "South Africa", "ES": "Spain",
                    "SE": "Sweden", "CH": "Switzerland", "TH": "Thailand", "TR": "Turkey",
                    "AE": "United Arab Emirates", "GB": "United Kingdom", "US": "United States",
                    "UY": "Uruguay", "VE": "Venezuela"
                }
                full_name = country_names.get(country_code, country_code)
                description = f"{country_code} - {full_name}"
            else:
                description = "Country: Unknown"
            
            options.append(discord.SelectOption(
                label=label,
                value=name,
                description=description
            ))
        
        super().__init__(
            placeholder="Select a driver to view stats...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.data_source = data_source

    async def callback(self, interaction: discord.Interaction):
        driver_name = self.values[0]
        
        # Get available seasons for stats
        seasons = list_seasons()
        current_season = current_season_name()
        
        # If no current season, use the first available season or None
        if current_season:
            season = current_season
            season_map = load_season_drivers(season)
        elif seasons:
            season = seasons[0]  # Use first available season
            season_map = load_season_drivers(season)
        else:
            season = None
            season_map = {}
        
        # Render the stats embed
        emb = render_stats_embed(driver_name, season, season_map, self.data_source)
        if not emb:
            await interaction.response.send_message(f"❌ No stats found for `{driver_name}`.", ephemeral=True)
            return
        
        # Create stats view with navigation buttons
        stats_view = StatsView(driver_name, season, self.data_source)
        await interaction.response.edit_message(
            content=f"Stats for **{driver_name}**:",
            embed=emb,
            view=stats_view
        )

# ========= Views: Drivers list (season dropdown only) =========
class DriversSeasonDropdown(discord.ui.Select):
    def __init__(self, current: Optional[str]):
        options = [discord.SelectOption(label="♾️ All Time", value="__CAREER__", default=(current == "__CAREER__"))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=f"📅 {s}", value=s, default=(current == s)))
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
            title = f"{season}"
        # Create list with flags and names, then sort by full driver name (not by flag)
        driver_entries = [(name, f"{flag_shortcode(d.get('country') or '')} {name}") for name, d in dmap.items()]
        driver_entries.sort(key=lambda x: x[0].lower())  # Sort by full driver name
        names = [entry[1] for entry in driver_entries]  # Extract the formatted strings
        # update parent view with dataset for pagination
        view: "DriversView" = self.view  # type: ignore
        if isinstance(view, DriversView):
            view.names = names
            view.title = title
            view.page = 0  # Reset to first page when season changes
        # Use the render_description method to include showing info
        desc = view.render_description() if isinstance(view, DriversView) else "_No drivers found._"
        emb = discord.Embed(title=f"{title}", description=desc, color=discord.Color.teal())
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
        
        # Add search button
        self.add_item(DriverSearchButton())
        
        # Add pagination buttons (will be conditionally shown)
        self.prev_button = DriversPrevButton()
        self.next_button = DriversNextButton()
        self.add_item(self.prev_button)
        self.add_item(self.next_button)

    def render_description(self) -> str:
        if not self.names:
            return "_No drivers found._"
        start = self.page * PAGE_SIZE
        end = start + PAGE_SIZE
        sliced = self.names[start:end]
        
        # Add showing drivers info
        showing_info = f"Showing drivers in list {start + 1}-{min(end, len(self.names))} of {len(self.names)}\n\n"
        
        return showing_info + "\n".join(f"{i+1+start}. {n}" for i, n in enumerate(sliced))

    async def rerender(self, interaction: discord.Interaction):
        emb = discord.Embed(title=f"{self.title}", description=self.render_description(), color=discord.Color.teal())
        
        # Calculate total pages
        total_pages = max(1, (len(self.names) + PAGE_SIZE - 1) // PAGE_SIZE)
        
        # Show/hide pagination buttons based on number of drivers
        if len(self.names) < PAGE_SIZE:
            # Less than 20 drivers, hide pagination buttons
            self.prev_button.disabled = True
            self.next_button.disabled = True
        else:
            # 20+ drivers, show pagination buttons with proper states
            self.prev_button.disabled = (self.page <= 0)
            self.next_button.disabled = (self.page >= total_pages - 1)
        
        try:
            await interaction.response.edit_message(embed=emb, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=emb, view=self)

class DriversPrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Previous", emoji="⬅️", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: "DriversView" = self.view  # type: ignore
        if not isinstance(view, DriversView):
            return
        if view.page > 0:
            view.page -= 1
        await view.rerender(interaction)

class DriversNextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Next", emoji="➡️", row=1)

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

# ========= Backup Functions =========
def backup_due() -> bool:
    """Check if backup is due (7 days since last backup)"""
    try:
        if not os.path.exists(BACKUP_STATE):
            return True
        
        with open(BACKUP_STATE, "r") as f:
            last_backup_str = f.read().strip()
        
        if not last_backup_str:
            return True
        
        last_backup = datetime.datetime.fromisoformat(last_backup_str)
        days_since = (datetime.datetime.now() - last_backup).days
        return days_since >= 7
        
    except Exception:
        return True

def save_backup_to_disk() -> str:
    """Save a backup of all bot data to disk"""
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.zip"
        backup_path = os.path.join(BACKUPS_DIR, backup_filename)
        
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add data directory
            if os.path.exists(DATA_ROOT):
                for root, dirs, files in os.walk(DATA_ROOT):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, os.path.dirname(DATA_ROOT))
                        zipf.write(file_path, arcname)
            
            # Add config file
            if os.path.exists(CONFIG_FILE):
                zipf.write(CONFIG_FILE, "config.json")
        
        # Update last backup timestamp
        with open(BACKUP_STATE, "w") as f:
            f.write(datetime.datetime.now().isoformat())
        
        print(console_safe(f"✅ Backup saved: {backup_filename}"))
        return backup_path
        
    except Exception as e:
        print(console_safe(f"❌ Backup failed: {e}"))
        return ""

def create_backup_zip() -> Tuple[discord.File, str]:
    """Create a backup zip file and return it as a Discord file attachment"""
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

# ========= Background Tasks =========
@tasks.loop(hours=24)
async def auto_backup():
    """Automatic daily backup task that runs every 24 hours"""
    try:
        print(console_safe("🔄 Starting automatic backup..."))
        
        # Check if backup is due (only backup when >= 7 days since last)
        if not backup_due():
            print(console_safe("⏰ Backup not due yet, skipping..."))
            return
        
        # Perform the backup
        path = save_backup_to_disk()
        if path:
            print(console_safe(f"✅ Automatic backup completed: {os.path.basename(path)}"))
            
            # Send notification to logs channel if configured
            try:
                emb = discord.Embed(
                    title="🔄 Automatic Backup Completed",
                    description=f"Saved `{os.path.basename(path)}`",
                    color=discord.Color.green(),
                    timestamp=tz_now()
                )
                await send_to_logs(emb)
            except Exception as e:
                print(console_safe(f"⚠️ Could not send backup notification: {e}"))
        else:
            print(console_safe("❌ Automatic backup failed"))
            
    except Exception as e:
        print(console_safe(f"❌ Error in automatic backup: {e}"))
        # Try to send error notification
        try:
            emb = discord.Embed(
                title="❌ Automatic Backup Failed",
                description=f"Error: {str(e)}",
                color=discord.Color.red(),
                timestamp=tz_now()
            )
            await send_to_logs(emb)
        except:
            pass

@tasks.loop(minutes=30)
async def health_check():
    """Health check task that runs every 30 minutes to ensure bot is responsive"""
    try:
        # Simple health check - just log that we're alive
        current_time = tz_now().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(console_safe(f"💓 Health check passed at {current_time}"))
        
        # Check if bot is still connected to Discord
        if not bot.is_ready():
            print(console_safe("⚠️ Bot not ready, attempting to reconnect..."))
            # The bot will automatically attempt to reconnect
        else:
            print(console_safe(f"✅ Bot healthy - Connected to {len(bot.guilds)} guild(s)"))
            
    except Exception as e:
        print(console_safe(f"❌ Health check failed: {e}"))

@tasks.loop(hours=6)
async def cleanup_old_backups():
    """Clean up old backup files to prevent disk space issues"""
    try:
        print(console_safe("🧹 Starting backup cleanup..."))
        
        # Get list of backup files
        backup_files = []
        for file in os.listdir(BACKUPS_DIR):
            if file.endswith('.zip') and file != '.last_backup.txt':
                file_path = os.path.join(BACKUPS_DIR, file)
                backup_files.append((file_path, os.path.getmtime(file_path)))
        
        # Sort by modification time (oldest first)
        backup_files.sort(key=lambda x: x[1])
        
        # Remove old backups if we have more than MAX_BACKUPS
        if len(backup_files) > MAX_BACKUPS:
            files_to_remove = len(backup_files) - MAX_BACKUPS
            removed_count = 0
            
            for file_path, _ in backup_files[:files_to_remove]:
                try:
                    os.remove(file_path)
                    removed_count += 1
                    print(console_safe(f"🗑️ Removed old backup: {os.path.basename(file_path)}"))
                except Exception as e:
                    print(console_safe(f"⚠️ Could not remove backup {os.path.basename(file_path)}: {e}"))
            
            if removed_count > 0:
                print(console_safe(f"✅ Cleanup completed: removed {removed_count} old backup(s)"))
                
                # Send notification to logs channel
                try:
                    emb = discord.Embed(
                        title="🧹 Backup Cleanup Completed",
                        description=f"Removed {removed_count} old backup file(s)",
                        color=discord.Color.blue(),
                        timestamp=tz_now()
                    )
                    await send_to_logs(emb)
                except Exception as e:
                    print(console_safe(f"⚠️ Could not send cleanup notification: {e}"))
        else:
            print(console_safe(f"✅ No cleanup needed - {len(backup_files)} backups (max: {MAX_BACKUPS})"))
            
    except Exception as e:
        print(console_safe(f"❌ Error in backup cleanup: {e}"))

@tasks.loop(hours=12)
async def sync_commands_periodic():
    """Periodically sync slash commands to ensure they stay registered"""
    try:
        print(console_safe("🔄 Periodic command sync starting..."))
        
        if GUILD_OBJECTS:
            total_synced = 0
            for guild_obj in GUILD_OBJECTS:
                try:
                    synced_commands = await tree.sync(guild=guild_obj)
                    total_synced += len(synced_commands)
                    print(console_safe(f"✅ Synced {len(synced_commands)} commands to guild {guild_obj.id}"))
                except Exception as guild_error:
                    print(console_safe(f"❌ Failed to sync to guild {guild_obj.id}: {guild_error}"))
            
            print(console_safe(f"🎯 Periodic sync completed: {total_synced} total commands synced"))
        else:
            synced_commands = await tree.sync()
            print(console_safe(f"✅ Periodic sync completed: {len(synced_commands)} commands synced globally"))
            
    except Exception as e:
        print(console_safe(f"❌ Error in periodic command sync: {e}"))

# ========= Events =========
@bot.event
async def on_ready():
    print(console_safe(f"✅ Logged in as {bot.user} | Guilds: {[ (g.name, g.id) for g in bot.guilds ]}"))
    print(console_safe("🚀 Bot is ready to receive commands!"))

    # Start all background tasks
    auto_backup.start()
    health_check.start()
    cleanup_old_backups.start()
    sync_commands_periodic.start()
    
    print(console_safe("🔄 Background tasks started successfully!"))

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

@tree.command(name="admin_setup", description="Configure channels and roles")
@GDEC
async def setup_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.send_message("⚙️ **Setup — Channels**\nSelect the target channels for bot features.", view=SetupChannelsView(), ephemeral=True)

# ========= Upload (Admin) — select season =========
@tree.command(name="admin_upload", description="Upload an iRacing event_result JSON file into a selected season.")
@GDEC
@app_commands.describe(file="JSON file to upload")
async def upload_cmd(interaction: discord.Interaction, file: discord.Attachment):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    
    if not file.filename.lower().endswith(".json"):
        await interaction.response.send_message("⚠️ Please upload a `.json` iRacing event_result.", ephemeral=True); return

    # Get available seasons
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons available. Create one with `/season_create` first.", ephemeral=True); return
    
    # Store the file temporarily for processing
    raw = await file.read()
    
    # Check for duplicate content
    is_duplicate, existing_file = _is_duplicate_json(raw)
    if is_duplicate:
        await interaction.response.send_message(f"⚠️ **Duplicate detected!** This JSON content already exists in `{existing_file}`. Upload cancelled to prevent duplicate data.", ephemeral=True)
        return
    
    # Create season selection view
    view = UploadSeasonSelectView(file.filename, raw)
    await interaction.response.send_message(
        f"📁 **File ready:** `{file.filename}`\n\nSelect which season to upload this file into:",
        view=view,
        ephemeral=True
    )

# ========= Refresh Commands (Admin) =========
@tree.command(name="admin_refresh_commands", description="Refresh/sync all slash commands.")
@GDEC
async def refresh_commands_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    try:
        # Get command count before sync
        commands_before = tree.get_commands()
        before_count = len(commands_before)
        
        # Clear global commands first to prevent duplicates
        try:
            await tree.sync()  # Clear global commands
            print(console_safe("🧹 Cleared global commands to prevent duplicates"))
        except:
            pass
        
        # Sync commands to all configured guilds
        total_synced = 0
        guild_sync_results = []
        
        for guild_obj in GUILD_OBJECTS:
            try:
                synced_commands = await tree.sync(guild=guild_obj)
                guild_synced = len(synced_commands)
                total_synced += guild_synced
                guild_sync_results.append(f"• Guild {guild_obj.id}: {guild_synced} commands")
                print(console_safe(f"✅ Synced {guild_synced} commands to guild {guild_obj.id}"))
            except Exception as guild_error:
                guild_sync_results.append(f"• Guild {guild_obj.id}: ❌ Failed - {guild_error}")
                print(console_safe(f"❌ Failed to sync to guild {guild_obj.id}: {guild_error}"))
        
        # Detailed response with command info
        response = f"✅ **Successfully refreshed all slash commands!**\n\n"
        response += f"📊 **Command Summary:**\n"
        response += f"• Commands registered: {before_count}\n"
        response += f"• Total commands synced across all guilds: {total_synced}\n"
        response += f"• Global commands cleared to prevent duplicates\n\n"
        
        response += f"🏠 **Guild Sync Results:**\n"
        response += "\n".join(guild_sync_results)
        
        if before_count != total_synced:
            response += f"\n\n⚠️ **Note:** Command count mismatch detected. Some commands may not have synced properly."
        
        response += f"\n\n💡 **Use `/help` to see the updated command list** (auto-generated from current commands)"
        response += f"\n🔍 **Use `/commands_info` for detailed command analysis**"
        
        await interaction.followup.send(response, ephemeral=True)
        
        # Log to console
        print(console_safe(f"🔄 Commands refreshed: {before_count} registered → {total_synced} synced across {len(GUILD_OBJECTS)} guilds"))
        
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to refresh commands: `{e}`", ephemeral=True)

# ========= Leaderboard =========
@tree.command(name="leaderboard", description="Show Top-5 leaderboard with season & metric dropdowns (Career included).")
@GDEC
async def leaderboard_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    seasons = list_seasons()
    default_choice = "__CAREER__"  # Always default to career mode for full stats
    view = LeaderboardView(initial_season=default_choice, metric="points")
    await view.show(interaction)



# ========= Stats =========
@tree.command(name="driver_stats", description="Show a driver's Season & Career stats.")
@GDEC
async def driver_stats_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons yet.", ephemeral=True); return
    
    # Get all drivers from career data (All Time list) - same as leaderboard
    career_map = _aggregate_career()
    if not career_map:
        await interaction.response.send_message("⚠️ No driver data found.", ephemeral=True); return
    
    # Default to first available season (no more "All Time" option)
    default_season = seasons[0] if seasons else None
    
    # Create dropdown view with all drivers and season selection
    view = DriverStatsView(career_map, default_season)
    await interaction.response.send_message("", view=view, ephemeral=True)



@tree.command(name="my_stats", description="Show your own Season & Career stats (requires linked account).")
@GDEC
async def my_stats_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    # Check if user has linked their Discord to iRacing
    iracing_name = get_iracing_name(interaction.user.id)
    if not iracing_name:
        await interaction.response.send_message(
            content="🔗 **Link Required**\n\nYou need to link your Discord account to your iRacing name first.\n\n"
            "Use `/link_account <iRacing_name>` to create the link.",
            ephemeral=True
        )
        return
    
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons yet.", ephemeral=True); return
    season = current_season_name() or seasons[-1]
    season_map = load_season_drivers(season)
    career_map = _aggregate_career()
    emb = render_stats_embed(iracing_name, season, season_map, career_map)
    if not emb:
        await interaction.response.send_message(f"❌ No stats found for `{iracing_name}`.", ephemeral=True); return
    await interaction.response.send_message(embed=emb, view=StatsView(iracing_name, season, career_map), ephemeral=True)

# ========= Drivers =========
@tree.command(name="driver_list", description="List drivers with a season dropdown (no text input).")
@GDEC
async def drivers_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    seasons = list_seasons()
    if not seasons:
        await interaction.response.send_message("⚠️ No seasons found.", ephemeral=True); return
    season = current_season_name() or seasons[-1]
    dmap = load_season_drivers(season)
    # Create list with flags and names, then sort by full driver name (not by flag)
    driver_entries = [(name, f"{flag_shortcode(d.get('country') or '')} {name}") for name, d in dmap.items()]
    driver_entries.sort(key=lambda x: x[0].lower())  # Sort by full driver name
    names = [entry[1] for entry in driver_entries]  # Extract the formatted strings
    view = DriversView(season)
    view.names = names
    
    # Set title
    if season == "__CAREER__":
        view.title = "All Time"
    else:
        view.title = f"{season}"
    
    desc = view.render_description()
    emb = discord.Embed(title=f"{view.title}", description=desc, color=discord.Color.teal())
    await interaction.response.send_message(embed=emb, view=view, ephemeral=True)

# ========= Season management =========
@tree.command(name="admin_season_create", description="Create a new season")
@GDEC
@app_commands.describe(name="Season name (folder)")
async def season_create_cmd(interaction: discord.Interaction, name: str):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    ensure_season_dir(name)
    await interaction.response.send_message(f"✅ Created season **{name}**.", ephemeral=True)

class SeasonDeleteDropdown(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"📅 {s}", value=s) for s in list_seasons()]
        super().__init__(placeholder="Pick a season to delete…", options=options, min_values=1, max_values=1)
    async def callback(self, i: discord.Interaction):
        if not is_admin(i.user):
            await i.response.send_message("🚫 Admins only.", ephemeral=True); return
        season = self.values[0]
        # refuse to delete if it's current season
        if current_season_name() == season:
            await i.response.send_message("⚠️ Unset current season first with `/season_set_current`.", ephemeral=True); return
        
        # Show confirmation dialog instead of directly deleting
        await i.response.send_message(
            f"⚠️ **Confirm Season Deletion**\n\nAre you sure you want to delete season **📅 {season}**?\n\nThis will permanently delete:\n• All driver data for this season\n• All race statistics\n• Season configuration\n\n**This action cannot be undone.**",
            view=SeasonDeleteConfirmView(season),
            ephemeral=True
        )

class SeasonDeleteConfirmView(discord.ui.View):
    def __init__(self, season_to_delete: str):
        super().__init__(timeout=60)
        self.season_to_delete = season_to_delete

    @discord.ui.button(label="✅ Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("🚫 Admins only.", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
        
        import shutil
        try:
            shutil.rmtree(ensure_season_dir(self.season_to_delete))
            await interaction.followup.send(f"🗑 **Season Deleted Successfully**\n\nSeason **📅 {self.season_to_delete}** has been permanently removed.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ **Deletion Failed**\n\nCould not delete season **📅 {self.season_to_delete}**: `{e}`", ephemeral=True)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(f"✅ **Deletion Cancelled**\n\nSeason **📅 {self.season_to_delete}** was not deleted.", ephemeral=True)

class SeasonSetDropdown(discord.ui.Select):
    def __init__(self):
        cur = current_season_name()
        options = [discord.SelectOption(label="🏖️ No Season", value="__NONE__", default=(cur in (None, "",)))]
        for s in list_seasons():
            options.append(discord.SelectOption(label=f"📅 {s}", value=s, default=(s == cur)))
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
            await i.response.send_message(f"✅ Current season set to **📅 {choice}**.", ephemeral=True)

@tree.command(name="admin_season_delete", description="Delete a season via dropdown")
@GDEC
async def season_delete_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    if not list_seasons():
        await interaction.response.send_message("⚠️ No seasons to delete.", ephemeral=True); return
    view = discord.ui.View(timeout=180)
    view.add_item(SeasonDeleteDropdown())
    await interaction.response.send_message("Pick a season to delete:", view=view, ephemeral=True)

@tree.command(name="admin_season_set_current", description="Set the current season via dropdown")
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
            await interaction.response.send_message(f"✅ Renamed **📅 {old_name}** → **📅 {new_name}**.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Rename failed: `{e}`", ephemeral=True)

class SeasonRenameDropdown(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=f"📅 {s}", value=s) for s in list_seasons()]
        super().__init__(placeholder="Pick a season to rename…", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.send_modal(SeasonRenameModal(target))

@tree.command(name="admin_season_rename", description="Rename a season via dropdown + modal")
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
@tree.command(name="admin_career_wipe_driver", description="Wipe a single driver's stats from ALL seasons")
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
@tree.command(name="admin_backup_now", description="Create a backup and store on disk")
@GDEC
async def backup_now(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    path = save_backup_to_disk()
    await interaction.followup.send(f"✅ Backup saved: `{os.path.basename(path)}` (max {MAX_BACKUPS} backups).", ephemeral=True)

@tree.command(name="admin_backup_info", description="Show backup status (last run & retention)")
@GDEC
async def backup_info(interaction: discord.Interaction):
    try:
        with open(BACKUP_STATE, "r", encoding="utf-8") as f:
            last = datetime.datetime.fromisoformat(f.read().strip())
        diff = tz_now() - last
        await interaction.response.send_message(f"🗓️ Last backup: {last.strftime('%Y-%m-%d %H:%M')}\n�� Backup interval: {BACKUP_INTERVAL_HOURS} hours", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to read backup state: `{e}`", ephemeral=True)

# ========= Data sync =========
@tree.command(name="admin_sync_all_data", description="Sync all leaderboard and driver stats data")
@GDEC
async def sync_all_data_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Get all available seasons
        seasons = list_seasons()
        total_seasons = len(seasons)
        
        # Get all uploaded JSON files
        uploaded_files = list_uploaded_jsons()
        total_files = len(uploaded_files)
        
        # Get career data stats
        career_data = _aggregate_career()
        total_drivers = len(career_data) if career_data else 0
        
        # Initialize response variable
        response = ""
        
        # Check for unprocessed JSON files and auto-process them
        processed_files = []
        if total_files > 0 and total_seasons > 0:
            current_season = current_season_name()
            if current_season:
                response += f"🔄 **Auto-Processing JSON Files...**\n\n"
                
                for filename in uploaded_files:
                    try:
                        # Check if this file has already been processed by looking at season data
                        season_data = load_season_drivers(current_season)
                        file_processed = False
                        
                        # Simple check: if season has drivers, assume files were processed
                        if season_data and len(season_data) > 0:
                            file_processed = True
                        
                        if not file_processed:
                            # Try to process the file
                            file_path = os.path.join(UPLOADS_STORE_DIR, filename)
                            if os.path.exists(file_path):
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    file_content = f.read()
                                
                                # Process the file into the current season
                                processed = process_json_into_season(file_content, current_season)
                                if processed:
                                    processed_files.append(filename)
                                    response += f"✅ **Processed:** `{filename}` → {current_season}\n"
                                else:
                                    response += f"⚠️ **Failed to process:** `{filename}`\n"
                            else:
                                response += f"⚠️ **File not found:** `{filename}`\n"
                        else:
                            response += f"ℹ️ **Already processed:** `{filename}`\n"
                    except Exception as e:
                        response += f"❌ **Error processing:** `{filename}` - {e}\n"
                
                if processed_files:
                    response += f"\n🔄 **Refreshing data after processing...**\n"
                    # Refresh career data after processing
                    career_data = _aggregate_career()
                    total_drivers = len(career_data) if career_data else 0
                
                response += "\n"
        
        # Create detailed response
        response += f"🔄 **Data Sync Status Report**\n\n"
        response += f"📊 **Current Data Overview:**\n"
        response += f"• **Seasons:** {total_seasons}\n"
        response += f"• **Uploaded Files:** {total_files}\n"
        response += f"• **Total Drivers:** {total_drivers}\n\n"
        
        if total_files > 0:
            response += f"📁 **Available JSON Files:**\n"
            for i, filename in enumerate(uploaded_files[:10], 1):  # Show first 10
                response += f"  {i}. `{filename}`\n"
            if total_files > 10:
                response += f"  ... and {total_files - 10} more files\n"
            response += "\n"
        
        if total_seasons > 0:
            response += f"📅 **Available Seasons:**\n"
            for i, season in enumerate(seasons[:5], 1):  # Show first 5
                response += f"  {i}. `{season}`\n"
            if total_seasons > 5:
                response += f"  ... and {total_seasons - 5} more seasons\n"
            response += "\n"
        
        # Check data integrity
        response += f"🔍 **Data Integrity Check:**\n"
        
        # Check if career data exists and has content
        if career_data and total_drivers > 0:
            response += f"✅ Career data: **{total_drivers}** drivers found\n"
        else:
            response += f"⚠️ Career data: No drivers found\n"
        
        # Check if current season exists
        current_season = current_season_name()
        if current_season:
            season_data = load_season_drivers(current_season)
            season_drivers = len(season_data) if season_data else 0
            response += f"✅ Current season: **{current_season}** ({season_drivers} drivers)\n"
        else:
            response += f"⚠️ Current season: Not set\n"
        
        # Check if uploads directory has files
        if total_files > 0:
            response += f"✅ Uploads: **{total_files}** JSON files available\n"
        else:
            response += f"⚠️ Uploads: No JSON files found\n"
        
        response += f"\n💡 **Recommendations:**\n"
        if total_files == 0:
            response += "• Upload JSON files using `/admin_upload`\n"
        if total_seasons == 0:
            response += "• Create seasons using `/admin_season_create`\n"
        if not current_season:
            response += "• Set current season using `/admin_season_set_current`\n"
        if total_drivers == 0:
            response += "• Process uploaded files to generate driver stats\n"
        
        response += f"\n🔄 **Sync Complete** - All data has been refreshed and verified!"
        
        await interaction.followup.send(response, ephemeral=True)
        
        # Log to console
        print(console_safe(f"🔄 Admin data sync completed by {interaction.user.name} ({interaction.user.id})"))
        print(console_safe(f"📊 Sync results: {total_seasons} seasons, {total_files} files, {total_drivers} drivers"))
        
    except Exception as e:
        error_msg = f"❌ **Data Sync Failed**\n\nError: `{e}`\n\nPlease check the bot logs for more details."
        await interaction.followup.send(error_msg, ephemeral=True)
        print(console_safe(f"❌ Data sync failed: {e}"))

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
                    
                                    # Channel notification removed - no more automatic messages to channels
                else:
                    nv = UploadsMultiManageView()
                    await interaction.response.send_message(content=f"ℹ️ File `{filename}` was already deleted.", ephemeral=True)
                    
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
                
                # Channel notification removed - no more automatic messages to channels
                
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to process multiple deletions: `{e}`", ephemeral=True)

class UploadsManageView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.selected_files: list[str] = []
        self.season = current_season_name()
        files = list_uploaded_jsons()
        has_files = len(files) > 0
        
        self.add_item(UploadsDropdown())
        
        # Add buttons but disable them if no files
        delete_button = UploadsDeleteButton()
        delete_button.disabled = not has_files
        self.add_item(delete_button)
        
        delete_all_button = DeleteAllButton()
        delete_all_button.disabled = not has_files
        self.add_item(delete_all_button)

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
                        
                        seasons_affected = await _remove_ingested_data(file_content)
                        os.remove(path)
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
            
            # Send notification to the designated channel
            try:
                channel = interaction.client.get_channel(RESTRICTED_CHANNEL_ID)
                if channel:
                    if len(filenames) == 1:
                        await channel.send(
                            f"🗑 **JSON File Deleted!**\n\n"
                            f"📁 **File:** `{filenames[0]}`\n"
                            f"👤 **Deleted by:** {interaction.user.mention}\n"
                            f"📊 **Seasons affected:** {', '.join(sorted(total_seasons_affected)) if total_seasons_affected else 'None'}\n\n"
                            f"⚠️ Data has been removed from the system."
                        )
                    else:
                        await channel.send(
                            f"🗑 **Multiple JSON Files Deleted!**\n\n"
                            f"📁 **Files deleted:** {total_deleted}\n"
                            f"👤 **Deleted by:** {interaction.user.mention}\n"
                            f"📊 **Seasons affected:** {', '.join(sorted(total_seasons_affected)) if total_seasons_affected else 'None'}\n\n"
                            f"⚠️ Data has been removed from the system."
                        )
            except Exception as e:
                print(f"Failed to send channel notification: {e}")
            
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to process deletions: `{e}`", ephemeral=True)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(content="❌ Deletion cancelled.", ephemeral=True)



class UploadSeasonSelectView(discord.ui.View):
    def __init__(self, filename: str, file_content: bytes):
        super().__init__(timeout=300)
        self.filename = filename
        self.file_content = file_content
        self.add_item(UploadSeasonDropdown(filename, file_content))



class UploadSeasonDropdown(discord.ui.Select):
    def __init__(self, filename: str, file_content: bytes):
        # Get available seasons
        seasons = list_seasons()
        
        # Create options for each season
        options = []
        for season in seasons:
            options.append(discord.SelectOption(
                label=f"📅 {season}",
                value=season,
                description=f"Upload to {season}"
            ))
        
        super().__init__(
            placeholder="Select season to upload to...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.filename = filename
        self.file_content = file_content

    async def callback(self, interaction: discord.Interaction):
        try:
            selected_season = self.values[0]
            print(f"DEBUG: Selected season: {selected_season}")
            await interaction.response.defer(ephemeral=True)
            
            # Parse and ingest the JSON data
            print(f"DEBUG: Parsing JSON content of {len(self.file_content)} bytes")
            data = json.loads(self.file_content.decode("utf-8"))
            print(f"DEBUG: JSON parsed successfully")
            
            ensure_season_dir(selected_season)
            print(f"DEBUG: Season directory ensured")
            
            updated, processed = ingest_iracing_event(data, selected_season)
            print(f"DEBUG: Ingestion complete - updated: {updated}, processed: {processed}")
            
            # Store a copy to disk for management
            fname = _sanitize_filename(self.filename)
            stamp = tz_now().strftime("%Y%m%d_%H%M%S")
            out_name = f"{stamp}_{fname}"
            with open(os.path.join(UPLOADS_STORE_DIR, out_name), "wb") as f:
                f.write(self.file_content)
            print(f"DEBUG: File saved as: {out_name}")
            
            season_display = selected_season
            
            # Send success message
            await interaction.followup.send(
                f"✅ **Upload Complete!**\n\n"
                f"📁 File: `{self.filename}`\n"
                f"📅 Season: **{season_display}**\n"
                f"📊 Processed: **{processed}** rows\n"
                f"👥 Updated: **{updated}** drivers\n\n"
                f"🔄 Stats automatically synced!",
                ephemeral=True
            )
            
            # Channel notification removed - no more automatic messages to channels
            
        except Exception as e:
            print(f"DEBUG: Error occurred: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.followup.send(f"❌ Failed to ingest into season: `{e}`", ephemeral=True)
            except:
                try:
                    await interaction.response.send_message(f"❌ Failed to process upload: `{e}`", ephemeral=True)
                except:
                    print(f"DEBUG: Could not send error message to user: {e}")


class UploadsMultiManageView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.selected_files: list[str] = []
        files = list_uploaded_jsons()
        has_files = len(files) > 0
        
        self.add_item(UploadsMultiDropdown())
        
        # Add button but disable if no files
        delete_button = UploadsMultiDeleteButton()
        delete_button.disabled = not has_files
        self.add_item(delete_button)

@tree.command(name="admin_uploads_manage", description="Manage uploaded JSON files: select and delete.")
@GDEC
async def uploads_manage(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("🚫 Admins only.", ephemeral=True); return
    await interaction.response.send_message("Manage uploaded JSON files:", view=UploadsManageView(), ephemeral=True)



# ========= Account linking commands =========
@tree.command(name="link_account", description="Link your Discord account to your iRacing name")
@GDEC
@app_commands.describe(iracing_name="Your exact iRacing driver name")
async def link_account_cmd(interaction: discord.Interaction, iracing_name: str):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
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
        # Prevent changing iRacing name - require unlink first
        await interaction.response.send_message(
            f"⚠️ **Account Already Linked**\n\nYour Discord account is already linked to: **{existing_link}**\n\n"
            f"If you want to link to a different iRacing name, you must first use `/unlink_account` to remove the current link.",
            ephemeral=True
        )
        return
    else:
        # Create new link
        if link_discord_to_iracing(interaction.user.id, iracing_name):
            await interaction.response.send_message(
                f"✅ **Account Linked!**\n\nYour Discord account is now linked to: **{iracing_name}**\n\n"
                "You can now use the 'Find Me' button in leaderboards to see your position!\n\n"
                "💡 **Note:** If you have any leaderboards currently open, refresh them to see your linked driver status.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ Failed to create link. Please try again.", ephemeral=True)

@tree.command(name="unlink_account", description="Remove the link between your Discord account and iRacing name")
@GDEC
async def unlink_account_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    if unlink_discord(interaction.user.id):
        await interaction.response.send_message(
            "✅ **Account Unlinked!**\n\nYour Discord account is no longer linked to any iRacing name.\n\n"
            "You can now link to a new name with `/link_account <iRacing_name>`.\n\n"
            "💡 **Note:** If you have any leaderboards currently open, refresh them to see the updated linked driver status.",
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
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
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

@tree.command(name="admin_unlink", description="Remove a Discord-to-iRacing link")
@GDEC
@app_commands.describe(discord_user="Discord user to unlink")
async def admin_unlink_cmd(interaction: discord.Interaction, discord_user: discord.Member):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
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
            f"✅ **Admin Unlink Complete**\n\nRemoved link between {discord_user.mention} and **{iracing_name}**.\n\n"
            f"💡 **Note:** {discord_user.mention} should refresh any open leaderboards to see the updated status.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message("❌ Failed to unlink. Please try again.", ephemeral=True)



# ========= Help and commands info =========
def generate_dynamic_help() -> str:
    """Generate help text dynamically from registered commands"""
    
    help_text = "🤖 **WiRL Stats Bot - Help**\n\n**Available commands for the WiRL Stats Bot**\n\n"
    
    # Stats & Leaderboards
    help_text += "📊 **Stats & Leaderboards**\n"
    help_text += "• `/leaderboard` - Leaderboard with individuals season and career stats\n"
    help_text += "• `/driver_stats <name>` - Driver statistics\n"
    help_text += "• `/driver_list` - List of all drivers\n"
    help_text += "• `/my_stats` - Your personal stats\n\n"
    
    # Account Management
    help_text += "🔗 **Account Management**\n"
    help_text += "• `/link_account <name>` - Link Discord to your iRacing Name\n"
    help_text += "• `/unlink_account` - Remove link\n"
    help_text += "• `/my_link` - Check your link\n\n"
    
    # Admin Commands
    help_text += "⚙️ **Admin Commands**\n"
    help_text += "• `/admin_setup` - Configure bot\n"
    help_text += "• `/admin_upload` - Upload race results\n"
    help_text += "• `/admin_refresh_commands` - Refresh commands\n"
    help_text += "• `/admin_season_create` - Create new season\n"
    help_text += "• `/admin_season_delete` - Delete season\n"
    help_text += "• `/admin_season_set_current` - Set current season\n"
    help_text += "• `/admin_season_rename` - Rename season\n"
    help_text += "• `/admin_career_wipe_driver` - Wipe driver stats\n"
    help_text += "• `/admin_backup_now` - Create backup\n"
    help_text += "• `/admin_backup_info` - Backup status\n"
    help_text += "• `/admin_sync_all_data` - Sync all data\n"
    help_text += "• `/admin_uploads_manage` - Manage uploads\n"
    help_text += "• `/admin_unlink` - Remove Discord - iRacing link\n\n"
    
    # Utility
    help_text += "🛠️ **Utility**\n"
    help_text += "• `/ping` - Check bot status\n"
    help_text += "• `/status` - Bot health info\n"
    help_text += "• `/help` - This help message\n"
    
    return help_text

@tree.command(name="help", description="Show all available commands and their descriptions.")
@GDEC
async def help_cmd(interaction: discord.Interaction):
    if not check_channel_restriction(interaction):
        await interaction.response.send_message("🚫 This bot can only be used in the designated channel.", ephemeral=True)
        return
    try:
        help_text = generate_dynamic_help()
        await interaction.response.send_message(help_text, ephemeral=True)
    except discord.errors.NotFound:
        # Interaction expired, try to send a new message
        try:
            await interaction.followup.send("⚠️ Interaction expired. Here are the available commands:", ephemeral=True)
            help_text = generate_dynamic_help()
            await interaction.followup.send(help_text, ephemeral=True)
        except Exception as e:
            print(f"Error in help command fallback: {e}")
    except Exception as e:
        print(f"Error in help command: {e}")
        try:
            await interaction.response.send_message("❌ An error occurred while generating help. Please try again.", ephemeral=True)
        except:
            pass





# ========= Bot Events =========
@bot.event
async def on_ready():
    print(console_safe(f"✅ {bot.user} is ready and online!"))
    print(console_safe(f"🏠 Connected to {len(bot.guilds)} guild(s)"))
    
    # Debug command registration
    registered_commands = tree.get_commands()
    print(console_safe(f"🔍 Debug: Found {len(registered_commands)} registered commands in tree"))
    if registered_commands:
        print(console_safe(f"🔍 Debug: First few commands: {[cmd.name for cmd in registered_commands[:5]]}"))
    
    # Sync slash commands with Discord
    try:
        if GUILD_OBJECTS:
            # Clear any global commands first to prevent duplicates
            try:
                await tree.sync()  # Clear global commands
                print(console_safe("🧹 Cleared global commands to prevent duplicates"))
            except:
                pass
            
            # Sync commands to all specified guilds
            total_synced = 0
            for guild_obj in GUILD_OBJECTS:
                try:
                    synced_commands = await tree.sync(guild=guild_obj)
                    total_synced += len(synced_commands)
                    print(console_safe(f"✅ Synced {len(synced_commands)} slash commands to guild {guild_obj.id}"))
                except Exception as guild_error:
                    print(console_safe(f"❌ Failed to sync to guild {guild_obj.id}: {guild_error}"))
            
            print(console_safe(f"🎯 Total commands synced across all guilds: {total_synced}"))
        else:
            # Only sync globally if no guilds specified
            synced_commands = await tree.sync()
            print(console_safe(f"✅ Synced {len(synced_commands)} slash commands globally"))
    except Exception as e:
        print(console_safe(f"❌ Failed to sync commands: {e}"))
    
    print(console_safe("🚀 Bot is ready to receive commands!"))

# ========= Run =========
if not TOKEN or TOKEN == "YOUR_DISCORD_BOT_TOKEN":
    print(console_safe("❌ Put your real bot token into config.json under key 'token'."))
else:
    print(console_safe("🚀 Starting WiRL Stats Bot with 24/7 capabilities..."))
    bot.run(TOKEN)