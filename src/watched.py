import os
import json
import re
import copy
from typing import Dict, List, Any
from pydantic import BaseModel, Field
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

from src.functions import search_mapping, CONFIG_DIR

# Load env again just in case, using CONFIG_DIR
load_dotenv(os.path.join(CONFIG_DIR, ".env"), override=True)

watched_env = os.getenv("WATCHED_STATE_FILE")
if watched_env and os.path.isabs(watched_env):
    WATCHED_STATE_FILE = watched_env
elif watched_env:
    WATCHED_STATE_FILE = os.path.join(CONFIG_DIR, watched_env)
else:
    WATCHED_STATE_FILE = os.path.join(CONFIG_DIR, "watched_state.json")
MAX_THREADS = int(os.getenv("MAX_THREADS", "10"))

# ---------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------
class MediaIdentifiers(BaseModel):
    title: str | None = None
    locations: tuple[str, ...] = tuple()
    imdb_id: str | None = None
    tvdb_id: str | None = None
    tmdb_id: str | None = None
    plex_guid: str | None = None
    # Playlist items also need synced_to_servers (forward reference)
    synced_to_servers: Dict[str, 'ServerSyncInfo'] = Field(default_factory=dict)

class WatchedStatus(BaseModel):
    completed: bool
    time: int
    last_viewed_at: int | None = None

class ServerSyncInfo(BaseModel):
    """Server synchronization information"""
    synced_at: int  # synced timestamp
    synced_status: WatchedStatus  # Status at sync time

class MediaItem(BaseModel):
    identifiers: MediaIdentifiers
    status: WatchedStatus
    # Server sync status: {server_id: ServerSyncInfo}
    synced_to_servers: Dict[str, ServerSyncInfo] = Field(default_factory=dict)

class Series(BaseModel):
    identifiers: MediaIdentifiers
    episodes: List[MediaItem] = Field(default_factory=list)
    
# ... (omitted) ...


class LibraryData(BaseModel):
    title: str
    movies: List[MediaItem] = Field(default_factory=list)
    series: List[Series] = Field(default_factory=list)

class UserData(BaseModel):
    libraries: Dict[str, LibraryData] = Field(default_factory=dict)

class WatchedState(BaseModel):
    users: Dict[str, UserData] = Field(default_factory=dict)
    # server_sync_state removed - now stored in each item



# ---------------------------------------------------------
# IDENTIFIER COMPARISON LOGIC
# ---------------------------------------------------------
def check_guid_match(item1: MediaIdentifiers | MediaItem, item2: MediaIdentifiers | MediaItem) -> bool:
    ids1 = item1.identifiers if isinstance(item1, MediaItem) else item1
    ids2 = item2.identifiers if isinstance(item2, MediaItem) else item2
    
    if not ids1 or not ids2:
        return False
        
    if (ids1.imdb_id and ids2.imdb_id and ids1.imdb_id == ids2.imdb_id) or \
       (ids1.tvdb_id and ids2.tvdb_id and ids1.tvdb_id == ids2.tvdb_id) or \
       (ids1.tmdb_id and ids2.tmdb_id and ids1.tmdb_id == ids2.tmdb_id):
        return True

    # PLEX GUID Match (Exact + Flexible)
    if ids1.plex_guid and ids2.plex_guid:
        # 1. Exact Match
        if ids1.plex_guid == ids2.plex_guid:
            return True
        
        # 2. Flexible Match (Compare values only, ignoring scheme/prefix)
        # Handle 'scheme://id' vs 'id' or 'scheme1://id' vs 'scheme2://id'
        v1 = ids1.plex_guid.split("://")[-1]
        v2 = ids2.plex_guid.split("://")[-1]
        
        if v1 == v2:
            return True

    return False

def check_same_identifiers(
    item1: MediaIdentifiers | MediaItem, item2: MediaIdentifiers | MediaItem
) -> bool:
    # 1. Check GUIDs (Strongest Check)
    if check_guid_match(item1, item2):
        return True

    attr1 = item1.identifiers if isinstance(item1, MediaItem) else item1
    attr2 = item2.identifiers if isinstance(item2, MediaItem) else item2
    
    # 2. Location Check (Filename match)
    if attr1.locations and attr2.locations:
         # Normalize to filename only
         fn_set1 = set(l.replace("\\", "/").split("/")[-1] for l in attr1.locations)
         fn_set2 = set(l.replace("\\", "/").split("/")[-1] for l in attr2.locations)
         
         if not fn_set1.isdisjoint(fn_set2):
             return True
    
    return False

# ---------------------------------------------------------
# STATE MANAGEMENT
# ---------------------------------------------------------
def load_watched_state() -> WatchedState:
    if not os.path.exists(WATCHED_STATE_FILE):
        return WatchedState()

    # Check for empty file
    if os.path.getsize(WATCHED_STATE_FILE) == 0:
        logger.warning(f"[System] Watched state file {WATCHED_STATE_FILE} is empty. Returning empty state.")
        return WatchedState()

    try:
        with open(WATCHED_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return WatchedState(**data)
    except json.JSONDecodeError as e:
        logger.error(f"[System] Failed to decode watched state: {e}")
        # Backup corrupted file
        backup_file = WATCHED_STATE_FILE + ".corrupted"
        try:
            if os.path.exists(WATCHED_STATE_FILE):
                import shutil
                shutil.copy2(WATCHED_STATE_FILE, backup_file)
                logger.info(f"[System] Corrupted watched state file backed up to {backup_file}")
        except Exception as backup_error:
             logger.error(f"[System] Failed to backup corrupted watched state file: {backup_error}")
        return WatchedState()
    except Exception as e:
        logger.error(f"[System] Failed to load watched state: {e}")
        return WatchedState()

def save_watched_state(state: WatchedState) -> None:
    try:
        # Direct write to avoid Docker bind mount issues (Errno 16 Device or resource busy)
        with open(WATCHED_STATE_FILE, "w", encoding="utf-8") as f:
             f.write(state.model_dump_json(indent=4))
    except Exception as e:
        logger.error(f"Failed to save watched state: {e}")

# ---------------------------------------------------------
# SYNC LOGIC HELPERS
# ---------------------------------------------------------
def merge_media_item_to_list(target_list: List[MediaItem], source_item: MediaItem) -> bool:
    for i, target_item in enumerate(target_list):
        if check_same_identifiers(target_item, source_item):
            # Resolve conflict
            ts_target = target_item.status.last_viewed_at or 0
            ts_source = source_item.status.last_viewed_at or 0
            
            # Detect recent changes: Check synced_to_servers
            # Check if source_item status recently changed on any server
            source_has_recent_change = False
            for server_id, sync_info in source_item.synced_to_servers.items():
                # Compare previous synced status with current status
                if sync_info.synced_status.completed != source_item.status.completed:
                    # Status change detected! (especially true->false unmark)
                    source_has_recent_change = True
                    break
            
            target_has_recent_change = False
            for server_id, sync_info in target_item.synced_to_servers.items():
                if sync_info.synced_status.completed != target_item.status.completed:
                    target_has_recent_change = True
                    break
            
            # Items with recent changes take precedence
            if source_has_recent_change and not target_has_recent_change:
                target_list[i] = source_item
                return True
            if target_has_recent_change and not source_has_recent_change:
                return False
            
            # Newer timestamp wins
            if ts_source > ts_target:
                target_list[i] = source_item
                return True
            
            # If timestamp same/missing, prefer completed
            if not target_item.status.completed and source_item.status.completed:
                target_list[i] = source_item
                return True
            
            # If both incomplete, prefer higher progress
            if not target_item.status.completed and not source_item.status.completed:
                if source_item.status.time > target_item.status.time:
                    target_list[i] = source_item
                    return True
            
            return False
            
    target_list.append(source_item)
    return True

def create_diff_library(global_lib: LibraryData, server_id: str) -> LibraryData | None:
    """
    Returns items from global state that need syncing to a specific server.
    
    Args:
        global_lib: Current global state (merged from all servers)
        server_id: Target server ID
    
    Returns:
        LibraryData containing only items needing sync, or None
    """
    import time
    needs_update_something = False
    diff_lib = LibraryData(title=global_lib.title)
    
    # Movies
    for g_mov in global_lib.movies:
        should_add = False
        
        if server_id not in g_mov.synced_to_servers:
            # New item never synced to this server
            should_add = True
        else:
            synced_info = g_mov.synced_to_servers[server_id]
            synced_status = synced_info.synced_status
            current_status = g_mov.status
            
            # Compare status
            if current_status.completed != synced_status.completed:
                should_add = True
            elif not current_status.completed and abs(current_status.time - synced_status.time) >= 60000:
                # Incomplete status and time difference >= 60 seconds
                should_add = True
        
        if should_add:
            diff_lib.movies.append(g_mov)
            needs_update_something = True

    # Series (Episodes)
    for g_ser in global_lib.series:
        diff_episodes = []
        
        for g_ep in g_ser.episodes:
            should_add = False
            
            if server_id not in g_ep.synced_to_servers:
                # New episode never synced to this server
                should_add = True
                logger.debug(f"Episode '{g_ep.identifiers.title}' not synced to this server yet")
            else:
                synced_info = g_ep.synced_to_servers[server_id]
                synced_status = synced_info.synced_status
                current_status = g_ep.status
                
                # Compare status
                if current_status.completed != synced_status.completed:
                    should_add = True
                    if current_status.completed:
                        logger.debug(f"Episode '{g_ep.identifiers.title}' needs to be marked watched")
                    else:
                        logger.debug(f"Episode '{g_ep.identifiers.title}' needs to be unmarked")
                elif not current_status.completed and abs(current_status.time - synced_status.time) >= 60000:
                    should_add = True
                    logger.debug(f"Episode '{g_ep.identifiers.title}' needs progress update (current: {current_status.time}, synced: {synced_status.time})")
            
            if should_add:
                diff_episodes.append(g_ep)
        
        if diff_episodes:
            new_series = Series(identifiers=g_ser.identifiers, episodes=diff_episodes)
            diff_lib.series.append(new_series)
            needs_update_something = True
            
    return diff_lib if needs_update_something else None

# ---------------------------------------------------------
# MAIN SYNC FUNCTION
# ---------------------------------------------------------
def synchronize_watched(
    servers: list,
    blacklist_users: list, whitelist_users: list,
    blacklist_library: list, whitelist_library: list,
    blacklist_library_type: list, whitelist_library_type: list,
    user_mapping: dict | None, library_mapping: dict | None,
    dryrun: bool
) -> WatchedState:
    state = load_watched_state()
    # logger.info("[System] Skipping Watched Collection and Sync (Dev Mode)")
    # return state
    
    server_watched_data = {} # {server_id: {user: UserData}} (Raw Data)
    
    # -------------------------------------------------------------------------
    # 1. FETCH DATA FROM ALL SERVERS
    # -------------------------------------------------------------------------
    def fetch_server_data(server):
        logger.info(f"[{server.info()}] Fetching watched status")
        
        all_users = server.get_users()
        filtered_users = []
        for u in all_users:
            u_name_chk = u.title or u.username
            if whitelist_users and u_name_chk not in whitelist_users: continue
            if blacklist_users and u_name_chk in blacklist_users: continue
            filtered_users.append(u)
        
        all_libs = server.get_libraries()
        filtered_libs = {}
        for l_name, l_type in all_libs.items():
            if whitelist_library and l_name not in whitelist_library: continue
            if blacklist_library and l_name in blacklist_library: continue
    
    # -------------------------------------------------------------------------
    # 1. FETCH DATA FROM ALL SERVERS (Parallel)
    # -------------------------------------------------------------------------
    server_watched_data: dict[str, dict[str, UserData]] = {}
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        for server in servers:
            logger.info(f"[{server.info()}] Fetching watched status")
            
            all_users = server.get_users()
            filtered_users = []
            for u in all_users:
                u_name_chk = u.title or u.username
                if whitelist_users and u_name_chk not in whitelist_users: continue
                if blacklist_users and u_name_chk in blacklist_users: continue
                filtered_users.append(u)
            
            all_libs = server.get_libraries()
            filtered_libs = {}
            for l_name, l_type in all_libs.items():
                if whitelist_library and l_name not in whitelist_library: continue
                if blacklist_library and l_name in blacklist_library: continue
                if whitelist_library_type and l_type not in whitelist_library_type: continue
                if blacklist_library_type and l_type in blacklist_library_type: continue
                filtered_libs[l_name] = l_type
                
            futures[executor.submit(server.get_watched, filtered_users, filtered_libs, state)] = server
        
        for future in as_completed(futures):
            server = futures[future]
            try:
                data = future.result()
                server_watched_data[server.plex.machineIdentifier] = data
            except Exception as e:
                logger.error(f"[{server.info()}] Failed to fetch watched status: {e}")

    # -------------------------------------------------------------------------
    # 2. DETECT LOCAL DELETIONS (PRUNE STATE)
    # Check if items in State are missing from connected Servers -> Mark as Deleted
    # -------------------------------------------------------------------------
    tombstones = [] # Track items removed in this session to prevent re-adding them in Step 3
    users_to_check = list(state.users.keys())
    
    logger.info(f"[System] Checking {len(users_to_check)} users for local deletions...")
    
    for i, g_user in enumerate(users_to_check):
        if (i+1) % 5 == 0: logger.info(f"[System] Pruning users: {i+1}/{len(users_to_check)}")
        g_u_data = state.users[g_user]
        
        libs_to_check = list(g_u_data.libraries.keys())
        for g_lib_name in libs_to_check:
            g_lib_data = g_u_data.libraries[g_lib_name]
            
            # Movies Pruning
            movies_to_remove = []
            for g_mov in g_lib_data.movies:
                # Check connected servers
                for server in servers:
                    s_id = server.plex.machineIdentifier
                    if s_id not in server_watched_data: continue
                    
                    s_data = server_watched_data[s_id]
                    
                    # Find matching user in this server
                    target_s_user = None
                    for s_u_key in s_data.keys():
                        mapped_g_user = search_mapping(user_mapping, s_u_key) if user_mapping else s_u_key
                        if mapped_g_user.lower() == g_user.lower():
                            target_s_user = s_u_key
                            break
                    
                    if not target_s_user: continue # User not on this server, skip check
                    
                    s_u_data = s_data[target_s_user]
                    
                    # Find matching library
                    target_s_lib = None
                    for s_l_key in s_u_data.libraries.keys():
                         mapped_g_lib = search_mapping(library_mapping, s_l_key) if library_mapping else s_l_key
                         if mapped_g_lib.lower() == g_lib_name.lower():
                             target_s_lib = s_l_key
                             break
                    
                    if not target_s_lib: continue # Library not on this server, skip check
                    
                    # Server has User & Library. Check if Movie exists.
                    s_lib_data = s_u_data.libraries[target_s_lib]
                    
                    found = False
                    for s_mov in s_lib_data.movies:
                        if check_same_identifiers(g_mov, s_mov):
                            found = True
                            break
                    
                    if not found:
                        # Missing in a connected server! Treat as Deleted.
                        movies_to_remove.append(g_mov)
                        break # One deletion is enough
            
            for m in movies_to_remove:
                tombstones.append(m) # Add to tombstones
                if m in g_lib_data.movies: g_lib_data.movies.remove(m)

            # Series Pruning
            series_to_remove = []
            for g_ser in g_lib_data.series:
                for server in servers:
                    s_id = server.plex.machineIdentifier
                    if s_id not in server_watched_data: continue
                    s_data = server_watched_data[s_id]
                    
                    # Find User
                    target_s_user = None
                    for s_u_key in s_data.keys():
                        mapped_g_user = search_mapping(user_mapping, s_u_key) if user_mapping else s_u_key
                        if mapped_g_user.lower() == g_user.lower():
                            target_s_user = s_u_key
                            break
                    if not target_s_user: continue
                    s_u_data = s_data[target_s_user]
                    
                    # Find Library
                    target_s_lib = None
                    for s_l_key in s_u_data.libraries.keys():
                         mapped_g_lib = search_mapping(library_mapping, s_l_key) if library_mapping else s_l_key
                         if mapped_g_lib.lower() == g_lib_name.lower():
                             target_s_lib = s_l_key
                             break
                    if not target_s_lib: continue
                    s_lib_data = s_u_data.libraries[target_s_lib]
                    
                    # Check Series Existence
                    s_ser_match = None
                    for s_s in s_lib_data.series:
                        if check_same_identifiers(g_ser.identifiers, s_s.identifiers):
                            s_ser_match = s_s
                            break
                            
                    if not s_ser_match:
                         # Whole series missing -> Delete
                         series_to_remove.append(g_ser)
                         break
                    else:
                         # Series exists, check episodes
                         eps_to_remove = []
                         for g_ep in g_ser.episodes:
                             ep_found = False
                             for s_ep in s_ser_match.episodes:
                                 if check_same_identifiers(g_ep, s_ep):
                                     ep_found = True
                                     break
                             if not ep_found:
                                 eps_to_remove.append(g_ep)
                         
                         for ep in eps_to_remove:
                             tombstones.append(ep)
                             if ep in g_ser.episodes: g_ser.episodes.remove(ep)
                         
                         if not g_ser.episodes:
                             series_to_remove.append(g_ser)
                             break 
            
            for s in series_to_remove:
                tombstones.append(s) # Tombstone the series
                if s in g_lib_data.series: g_lib_data.series.remove(s)

    # -------------------------------------------------------------------------
    # 3. MERGE (ADD NEW WATCHED ITEMS)
    # -------------------------------------------------------------------------
    logger.info(f"[System] Merging data from {len(server_watched_data)} servers...")
    
    for s_id, data in server_watched_data.items():
        for u_name, u_data in data.items():
            g_user = search_mapping(user_mapping, u_name) if user_mapping else u_name
            if g_user not in state.users: state.users[g_user] = UserData()
            g_u_data = state.users[g_user]
            
            for l_name, l_data in u_data.libraries.items():
                g_lib_name = search_mapping(library_mapping, l_name) if library_mapping else l_name
                if g_lib_name not in g_u_data.libraries: g_u_data.libraries[g_lib_name] = LibraryData(title=g_lib_name)
                g_lib_data = g_u_data.libraries[g_lib_name]
                
                for mov in l_data.movies: 
                    # Check tombstone (only MediaItem, not Series)
                    if any(check_same_identifiers(mov, t) for t in tombstones if isinstance(t, MediaItem)):
                        continue
                    merge_media_item_to_list(g_lib_data.movies, mov)
                
                for ser in l_data.series:
                    # Check tombstone (Whole Series)
                    # Note: We didn't explicitly tombstone Series objects in the pruning logic above 
                    # (I only added it for movies in the specific chunk, need to ensure I add it for Series/Episodes too).
                    # Let's fix the Series Pruning block too.
                    pass 
                    
                    # Logic continues below...
                    g_ser = next((s for s in g_lib_data.series if check_same_identifiers(s.identifiers, ser.identifiers)), None)
                    
                    # If series was pruned, 'g_ser' will be None.
                    # We need to check if it matches a tombstone before creating new.
                    is_series_tombstoned = any(check_same_identifiers(ser.identifiers, t.identifiers if isinstance(t, Series) else t) for t in tombstones if isinstance(t, Series))
                    if is_series_tombstoned:
                        continue

                    if not g_ser:
                        g_ser = Series(identifiers=ser.identifiers)
                        g_lib_data.series.append(g_ser)
                    
                    for ep in ser.episodes: 
                        if any(check_same_identifiers(ep, t) for t in tombstones if isinstance(t, MediaItem)):
                             continue
                        merge_media_item_to_list(g_ser.episodes, ep)

    save_watched_state(state)
    logger.info("[System] Global Watched State saved (Pruned & Merged).")
    
    # -------------------------------------------------------------------------
    # 3.5 Initial SYNC status marking (First run optimization)
    # Compare server state with global state and mark already identical items as synced
    # -------------------------------------------------------------------------
    import time
    current_ts = int(time.time())
    
    logger.info("[System] Marking already-synced items to avoid unnecessary updates...")
    for server in servers:
        s_id = server.plex.machineIdentifier
        if s_id not in server_watched_data:
            continue
        
        server_data = server_watched_data[s_id]
        
        for s_user, s_u_data in server_data.items():
            # Find global user
            g_user = search_mapping(user_mapping, s_user) if user_mapping else s_user
            if g_user not in state.users:
                continue
            
            g_u_data = state.users[g_user]
            
            for s_lib_name, s_lib_data in s_u_data.libraries.items():
                # Find global library
                g_lib_name = search_mapping(library_mapping, s_lib_name) if library_mapping else s_lib_name
                if g_lib_name not in g_u_data.libraries:
                    continue
                
                g_lib_data = g_u_data.libraries[g_lib_name]
                
                # Compare movies
                for s_mov in s_lib_data.movies:
                    for g_mov in g_lib_data.movies:
                        if check_same_identifiers(s_mov, g_mov):
                            # Check if status is identical
                            if (s_mov.status.completed == g_mov.status.completed and
                                (s_mov.status.completed or abs(s_mov.status.time - g_mov.status.time) < 60000)):
                                # Already synced - mark it
                                g_mov.synced_to_servers[s_id] = ServerSyncInfo(
                                    synced_at=current_ts,
                                    synced_status=copy.deepcopy(g_mov.status)
                                )
                            break
                
                # Compare episodes
                for s_ser in s_lib_data.series:
                    for g_ser in g_lib_data.series:
                        if check_same_identifiers(s_ser.identifiers, g_ser.identifiers):
                            for s_ep in s_ser.episodes:
                                for g_ep in g_ser.episodes:
                                    if check_same_identifiers(s_ep, g_ep):
                                        # Check if status is identical
                                        if (s_ep.status.completed == g_ep.status.completed and
                                            (s_ep.status.completed or abs(s_ep.status.time - g_ep.status.time) < 60000)):
                                            # Already synced - mark it
                                            g_ep.synced_to_servers[s_id] = ServerSyncInfo(
                                                synced_at=current_ts,
                                                synced_status=copy.deepcopy(g_ep.status)
                                            )
                                        break
                            break
    
    # Save state again after marking
    save_watched_state(state)
    logger.info("[System] Already-synced items marked.")

    # -------------------------------------------------------------------------
    # 4. PROPAGATE CHANGES (SYNC & UNMARK) - PARALLEL
    # -------------------------------------------------------------------------
    def process_server_sync(server):
        s_id = server.plex.machineIdentifier
        current_data = server_watched_data.get(s_id, {})
        
        update_payload = {}
        remove_payload = {}
        
        for g_user, g_u_data in state.users.items():
            # Find target server user
            target_s_user = None
            if g_user in current_data: target_s_user = g_user
            elif user_mapping:
                 for s_u_key in current_data.keys():
                    if search_mapping(user_mapping, s_u_key) == g_user:
                        target_s_user = s_u_key
                        break
            
            if not target_s_user: 
                # User does not exist on this server, skip sync
                continue
            
            s_u_data = current_data[target_s_user]
            
            # Check if user has any libraries on this server
            if not s_u_data.libraries:
                # User exists but has no libraries (e.g. no shares), skip
                continue

            diff_user_data = UserData()
            unmark_user_data = UserData()
            has_diff = False
            has_unmark = False
            
            lib_items = list(g_u_data.libraries.items())
            total_libs = len(lib_items)
            
            for l_idx, (g_lib_name, g_lib_data) in enumerate(lib_items):
                # Progress logging for large libraries could go here if needed, 
                # but usually user count is the bottleneck or network.
                # simpler to just log periodically at user level (done in loop above) or server level
                
                tg_s_lib = None
                if g_lib_name in s_u_data.libraries: tg_s_lib = g_lib_name
                elif library_mapping:
                     for s_l_key in s_u_data.libraries.keys():
                        if search_mapping(library_mapping, s_l_key) == g_lib_name:
                            tg_s_lib = s_l_key
                            break
                
                if not tg_s_lib: continue
                
                # A) Calculate Additions/Updates (Check item sync status)
                diff_lib = create_diff_library(g_lib_data, s_id)
                if diff_lib:
                    diff_user_data.libraries[tg_s_lib] = diff_lib
                    has_diff = True

                # B) Calculate Removals (Unmark) - Server state required
                s_lib_data = s_u_data.libraries[tg_s_lib]
                unmark_lib = LibraryData(title=tg_s_lib)
                needs_unmark = False
                
                # Movies
                for s_mov in s_lib_data.movies:
                    found_in_state = any(check_same_identifiers(g_mov, s_mov) for g_mov in g_lib_data.movies)
                    if not found_in_state:
                        unmark_lib.movies.append(s_mov)
                        needs_unmark = True
                
                # Series
                for s_ser in s_lib_data.series:
                    g_ser_match = next((g for g in g_lib_data.series if check_same_identifiers(g.identifiers, s_ser.identifiers)), None)
                    if not g_ser_match:
                         unmark_lib.series.append(s_ser)
                         needs_unmark = True
                    else:
                         unmark_eps = []
                         for s_ep in s_ser.episodes:
                             found_ep = any(check_same_identifiers(g_ep, s_ep) for g_ep in g_ser_match.episodes)
                             if not found_ep:
                                 unmark_eps.append(s_ep)
                         if unmark_eps:
                             unmark_ser = Series(identifiers=s_ser.identifiers, episodes=unmark_eps)
                             unmark_lib.series.append(unmark_ser)
                             needs_unmark = True
                
                if needs_unmark:
                    unmark_user_data.libraries[tg_s_lib] = unmark_lib
                    has_unmark = True

            if has_diff: update_payload[target_s_user] = diff_user_data
            if has_unmark: remove_payload[target_s_user] = unmark_user_data 
        
        if update_payload or remove_payload:
            logger.info(f"[{server.info()}] Syncing State to Server")
            server.update_watched(update_payload, user_mapping, library_mapping, dryrun, items_to_remove_list=remove_payload)
            
            # Sync successful - Update synced_to_servers for each item
            import time
            current_ts = int(time.time())
            
            for g_user, diff_u_data in update_payload.items():
                # Find global user
                global_user = search_mapping(user_mapping, g_user) if user_mapping else g_user
                if global_user in state.users:
                    for lib_name, diff_lib in diff_u_data.libraries.items():
                        # Find global library
                        global_lib_name = search_mapping(library_mapping, lib_name) if library_mapping else lib_name
                        if global_lib_name in state.users[global_user].libraries:
                            global_lib = state.users[global_user].libraries[global_lib_name]
                            
                            # Update synced movies
                            for diff_mov in diff_lib.movies:
                                for glob_mov in global_lib.movies:
                                    if check_same_identifiers(diff_mov, glob_mov):
                                        glob_mov.synced_to_servers[s_id] = ServerSyncInfo(
                                            synced_at=current_ts,
                                            synced_status=copy.deepcopy(glob_mov.status)
                                        )
                                        break
                            
                            # Update synced episodes
                            for diff_ser in diff_lib.series:
                                for glob_ser in global_lib.series:
                                    if check_same_identifiers(diff_ser.identifiers, glob_ser.identifiers):
                                        for diff_ep in diff_ser.episodes:
                                            for glob_ep in glob_ser.episodes:
                                                if check_same_identifiers(diff_ep, glob_ep):
                                                    glob_ep.synced_to_servers[s_id] = ServerSyncInfo(
                                                        synced_at=current_ts,
                                                        synced_status=copy.deepcopy(glob_ep.status)
                                                    )
                                                    break
                                        break




    # Sequential Execution of Sync/Unmark
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_server_sync, server): server for server in servers}
        for future in as_completed(futures):
            server = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"[{server.info()}] Failed to sync server: {e}")
    
    # Save final state including server_sync_state after all servers synced
    save_watched_state(state)
    logger.info("[System] Server sync states saved.")

    return state
