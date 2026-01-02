import copy
import json
import os
from pydantic import BaseModel, Field
from loguru import logger
from typing import Any, Literal
from dataclasses import dataclass
from dotenv import load_dotenv

from src.watched import MediaIdentifiers, check_same_identifiers, check_guid_match
from src.functions import search_mapping, CONFIG_DIR

# Load env again just in case, using CONFIG_DIR
load_dotenv(os.path.join(CONFIG_DIR, ".env"), override=True)

class Playlist(BaseModel):
    title: str
    items: list[MediaIdentifiers] = Field(default_factory=list)


class UserPlaylists(BaseModel):
    # Key is playlist title
    playlists: dict[str, Playlist] = Field(default_factory=dict)


class PlaylistState(BaseModel):
    # Key is user identifier (e.g. username)
    users: dict[str, UserPlaylists] = Field(default_factory=dict)

playlist_env = os.getenv("PLAYLIST_STATE_FILE")
if playlist_env and os.path.isabs(playlist_env):
    STATE_FILE = playlist_env
elif playlist_env:
    STATE_FILE = os.path.join(CONFIG_DIR, playlist_env)
else:
    STATE_FILE = os.path.join(CONFIG_DIR, "playlist_state.json")

def load_state() -> PlaylistState:
    if not os.path.exists(STATE_FILE):
        return PlaylistState()
    
    # Check for empty file
    if os.path.getsize(STATE_FILE) == 0:
        logger.warning(f"Playlist state file {STATE_FILE} is empty. Returning empty state.")
        return PlaylistState()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return PlaylistState(**data)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode playlist state: {e}")
        # Backup corrupted file
        backup_file = STATE_FILE + ".corrupted"
        try:
            if os.path.exists(STATE_FILE):
                import shutil
                shutil.copy2(STATE_FILE, backup_file)
                logger.info(f"Corrupted state file backed up to {backup_file}")
        except Exception as backup_error:
             logger.error(f"Failed to backup corrupted state file: {backup_error}")
        return PlaylistState()
    except Exception as e:
        logger.error(f"Failed to load playlist state: {e}")
        return PlaylistState()

def save_state(state: PlaylistState) -> None:
    try:
        # Direct write to avoid Docker bind mount issues (Errno 16 Device or resource busy)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(state.model_dump_json(indent=4))
    except Exception as e:
        logger.error(f"Failed to save playlist state: {e}")

@dataclass
class SyncAction:
    action: Literal["create_playlist", "delete_playlist", "add_item", "remove_item"]
    playlist_title: str
    item: MediaIdentifiers | None = None

def merge_identifiers(target: MediaIdentifiers, source: MediaIdentifiers):
    if not target.imdb_id and source.imdb_id: target.imdb_id = source.imdb_id
    if not target.tvdb_id and source.tvdb_id: target.tvdb_id = source.tvdb_id
    if not target.tmdb_id and source.tmdb_id: target.tmdb_id = source.tmdb_id
    if source.locations:
        target.locations = tuple(set(target.locations + source.locations))

def synchronize_playlists(
    server_playlists: dict[str, dict[str, UserPlaylists]], # ServerName -> User -> Playlists
    previous_state: PlaylistState,
    user_mapping: dict[str, str] | None = None,
    servers: list = None,
) -> tuple[PlaylistState, dict[str, dict[str, list[SyncAction]]]]:
    """
    Playlist synchronization using similar structure to Watched
    
    1. Fetch server data (already done - server_playlists)
    2. Merge into global state
    3. Mark already synced items
    4. Sync differences to each server
    """
    import time
    from src.watched import ServerSyncInfo, WatchedStatus
    
    # Step 1: Start based on existing state
    state = copy.deepcopy(previous_state)
    current_ts = int(time.time())
    
    # Create server_id mapping (server_name -> server_id)
    server_id_map = {}
    if servers:
        for server in servers:
            server_id_map[server.info()] = server.plex.machineIdentifier
    
    # Step 2: Merge server data into global state + Detect deletions
    logger.info("[Playlist] Merging playlist data from all servers...")
    
    # Track items validly deleted in this cycle to prevent re-addition
    # Key: (user_name, playlist_title), Value: list[MediaIdentifiers]
    trashed_registry: dict[tuple[str, str], list] = {}

    # Phase 1: Detect Deletions FIRST
    for server_name, s_users in server_playlists.items():
        if server_name not in server_id_map:
            continue
        s_id = server_id_map[server_name]
        
        for s_user, s_pl in s_users.items():
            l_user = s_user
            if user_mapping:
                l_user = search_mapping(user_mapping, s_user) or s_user
            
            if l_user not in state.users:
                continue
            
            user_state = state.users[l_user]
            
            for pl_title, playlist in s_pl.playlists.items():
                if pl_title not in user_state.playlists:
                    continue
                
                state_pl = user_state.playlists[pl_title]
                
                # Detect deletions: Items in global state but missing from server
                items_to_remove = []
                for st_item in state_pl.items:
                    # Check if partially synced to this server before
                    if s_id in st_item.synced_to_servers:
                        # Was synced before but missing now -> Deleted!
                        found_in_server = any(check_same_identifiers(st_item, s_item) for s_item in playlist.items)
                        if not found_in_server:
                            logger.info(f"[Playlist] Item deletion detected: '{st_item.title}' removed from '{pl_title}' on {server_name}")
                            items_to_remove.append(st_item)
                
                # Apply deletions and register them
                if items_to_remove:
                    reg_key = (l_user, pl_title)
                    if reg_key not in trashed_registry:
                        trashed_registry[reg_key] = []
                    trashed_registry[reg_key].extend(items_to_remove)

                    for item in items_to_remove:
                        if item in state_pl.items:
                            state_pl.items.remove(item)

    # Phase 2: Merge Additions (Skip trashed items)
    for server_name, s_users in server_playlists.items():
        if server_name not in server_id_map:
            continue
        
        for s_user, s_pl in s_users.items():
            l_user = s_user
            if user_mapping:
                l_user = search_mapping(user_mapping, s_user) or s_user
            
            if l_user not in state.users:
                state.users[l_user] = UserPlaylists()
            
            user_state = state.users[l_user]
            
            # Merge playlists
            for pl_title, playlist in s_pl.playlists.items():
                if pl_title not in user_state.playlists:
                    user_state.playlists[pl_title] = Playlist(title=pl_title, items=[])
                
                state_pl = user_state.playlists[pl_title]
                reg_key = (l_user, pl_title)
                
                # Merge items
                for s_item in playlist.items:
                    # Check if this item was just deleted on another server
                    if reg_key in trashed_registry:
                        if any(check_same_identifiers(s_item, t_item) for t_item in trashed_registry[reg_key]):
                            continue # Skip re-adding deleted item

                    found = False
                    for st_item in state_pl.items:
                        if check_same_identifiers(s_item, st_item):
                            # Already exists - merge metadata
                            merge_identifiers(st_item, s_item)
                            found = True
                            break
                    
                    if not found:
                        # Add new item
                        state_pl.items.append(s_item)
    
    # Step 3: Mark already-synced items (First run optimization)
    logger.info("[Playlist] Marking already-synced items...")
    
    for server_name, s_users in server_playlists.items():
        if server_name not in server_id_map:
            continue
        s_id = server_id_map[server_name]
        
        for s_user, s_pl in s_users.items():
            l_user = search_mapping(user_mapping, s_user) if user_mapping else s_user
            if l_user not in state.users:
                continue
            
            user_state = state.users[l_user]
            
            for pl_title, s_playlist in s_pl.playlists.items():
                if pl_title not in user_state.playlists:
                    continue
                
                state_pl = user_state.playlists[pl_title]
                
                # Compare each server item with global state
                for s_item in s_playlist.items:
                    for g_item in state_pl.items:
                        if check_same_identifiers(s_item, g_item):
                            # Identical item - mark as synced
                            g_item.synced_to_servers[s_id] = ServerSyncInfo(
                                synced_at=current_ts,
                                synced_status=WatchedStatus(completed=True, time=0, last_viewed_at=None)
                            )
                            break
    
    save_state(state)
    logger.info("[Playlist] Already-synced items marked.")
    
    # Step 4: Create sync actions for each server
    actions: dict[str, dict[str, list[SyncAction]]] = {}
    for s_name in server_playlists:
        actions[s_name] = {}
    
    for server_name, s_users in server_playlists.items():
        if server_name not in server_id_map:
            continue
        s_id = server_id_map[server_name]
        
        for s_user, s_pl in s_users.items():
            l_user = search_mapping(user_mapping, s_user) if user_mapping else s_user
            if l_user not in state.users:
                continue
            
            user_state = state.users[l_user]
            server_actions = actions[server_name].setdefault(s_user, [])
            
            # Check each playlist in global state
            for pl_title, state_pl in user_state.playlists.items():
                if pl_title not in s_pl.playlists:
                    # Playlist missing on server - Need creation
                    server_actions.append(SyncAction("create_playlist", pl_title))
                    # Item addition handled below
                    server_pl_items = []
                else:
                    server_pl_items = s_pl.playlists[pl_title].items
                
                # Sync items (Differences only)
                for g_item in state_pl.items:
                    # Check if already synced
                    if s_id in g_item.synced_to_servers:
                        continue  # Already synced - Skip!
                    
                    # Check if exists on server
                    found_in_server = any(check_same_identifiers(g_item, s_item) for s_item in server_pl_items)
                    
                    if not found_in_server:
                        # Missing on server - Add needed
                        server_actions.append(SyncAction("add_item", pl_title, g_item))
                
                # Check deletions (Items on server but missing in global state)
                if pl_title in s_pl.playlists:
                    for s_item in s_pl.playlists[pl_title].items:
                        found_in_state = any(check_same_identifiers(s_item, g_item) for g_item in state_pl.items)
                        if not found_in_state:
                            server_actions.append(SyncAction("remove_item", pl_title, s_item))
    
    return state, actions
