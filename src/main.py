import os
import traceback
import json
import sys
from dotenv import load_dotenv
from time import sleep, perf_counter
from datetime import datetime
from loguru import logger
from croniter import croniter

from src.plex import Plex
from src.functions import (
    parse_string_to_list,
    str_to_bool,
    str_to_bool,
    search_mapping,
    CONFIG_DIR,
)
from src.watched import synchronize_watched
from src.users import (
    generate_user_list,
    generate_server_users,
    filter_user_lists,
)
from src.playlists import (
    load_state,
    save_state,
    synchronize_playlists,
    UserPlaylists,
)
from src.black_white import setup_black_white_lists
from src.connection import generate_server_connections

# Re-load dotenv to ensure main sees overrides if called directly, though functions loaded it.
load_dotenv(os.path.join(CONFIG_DIR, ".env"), override=True)

log_env = os.getenv("LOG_FILE")
if log_env and os.path.isabs(log_env):
    log_file = log_env
elif log_env:
    log_file = os.path.join(CONFIG_DIR, log_env)
else:
    log_file = os.path.join(CONFIG_DIR, "log.log")

level = os.getenv("DEBUG_LEVEL", "INFO").upper()


def configure_logger() -> None:
    logger.remove()
    if level not in ["INFO", "DEBUG", "TRACE"]:
        logger.add(sys.stdout)
        raise Exception("Invalid DEBUG_LEVEL, please choose between INFO, DEBUG, TRACE")
    logger.add(log_file, level=level, mode="w")
    logger.add(sys.stdout, level=level)


def main_loop(servers: list[Plex]) -> None:
    dryrun = str_to_bool(os.getenv("DRYRUN", "False"))
    logger.debug(f"[System] Dryrun: {dryrun}")
    
    user_mapping_env = os.getenv("USER_MAPPING", None)
    user_mapping = None
    if user_mapping_env:
        user_mapping = json.loads(user_mapping_env.lower())
    if user_mapping:
        logger.debug(f"[System] User Mapping: {user_mapping}")

    library_mapping_env = os.getenv("LIBRARY_MAPPING", None)
    library_mapping = None
    if library_mapping_env:
        library_mapping = json.loads(library_mapping_env)
    if library_mapping:
         logger.debug(f"[System] Library Mapping: {library_mapping}")

    logger.debug("[System] Initializing Black/White lists")
    blacklist_library = parse_string_to_list(os.getenv("BLACKLIST_LIBRARY", None))
    whitelist_library = parse_string_to_list(os.getenv("WHITELIST_LIBRARY", None))
    blacklist_library_type = parse_string_to_list(os.getenv("BLACKLIST_LIBRARY_TYPE", None))
    whitelist_library_type = parse_string_to_list(os.getenv("WHITELIST_LIBRARY_TYPE", None))
    blacklist_users = parse_string_to_list(os.getenv("BLACKLIST_USERS", None))
    whitelist_users = parse_string_to_list(os.getenv("WHITELIST_USERS", None))

    (
        blacklist_library,
        whitelist_library,
        blacklist_library_type,
        whitelist_library_type,
        blacklist_users,
        whitelist_users,
    ) = setup_black_white_lists(
        blacklist_library,
        whitelist_library,
        blacklist_library_type,
        whitelist_library_type,
        blacklist_users,
        whitelist_users,
        library_mapping,
        user_mapping,
    )

    # Servers are passed in


    # ---------------------------------------------------------
    # PART 1: Watched Status Sync
    # ---------------------------------------------------------
    logger.info("[System] Starting Watched Status Synchronization...")
    watched_state = synchronize_watched(
        servers,
        blacklist_users,
        whitelist_users,
        blacklist_library,
        whitelist_library,
        blacklist_library_type,
        whitelist_library_type,
        user_mapping,
        library_mapping,
        dryrun,
    )
    logger.info("[System] Finished Watched Status Synchronization.")


    # ---------------------------------------------------------
    # PART 2: Playlist Sync
    # ---------------------------------------------------------
    sync_playlists = str_to_bool(os.getenv("SYNC_PLAYLISTS", "True"))
    if sync_playlists:
        logger.info("[System] Starting Global Playlist Synchronization")
        try:
            # 1. Load State
            playlist_state = load_state()
            
            # 2. Gather Playlists
            server_data = {}
            for server in servers:
                raw_user_names = generate_user_list(server)
                
                user_map_for_filtering = {}
                for u_name in raw_user_names:
                    mapped_name = u_name
                    if user_mapping:
                        found = search_mapping(user_mapping, u_name)
                        if found: mapped_name = found
                    user_map_for_filtering[u_name] = mapped_name
                
                filtered_user_map = filter_user_lists(user_map_for_filtering, blacklist_users, whitelist_users)
                s_users = generate_server_users(server, filtered_user_map)
                
                if not s_users:
                    logger.debug(f"[{server.info()}] No users to sync after filtering.")
                    server_data[server.info()] =  {}
                else:
                    logger.debug(f"[{server.info()}] Syncing for {len(s_users)} users")
                    s_playlists = server.get_playlists(s_users, playlist_state)
                    server_data[server.info()] = s_playlists

                if server.info() in server_data and server_data[server.info()]:
                    for u_name, u_pl in server_data[server.info()].items():
                        if u_pl.playlists:
                            details = [f"'{title}' ({len(pl.items)} items)" for title, pl in u_pl.playlists.items()]
                            logger.info(f"[{server.info()}] [{u_name}] has {len(u_pl.playlists)} playlists: {', '.join(details)}")

            # 3. Synchronize Logic
            new_state, actions = synchronize_playlists(server_data, playlist_state, user_mapping, servers)
            
            # 4. Execute Actions
            for s_name, user_actions in actions.items():
                total_actions = sum(len(acts) for acts in user_actions.values())
                if total_actions > 0:
                    logger.info(f"[{s_name}] Planned Actions: {total_actions} actions")
                    for u, acts in user_actions.items():
                        if acts: logger.debug(f"[{s_name}] [{u}] Planned: {[a.action + ' ' + a.playlist_title for a in acts]}")
                else:
                    logger.info(f"[{s_name}] No playlist actions needed")

            for server in servers:
                s_name = server.info()
                if s_name in actions and actions[s_name]:
                    logger.info(f"[{s_name}] Executing Playlist Actions")
                    users_to_update = {}
                    
                    for user_name, user_actions in actions[s_name].items():
                        for action in user_actions:
                            if action.action == "create_playlist" or action.action == "add_item":
                                if user_name not in users_to_update:
                                    users_to_update[user_name] = UserPlaylists()
                                
                                log_level = "INFO" if action.action == "create_playlist" else "DEBUG"
                                logger.log(log_level, f"[{s_name}] [{user_name}] Action {action.action} for playlist '{action.playlist_title}'")

                                state_pl = new_state.users[user_name].playlists[action.playlist_title]
                                users_to_update[user_name].playlists[action.playlist_title] = state_pl
                            
                            elif action.action == "delete_playlist":
                                logger.info(f"{s_name}: Action delete_playlist '{action.playlist_title}' (User: {user_name})")
                                target_user_obj = None
                                # Resolve User
                                for u in server.users:
                                    u_name = u.username or u.title
                                    if u_name.lower() == user_name.lower():
                                        target_user_obj = u
                                        break
                                
                                if target_user_obj:
                                     server.delete_playlist_by_title(target_user_obj, action.playlist_title, dryrun)
                                else:
                                     logger.error(f"Could not resolve Plex user {user_name} for deletion")

                            elif action.action == "remove_item":
                                logger.debug(f"{s_name}: Action remove_item from playlist '{action.playlist_title}' (User: {user_name})")
                                target_user_obj = None
                                # Resolve User
                                for u in server.users:
                                    u_name = u.username or u.title
                                    if u_name.lower() == user_name.lower():
                                        target_user_obj = u
                                        break
                                
                                if target_user_obj:
                                    server.remove_item_from_playlist(target_user_obj, action.playlist_title, action.item, dryrun)

                    if users_to_update:
                        server.update_playlists(users_to_update, user_mapping, dryrun)

            save_state(new_state)
            
        except Exception as e:
            logger.error(f"Error in Global Playlist Sync: {e}")
            logger.error(traceback.format_exc())

    pass


@logger.catch
def main() -> None:
    configure_logger()
    run_only_once = str_to_bool(os.getenv("RUN_ONLY_ONCE", "False"))
    sleep_duration = float(os.getenv("SLEEP_DURATION", "3600"))  # Default 1 hour
    sync_cron = os.getenv("SYNC_CRON", None)  # Optional Cron expression

    logger.info("[System] Initializing server connections...")
    try:
        servers = generate_server_connections()
    except Exception as e:
        logger.error(f"Failed to create server connections: {e}")
        return

    times: list[float] = []
    try:
        while True:
            try:
                start = perf_counter()
                main_loop(servers)
                end = perf_counter()
                times.append(end - start)

                if len(times) > 0:
                    logger.info(f"[System] Cycle completed. Average time: {sum(times) / len(times):.2f}s")

                if run_only_once:
                    break

                # Determine Sleep Duration
                wait_seconds = sleep_duration
                
                if sync_cron:
                    try:
                        now = datetime.now()
                        cron = croniter(sync_cron, now)
                        next_run = cron.get_next(datetime)
                        wait_seconds = (next_run - now).total_seconds()
                        logger.info(f"[System] Next run scheduled at {next_run} (Cron: '{sync_cron}')")
                    except Exception as e:
                        logger.error(f"[System] Invalid Cron expression '{sync_cron}': {e}. Falling back to SLEEP_DURATION.")
                
                logger.info(f"[System] Sleeping for {wait_seconds:.2f}s...")
                sleep(wait_seconds)

            except Exception as error:
                if isinstance(error, list):
                    for message in error:
                        logger.error(message)
                else:
                    logger.error(error)

                logger.error(traceback.format_exc())

                if run_only_once:
                    break

                logger.info(f"Retrying in {sleep_duration}")
                sleep(sleep_duration)

    except KeyboardInterrupt:
        if len(times) > 0:
            logger.info(f"Average time: {sum(times) / len(times)}")
        logger.info("Exiting")
        return
    finally:
        if 'servers' in locals() and servers:
            logger.info("[System] Closing server connections")
            for server in servers:
                try:
                    server.close()
                except Exception as e:
                    logger.warning(f"Error closing server connection: {e}")
        try:
            os._exit(0)
        except:
            pass
