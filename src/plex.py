import os
import requests
import unicodedata
import re
from math import floor
from dotenv import load_dotenv
from loguru import logger
from typing import Any

from urllib3.poolmanager import PoolManager
from datetime import datetime, timezone

from urllib3.exceptions import ReadTimeoutError
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter as RequestsHTTPAdapter

from plexapi.video import Show, Episode, Movie
from plexapi.server import PlexServer
from plexapi.myplex import MyPlexAccount, MyPlexUser
from plexapi.library import MovieSection, ShowSection

from src.functions import (
    search_mapping,
    log_marked,
    str_to_bool,
)
from src.watched import (
    LibraryData,
    MediaIdentifiers,
    MediaItem,
    WatchedStatus,
    WatchedState,
    Series,
    UserData,
    UserData,
    check_same_identifiers,
)
from src.playlists import (
    Playlist,
    UserPlaylists,
)

load_dotenv(override=True)


generate_guids = str_to_bool(os.getenv("GENERATE_GUIDS", "True"))
generate_locations = str_to_bool(os.getenv("GENERATE_LOCATIONS", "True"))


# Bypass hostname validation for ssl. Taken from https://github.com/pkkid/python-plexapi/issues/143#issuecomment-775485186
class HostNameIgnoringAdapter(RequestsHTTPAdapter):
    def init_poolmanager(
        self, connections: int, maxsize: int | None, block=..., **pool_kwargs
    ) -> None:
        pool_kwargs["assert_hostname"] = False
        # Increase timeout for large libraries
        pool_kwargs["timeout"] = 120  # Increased timeout from 30s to 120s
        return super().init_poolmanager(
            connections, maxsize, block, **pool_kwargs
        )


def extract_guids_from_item(item: Movie | Show | Episode, server_info: str = "") -> dict[str, str]:
    # If GENERATE_GUIDS is set to False, then return an empty dict
    if not generate_guids:
        return {}

    prefix = f"[{server_info}] " if server_info else ""
    logger.debug(f"{prefix}Extracting GUIDs for '{item.title}'")
    
    # Check for guids in the object
    guids_list = getattr(item, 'guids', [])

    guids: dict[str, str] = {}
    plex_guid_found = False
    
    for guid in guids_list:
        if guid.id and len(guid.id.strip()) > 0:
            parts = guid.id.split("://")
            if len(parts) == 2:
                scheme = parts[0]
                value = parts[1]
                if scheme == 'plex':
                    guids[scheme] = guid.id # Keep full string for plex
                    plex_guid_found = True
                elif scheme == 'imdb' and 'imdb' not in guids:
                    guids['imdb'] = value.split('?')[0]
                elif 'thetvdb' in scheme and 'tvdb' not in guids:
                    guids['tvdb'] = value.split('?')[0]
                elif 'themoviedb' in scheme and 'tmdb' not in guids:
                    guids['tmdb'] = value.split('?')[0]

    # Fallback: item.guid (only if no standard plex GUID was found)
    if not plex_guid_found and hasattr(item, "guid") and item.guid:
        parts = item.guid.split("://")
        if len(parts) == 2:
            scheme, value = parts
            if scheme == 'plex':
                guids['plex'] = item.guid # Keep full string
            # Legacy agent mapping
            elif 'imdb' not in guids and 'imdb' in scheme:
                guids['imdb'] = value.split('?')[0]
            elif 'tvdb' not in guids and 'thetvdb' in scheme:
                guids['tvdb'] = value.split('?')[0]
            elif 'tmdb' not in guids and 'themoviedb' in scheme:
                guids['tmdb'] = value.split('?')[0]
            else:
                # Fully support custom agent GUIDs - compatible with search(guid=...)
                guids['plex'] = item.guid # Keep full custom agent GUID

    if guids:
        prefix = f"[{server_info}] " if server_info else ""
        logger.debug(f"{prefix}Extracted GUIDs for '{item.title}': {guids}")
    
    return guids




def extract_identifiers_from_item(item: Movie | Show | Episode, server_info: str = "") -> MediaIdentifiers:
    guids = extract_guids_from_item(item, server_info)

    ids = MediaIdentifiers(
        title=item.title,
        locations=(
            tuple([location.split("/")[-1] for location in item.locations])
            if generate_locations
            else tuple()
        ),
        imdb_id=guids.get("imdb"),
        tvdb_id=guids.get("tvdb"),
        tmdb_id=guids.get("tmdb"),
        plex_guid=guids.get("plex"),
    )
    return ids



def get_mediaitem(item: Movie | Episode, completed: bool, existing_identifiers: MediaIdentifiers | None = None, server_info: str = "") -> MediaItem:
    last_viewed = getattr(item, "lastViewedAt", None)
    if last_viewed and hasattr(last_viewed, "timestamp"):
        last_viewed_timestamp = int(last_viewed.timestamp())
    else:
        last_viewed_timestamp = None

    # Reuse existing identifiers if provided, otherwise extract new ones
    if existing_identifiers and existing_identifiers.plex_guid:
        identifiers = existing_identifiers
    else:
        identifiers = extract_identifiers_from_item(item, server_info)

    return MediaItem(
        identifiers=identifiers,
        status=WatchedStatus(
            completed=completed,
            time=getattr(item, "viewOffset", 0) or 0,
            last_viewed_at=last_viewed_timestamp,
        ),
    )


def update_user_watched(
    user: MyPlexAccount,
    user_plex: PlexServer,
    library_data: LibraryData,
    library_name: str,
    dryrun: bool,
    items_to_remove: LibraryData | None = None,
) -> None:
    try:
        # Early exit if there are no items to sync
        has_items_to_sync = (library_data.movies or library_data.series)
        has_items_to_remove = items_to_remove and (items_to_remove.movies or items_to_remove.series)
        
        if not has_items_to_sync and not has_items_to_remove:
            return

        # Prepare context info for logging
        # Log format requested: [ServerName] [UserName] (So we pass "ServerName] [UserName" because prefix adds brackets)
        server_context = f"{user_plex.friendlyName}] [{user.title}"

        # Initialize task counters
        mark_actions_count = 0
        unmark_actions_count = 0

        library_section = user_plex.library.section(library_name)

        # ---------------------------------------------------------
        # A. HANDLE MARK WATCHED / PROGRESS
        # ---------------------------------------------------------
        # ---------------------------------------------------------
        # A. HANDLE MARK WATCHED / PROGRESS
        # ---------------------------------------------------------
        # ---------------------------------------------------------
        # A. HANDLE MARK WATCHED / PROGRESS
        # ---------------------------------------------------------
        if library_data.movies or library_data.series:
            # Update movies.
            # Update movies.
            # Update movies.
            if library_data.movies:
                logger.debug(f"[{user_plex.friendlyName}] Processing {len(library_data.movies)} watched movies from state file.")
                
                for stored_movie in library_data.movies:
                    # GUID Priority: plex_guid > imdb_id > tmdb_id > tvdb_id
                    search_guid = stored_movie.identifiers.plex_guid
                    
                    # Fallback to other IDs if plex_guid is missing
                    if not search_guid:
                        if stored_movie.identifiers.imdb_id:
                            search_guid = f"com.plexapp.agents.imdb://{stored_movie.identifiers.imdb_id}"
                        elif stored_movie.identifiers.tmdb_id:
                            search_guid = f"com.plexapp.agents.themoviedb://{stored_movie.identifiers.tmdb_id}"
                        elif stored_movie.identifiers.tvdb_id:
                            search_guid = f"com.plexapp.agents.thetvdb://{stored_movie.identifiers.tvdb_id}"
                    
                    # Skip if no GUID found (Location-based matching is used only in unmark logic)
                    if not search_guid:
                        logger.debug(f"[{user_plex.friendlyName}] Skipping movie with no identifiable GUID: {stored_movie.identifiers.title}")
                        continue

                    # Search specifically for this item by GUID
                    results = library_section.search(guid=search_guid)
                    
                    if not results:
                        # Log on search failure
                        logger.debug(f"[{user_plex.friendlyName}] No results found for movie '{stored_movie.identifiers.title}' with GUID: {search_guid}")
                        continue
                        
                    for plex_movie in results:
                        # Double check isWatched state to avoid redundant calls
                        if plex_movie.isWatched and stored_movie.status.completed:
                            continue

                        if stored_movie.status.completed:
                            msg = f"[{user_plex.friendlyName}] [{user.title}] Marking '{plex_movie.title}' as watched (GUID Match)"
                            if not dryrun:
                                try:
                                    plex_movie.markWatched()
                                    mark_actions_count += 1
                                except Exception as e:
                                    logger.error(f"[{user_plex.friendlyName}] [{user.title}] Failed to mark '{plex_movie.title}' as watched: {e}")
                            else:
                                mark_actions_count += 1
                            logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
                            log_marked(user_plex.friendlyName, user_plex.friendlyName, user.title, library_name, plex_movie.title, None, None)
                        else:
                            current_offset = getattr(plex_movie, "viewOffset", 0) or 0
                            target_offset = stored_movie.status.time
                            if abs(current_offset - target_offset) >= 60000:
                                msg = f"[{user_plex.friendlyName}] [{user.title}] Updating progress for '{plex_movie.title}' to {floor(stored_movie.status.time / 60_000)}m (GUID Match)"
                                if not dryrun:
                                    plex_movie.updateTimeline(stored_movie.status.time)
                                    mark_actions_count += 1
                                else:
                                    mark_actions_count += 1
                                logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
                                log_marked(user_plex.friendlyName, user_plex.friendlyName, user.title, library_name, plex_movie.title, duration=stored_movie.status.time)


            # Update TV Shows.
            # Update TV Shows.
            if library_data.series:
                logger.debug(f"[{user_plex.friendlyName}] Processing {len(library_data.series)} watched series from state file.")
                
                for stored_series in library_data.series:
                    # Iterate through episodes in the stored series
                    for stored_ep in stored_series.episodes:
                        # GUID Priority: plex_guid > imdb_id > tmdb_id > tvdb_id
                        search_guid = stored_ep.identifiers.plex_guid
                        
                        # Fallback to other IDs if plex_guid is missing
                        if not search_guid:
                            if stored_ep.identifiers.imdb_id:
                                search_guid = f"com.plexapp.agents.imdb://{stored_ep.identifiers.imdb_id}"
                            elif stored_ep.identifiers.tmdb_id:
                                search_guid = f"com.plexapp.agents.themoviedb://{stored_ep.identifiers.tmdb_id}"
                            elif stored_ep.identifiers.tvdb_id:
                                search_guid = f"com.plexapp.agents.thetvdb://{stored_ep.identifiers.tvdb_id}"
                        
                        if not search_guid:
                            logger.debug(f"[{user_plex.friendlyName}] Skipping episode with no identifiable GUID: {stored_ep.identifiers.title}")
                            continue
                        
                        # Search specifically for this episode by GUID
                        # libtype='episode' is crucial here to tell Plex we are looking for episodes, not shows/movies
                        results = library_section.search(guid=search_guid, libtype='episode')
                        
                        if not results:
                            continue

                        for plex_episode in results:
                            # Verify Parent Show Title / GUID match if needed?
                            # GUID collision for episodes is rare, so we trust it.
                            
                            if plex_episode.isWatched and stored_ep.status.completed:
                                continue

                            if stored_ep.status.completed:
                                if not plex_episode.isWatched:
                                     msg = f"[{user_plex.friendlyName}] [{user.title}] Marking '{plex_episode.grandparentTitle} - {plex_episode.title}' as watched (GUID Match)"
                                     if not dryrun:
                                         try:
                                             plex_episode.markWatched()
                                             mark_actions_count += 1
                                         except Exception as e:
                                             logger.error(f"[{user_plex.friendlyName}] [{user.title}] Failed to mark '{plex_episode.title}' as watched: {e}")
                                     else:
                                         mark_actions_count += 1
                                     logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
                                     log_marked(user_plex.friendlyName, user_plex.friendlyName, user.title, library_name, plex_episode.grandparentTitle, plex_episode.title)
                            else:
                                current_offset = getattr(plex_episode, "viewOffset", 0) or 0
                                target_offset = stored_ep.status.time
                                if abs(current_offset - target_offset) >= 60000:
                                    msg = f"[{user_plex.friendlyName}] [{user.title}] Updating progress for '{plex_episode.grandparentTitle} - {plex_episode.title}' to {floor(stored_ep.status.time / 60_000)}m (GUID Match)"
                                    if not dryrun:
                                        plex_episode.updateTimeline(stored_ep.status.time)
                                        mark_actions_count += 1
                                    else:
                                        mark_actions_count += 1
                                    logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
                                    log_marked(user_plex.friendlyName, user_plex.friendlyName, user.title, library_name, plex_episode.grandparentTitle, plex_episode.title, stored_ep.status.time)


        # ---------------------------------------------------------
        # B. HANDLE UNMARK (REMOVE WATCHED)
        # ---------------------------------------------------------
        if items_to_remove and (items_to_remove.movies or items_to_remove.series):
            # Optimized Removal Loop
            
            # Movies
            if items_to_remove.movies:
                remove_movies_by_file = {}
                for m in items_to_remove.movies:
                     for loc in m.identifiers.locations:
                         fn = loc.replace("\\", "/").split("/")[-1]
                         remove_movies_by_file[fn] = m

                for plex_movie in library_section.search(unwatched=False):
                    matched_remove_movie = None
                    p_locations = getattr(plex_movie, 'locations', [])
                    p_filenames = [l.replace("\\", "/").split("/")[-1] for l in p_locations]
                    
                    for p_fn in p_filenames:
                        if p_fn in remove_movies_by_file:
                            matched_remove_movie = remove_movies_by_file[p_fn]
                            break
                    
                    if not matched_remove_movie:
                        plex_identifiers = extract_identifiers_from_item(plex_movie, server_context)
                        for remove_movie in items_to_remove.movies:
                            if check_same_identifiers(plex_identifiers, remove_movie.identifiers):
                                matched_remove_movie = remove_movie
                                break
                    
                    if matched_remove_movie:
                         msg = f"[{user_plex.friendlyName}] [{user.title}] Unmarking '{plex_movie.title}' (Setting Unwatched)"
                         if not dryrun:
                             plex_movie.markUnwatched()
                         logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
            
            # Series
            if items_to_remove.series:
                for plex_show in library_section.search(unwatched=False):
                    matched_remove_series = None
                    plex_show_identifiers = extract_identifiers_from_item(plex_show, server_context)
                    
                    for remove_series in items_to_remove.series:
                         if check_same_identifiers(plex_show_identifiers, remove_series.identifiers):
                             matched_remove_series = remove_series
                             break
                    
                    if matched_remove_series:
                         remove_eps_by_file = {}
                         for ep in matched_remove_series.episodes:
                             for loc in ep.identifiers.locations:
                                 fn = loc.replace("\\", "/").split("/")[-1]
                                 remove_eps_by_file[fn] = ep

                         plex_episodes = plex_show.watched() 
                         for plex_ep in plex_episodes:
                             matched_remove_ep = None
                             pe_locations = getattr(plex_ep, 'locations', [])
                             pe_filenames = [l.replace("\\", "/").split("/")[-1] for l in pe_locations]
                             
                             for pe_fn in pe_filenames:
                                 if pe_fn in remove_eps_by_file:
                                     matched_remove_ep = remove_eps_by_file[pe_fn]
                                     break
                             
                             if not matched_remove_ep:
                                 ep_ids = extract_identifiers_from_item(plex_ep, server_context)
                                 for remove_ep in matched_remove_series.episodes:
                                     if check_same_identifiers(ep_ids, remove_ep.identifiers):
                                         matched_remove_ep = remove_ep
                                         break

                             if matched_remove_ep:
                                  msg = f"[{user_plex.friendlyName}] [{user.title}] Unmarking '{plex_show.title} - {plex_ep.title}'"
                                  if not dryrun:
                                      plex_ep.markUnwatched()
                                  logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")

    except Exception as e:
        logger.error(
            f"[{user_plex.friendlyName}] [{user.title}] Failed to update watched status in '{library_name}': {e}",
        )
        raise e


# class plex accept base url and token and username and password but default with none
class Plex:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        user_name: str | None = None,
        password: str | None = None,
        server_name: str | None = None,
        ssl_bypass: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        self.server_type: str = "Plex"
        self.ssl_bypass: bool = ssl_bypass
        if session is None:
            session = requests.Session()

        if ssl_bypass:
            # Session for ssl bypass
            # By pass ssl hostname check https://github.com/pkkid/python-plexapi/issues/143#issuecomment-775485186
             session.mount("https://", HostNameIgnoringAdapter())
        
        self.session = session
        self.plex: PlexServer = self.login(
            base_url, token, user_name, password, server_name
        )

        self.base_url: str = self.plex._baseurl

        self.admin_user: MyPlexAccount = self.plex.myPlexAccount()
        self.users: list[MyPlexUser | MyPlexAccount] = self.get_users()
        
        # Cache for user PlexServer instances to prevent repeated logins/connections
        self.server_cache: dict[str, PlexServer] = {}

    def login(
        self,
        base_url: str | None,
        token: str | None,
        user_name: str | None,
        password: str | None,
        server_name: str | None,
    ) -> PlexServer:
        try:
            if base_url and token:
                plex: PlexServer = PlexServer(base_url, token, session=self.session)
            elif user_name and password and server_name:
                # Login via plex account with controlled session
                account = MyPlexAccount(user_name, password, session=self.session)
                plex = account.resource(server_name).connect()
                # Enforce our session on the connected server instance
                if hasattr(plex, '_session'):
                    plex._session = self.session
            else:
                raise Exception("No complete plex credentials provided")

            return plex
        except Exception as e:
            if user_name:
                msg = f"Failed to login via plex account {user_name}"
                logger.error(f"[System] Failed to login, {msg}, Error: {e}")
            else:
                logger.error(f"[System] Failed to login, Error: {e}")
            raise Exception(e)

    def info(self) -> str:
        return self.plex.friendlyName

    def search(self, query: str):
        return self.plex.search(query)

    def get_users(self) -> list[MyPlexUser | MyPlexAccount]:
        try:
            # 1. Get all friends (shared users)
            all_friends = self.plex.myPlexAccount().users()
            allowed_users: list[MyPlexUser | MyPlexAccount] = []
            
            # 2. Get Current Server's Machine Identifier
            current_machine_id = self.plex.machineIdentifier

            # 3. Filter users based on server access
            for user in all_friends:
                has_access = False
                # user.servers contains list of servers shared with this user
                if hasattr(user, 'servers') and user.servers:
                    for shared_server in user.servers:
                        if shared_server.machineIdentifier == current_machine_id:
                            has_access = True
                            break
                
                if has_access:
                    allowed_users.append(user)
                else:
                    # Log at debug level to avoid clutter, but informative enough
                    logger.debug(f"[{self.plex.friendlyName}] User '{user.title}' skipped (No access to server)")

            # 4. Append Admin User (Self) - Always has access
            allowed_users.append(self.plex.myPlexAccount())

            logger.info(f"[{self.plex.friendlyName}] Found {len(allowed_users)} authorized users")
            return allowed_users

        except Exception as e:
            logger.error(f"[{self.plex.friendlyName}] Failed to get users: {e}")
            raise Exception(e)

    def get_libraries(self) -> dict[str, str]:
        try:
            output = {}

            libraries = self.plex.library.sections()
            logger.debug(
                f"Plex: All Libraries {[library.title for library in libraries]}"
            )

            for library in libraries:
                library_title = library.title
                library_type = library.type

                if library_type not in ["movie", "show"]:
                    logger.debug(
                        f"Plex: Skipping Library {library_title} found type {library_type}",
                    )
                    continue

                output[library_title] = library_type

            logger.info(f"[{self.plex.friendlyName}] Found {len(output)} libraries")
            return output
        except Exception as e:
            logger.error(f"[{self.plex.friendlyName}] Failed to get libraries: {e}")
            raise Exception(e)

    def get_user_library_watched(
        self, user_name: str, user_plex: PlexServer, library: MovieSection | ShowSection, existing_library_data: LibraryData | None = None
    ) -> LibraryData:
        try:
            logger.info(
                f"[{self.plex.friendlyName}] [{user_name}] Fetching watched data from '{library.title}'",
            )
            watched = LibraryData(title=library.title)
            
            # Prepare context info for logging
            server_context = f"{self.plex.friendlyName} - {user_name}"

            library_videos = user_plex.library.section(library.title)

            if library.type == "movie":
                # Build a fast lookup map for movies by filenames
                movie_map = {}
                if existing_library_data:
                    for m in existing_library_data.movies:
                        for loc in m.identifiers.locations:
                            movie_map[loc] = m.identifiers

                for video in library_videos.search(unwatched=False) + library_videos.search(inProgress=True):
                    if video.isWatched or video.viewOffset >= 60000:
                        existing_identifiers = None
                        
                        # 1. Normalize GUID
                        v_guid = getattr(video, 'guid', None)
                        # if v_guid and v_guid.startswith("plex://"): 
                        #     v_guid = v_guid.replace("plex://", "", 1)
                        
                        # 2. Extract Filenames (Handle both / and \)
                        v_filenames = []
                        for loc in video.locations:
                            v_filenames.append(loc.replace("\\", "/").split("/")[-1])

                        # 3. Match by Filename (O(1))
                        for fn in v_filenames:
                            if fn in movie_map:
                                existing_identifiers = movie_map[fn]
                                break
                        
                        # 4. Fallback to GUID match
                        if not existing_identifiers and v_guid and existing_library_data:
                             for m in existing_library_data.movies:
                                 e_guid = m.identifiers.plex_guid
                                 if e_guid == v_guid:
                                     existing_identifiers = m.identifiers
                                     break
                                 # Fallback for prefix mismatch (Dynamic)
                                 if e_guid and v_guid:
                                     eg_clean = e_guid.split("://")[-1]
                                     vg_clean = v_guid.split("://")[-1]
                                     if eg_clean == vg_clean:
                                         existing_identifiers = m.identifiers
                                         break
                        
                        # 5. Last Fallback: Exact Title Match -- REMOVED per user request
                        # if not existing_identifiers and existing_library_data:
                        #      vt = unicodedata.normalize('NFC', video.title)
                        #      for m in existing_library_data.movies:
                        #          if unicodedata.normalize('NFC', m.identifiers.title or "") == vt:
                        #              existing_identifiers = m.identifiers
                        #              break
                        
                        watched.movies.append(get_mediaitem(video, video.isWatched, existing_identifiers, server_context))


            elif library.type == "show":
                # Build a fast lookup map for series by guid
                series_guid_map = {} # {plex_guid: Show}
                series_location_map = {} 
                
                if existing_library_data:
                    for s in existing_library_data.series:
                        if s.identifiers.plex_guid: 
                            series_guid_map[s.identifiers.plex_guid] = s
                            # Redundant key for flexible matching (Dynamic)
                            parts = s.identifiers.plex_guid.split("://")
                            if len(parts) > 1:
                                series_guid_map[parts[-1]] = s
                        
                        # Populate location map
                        for loc in s.identifiers.locations:
                             norm_loc = loc.replace("\\", "/").strip("/")
                             series_location_map[norm_loc] = s

                # Keep track of processed shows to reduce duplicate shows
                processed_shows = []
                for show in library_videos.search(
                    unwatched=False
                ) + library_videos.search(inProgress=True):
                    if show.key in processed_shows:
                        continue
                    processed_shows.append(show.key)
                    
                    # 1. Fast lookup for existing series
                    existing_series = None
                    s_guid = getattr(show, 'guid', None)
                    # if s_guid and s_guid.startswith("plex://"): s_guid = s_guid.replace("plex://", "", 1)
                    
                    if s_guid in series_guid_map:
                        existing_series = series_guid_map[s_guid]
                    
                    if not existing_series and s_guid:
                        s_guid_clean = s_guid.split("://")[-1]
                        if s_guid_clean in series_guid_map:
                            existing_series = series_guid_map[s_guid_clean]

                    # Fallback lookup: Location
                    if not existing_series:
                        for loc in show.locations:
                             norm_loc = loc.replace("\\", "/").strip("/")
                             if norm_loc in series_location_map:
                                 existing_series = series_location_map[norm_loc]
                                 break
                    
                    if not existing_series:
                        logger.trace(f"No match found for series '{show.title}' (GUID: {s_guid})")
                    
                    # 2. Match show/series identifiers
                    if existing_series:
                        show_identifiers = existing_series.identifiers
                    else:
                        show_identifiers = extract_identifiers_from_item(show, server_context)
                    
                    # 3. Build fast lookup maps for episodes (Normalized)
                    ep_filename_map = {} 
                    ep_guid_map = {}     
                    if existing_series:
                        for ep_obj in existing_series.episodes:
                            for loc in ep_obj.identifiers.locations:
                                ep_filename_map[loc] = ep_obj.identifiers
                            e_eg = ep_obj.identifiers.plex_guid
                            if e_eg:
                                ep_guid_map[e_eg] = ep_obj.identifiers
                                # Redundant key for flexible matching (Dynamic)
                                parts = e_eg.split("://")
                                if len(parts) > 1:
                                    ep_guid_map[parts[-1]] = ep_obj.identifiers

                    episode_mediaitem = []
                    # 4. Fetch and match episodes
                    for episode in show.watched() + show.episodes(viewOffset__gte=60_000):
                        matching_e_ids = None
                        v_eg = getattr(episode, 'guid', None)
                        # if v_eg and v_eg.startswith("plex://"): v_eg = v_eg.replace("plex://", "", 1)
                        
                        v_ef = [l.replace("\\", "/").split("/")[-1] for l in episode.locations] if generate_locations else []
                        
                        # Match: FileName -> GUID
                        for fn in v_ef:
                            if fn in ep_filename_map:
                                matching_e_ids = ep_filename_map[fn]
                                break
                        
                        if not matching_e_ids and v_eg in ep_guid_map:
                            matching_e_ids = ep_guid_map[v_eg]
                        
                        # Fallback lookup clean/raw (Dynamic)
                        if not matching_e_ids and v_eg:
                             v_eg_clean = v_eg.split("://")[-1]
                             if v_eg_clean in ep_guid_map:
                                 matching_e_ids = ep_guid_map[v_eg_clean]
                            
                        # Removed Title Fallback per user request
                        
                        episode_mediaitem.append(get_mediaitem(episode, episode.isWatched, matching_e_ids, server_context))

                    if episode_mediaitem:
                        watched.series.append(
                            Series(identifiers=show_identifiers, episodes=episode_mediaitem)
                        )

            return watched

        except Exception as e:
            logger.error(
                f"[{self.plex.friendlyName}] [{user_name}] Failed to get watched data from '{library.title}': {e}",
            )
            # Return None to skip processing as returning an empty object might be mistaken for "deleted"
            return None

    def get_watched(
        self, users: list[MyPlexUser | MyPlexAccount], sync_libraries: list[str], existing_state: WatchedState | None = None
    ) -> dict[str, UserData]:
        try:
            users_watched: dict[str, UserData] = {}

            for user in users:
                if self.admin_user == user:
                    user_plex = self.plex
                else:
                    # Check Cache First
                    cache_key = user.title or user.username
                    if cache_key in self.server_cache:
                        user_plex = self.server_cache[cache_key]
                    else:
                        token = user.get_token(self.plex.machineIdentifier)
                        if token:
                            user_plex = self.login(self.base_url, token, None, None, None)
                            self.server_cache[cache_key] = user_plex
                        else:
                            logger.error(
                                f"Plex: Failed to get token for {user.title}, skipping",
                            )
                            continue

                user_name: str = (
                    user.username.lower() if user.username else user.title.lower()
                )

                libraries = user_plex.library.sections()

                for library in libraries:
                    if library.title not in sync_libraries:
                        continue

                    # Get existing library data from state for GUID reuse
                    existing_library_data = None
                    if existing_state:
                        # Normalize keys for comparison
                        user_name_nfc = unicodedata.normalize('NFC', user_name)
                        state_users_nfc = {unicodedata.normalize('NFC', k): v for k, v in existing_state.users.items()}
                        
                        if user_name_nfc in state_users_nfc:
                            existing_library_data = state_users_nfc[user_name_nfc].libraries.get(library.title)
                            if not existing_library_data:
                                # Try normalized library title as well
                                lib_title_nfc = unicodedata.normalize('NFC', library.title)
                                state_libs_nfc = {unicodedata.normalize('NFC', k): v for k, v in state_users_nfc[user_name_nfc].libraries.items()}
                                existing_library_data = state_libs_nfc.get(lib_title_nfc)

                    library_data = self.get_user_library_watched(
                        user_name, user_plex, library, existing_library_data
                    )

                    if library_data is None:
                        logger.warning(f"[{self.plex.friendlyName}] Skipping library '{library.title}' due to fetch error.")
                        continue

                    if user_name not in users_watched:
                        users_watched[user_name] = UserData()

                    users_watched[user_name].libraries[library.title] = library_data

            return users_watched
        except Exception as e:
            logger.error(f"Plex: Failed to get watched, Error: {e}")
            raise Exception(e)

    def get_playlists(
        self, users: list[MyPlexUser | MyPlexAccount], 
        existing_playlist_state: Any | None = None
    ) -> dict[str, UserPlaylists]:
        try:
            users_playlists: dict[str, UserPlaylists] = {}

            for user in users:
                if self.admin_user == user:
                    user_plex = self.plex
                else:
                    cache_key = user.title or user.username
                    if cache_key in self.server_cache:
                        user_plex = self.server_cache[cache_key]
                    else:
                        token = user.get_token(self.plex.machineIdentifier)
                        if token:
                            user_plex = PlexServer(self.base_url, token, session=self.session)
                            self.server_cache[cache_key] = user_plex
                        else:
                            logger.error(f"[{self.plex.friendlyName}] Failed to get token for {user.title}, skipping")
                            continue
            
                # Check for shared libraries (Skip if none)
                if not user_plex.library.sections():
                    logger.debug(f"[{self.plex.friendlyName}] User {user.title} has no shared libraries, skipping playlist fetch.")
                    continue

                user_name: str = (user.username.lower() if user.username else user.title.lower())
                logger.info(f"[{self.plex.friendlyName}] [{user_name}] Fetching playlists")

                if user_name not in users_playlists:
                    users_playlists[user_name] = UserPlaylists()

                # Build a fast lookup from PREVIOUS PLAYLIST STATE only (Normalized)
                prev_items_map = {} # {fname/guid: MediaIdentifiers}
                if existing_playlist_state:
                    u_pl = existing_playlist_state.users.get(user_name) or existing_playlist_state.users.get(user_name.lower())
                    if u_pl:
                        for pl_obj in u_pl.playlists.values():
                            for item in pl_obj.items:
                                if item.plex_guid: 
                                    g_val = item.plex_guid
                                    prev_items_map[g_val] = item
                                    # Redundant key for flexible matching (Dynamic)
                                    parts = g_val.split("://")
                                    if len(parts) > 1:
                                        prev_items_map[parts[-1]] = item

                                for loc in item.locations: 
                                    fn = loc.replace("\\", "/").split("/")[-1]
                                    prev_items_map[fn] = item

                playlists = user_plex.playlists()
                for pl in playlists:
                    if pl.smart: continue
                    
                    pl_title_nfc = unicodedata.normalize('NFC', pl.title)
                    media_items = []
                    
                    for item in pl.items():
                        if item.type not in ['movie', 'episode']: continue
                        
                        existing_ids = None
                        v_guid = getattr(item, 'guid', None)
                        # if v_guid and v_guid.startswith("plex://"): 
                        #     v_guid = v_guid.replace("plex://", "", 1)
                        
                        v_fnames = [l.replace("\\", "/").split("/")[-1] for l in item.locations] if generate_locations else []
                        
                        # Match: FileName -> GUID
                        for fn in v_fnames:
                            if fn in prev_items_map:
                                existing_ids = prev_items_map[fn]
                                break
                        
                        if not existing_ids and v_guid in prev_items_map:
                            existing_ids = prev_items_map[v_guid]
                        
                        # Fallback lookup clean/raw (Dynamic)
                        if not existing_ids and v_guid:
                             v_guid_clean = v_guid.split("://")[-1]
                             if v_guid_clean in prev_items_map:
                                 existing_ids = prev_items_map[v_guid_clean]
                        
                        # [OPTIMIZATION] Skip reload to prevent server overload
                        # if not existing_ids and not v_fnames:
                        #    pass

                        if not existing_ids:
                            existing_ids = extract_identifiers_from_item(item)
                        
                        media_items.append(existing_ids)

                    if media_items:
                        users_playlists[user_name].playlists[pl_title_nfc] = Playlist(title=pl_title_nfc, items=media_items)

            return users_playlists

        except Exception as e:
            logger.error(f"Plex: Failed to get playlists, Error: {e}")
            raise Exception(e)

    def update_watched(
        self,
        watched_list: dict[str, UserData],
        user_mapping: dict[str, str] | None = None,
        library_mapping: dict[str, str] | None = None,
        dryrun: bool = False,
        items_to_remove_list: dict[str, UserData] | None = None,
    ) -> None:
        try:
            all_users = set(watched_list.keys())
            if items_to_remove_list:
                all_users.update(items_to_remove_list.keys())

            for user_key in all_users:
                user_data = watched_list.get(user_key, UserData())
                remove_data = items_to_remove_list.get(user_key) if items_to_remove_list else None

                user_other = None
                if user_mapping:
                    user_other = search_mapping(user_mapping, user_key)

                user = None
                for index, value in enumerate(self.users):
                    username_title = (
                        value.username.lower()
                        if value.username
                        else value.title.lower()
                    )

                    if user_key.lower() == username_title:
                        user = self.users[index]
                        break
                    elif user_other and user_other.lower() == username_title:
                        user = self.users[index]
                        break
                
                if not user:
                    # Try fetch by string if not in cached users
                    if isinstance(user_key, str):
                         try:
                            user = self.plex.myPlexAccount().user(user_key)
                         except:
                            logger.warning(f"Plex: Could not find user {user_key}")
                            continue

                if not user:
                     continue

                if self.admin_user == user:
                    user_plex = self.plex
                else:
                    if isinstance(user, str): # Fallback (should be covered above)
                        logger.debug(
                            f"Plex: {user} is not a plex object, attempting to get object for user",
                        )
                        user = self.plex.myPlexAccount().user(user)

                    if not isinstance(user, MyPlexUser):
                        logger.error(f"Plex: {user} failed to get PlexUser")
                        continue

                    # Check Cache First
                    cache_key = user.title or user.username
                    if cache_key in self.server_cache:
                        user_plex = self.server_cache[cache_key]
                    else:
                        token = user.get_token(self.plex.machineIdentifier)
                        if token:
                            user_plex = PlexServer(
                                self.base_url,
                                token,
                                session=self.session,
                            )
                            self.server_cache[cache_key] = user_plex
                        else:
                            logger.error(
                                f"Plex: Failed to get token for {user.title}, skipping",
                            )
                            continue

                if not user_plex:
                    logger.error(f"Plex: {user} Failed to get PlexServer")
                    continue

                # Union of libraries to update (add/mark) and remove (unmark)
                all_libs = set(user_data.libraries.keys())
                if remove_data:
                    all_libs.update(remove_data.libraries.keys())

                for library_name in all_libs:
                    library_data = user_data.libraries.get(library_name, LibraryData(title=library_name))
                    items_to_remove = remove_data.libraries.get(library_name) if remove_data else None
                    
                    library_other = None
                    if library_mapping:
                        library_other = search_mapping(library_mapping, library_name)
                    
                    # if library in plex library list
                    library_list = user_plex.library.sections()
                    
                    # Resolve real library name on server
                    target_library_name = library_name
                    
                    if library_name.lower() not in [x.title.lower() for x in library_list]:
                        if library_other:
                            if library_other.lower() in [x.title.lower() for x in library_list]:
                                logger.debug(
                                    f"Plex: Library {library_name} not found, but {library_other} found, using {library_other}",
                                )
                                target_library_name = library_other
                            else:
                                logger.debug(
                                    f"Plex: Library {library_name} or {library_other} not found in library list",
                                )
                                continue
                        else:
                            logger.debug(
                                f"Plex: Library {library_name} not found in library list",
                            )
                            continue

                    update_user_watched(
                        user,
                        user_plex,
                        library_data,
                        target_library_name,
                        dryrun,
                        items_to_remove=items_to_remove,
                    )

        except Exception as e:
            logger.error(f"Plex: Failed to update watched, Error: {e}")
            raise Exception(e)

    def update_playlists(
        self,
        playlists_to_sync: dict[str, UserPlaylists],
        user_mapping: dict[str, str] | None = None,
        dryrun: bool = False,
    ) -> None:
        try:
            for user_key, user_playlists in playlists_to_sync.items():
                user_other = None
                if user_mapping:
                    user_other = search_mapping(user_mapping, user_key)
                
                # Find the target user object
                target_user = None
                for index, value in enumerate(self.users):
                    username_title = (
                        value.username.lower() if value.username else value.title.lower()
                    )
                    if user_key.lower() == username_title:
                        target_user = self.users[index]
                        break
                    elif user_other and user_other.lower() == username_title:
                        target_user = self.users[index]
                        break
                
                if not target_user:
                    logger.error(f"Plex: User {user_key} not found for playlist sync")
                    continue
                
                # Connect as user
                if self.admin_user == target_user:
                    user_plex = self.plex
                else:
                    if isinstance(target_user, str):
                         target_user = self.plex.myPlexAccount().user(target_user)
                    
                    # Check Cache First
                    cache_key = target_user.title or target_user.username
                    if cache_key in self.server_cache:
                        user_plex = self.server_cache[cache_key]
                    else:
                        token = target_user.get_token(self.plex.machineIdentifier)
                        if token:
                            user_plex = PlexServer(self.base_url, token, session=self.session)
                            self.server_cache[cache_key] = user_plex
                        else:
                            logger.error(f"Plex: Failed to get token for {target_user.title}")
                            continue

                # Process playlists
                existing_playlists = {pl.title: pl for pl in user_plex.playlists()}
                
                for playlist_name, playlist_data in user_playlists.playlists.items():
                    logger.info(f"Plex: Processing playlist '{playlist_name}' for {target_user.title}")
                    
                    target_playlist = existing_playlists.get(playlist_name)
                    
                    # If playlist doesn't exist, create it (if there are items)
                    if not target_playlist:
                        if not playlist_data.items:
                            logger.debug(f"Plex: Playlist {playlist_name} empty and doesn't exist, skipping creation.")
                            continue
                            
                        # We need initial items to create a playlist
                        # We have to find these items in the Plex server first
                        # syncing items...
                        items_to_add = []
                        for item_data in playlist_data.items:
                            # Search for the item in Plex
                            # We search by guid logic which is complex in Plex API without checking every item
                            # We can try search by title and match guids
                             found_item = self._find_item_by_identifiers(user_plex, item_data)
                             if found_item:
                                 items_to_add.append(found_item)
                        
                        if items_to_add:
                            msg = f"Plex: Creating playlist '{playlist_name}' with {len(items_to_add)} items for {target_user.title}"
                            if not dryrun:
                                user_plex.createPlaylist(playlist_name, items=items_to_add)
                            logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
                        else:
                            logger.warning(f"Plex: Could not find any items to create playlist '{playlist_name}'")

                    else:
                        # Playlist exists, add missing items
                        items_to_add = []
                        for item_data in playlist_data.items:
                             found_item = self._find_item_by_identifiers(user_plex, item_data)
                             if found_item:
                                 items_to_add.append(found_item)

                        if items_to_add:
                             msg = f"Plex: Adding {len(items_to_add)} items to playlist '{playlist_name}' for {target_user.title}"
                             if not dryrun:
                                 target_playlist.addItems(items_to_add)
                             logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")

        except Exception as e:
            logger.error(f"Plex: Failed to update playlists, Error: {e}")
            raise Exception(e)

    def _find_item_by_identifiers(self, plex_server: PlexServer, identifiers: MediaIdentifiers):
        # Helper to find an item in Plex library based on identifiers
        # Search by title first
        results = plex_server.search(identifiers.title)
        for result in results:
             if result.type not in ['movie', 'episode']:
                 continue
             
             result_identifiers = extract_identifiers_from_item(result)
             if check_same_identifiers(identifiers, result_identifiers):
                 return result
        return None

    def delete_playlist(self, playlist: Playlist, dryrun: bool = False) -> None:
        try:
            # We need the actual Plex Playlist object or keys to delete
            # But here we might receive our internal Playlist object which doesn't hold the key directly?
            # Actually, the calling logic will likely pass the Plex Playlist object or we need to find it first.

            # However, simpler if we pass the Plex object directly if possible, or search it.
            # Let's assume we find it by title for the specific user context.
            pass
            # Wait, implementing generic way in 'update_playlists' might be better, 
            # OR expose a method that takes the playlist Title string and User object/name.
        except Exception as e:
            raise e

    def delete_playlist_by_title(self, user: MyPlexUser | MyPlexAccount, playlist_title: str, dryrun: bool = False) -> None:
        try:
            if self.admin_user == user:
                user_plex = self.plex
            else:
                 token = user.get_token(self.plex.machineIdentifier)
                 if token:
                     user_plex = PlexServer(self.base_url, token, session=self.session)
                 else:
                     logger.error(f"Plex: Failed to get token for {user.title}")
                     return

            # Find playlist
            playlist = None
            for pl in user_plex.playlists():
                if pl.title == playlist_title:
                    playlist = pl
                    break
            
            if playlist:
                msg = f"Plex: Deleting playlist '{playlist_title}' for {user.title}"
                if not dryrun:
                    playlist.delete()
                logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")
            else:
                logger.warning(f"[{self.plex.friendlyName}] Playlist '{playlist_title}' not found for deletion")

        except Exception as e:
             logger.error(f"[{self.plex.friendlyName}] Failed to delete playlist {playlist_title}, Error: {e}")
             raise Exception(e)

    def remove_item_from_playlist(self, user: MyPlexUser | MyPlexAccount, playlist_title: str, item_identifier: MediaIdentifiers, dryrun: bool = False) -> None:
        try:
            if self.admin_user == user:
                user_plex = self.plex
            else:
                 token = user.get_token(self.plex.machineIdentifier)
                 if token:
                     user_plex = PlexServer(self.base_url, token, session=self.session)
                 else:
                     logger.error(f"[{self.plex.friendlyName}] Failed to get token for {user.title}")
                     return

            playlist = None
            for pl in user_plex.playlists():
                if pl.title == playlist_title:
                    playlist = pl
                    break
            
            if not playlist:
                logger.warning(f"[{self.plex.friendlyName}] Playlist '{playlist_title}' not found")
                return

            # Find item in playlist
            # This is tricky because we need the specific item within the playlist
            items_to_remove = []
            for item in playlist.items():
                 # Match identifiers
                 # Assuming extract_identifiers_from_item works on playlist items (it should as they are movies/episodes)
                 if item.type not in ['movie', 'episode']: continue
                 item_ids = extract_identifiers_from_item(item)
                 if check_same_identifiers(item_ids, item_identifier):
                     items_to_remove.append(item)
            
            if items_to_remove:
                msg = f"[{self.plex.friendlyName}] Removing {len(items_to_remove)} items from playlist '{playlist_title}' for {user.title}"
                if not dryrun:
                    playlist.removeItems(items_to_remove)
                logger.success(f"{'[DRYRUN] ' if dryrun else ''}{msg}")

        except Exception as e:
            logger.error(f"[{self.plex.friendlyName}] Failed to remove item from playlist {playlist_title}, Error: {e}")
            raise Exception(e)

    def close(self):
        if self.session:
            self.session.close()
            logger.debug(f"[{self.plex.friendlyName}] Connection closed")
