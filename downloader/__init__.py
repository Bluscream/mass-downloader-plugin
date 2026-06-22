"""
Downloader Plugin Provider for Music Assistant.
Allows downloading tracks, albums, artists, and playlists from cloud providers to local filesystems.
"""

from __future__ import annotations

import asyncio
import logging
import os
import urllib.parse
from typing import TYPE_CHECKING, Any

from music_assistant_models.enums import ContentType, MediaType, ProviderFeature
from music_assistant_models.errors import MediaNotFoundError
from music_assistant_models.media_items import AudioFormat
from music_assistant_models.queue_item import QueueItem
from music_assistant.helpers.ffmpeg import get_ffmpeg_stream
from music_assistant.models.plugin import PluginProvider
from music_assistant.helpers.api import api_command

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigEntry, ConfigValueType, ProviderConfig
    from music_assistant_models.provider import ProviderManifest
    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType

SUPPORTED_FEATURES = set()

async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider(instance) with given configuration."""
    return DownloaderPlugin(mass, manifest, config, SUPPORTED_FEATURES)

async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,
    action: str | None = None,
    values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """Return Config entries to setup this provider."""
    from music_assistant_models.config_entries import ConfigEntry, ConfigEntryType, ConfigValueOption
    
    # Dynamically build choices for target local providers
    target_provider_options = []
    for prov in mass.music.providers:
        if prov.domain == "filesystem_local" and prov.available:
            target_provider_options.append(ConfigValueOption(prov.name, prov.instance_id))

    return (
        ConfigEntry(
            key="default_target_provider",
            type=ConfigEntryType.STRING,
            label="Default Target Provider",
            default_value=target_provider_options[0].value if target_provider_options else "",
            options=target_provider_options,
            required=True,
            description="Select the default local filesystem provider to copy downloaded files to.",
        ),
        ConfigEntry(
            key="target_format",
            type=ConfigEntryType.STRING,
            label="Target Audio Format",
            default_value="mp3",
            options=[
                ConfigValueOption("MP3", "mp3"),
                ConfigValueOption("FLAC", "flac"),
            ],
            required=True,
        ),
        ConfigEntry(
            key="path_format",
            type=ConfigEntryType.STRING,
            label="Download Path Format",
            default_value="Music Assistant/{source_provider}/{download_type}/{parent_name}/{filename}",
            description="Placeholders: {source_provider}, {download_type}, {parent_name}, {filename}, {artist}, {album}, {title}, {track_number}, {md5}",
            required=True,
        ),
        ConfigEntry(
            key="queue_playlist",
            type=ConfigEntryType.STRING,
            label="Download Queue Playlist Name",
            default_value="Download Queue",
            description="Name of the playlist used as a download queue. Added tracks will be automatically downloaded and removed.",
            required=True,
        ),
        ConfigEntry(
            key="instant_downloads",
            type=ConfigEntryType.BOOLEAN,
            label="Enable Instant Downloads",
            default_value=True,
            description="Trigger download immediately when a song is added to the download queue. If disabled, downloads will only run on the scheduled task interval.",
            required=True,
        ),
        ConfigEntry(
            key="embed_unsynced_lyrics",
            type=ConfigEntryType.BOOLEAN,
            label="Embed Unsynced Lyrics",
            default_value=True,
            description="Embed plain text unsynchronized lyrics directly inside the audio files.",
            required=True,
        ),
        ConfigEntry(
            key="embed_synced_lyrics",
            type=ConfigEntryType.BOOLEAN,
            label="Embed Synced Lyrics",
            default_value=True,
            description="Embed synchronized lyrics (SYLT in MP3, LYRICS in FLAC) directly inside the audio files.",
            required=True,
        ),
        ConfigEntry(
            key="save_lrc_file",
            type=ConfigEntryType.BOOLEAN,
            label="Save Lyrics as LRC file next to the song",
            default_value=True,
            description="Save a separate .lrc file in the same folder next to the downloaded audio file.",
            required=True,
        ),
        ConfigEntry(
            key="save_srt_file",
            type=ConfigEntryType.BOOLEAN,
            label="Save Lyrics as SRT file next to the song",
            default_value=False,
            description="Save a separate .srt file in the same folder next to the downloaded audio file.",
            required=True,
        ),
        ConfigEntry(
            key="add_to_downloads_playlist",
            type=ConfigEntryType.BOOLEAN,
            label="Add Finished Downloads to Playlist",
            default_value=True,
            description="After each successful download, automatically add the local track to a 'Downloads' playlist.",
            required=True,
        ),
        ConfigEntry(
            key="downloads_playlist",
            type=ConfigEntryType.STRING,
            label="Downloads Playlist Name",
            default_value="Downloads",
            description="Name of the playlist where finished downloads are collected. Created automatically if it doesn't exist.",
            required=True,
        ),
    )

class DownloaderPlugin(PluginProvider):
    """Downloader Plugin provider."""

    async def loaded_in_mass(self) -> None:
        """Call after the provider has been loaded."""
        # Custom API Command is registered here
        self.mass.register_api_command("plugin/downloader/download", self.download)
        
        # Subscribe to library updates to listen for additions to the Download Queue playlist
        from music_assistant_models.enums import EventType
        self.mass.subscribe(self._on_mass_event, (EventType.MEDIA_ITEM_ADDED, EventType.MEDIA_ITEM_UPDATED))
        self._download_lock = asyncio.Lock()
        self._queue_playlist_id = None
        self._downloads_playlist_id = None

        # Register recurring task to check queue playlist every hour
        from music_assistant_models.background_task import TaskSchedule
        self.mass.tasks.register_scheduled_task(
            task_id=f"downloader_queue_check_{self.instance_id}",
            name="Download queue periodic check",
            handler=self._run_scheduled_queue_check,
            schedule=TaskSchedule.hourly(every=1),
        )

        # Create Download Queue playlist automatically if it doesn't exist
        self.mass.loop.create_task(self._create_queue_playlist_if_missing())

        # Create Downloads (finished) playlist automatically if enabled
        self.mass.loop.create_task(self._ensure_downloads_playlist())

        # Resolve ffmpeg version
        self.ffmpeg_version = await self._resolve_ffmpeg_version()

    async def unload(self, is_removed: bool = False) -> None:
        """Handle unload/close of the provider."""
        self.mass.tasks.unregister_scheduled_task(
            f"downloader_queue_check_{self.instance_id}",
            clear_persisted_state=is_removed,
        )

    async def _ensure_downloads_playlist(self) -> None:
        """Create (or resolve) the Downloads playlist on boot if the feature is enabled."""
        if not self.config.get_value("add_to_downloads_playlist", True):
            return
        # Allow the server to load fully
        await asyncio.sleep(5)
        playlist_name = self.config.get_value("downloads_playlist", "Downloads")
        try:
            playlists = await self.mass.music.playlists.library_items()
            for playlist in playlists:
                if playlist.name.lower() == playlist_name.lower():
                    self.logger.info("Downloads playlist '%s' already exists.", playlist.name)
                    try:
                        self._downloads_playlist_id = int(playlist.item_id)
                    except ValueError:
                        self._downloads_playlist_id = playlist.item_id
                    return
            # Create it on the builtin provider
            self.logger.info("Creating Downloads playlist: %s", playlist_name)
            playlist = await self.mass.music.playlists.create_playlist(
                name=playlist_name,
                provider_instance_or_domain="builtin"
            )
            try:
                self._downloads_playlist_id = int(playlist.item_id)
            except ValueError:
                self._downloads_playlist_id = playlist.item_id
        except Exception as e:
            self.logger.warning("Failed to auto-create Downloads playlist: %s", e)

    async def _add_track_to_downloads_playlist(
        self,
        dest_file: str,
        target_provider: str,
        track_name: str,
    ) -> None:
        """Find the local track by file path and add it to the Downloads playlist."""
        if not self.config.get_value("add_to_downloads_playlist", True):
            return
        if self._downloads_playlist_id is None:
            self.logger.debug("Downloads playlist not yet initialised, skipping add for '%s'.", track_name)
            return
        try:
            # Normalise path separators so the comparison is OS-agnostic
            norm_dest = os.path.normpath(dest_file).lower()

            # Search within the local provider for the track by matching its filename/path.
            # We use a short retry loop because the sync may still be in progress.
            local_track = None
            from music_assistant_models.enums import MediaType
            for attempt in range(6):
                # Search by the track's basename (title without extension)
                basename = os.path.splitext(os.path.basename(dest_file))[0]
                results = await self.mass.music.search(basename, [MediaType.TRACK], limit=25)
                if results and results.tracks:
                    for candidate in results.tracks:
                        for pm in candidate.provider_mappings:
                            if pm.provider_instance != target_provider:
                                continue
                            # item_id for filesystem tracks is the URL-encoded file path
                            try:
                                candidate_path = urllib.parse.unquote(pm.item_id)
                            except Exception:
                                candidate_path = pm.item_id
                            if os.path.normpath(candidate_path).lower() == norm_dest:
                                local_track = candidate
                                break
                        if local_track:
                            break
                if local_track:
                    break
                # Not found yet — wait for the sync to pick it up
                if attempt < 5:
                    await asyncio.sleep(10)

            if not local_track:
                self.logger.warning(
                    "Could not find local track '%s' in provider '%s' to add to Downloads playlist.",
                    track_name, target_provider
                )
                return

            # Resolve the media-item URI expected by add_playlist_tracks
            local_mapping = next(
                (pm for pm in local_track.provider_mappings if pm.provider_instance == target_provider),
                next(iter(local_track.provider_mappings), None)
            )
            if not local_mapping:
                return

            playlist_id = str(self._downloads_playlist_id)

            # Collect existing track URIs so we can prepend the new one (newest-first ordering).
            existing_uris: list[str] = []
            existing_positions: list[int] = []
            try:
                pos = 1
                async for t in self.mass.music.playlists.tracks(playlist_id, "library", force_refresh=True):
                    existing_uris.append(t.uri)
                    existing_positions.append(pos)
                    pos += 1
            except Exception as ex:
                self.logger.warning("Could not read existing Downloads playlist tracks: %s", ex)

            new_uri = local_track.uri

            # Skip adding if already present (de-duplicate)
            if new_uri in existing_uris:
                self.logger.info("'%s' is already in the Downloads playlist, skipping.", track_name)
                return

            # Remove all existing tracks so we can re-insert in the desired order
            if existing_positions:
                try:
                    await self.mass.music.playlists._handle_remove_playlist_tracks(
                        self._downloads_playlist_id, tuple(existing_positions)
                    )
                except Exception as ex:
                    self.logger.warning("Could not clear Downloads playlist for reorder: %s", ex)
                    # Fall back to plain append
                    existing_uris = []

            # Re-add: new track first, then the rest in their original order
            ordered_uris = [new_uri] + existing_uris
            await self.mass.music.playlists._handle_add_playlist_tracks(
                self._downloads_playlist_id, ordered_uris
            )
            self.logger.info(
                "Prepended '%s' to Downloads playlist (playlist_id=%s, total=%d tracks).",
                track_name, self._downloads_playlist_id, len(ordered_uris)
            )
        except Exception as e:
            self.logger.warning("Failed to add track '%s' to Downloads playlist: %s", track_name, e)

    async def _create_queue_playlist_if_missing(self) -> None:
        """Helper to create the download queue playlist on boot if it does not exist."""
        # Allow the server to load fully
        await asyncio.sleep(5)
        queue_name = self.config.get_value("queue_playlist", "Download Queue")
        
        try:
            # Check if playlist already exists in the library
            playlists = await self.mass.music.playlists.library_items()
            for playlist in playlists:
                if playlist.name.lower() == queue_name.lower():
                    self.logger.info("Download queue playlist '%s' already exists. Triggering boot scan...", playlist.name)
                    # Convert item_id to int in case it is passed as a string representation of the database ID
                    try:
                        db_playlist_id = int(playlist.item_id)
                    except ValueError:
                        db_playlist_id = playlist.item_id
                    self._queue_playlist_id = db_playlist_id
                    self.mass.loop.create_task(self._process_queue(db_playlist_id))
                    return
            
            # Create a new playlist on the builtin provider
            self.logger.info("Creating download queue playlist: %s", queue_name)
            playlist = await self.mass.music.playlists.create_playlist(
                name=queue_name,
                provider_instance_or_domain="builtin"
            )
            try:
                db_playlist_id = int(playlist.item_id)
            except ValueError:
                db_playlist_id = playlist.item_id
            self._queue_playlist_id = db_playlist_id
        except Exception as e:
            self.logger.warning("Failed to auto-create download queue playlist: %s", e)

    async def _on_mass_event(self, event: Any) -> None:
        """Handle library events to detect additions to the Download Queue playlist."""
        if not self.config.get_value("instant_downloads", True):
            return
        # The object_id is the playlist URI or DB ID.
        # We check if it is a playlist and its name matches the configured queue_playlist.
        if not event.object_id or not event.object_id.startswith("library://playlist/"):
            return
        
        try:
            db_id_str = event.object_id.split("/")[-1]
            db_playlist_id = int(db_id_str)
        except Exception:
            return

        playlist = await self.mass.music.playlists.get_library_item(db_playlist_id)
        if not playlist:
            return

        queue_name = self.config.get_value("queue_playlist", "Download Queue")
        if playlist.name.lower() != queue_name.lower():
            return

        self._queue_playlist_id = db_playlist_id
        await self._process_queue(db_playlist_id)

    async def _run_scheduled_queue_check(self) -> None:
        """Scheduled task handler to check and process the download queue."""
        queue_name = self.config.get_value("queue_playlist", "Download Queue")
        db_playlist_id = getattr(self, "_queue_playlist_id", None)
        
        if db_playlist_id is None:
            # Try to resolve playlist dynamically
            try:
                playlists = await self.mass.music.playlists.library_items()
                for playlist in playlists:
                    if playlist.name.lower() == queue_name.lower():
                        try:
                            db_playlist_id = int(playlist.item_id)
                        except ValueError:
                            db_playlist_id = playlist.item_id
                        self._queue_playlist_id = db_playlist_id
                        break
            except Exception as e:
                self.logger.warning("Failed to resolve download queue playlist in scheduled check: %s", e)
                return

        if db_playlist_id is not None:
            await self._process_queue(db_playlist_id)
        else:
            self.logger.debug("Download queue playlist '%s' does not exist yet.", queue_name)

    async def _process_queue(self, db_playlist_id: str | int) -> None:
        """Process tracks in the download queue playlist."""
        playlist = await self.mass.music.playlists.get_library_item(db_playlist_id)
        if not playlist:
            return

        # Acquire lock so we process one update at a time
        async with self._download_lock:
            # Fetch the tracks in the playlist with force_refresh=True to avoid cached desync
            tracks = []
            async for track in self.mass.music.playlists.tracks(str(db_playlist_id), "library", force_refresh=True):
                tracks.append(track)

            if not tracks:
                return

            self.logger.info("Found %d tracks in download queue playlist '%s'", len(tracks), playlist.name)
            
            # Download all tracks currently in the queue
            positions_to_remove = []
            has_failures = False
            for idx, track in enumerate(tracks):
                try:
                    # Find a source cloud provider mapping
                    prov_mapping = next(
                        (x for x in track.provider_mappings if not x.provider_domain.startswith("filesystem")),
                        next(iter(track.provider_mappings), None)
                    )
                    if not prov_mapping:
                        self.logger.warning("No valid provider mapping found for track: %s", track.name)
                        continue

                    self.logger.info("Auto-downloading queue track: %s (%s)", track.name, prov_mapping.item_id)
                    
                    # Call download method directly
                    target_prov = await self._find_default_target_provider("track")
                    await self._download_single_track(
                        track_id=prov_mapping.item_id,
                        source_provider=prov_mapping.provider_instance,
                        target_provider=target_prov
                    )
                    positions_to_remove.append(idx + 1)
                except Exception as ex:
                    self.logger.exception("Failed to auto-download queued track %s: %s", track.name, ex)
                    has_failures = True

            # Remove successfully downloaded tracks from the playlist
            if positions_to_remove:
                self.logger.info("Removing downloaded tracks at positions %s from %s", positions_to_remove, playlist.name)
                # Use _handle_remove_playlist_tracks directly to process the removal synchronously
                # inside our lock context, so we prevent concurrent/duplicate events from processing
                # the same track again.
                try:
                    await self.mass.music.playlists._handle_remove_playlist_tracks(db_playlist_id, tuple(positions_to_remove))
                except Exception as ex:
                    self.logger.exception("Failed to remove downloaded tracks from playlist: %s", ex)

                # Trigger local files sync
                try:
                    target_prov = await self._find_default_target_provider("track")
                    await self.mass.music.start_sync(providers=[target_prov])
                except Exception:
                    pass

            # If there were failures (like providers still loading), retry in 15 seconds
            if has_failures:
                self.logger.info("Some tracks failed to download. Retrying queue processing in 15 seconds...")
                self.mass.call_later(15, self._process_queue, db_playlist_id)

    async def download(
        self,
        media_type: str,
        item_id: str,
        source_provider: str,
        target_provider_instance_id: str | None = None,
    ) -> dict[str, Any]:
        """
        API Endpoint to trigger download of tracks, albums, playlists, or artists.
        """
        self.logger.info(
            "Starting download task for %s %s from %s",
            media_type,
            item_id,
            source_provider,
        )

        try:
            # Resolve target local provider dynamically if not specified
            if not target_provider_instance_id:
                target_provider_instance_id = await self._find_default_target_provider(media_type)

            # Trigger specific media downloader
            if media_type == "track":
                await self._download_single_track(item_id, source_provider, target_provider_instance_id)
            elif media_type == "album":
                await self._download_album(item_id, source_provider, target_provider_instance_id)
            elif media_type == "playlist":
                await self._download_playlist(item_id, source_provider, target_provider_instance_id)
            elif media_type == "artist":
                await self._download_artist(item_id, source_provider, target_provider_instance_id)
            else:
                raise ValueError(f"Unsupported media_type: {media_type}")

            # Trigger rescan/sync on local file provider so newly added files are cataloged
            self.logger.info("Triggering rescan on local provider: %s", target_provider_instance_id)
            await self.mass.music.start_sync(providers=[target_provider_instance_id])

            return {"success": True, "message": "Download task completed successfully."}
        except Exception as e:
            self.logger.exception("Download task failed: %s", e)
            return {"success": False, "message": str(e)}

    async def _find_default_target_provider(self, media_type: str) -> str:
        """Finds the default target local filesystem provider dynamically."""
        self.logger.info("Looking for default target provider for media_type: %s", media_type)
        
        # Check plugin setting configuration first
        config_provider = self.config.get_value("default_target_provider")
        if config_provider:
            prov = self.mass.get_provider(config_provider)
            if prov and prov.available:
                self.logger.info("Selected configured default target provider: %s", config_provider)
                return config_provider

        for prov in self.mass.music.providers:
            if prov.domain == "filesystem_local" and prov.available:
                content_type = await self.mass.config.get_provider_config_value(prov.instance_id, "content_type")
                self.logger.info("Checked provider: %s (%s), content_type: %s", prov.name, prov.instance_id, content_type)

                if media_type == "audiobook" and content_type == "audiobooks":
                    self.logger.info("Selected audiobook provider: %s", prov.instance_id)
                    return prov.instance_id
                if media_type == "podcast" and content_type == "podcasts":
                    self.logger.info("Selected podcast provider: %s", prov.instance_id)
                    return prov.instance_id
                if content_type == "music":
                    self.logger.info("Selected music provider: %s", prov.instance_id)
                    return prov.instance_id

        # Fallback to any enabled local filesystem provider
        for prov in self.mass.music.providers:
            if prov.domain == "filesystem_local" and prov.available:
                self.logger.info("Fallback selected provider: %s", prov.instance_id)
                return prov.instance_id

        raise ValueError("No local filesystem provider found or enabled in Music Assistant")

    async def _get_provider_path(self, instance_id: str) -> str:
        """Get the base directory path configured for a local filesystem provider."""
        path = await self.mass.config.get_provider_config_value(instance_id, "path")
        if not path:
            raise ValueError(f"No path configured for local filesystem provider {instance_id}")
        return path

    async def _download_single_track(
        self,
        track_id: str,
        source_provider: str,
        target_provider: str,
        parent_type: str = "Songs",
        parent_name: str | None = None,
    ) -> None:
        """Downloads, transcodes, and tags a single track."""
        track = await self.mass.music.tracks.get(track_id, source_provider)
        if not track:
            raise MediaNotFoundError(f"Track {track_id} not found on provider {source_provider}")

        artist_name = "Unknown Artist"
        if track.artists:
            artist_name = track.artists[0].name

        album_name = "Unknown Album"
        track_number = 1
        if track.album:
            album_name = track.album.name
            if hasattr(track, "track_number") and track.track_number:
                track_number = track.track_number

        # Filesystem-safe name cleaning
        def clean_name(name: str) -> str:
            for char in '<>:"/\\|?*':
                name = name.replace(char, "_")
            return name.strip()

        c_artist = clean_name(artist_name)
        c_album = clean_name(album_name)
        c_title = clean_name(track.name)

        # Get source provider friendly name
        prov = self.mass.get_provider(source_provider)
        source_provider_name = prov.name if prov else source_provider
        c_source_provider = clean_name(source_provider_name)

        # Map parent type and resolve parent folder name (album/playlist/artist/song name)
        if parent_type == "Albums":
            c_parent_name = c_album
        elif parent_type == "Playlists" and parent_name:
            c_parent_name = clean_name(parent_name)
        elif parent_type == "Artists":
            c_parent_name = c_artist
        else:
            parent_type = "Songs"
            c_parent_name = c_title

        target_format = self.config.get_value("target_format", "mp3")
        file_ext = target_format.lower()
        if parent_type == "Albums":
            filename = f"{track_number:02d} - {c_title}.{file_ext}"
        else:
            filename = f"{c_title}.{file_ext}"

        # Resolve MD5 if needed
        import hashlib
        track_id_hash = hashlib.md5(f"{source_provider}-{track_id}".encode()).hexdigest()

        # Parse config path format
        path_format_template = self.config.get_value(
            "path_format",
            "Music Assistant/{source_provider}/{download_type}/{parent_name}/{filename}"
        )

        # Build relative path replacing placeholders
        rel_path = path_format_template.format(
            source_provider=c_source_provider,
            download_type=parent_type,
            parent_name=c_parent_name,
            filename=filename,
            artist=c_artist,
            album=c_album,
            title=c_title,
            track_number=f"{track_number:02d}",
            md5=track_id_hash
        )

        base_path = await self._get_provider_path(target_provider)
        dest_file = os.path.join(base_path, rel_path)
        dest_dir = os.path.dirname(dest_file)

        # Async directory creation
        await asyncio.to_thread(os.makedirs, dest_dir, exist_ok=True)

        if await asyncio.to_thread(os.path.exists, dest_file):
            self.logger.info("File already exists: %s, skipping download", dest_file)
            return

        # Fetch cover art bytes first (if available)
        cover_bytes = None
        if track.metadata.images:
            cover_url = track.metadata.images[0].path
            cover_bytes = await self._fetch_cover_bytes(cover_url)
        elif track.album and hasattr(track.album, "metadata") and track.album.metadata.images:
            cover_url = track.album.metadata.images[0].path
            cover_bytes = await self._fetch_cover_bytes(cover_url)

        self.logger.info("Downloading track stream: %s", track.name)

        # Get provider mapping for this track and source provider
        prov_mapping = next(
            (x for x in track.provider_mappings if x.provider_instance == source_provider),
            next(iter(track.provider_mappings))
        )
        prov = self.mass.get_provider(prov_mapping.provider_instance)
        stream_details = await prov.get_stream_details(prov_mapping.item_id, MediaType.TRACK)

        # Get the PCM stream
        pcm_format = AudioFormat(
            content_type=ContentType.PCM_S16LE,
            sample_rate=44100,
            bit_depth=16,
            channels=2,
        )
        audio_stream = self.mass.streams.audio.get_media_stream(stream_details, pcm_format)

        # Get output audio format details
        if target_format == "flac":
            out_content_type = ContentType.FLAC
            bit_depth = 24
        else:
            out_content_type = ContentType.MP3
            bit_depth = 16

        out_format = AudioFormat(
            content_type=out_content_type,
            sample_rate=44100,
            bit_depth=bit_depth,
            channels=2,
        )

        # Transcode stream via ffmpeg helper
        transcoded_stream = get_ffmpeg_stream(
            audio_input=audio_stream,
            input_format=pcm_format,
            output_format=out_format
        )

        # Fetch transcoded chunks in memory
        chunks = []
        async for chunk in transcoded_stream:
            chunks.append(chunk)

        # Write transcoded chunks to file inside the executor
        def save_file():
            with open(dest_file, "wb") as f:
                for chunk in chunks:
                    f.write(chunk)

        await asyncio.to_thread(save_file)

        # Resolve album and other metadata in parallel
        source_album = None
        if track.album and track.album.name and track.album.name != "Unknown Album":
            source_album = track.album.name

        resolved_album, resolved_year, resolved_publisher, resolved_copyright = await self._resolve_metadata(
            track, track.name, artist_name, dest_file
        )

        final_album = source_album or resolved_album
        if not final_album:
            if artist_name and artist_name != "Unknown Artist":
                final_album = artist_name
            else:
                final_album = f"{track.name} - Single"

        encoder_tag = f"ffmpeg {getattr(self, 'ffmpeg_version', 'unknown')}"

        # Retrieve lyrics from metadata
        lrc_lyrics = getattr(track.metadata, "lrc_lyrics", None)
        plain_lyrics = getattr(track.metadata, "lyrics", None)

        # Write external LRC file if configured
        save_lrc_file = self.config.get_value("save_lrc_file", True)
        if save_lrc_file and (lrc_lyrics or plain_lyrics):
            lrc_file_path = os.path.splitext(dest_file)[0] + ".lrc"
            self.logger.info("Saving external LRC file to: %s", lrc_file_path)
            def write_lrc():
                with open(lrc_file_path, "w", encoding="utf-8") as lf:
                    lf.write(lrc_lyrics or plain_lyrics)
            await asyncio.to_thread(write_lrc)

        # Write external SRT file if configured
        save_srt_file = self.config.get_value("save_srt_file", False)
        if save_srt_file and lrc_lyrics:
            srt_content = self._lrc_to_srt(lrc_lyrics)
            if srt_content:
                srt_file_path = os.path.splitext(dest_file)[0] + ".srt"
                self.logger.info("Saving external SRT file to: %s", srt_file_path)
                def write_srt():
                    with open(srt_file_path, "w", encoding="utf-8") as sf:
                        sf.write(srt_content)
                await asyncio.to_thread(write_srt)

        # Apply metadata tags
        self.logger.info("Applying metadata tags to: %s", dest_file)
        if target_format == "flac":
            await asyncio.to_thread(
                self._tag_flac,
                dest_file,
                track.name,
                artist_name,
                final_album,
                track_number,
                cover_bytes,
                resolved_year,
                resolved_publisher,
                resolved_copyright,
                encoder_tag,
                lrc_lyrics,
                plain_lyrics
            )
        else:
            await asyncio.to_thread(
                self._tag_mp3,
                dest_file,
                track.name,
                artist_name,
                final_album,
                track_number,
                cover_bytes,
                resolved_year,
                resolved_publisher,
                resolved_copyright,
                encoder_tag,
                lrc_lyrics,
                plain_lyrics
            )

        self.logger.info("Successfully downloaded and tagged: %s", filename)

        # Add to Downloads playlist after tagging is complete (non-blocking)
        self.mass.loop.create_task(
            self._add_track_to_downloads_playlist(dest_file, target_provider, track.name)
        )

    async def _download_album(
        self,
        album_id: str,
        source_provider: str,
        target_provider: str
    ) -> None:
        """Downloads all tracks belonging to an album."""
        album = await self.mass.music.albums.get(album_id, source_provider)
        tracks = await self.mass.music.albums.tracks(album_id, source_provider)
        self.logger.info("Found %d tracks in album %s", len(tracks), album.name)
        for track in tracks:
            await self._download_single_track(
                track.item_id,
                source_provider,
                target_provider,
                parent_type="Albums",
                parent_name=album.name
            )

    async def _download_playlist(
        self,
        playlist_id: str,
        source_provider: str,
        target_provider: str
    ) -> None:
        """Downloads all tracks in a playlist."""
        playlist = await self.mass.music.playlists.get(playlist_id, source_provider)
        tracks = await self.mass.music.playlists.tracks(playlist_id, source_provider)
        self.logger.info("Found %d tracks in playlist %s", len(tracks), playlist.name)
        for track in tracks:
            await self._download_single_track(
                track.item_id,
                source_provider,
                target_provider,
                parent_type="Playlists",
                parent_name=playlist.name
            )

    async def _download_artist(
        self,
        artist_id: str,
        source_provider: str,
        target_provider: str
    ) -> None:
        """Downloads all tracks belonging to an artist."""
        artist = await self.mass.music.artists.get(artist_id, source_provider)
        tracks = await self.mass.music.artists.tracks(artist_id, source_provider)
        self.logger.info("Found %d tracks for artist %s", len(tracks), artist.name)
        for track in tracks:
            await self._download_single_track(
                track.item_id,
                source_provider,
                target_provider,
                parent_type="Artists",
                parent_name=artist.name
            )

    async def _fetch_cover_bytes(self, url: str) -> bytes | None:
        """Fetch cover art bytes from an image URL."""
        try:
            async with self.mass.http_session.get(url) as response:
                if response.status == 200:
                    return await response.read()
        except Exception as e:
            self.logger.warning("Failed to fetch cover art from %s: %s", url, e)
        return None

    def _tag_mp3(
        self,
        filepath: str,
        title: str,
        artist: str,
        album: str,
        track_number: int,
        cover_bytes: bytes | None,
        year: str | None = None,
        publisher: str | None = None,
        copyright_text: str | None = None,
        encoder: str | None = None,
        lrc_lyrics: str | None = None,
        plain_lyrics: str | None = None
    ) -> None:
        """Writes ID3 tags to MP3 file using mutagen."""
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3, APIC, TALB, TPE1, TRCK, TIT2, TDRC, TPUB, TCOP, TENC, USLT, SYLT

        audio = MP3(filepath, ID3=ID3)
        try:
            audio.add_tags()
        except Exception:
            pass

        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TALB(encoding=3, text=album))
        audio.tags.add(TRCK(encoding=3, text=str(track_number)))

        if year:
            audio.tags.add(TDRC(encoding=3, text=year))
        if publisher:
            audio.tags.add(TPUB(encoding=3, text=publisher))
        if copyright_text:
            audio.tags.add(TCOP(encoding=3, text=copyright_text))
        if encoder:
            audio.tags.add(TENC(encoding=3, text=encoder))

        # Embed unsynchronized lyrics
        embed_unsynced = self.config.get_value("embed_unsynced_lyrics", True)
        if embed_unsynced:
            unsynced_text = None
            if plain_lyrics:
                unsynced_text = plain_lyrics
            elif lrc_lyrics:
                import re
                clean_text = re.sub(r'\[\d+:\d+(?:\.\d+)?\]|<[^>]+>', '', lrc_lyrics).strip()
                clean_lines = [line.strip() for line in clean_text.splitlines() if line.strip()]
                if clean_lines:
                    unsynced_text = "\n".join(clean_lines)
            
            if unsynced_text:
                audio.tags.add(
                    USLT(
                        encoding=3,
                        lang='eng',
                        desc='Lyrics',
                        text=unsynced_text
                    )
                )

        # Embed synchronized lyrics
        embed_synced = self.config.get_value("embed_synced_lyrics", True)
        if embed_synced and lrc_lyrics:
            import re
            lrc_regex = re.compile(r"^\[(\d+):(\d+(?:\.\d+)?)\](.*)$")
            sync_data = []
            for line in lrc_lyrics.splitlines():
                match = lrc_regex.match(line.strip())
                if match:
                    minutes = int(match.group(1))
                    seconds = float(match.group(2))
                    ms = int((minutes * 60 + seconds) * 1000)
                    text = match.group(3).strip()
                    sync_data.append((text, ms))
            if sync_data:
                audio.tags.add(
                    SYLT(
                        encoding=3,
                        lang='eng',
                        format=2,  # millisecond timestamps
                        type=1,    # lyrics text type
                        desc='Lyrics',
                        text=sync_data
                    )
                )

        if cover_bytes:
            audio.tags.add(
                APIC(
                    encoding=3,
                    mime="image/jpeg",
                    type=3,  # Cover Front
                    desc="Cover",
                    data=cover_bytes
                )
            )
        audio.save()

    def _tag_flac(
        self,
        filepath: str,
        title: str,
        artist: str,
        album: str,
        track_number: int,
        cover_bytes: bytes | None,
        year: str | None = None,
        publisher: str | None = None,
        copyright_text: str | None = None,
        encoder: str | None = None,
        lrc_lyrics: str | None = None,
        plain_lyrics: str | None = None
    ) -> None:
        """Writes Vorbis Comments to FLAC file using mutagen."""
        from mutagen.flac import FLAC, Picture

        audio = FLAC(filepath)
        audio["title"] = title
        audio["artist"] = artist
        audio["album"] = album
        audio["tracknumber"] = str(track_number)

        if year:
            audio["date"] = year
        if publisher:
            audio["publisher"] = publisher
            audio["organization"] = publisher
        if copyright_text:
            audio["copyright"] = copyright_text
        if encoder:
            audio["encoded-by"] = encoder
            audio["encoder"] = encoder

        # Embed unsynchronized lyrics
        embed_unsynced = self.config.get_value("embed_unsynced_lyrics", True)
        if embed_unsynced:
            if plain_lyrics:
                audio["unsyncedlyrics"] = plain_lyrics
            elif lrc_lyrics:
                import re
                clean_text = re.sub(r'\[\d+:\d+(?:\.\d+)?\]|<[^>]+>', '', lrc_lyrics).strip()
                clean_lines = [line.strip() for line in clean_text.splitlines() if line.strip()]
                if clean_lines:
                    audio["unsyncedlyrics"] = "\n".join(clean_lines)

        # Embed synchronized lyrics
        embed_synced = self.config.get_value("embed_synced_lyrics", True)
        if embed_synced and lrc_lyrics:
            audio["lyrics"] = lrc_lyrics

        if cover_bytes:
            pic = Picture()
            pic.data = cover_bytes
            pic.type = 3  # Cover Front
            pic.mime = "image/jpeg"
            audio.add_picture(pic)
        audio.save()

    @staticmethod
    def _lrc_to_srt(lrc_content: str) -> str:
        """Convert standard LRC string with [mm:ss.xx] timestamps to SubRip (.srt) format."""
        import re
        lrc_regex = re.compile(r"^\[(\d+):(\d+(?:\.\d+)?)\](.*)$")
        events = []
        
        for line in lrc_content.splitlines():
            match = lrc_regex.match(line.strip())
            if match:
                minutes = int(match.group(1))
                seconds = float(match.group(2))
                ms = int((minutes * 60 + seconds) * 1000)
                text = match.group(3).strip()
                events.append({"start_ms": ms, "text": text})
                
        if not events:
            return ""
            
        events.sort(key=lambda x: x["start_ms"])
        
        for i in range(len(events) - 1):
            events[i]["end_ms"] = events[i+1]["start_ms"]
        events[-1]["end_ms"] = events[-1]["start_ms"] + 4000
        
        def format_srt_time(ms_val: int) -> str:
            hours = ms_val // 3600000
            minutes = (ms_val % 3600000) // 60000
            seconds = (ms_val % 60000) // 1000
            millis = ms_val % 1000
            return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"
            
        srt_lines = []
        for idx, ev in enumerate(events, 1):
            srt_lines.append(str(idx))
            srt_lines.append(f"{format_srt_time(ev['start_ms'])} --> {format_srt_time(ev['end_ms'])}")
            srt_lines.append(ev["text"])
            srt_lines.append("")
            
        return "\n".join(srt_lines)

    async def _resolve_ffmpeg_version(self) -> str:
        """Resolve the version of installed ffmpeg."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            first_line = stdout.decode("utf-8", errors="replace").splitlines()[0]
            if "version" in first_line:
                parts = first_line.split("version ")
                if len(parts) > 1:
                    return parts[1].split(" ")[0]
        except Exception as e:
            self.logger.warning("Failed to resolve ffmpeg version: %s", e)
        return "unknown"

    async def _resolve_metadata(
        self, track: Any, title: str, artist: str, filepath: str
    ) -> tuple[str | None, str | None, str | None, str | None]:
        """Resolves metadata from AcoustID, MusicBrainz, and MASS Search in parallel."""
        # Step 1: Get fingerprint from the file
        duration, fingerprint = await self._get_chromaprint(filepath)

        # Step 2: Query in parallel
        tasks = []
        if fingerprint and duration:
            tasks.append(self._query_acoustid(duration, fingerprint))
        else:
            tasks.append(asyncio.sleep(0, result=None))

        tasks.append(self._query_musicbrainz(title, artist))
        tasks.append(self._query_mass_internal(title, artist))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        acoustid_res = results[0] if not isinstance(results[0], Exception) else None
        musicbrainz_res = results[1] if not isinstance(results[1], Exception) else None
        mass_res = results[2] if not isinstance(results[2], Exception) else None

        album = None
        year = None
        publisher = None
        copyright_text = None

        # Step 3: Check if there is a YouTube / YouTube Music mapping to query its upload year
        yt_year = None
        if hasattr(track, "provider_mappings"):
            for pm in track.provider_mappings:
                if pm.provider_domain in ("ytmusic", "youtube"):
                    yt_year = await self._query_youtube_year(pm.item_id)
                    if yt_year:
                        break

        # Merge strategy: AcoustID -> MusicBrainz -> MASS Internal
        if yt_year:
            year = yt_year
        if acoustid_res and acoustid_res.get("status") == "ok":
            for res in acoustid_res.get("results", []):
                for rec in res.get("recordings", []):
                    for rel in rec.get("releases", []):
                        if not album and rel.get("title"):
                            album = rel.get("title")
                        if not year and rel.get("date"):
                            date_val = rel.get("date")
                            if isinstance(date_val, dict):
                                year = str(date_val.get("year"))
                            elif isinstance(date_val, str):
                                year = date_val.split("-")[0]
                        if not publisher and rel.get("label"):
                            publisher = rel.get("label")

        if musicbrainz_res and musicbrainz_res.get("recordings"):
            for rec in musicbrainz_res.get("recordings", []):
                for release in rec.get("releases", []):
                    if not album and release.get("title"):
                        album = release.get("title")
                    if not year and release.get("date"):
                        date_val = release.get("date")
                        if isinstance(date_val, str):
                            year = date_val.split("-")[0]
                    if not publisher and release.get("label-info"):
                        for label_info in release.get("label-info", []):
                            label = label_info.get("label")
                            if label and label.get("name"):
                                publisher = label.get("name")
                                break

        if mass_res:
            if not album and mass_res.get("album"):
                album = mass_res.get("album")
            if not year and mass_res.get("year"):
                year = str(mass_res.get("year"))

        if album == "Unknown Album":
            album = None

        return album, year, publisher, copyright_text

    async def _get_chromaprint(self, filepath: str) -> tuple[int, str]:
        """Generate audio chromaprint fingerprint using fpcalc."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "fpcalc", filepath,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            duration = 0
            fingerprint = ""
            for line in stdout.decode("utf-8", errors="replace").splitlines():
                if line.startswith("DURATION="):
                    duration = int(line.split("=")[1])
                elif line.startswith("FINGERPRINT="):
                    fingerprint = line.split("=")[1]
            return duration, fingerprint
        except Exception as e:
            self.logger.warning("fpcalc execution failed: %s", e)
            return 0, ""

    async def _query_acoustid(self, duration: int, fingerprint: str) -> dict | None:
        """Query AcoustID API."""
        url = f"https://api.acoustid.org/v2/lookup?client=8XaBELgH&duration={duration}&fingerprint={fingerprint}&meta=recordings+releases"
        try:
            async with self.mass.http_session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
        except Exception as e:
            self.logger.warning("AcoustID lookup failed: %s", e)
        return None

    async def _query_musicbrainz(self, title: str, artist: str) -> dict | None:
        """Query MusicBrainz API."""
        headers = {"User-Agent": "MusicAssistantDownloader/1.0 ( mailto:support@musicassistant.io )"}
        query = urllib.parse.quote(f'recording:"{title}" AND artist:"{artist}"')
        url = f"https://musicbrainz.org/ws/2/recording?query={query}&fmt=json"
        try:
            async with self.mass.http_session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
        except Exception as e:
            self.logger.warning("MusicBrainz lookup failed: %s", e)
        return None

    async def _query_mass_internal(self, title: str, artist: str) -> dict | None:
        """Query internal MASS music library search."""
        try:
            from music_assistant_models.enums import MediaType
            query = f"{artist} - {title}"
            results = await self.mass.music.search(query, [MediaType.TRACK], limit=5)
            if results and results.tracks:
                for track in results.tracks:
                    if track.album:
                        return {
                            "album": track.album.name,
                            "year": track.album.year,
                            "mbid": track.mbid,
                        }
        except Exception as e:
            self.logger.warning("MASS internal search failed: %s", e)
        return None

    async def _query_youtube_year(self, video_id: str) -> str | None:
        """Fetch YouTube watch page and extract the upload year."""
        url = f"https://www.youtube.com/watch?v={video_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        try:
            async with self.mass.http_session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    import re
                    # Look for datePublished or uploadDate or publishDate in HTML
                    match = re.search(r'itemprop="datePublished"\s+content="([^"]+)"', html)
                    if not match:
                        match = re.search(r'"uploadDate"\s*:\s*"([^"]+)"', html)
                    if not match:
                        match = re.search(r'"publishDate"\s*:\s*"([^"]+)"', html)
                    
                    if match:
                        date_str = match.group(1)
                        if len(date_str) >= 4 and date_str[:4].isdigit():
                            self.logger.info("Resolved YouTube year: %s", date_str[:4])
                            return date_str[:4]
        except Exception as e:
            self.logger.warning("Failed to extract YouTube year for %s: %s", video_id, e)
        return None
