"""
Downloader Plugin Provider for Music Assistant.
Allows downloading tracks, albums, artists, and playlists from cloud providers to local filesystems.
"""

from __future__ import annotations

import asyncio
import logging
import os
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

        # Create Download Queue playlist automatically if it doesn't exist
        self.mass.loop.create_task(self._create_queue_playlist_if_missing())

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
                    self.logger.info("Download queue playlist '%s' already exists.", playlist.name)
                    return
            
            # Create a new playlist on the builtin provider
            self.logger.info("Creating download queue playlist: %s", queue_name)
            await self.mass.music.playlists.create_playlist(
                name=queue_name,
                provider_instance_or_domain="builtin"
            )
        except Exception as e:
            self.logger.warning("Failed to auto-create download queue playlist: %s", e)

    async def _on_mass_event(self, event: Any) -> None:
        """Handle library events to detect additions to the Download Queue playlist."""
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

        # Acquire lock so we process one update at a time
        async with self._download_lock:
            # Fetch the tracks in the playlist
            tracks = []
            async for track in self.mass.music.playlists.tracks(str(db_playlist_id), "library"):
                tracks.append(track)

            if not tracks:
                return

            self.logger.info("Found %d tracks in download queue playlist '%s'", len(tracks), playlist.name)
            
            # Download all tracks currently in the queue
            positions_to_remove = []
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
                    positions_to_remove.append(idx)
                except Exception as ex:
                    self.logger.exception("Failed to auto-download queued track %s: %s", track.name, ex)

            # Remove successfully downloaded tracks from the playlist
            if positions_to_remove:
                self.logger.info("Removing downloaded tracks at positions %s from %s", positions_to_remove, playlist.name)
                # Music Assistant expects a tuple of positions to remove
                await self.mass.music.playlists.remove_playlist_tracks(db_playlist_id, tuple(positions_to_remove))

                # Trigger local files sync
                try:
                    target_prov = await self._find_default_target_provider("track")
                    self.mass.music.start_sync(providers=[target_prov])
                except Exception:
                    pass

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
            self.mass.music.start_sync(providers=[target_provider_instance_id])

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

        # Apply metadata tags
        self.logger.info("Applying metadata tags to: %s", dest_file)
        if target_format == "flac":
            await asyncio.to_thread(
                self._tag_flac,
                dest_file,
                track.name,
                artist_name,
                album_name,
                track_number,
                cover_bytes
            )
        else:
            await asyncio.to_thread(
                self._tag_mp3,
                dest_file,
                track.name,
                artist_name,
                album_name,
                track_number,
                cover_bytes
            )

        self.logger.info("Successfully downloaded and tagged: %s", filename)

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
        cover_bytes: bytes | None
    ) -> None:
        """Writes ID3 tags to MP3 file using mutagen."""
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3, APIC, TALB, TPE1, TRCK, TIT2

        audio = MP3(filepath, ID3=ID3)
        try:
            audio.add_tags()
        except Exception:
            pass

        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TALB(encoding=3, text=album))
        audio.tags.add(TRCK(encoding=3, text=str(track_number)))

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
        cover_bytes: bytes | None
    ) -> None:
        """Writes Vorbis Comments to FLAC file using mutagen."""
        from mutagen.flac import FLAC, Picture

        audio = FLAC(filepath)
        audio["title"] = title
        audio["artist"] = artist
        audio["album"] = album
        audio["tracknumber"] = str(track_number)

        if cover_bytes:
            pic = Picture()
            pic.data = cover_bytes
            pic.type = 3  # Cover Front
            pic.mime = "image/jpeg"
            audio.add_picture(pic)
        audio.save()
