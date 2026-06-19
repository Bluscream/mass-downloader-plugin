# Music Assistant Downloader Plugin

A custom plugin for [Music Assistant](https://github.com/music-assistant/server) that allows downloading media items (tracks, albums, artists, and playlists) from cloud/streaming providers (e.g. YouTube Music, Spotify) to your local filesystem directories.

## Features

- **Automated Download Queue Playlist:** A special playlist (defaults to `Download Queue`) is monitored. Simply add any track to it, and it will be downloaded automatically, tagged, and removed from the queue playlist.
- **Customizable Path Templates:** Organize your files exactly how you want. Define folder patterns using placeholders such as `{source_provider}`, `{download_type}`, `{parent_name}`, `{artist}`, `{album}`, `{title}`, `{track_number}`, and `{md5}`.
- **Metadata Tagging:** Automatically embeds high-quality tags (Title, Artist, Album, Track Number) and Cover Art into the downloaded files using `mutagen`.
- **Selectable Formats:** Choose between **MP3** (16-bit) and **FLAC** (24-bit) for the downloaded audio format.
- **Selectable Local Target:** Choose which local filesystem provider to copy downloaded files to via settings.

---

## Installation

Run the following command in a shell that has access to the host's Docker daemon (e.g., via the Advanced SSH & Web Terminal add-on with Protection Mode turned off):

```bash
curl -fsSL https://raw.githubusercontent.com/Bluscream/mass-downloader-plugin/main/scripts/install_provider.sh | sh
```

### Options
You can customize the installation by running the script with options:
```bash
sh install_provider.sh --force --ma-id <container_name> --python-version <python_version>
```

### Persistent Installation (Docker / Unraid)

To prevent custom providers from being wiped when the Music Assistant Docker container is updated or restarted, you can use a startup hook script:

1. Create a `custom_providers` directory in your persistent appdata volume (e.g., `/mnt/user/appdata/music-assistant/custom_providers/`).
2. Place the provider folder (`downloader`) inside that directory:
   `/mnt/user/appdata/music-assistant/custom_providers/downloader`
3. Create an entrypoint hook script at `/mnt/user/appdata/music-assistant/entrypoint_hook.sh` with the following content:

```bash
#!/bin/sh

# Find site-packages directory
PROVIDERS_DIR=$(find /app/venv/lib/ -name "providers" -path "*/music_assistant/providers" | head -n 1)

if [ -n "${PROVIDERS_DIR}" ]; then
    # Copy custom providers from /data/custom_providers/
    if [ -d "/data/custom_providers" ]; then
        for provider in /data/custom_providers/*; do
            if [ -d "$provider" ]; then
                name=$(basename "$provider")
                rm -rf "${PROVIDERS_DIR}/${name}"
                cp -R "$provider" "${PROVIDERS_DIR}/${name}"
            fi
        done
    fi

    # Install dependencies if simplyrics is present
    if [ -d "${PROVIDERS_DIR}/simplyrics" ]; then
        /app/venv/bin/uv pip install ytmusicapi
    fi
fi

# Run the original entrypoint logic
for path in /usr/lib/*/libjemalloc.so.2; do
    [ -f "$path" ] && export LD_PRELOAD="$path" MALLOC_CONF="background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000" && break
done
exec mass "$@"
```

4. Make the script executable:
   ```bash
   chmod +x /mnt/user/appdata/music-assistant/entrypoint_hook.sh
   ```
5. Map this hook script in your Docker/Unraid container volume config:
   - **Host Path**: `/mnt/user/appdata/music-assistant/entrypoint_hook.sh`
   - **Container Path**: `/usr/local/bin/entrypoint.sh`
   - **Mode**: `Read/Write` (or `Read Only`)


---

## Configuration Settings

Once installed and activated in **Music Assistant -> Settings -> Integration / Plugins -> Add Downloader**:

1. **Default Target Provider:** Choose the local filesystem provider where files should be written (e.g., your local music library folder).
2. **Target Audio Format:** Choose `mp3` or `flac`.
3. **Download Path Format:** Customize how folders and files are named. The default is:
   ```text
   Music Assistant/{source_provider}/{download_type}/{parent_name}/{filename}
   ```
   *Available Placeholders:*
   - `{source_provider}`: E.g., YouTube Music
   - `{download_type}`: `Songs`, `Albums`, `Playlists`, or `Artists`
   - `{parent_name}`: Folder name corresponding to the context (e.g., album name, playlist name, artist name)
   - `{filename}`: File name containing track number (if album) and song name
   - `{artist}`: First artist name
   - `{album}`: Album name
   - `{title}`: Song name
   - `{track_number}`: Two-digit track number (e.g., `03`)
   - `{md5}`: MD5 hash of the track ID

4. **Download Queue Playlist Name:** The name of the playlist to monitor for automatic downloading. Default is `Download Queue`.

---

## How It Works

### 1. The Download Queue (Recommended)
1. Add tracks from any streaming provider to the `Download Queue` playlist.
2. The plugin listens for playlist additions, streams the audio via Music Assistant's transcoding engine, writes it to the configured local directory, tags the file, and removes the track from the playlist.
3. Finally, it triggers a rescan on the local filesystem provider so it immediately appears in your local Music Assistant library.

### 2. Custom API Calls
You can also trigger downloads programmatically via the Music Assistant WebSocket API using the `plugin/downloader/download` command.

---

## License

MIT License. See [LICENSE](file:///LICENSE) for details.
