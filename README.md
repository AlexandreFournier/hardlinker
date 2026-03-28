# Hardlinker

A self-hosted Docker application that finds and hardlinks duplicate large files across mounted volumes to save disk space.

## Features

- **Automatic duplicate detection** using a 3-stage pipeline: size grouping, partial hash, full hash (xxhash for speed)
- **Atomic hardlinking** with safe rename-link-unlink pattern (no data loss on crash)
- **Existing hardlink detection** — discovers already-hardlinked files and tracks cumulative space savings
- **Periodic scheduling** via cron expression
- **Web dashboard** with real-time progress (SSE), run history, and statistics
- **Force run** from the UI at any time
- **Docker health check** built into the image
- **Hash caching** for faster incremental scans

## Quick Start

```yaml
# docker-compose.yml
services:
  hardlinker:
    image: ghcr.io/alexandrefournier/hardlinker:latest
    restart: unless-stopped
    ports:
      - "8000:8000"
    environment:
      - HARDLINKER_SCAN_DIRS=/data
      - HARDLINKER_MIN_SIZE=67108864
      - HARDLINKER_SCHEDULE=0 3 * * *
    volumes:
      - /path/to/your/storage:/data:rw
      - hardlinker_db:/app/data

volumes:
  hardlinker_db:
```

```bash
docker compose up -d
```

Open `http://localhost:8000` to access the dashboard.

## Configuration

All settings are configured via environment variables:

| Variable | Default | Description |
|---|---|---|
| `HARDLINKER_BASEURL` | `http://localhost:8000` | Base URL for the web UI |
| `HARDLINKER_SCAN_DIRS` | `/data` | Comma-separated directories to scan |
| `HARDLINKER_MIN_SIZE` | `67108864` | Minimum file size in bytes (default: 64 MiB) |
| `HARDLINKER_SCHEDULE` | `0 3 * * *` | Cron expression (default: daily at 3 AM) |
| `HARDLINKER_DB_PATH` | `/app/data/hardlinker.db` | SQLite database path |

### Important: Volume Mounting

Hardlinks can only be created between files on the **same filesystem**. Mount a single parent directory that contains all the subdirectories you want to scan:

```yaml
volumes:
  # Good: single mount, all subdirs on the same filesystem
  - /mnt/storage:/data:rw

  # Then set:
  # HARDLINKER_SCAN_DIRS=/data/media,/data/backups,/data/downloads
```

If directories are on different physical disks, hardlinks between them are impossible. The application handles this gracefully by grouping files by device — deduplication only occurs within a single filesystem.

## How It Works

### Duplicate Detection (3-stage pipeline)

1. **Size grouping**: Files with different sizes cannot be duplicates — group by `(device, size)` and discard unique sizes
2. **Partial hash**: Read the beginning, middle, and end of each candidate (64 KB each) and hash with xxh3_128 — eliminates most false positives cheaply
3. **Full hash**: Compute full file hash for remaining candidates — confirms true duplicates

### Safe Hardlinking

For each duplicate group, one file is chosen as the "source" (the one with the most existing links). Each duplicate is replaced atomically:

1. `rename(target, target.hardlinker.bak)` — move original to backup
2. `link(source, target)` — create hardlink
3. `unlink(backup)` — remove backup

If step 2 fails, the backup is restored. Leftover `.hardlinker.bak` files from crashes are cleaned up on the next run.

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Web dashboard |
| `GET` | `/api/health` | Health check (for Docker) |
| `POST` | `/api/run` | Trigger a manual run |
| `POST` | `/api/cancel` | Cancel a running scan |
| `GET` | `/api/progress` | SSE stream for real-time progress |
| `GET` | `/api/history` | Recent runs as JSON |
| `GET` | `/api/stats` | Aggregate statistics |
| `GET` | `/api/settings` | Current configuration |

## Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Run locally
uvicorn app.main:app --reload

# Lint
ruff check .
ruff format --check .
```

## Disclaimer

This tool was entirely vibe-coded. Its author cannot guarantee against data loss. Use at your own risk. Always maintain proper backups of your files before running any deduplication tool. Every machine is a smoke machine if you operate it wrong enough.

## License

MIT
