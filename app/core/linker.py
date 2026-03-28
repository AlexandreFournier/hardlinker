import logging
import os
from collections.abc import Callable
from dataclasses import dataclass

from app.core.scanner import DuplicateGroup

logger = logging.getLogger(__name__)

BACKUP_SUFFIX = ".hardlinker.bak"


@dataclass
class LinkResult:
    source_path: str
    linked_path: str
    file_size: int
    hash: str
    device: int
    success: bool
    error: str | None = None


def cleanup_stale_backups(scan_dirs: list[str]):
    """Find and recover leftover .hardlinker.bak files from previous crashes."""
    for scan_dir in scan_dirs:
        if not os.path.isdir(scan_dir):
            continue
        for dirpath, _dirnames, filenames in os.walk(scan_dir, followlinks=False):
            for filename in filenames:
                if not filename.endswith(BACKUP_SUFFIX):
                    continue
                backup_path = os.path.join(dirpath, filename)
                original_path = backup_path[: -len(BACKUP_SUFFIX)]
                try:
                    if os.path.exists(original_path):
                        # Both exist: the link succeeded but cleanup didn't. Remove backup.
                        os.unlink(backup_path)
                        logger.info("Cleaned up stale backup: %s", backup_path)
                    else:
                        # Original missing: restore from backup
                        os.rename(backup_path, original_path)
                        logger.info("Restored from stale backup: %s -> %s", backup_path, original_path)
                except OSError as e:
                    logger.error("Failed to clean up backup %s: %s", backup_path, e)


def hardlink_group(
    group: DuplicateGroup,
    progress_callback: Callable[[str], None] | None = None,
) -> list[LinkResult]:
    """
    Hardlink all duplicate files in a group to a single source file.
    Uses atomic rename-link-unlink pattern for safety.
    """
    results: list[LinkResult] = []

    if len(group.files) < 2:
        return results

    # Choose source: highest existing link count, then earliest mtime
    source = max(
        group.files,
        key=lambda fi: (
            _get_nlink(fi.path),
            -fi.mtime,
        ),
    )

    targets = [fi for fi in group.files if fi.inode != source.inode]

    for target in targets:
        result = _hardlink_one(source, target, group.hash, group.device)
        results.append(result)
        if progress_callback:
            status = "linked" if result.success else f"failed: {result.error}"
            progress_callback(f"{target.path} -> {source.path} ({status})")

    return results


def _get_nlink(path: str) -> int:
    try:
        return os.lstat(path).st_nlink
    except OSError:
        return 0


def _hardlink_one(source, target, hash_val: str, device: int) -> LinkResult:
    """Atomically replace target with a hardlink to source."""
    backup_path = target.path + BACKUP_SUFFIX

    try:
        # Pre-flight checks
        try:
            src_stat = os.lstat(source.path)
            tgt_stat = os.lstat(target.path)
        except FileNotFoundError as e:
            return LinkResult(
                source_path=source.path,
                linked_path=target.path,
                file_size=target.size,
                hash=hash_val,
                device=device,
                success=False,
                error=f"File disappeared: {e}",
            )

        if src_stat.st_dev != tgt_stat.st_dev:
            return LinkResult(
                source_path=source.path,
                linked_path=target.path,
                file_size=target.size,
                hash=hash_val,
                device=device,
                success=False,
                error="Files on different devices",
            )

        if src_stat.st_ino == tgt_stat.st_ino:
            return LinkResult(
                source_path=source.path,
                linked_path=target.path,
                file_size=target.size,
                hash=hash_val,
                device=device,
                success=False,
                error="Already hardlinked (same inode)",
            )

        if src_stat.st_size != tgt_stat.st_size:
            return LinkResult(
                source_path=source.path,
                linked_path=target.path,
                file_size=target.size,
                hash=hash_val,
                device=device,
                success=False,
                error="File size changed since scan",
            )

        # Atomic replacement: rename target -> backup, link source -> target, unlink backup
        os.rename(target.path, backup_path)
        try:
            os.link(source.path, target.path)
        except OSError:
            # Restore from backup on failure
            os.rename(backup_path, target.path)
            raise
        os.unlink(backup_path)

        logger.info("Hardlinked: %s -> %s (saved %d bytes)", target.path, source.path, target.size)
        return LinkResult(
            source_path=source.path,
            linked_path=target.path,
            file_size=target.size,
            hash=hash_val,
            device=device,
            success=True,
        )

    except OSError as e:
        logger.error("Failed to hardlink %s -> %s: %s", target.path, source.path, e)
        return LinkResult(
            source_path=source.path,
            linked_path=target.path,
            file_size=target.size,
            hash=hash_val,
            device=device,
            success=False,
            error=str(e),
        )
