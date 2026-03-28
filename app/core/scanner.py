import logging
import os
import threading
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

import xxhash

logger = logging.getLogger(__name__)


class ScanCancelledError(Exception):
    pass


@dataclass
class FileInfo:
    path: str
    size: int
    device: int
    inode: int
    mtime: float


@dataclass
class DuplicateGroup:
    hash: str
    size: int
    device: int
    files: list[FileInfo] = field(default_factory=list)


@dataclass
class ExistingHardlinkGroup:
    """Files that already share the same inode (already hardlinked to each other)."""

    inode: int
    device: int
    size: int
    paths: list[str] = field(default_factory=list)

    @property
    def space_saved(self) -> int:
        """Space saved = size * (count - 1), since only one copy occupies disk."""
        return self.size * (len(self.paths) - 1) if len(self.paths) > 1 else 0


@dataclass
class ScanResult:
    duplicate_groups: list[DuplicateGroup]
    existing_hardlinks: list[ExistingHardlinkGroup]
    total_files_scanned: int


def _check_cancel(cancel_event: threading.Event | None):
    if cancel_event and cancel_event.is_set():
        raise ScanCancelledError("Scan cancelled by user")


def _hash_partial(path: str, file_size: int, partial_size: int) -> str | None:
    """Hash beginning + middle + end of the file, combined with file size."""
    try:
        h = xxhash.xxh3_128()
        h.update(file_size.to_bytes(8, "little"))
        with open(path, "rb") as f:
            # Beginning
            h.update(f.read(partial_size))
            # Middle
            mid = max(0, (file_size // 2) - (partial_size // 2))
            f.seek(mid)
            h.update(f.read(partial_size))
            # End
            end = max(0, file_size - partial_size)
            f.seek(end)
            h.update(f.read(partial_size))
        return h.hexdigest()
    except OSError as e:
        logger.warning("Failed to read %s for partial hash: %s", path, e)
        return None


def _hash_full(
    path: str,
    file_size: int,
    chunk_size: int,
    file_progress_callback: Callable[[str, int, int], None] | None = None,
) -> str | None:
    """Stream full file through xxh3_128 for definitive duplicate verification."""
    try:
        h = xxhash.xxh3_128()
        bytes_read = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
                bytes_read += len(chunk)
                if file_progress_callback:
                    file_progress_callback(path, bytes_read, file_size)
        return h.hexdigest()
    except OSError as e:
        logger.warning("Failed to read %s for full hash: %s", path, e)
        return None


def _get_cache_key(fi: FileInfo) -> str:
    return f"{fi.path}:{fi.mtime}:{fi.size}"


def scan_for_duplicates(
    scan_dirs: list[str],
    min_size: int,
    partial_hash_size: int,
    hash_chunk_size: int,
    progress_callback: Callable[[str, int, int], None] | None = None,
    file_progress_callback: Callable[[str, int, int], None] | None = None,
    cancel_event: threading.Event | None = None,
    hash_cache: dict[str, str] | None = None,
) -> ScanResult:
    """
    Find duplicate files across scan_dirs using a 3-stage pipeline:
    1. Group by (device, size)
    2. Partial hash (beginning + middle + end of file) to filter unlikely matches
    3. Full hash to verify duplicates before hardlinking

    Also detects existing hardlinks (files already sharing the same inode).

    progress_callback(message, current, total) is called to report progress.
    cancel_event can be set to abort the scan.
    hash_cache maps "path:mtime:size" -> full hex digest for skipping re-hashing.
    """
    if hash_cache is None:
        hash_cache = {}

    def report(msg: str, current: int = 0, total: int = 0):
        if progress_callback:
            progress_callback(msg, current, total)

    # Stage 1: Walk and collect files
    all_files: list[FileInfo] = []
    for scan_dir in scan_dirs:
        if not os.path.isdir(scan_dir):
            logger.warning("Scan directory does not exist: %s", scan_dir)
            continue
        for dirpath, _dirnames, filenames in os.walk(scan_dir, followlinks=False):
            _check_cancel(cancel_event)
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    st = os.lstat(filepath)
                except OSError:
                    continue
                # Skip non-regular files and symlinks
                if not (st.st_mode & 0o100000):  # S_ISREG
                    continue
                if st.st_size < min_size:
                    continue
                all_files.append(
                    FileInfo(
                        path=filepath,
                        size=st.st_size,
                        device=st.st_dev,
                        inode=st.st_ino,
                        mtime=st.st_mtime,
                    )
                )
            if len(all_files) % 1000 == 0:
                report(f"Scanning: {len(all_files)} files found")

    report(f"Scan complete: {len(all_files)} files found", len(all_files), len(all_files))
    logger.info("Found %d files >= %d bytes", len(all_files), min_size)

    if not all_files:
        return ScanResult(duplicate_groups=[], existing_hardlinks=[], total_files_scanned=0)

    # Detect existing hardlinks: files sharing the same (device, inode) with nlink > 1
    inode_groups: dict[tuple[int, int], list[FileInfo]] = defaultdict(list)
    for fi in all_files:
        inode_groups[(fi.device, fi.inode)].append(fi)

    existing_hardlinks: list[ExistingHardlinkGroup] = []
    for (device, inode), files in inode_groups.items():
        if len(files) >= 2:
            existing_hardlinks.append(
                ExistingHardlinkGroup(
                    inode=inode,
                    device=device,
                    size=files[0].size,
                    paths=[f.path for f in files],
                )
            )

    if existing_hardlinks:
        total_existing_saved = sum(g.space_saved for g in existing_hardlinks)
        logger.info(
            "Found %d existing hardlink groups saving %d bytes",
            len(existing_hardlinks),
            total_existing_saved,
        )
        report(f"Found {len(existing_hardlinks)} existing hardlink groups")

    # Stage 2: Group by (device, size)
    device_size_groups: dict[tuple[int, int], list[FileInfo]] = defaultdict(list)
    for fi in all_files:
        device_size_groups[(fi.device, fi.size)].append(fi)

    # Filter: keep only groups with 2+ unique inodes
    candidate_groups: list[list[FileInfo]] = []
    for group in device_size_groups.values():
        unique_inodes = {fi.inode for fi in group}
        if len(unique_inodes) >= 2:
            candidate_groups.append(group)

    total_candidates = sum(len(g) for g in candidate_groups)
    report(
        f"Size grouping: {len(candidate_groups)} groups, {total_candidates} files",
        0,
        len(candidate_groups),
    )
    logger.info(
        "%d size groups with potential duplicates (%d files)",
        len(candidate_groups),
        total_candidates,
    )

    if not candidate_groups:
        return ScanResult(
            duplicate_groups=[],
            existing_hardlinks=existing_hardlinks,
            total_files_scanned=len(all_files),
        )

    # Stage 3: Partial hash (beginning + middle + end) to filter non-duplicates
    partial_hash_groups: dict[tuple[int, int, str], list[FileInfo]] = defaultdict(list)
    for i, group in enumerate(candidate_groups):
        _check_cancel(cancel_event)
        report(f"Partial hashing: group {i + 1}/{len(candidate_groups)}", i + 1, len(candidate_groups))
        for fi in group:
            h = _hash_partial(fi.path, fi.size, partial_hash_size)
            if h is not None:
                partial_hash_groups[(fi.device, fi.size, h)].append(fi)

    # Filter: keep only groups with 2+ unique inodes
    partial_candidates: list[list[FileInfo]] = []
    for group in partial_hash_groups.values():
        unique_inodes = {fi.inode for fi in group}
        if len(unique_inodes) >= 2:
            partial_candidates.append(group)

    total_partial = sum(len(g) for g in partial_candidates)
    logger.info(
        "After partial hash: %d groups, %d files",
        len(partial_candidates),
        total_partial,
    )

    if not partial_candidates:
        return ScanResult(
            duplicate_groups=[],
            existing_hardlinks=existing_hardlinks,
            total_files_scanned=len(all_files),
        )

    # Stage 4: Full hash to verify duplicates before hardlinking
    full_hash_groups: dict[tuple[int, str], list[FileInfo]] = defaultdict(list)
    files_to_hash = [fi for group in partial_candidates for fi in group]
    for i, fi in enumerate(files_to_hash):
        _check_cancel(cancel_event)
        report(f"Full hashing: {i + 1}/{len(files_to_hash)} files", i + 1, len(files_to_hash))

        cache_key = _get_cache_key(fi)
        h = hash_cache.get(cache_key)
        if h is None:
            h = _hash_full(fi.path, fi.size, hash_chunk_size, file_progress_callback)
            if h is not None:
                hash_cache[cache_key] = h

        if h is not None:
            full_hash_groups[(fi.device, h)].append(fi)

    # Build final duplicate groups
    results: list[DuplicateGroup] = []
    for (device, hash_val), group in full_hash_groups.items():
        unique_inodes = {fi.inode for fi in group}
        if len(unique_inodes) >= 2:
            results.append(
                DuplicateGroup(
                    hash=hash_val,
                    size=group[0].size,
                    device=device,
                    files=group,
                )
            )

    total_dupes = sum(len(g.files) for g in results)
    report(
        f"Found {len(results)} duplicate groups ({total_dupes} files)",
        len(results),
        len(results),
    )
    logger.info("Found %d duplicate groups (%d files total)", len(results), total_dupes)

    return ScanResult(
        duplicate_groups=results,
        existing_hardlinks=existing_hardlinks,
        total_files_scanned=len(all_files),
    )
