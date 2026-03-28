from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    hardlinker_baseurl: str = "http://localhost:8000"
    hardlinker_scan_dirs: str = "/data"
    hardlinker_min_size: int = 67108864  # 64MB
    hardlinker_schedule: str = "0 3 * * *"
    hardlinker_db_path: str = "/app/data/hardlinker.db"
    hardlinker_hash_chunk_size: int = 1048576  # 1MB
    hardlinker_partial_hash_size: int = 65536  # 64KB

    @property
    def scan_dirs_list(self) -> list[str]:
        return [d.strip() for d in self.hardlinker_scan_dirs.split(",") if d.strip()]
