import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Хранит настройки backend и инфраструктурных подключений."""

    storage_backend: str
    local_lake_dir: Path
    hdfs_namenode_url: str
    hdfs_root: str
    max_upload_size: int
    cors_origins: tuple[str, ...]

    @classmethod
    def from_environment(cls) -> "Settings":
        """Создаёт настройки из переменных окружения.

        Returns:
            Settings: Нормализованная конфигурация приложения.
        """
        backend_root = Path(__file__).resolve().parent.parent
        origins = os.getenv(
            "CORS_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,http://127.0.0.1:3000",
        )
        return cls(
            storage_backend=os.getenv("STORAGE_BACKEND", "local").strip().lower(),
            local_lake_dir=Path(os.getenv("LOCAL_LAKE_DIR", str(backend_root / "data" / "lake"))),
            hdfs_namenode_url=os.getenv("HDFS_NAMENODE_URL", "http://localhost:9870").rstrip("/"),
            hdfs_root=os.getenv("HDFS_ROOT", "/ekover").rstrip("/"),
            max_upload_size=int(os.getenv("MAX_UPLOAD_SIZE", str(25 * 1024 * 1024))),
            cors_origins=tuple(origin.strip() for origin in origins.split(",") if origin.strip()),
        )


settings = Settings.from_environment()
