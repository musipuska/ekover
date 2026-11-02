from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import quote

import httpx

from backend.config import Settings


class StorageBackend(Protocol):
    """Определяет контракт хранилища для файловых объектов."""

    async def put(self, object_key: str, content: bytes) -> str:
        """Сохраняет объект и возвращает его логический URI.

        Args:
            object_key: Относительный ключ объекта внутри хранилища.
            content: Бинарное содержимое сохраняемого объекта.

        Returns:
            str: URI сохранённого объекта.

        Raises:
            ValueError: Если ключ объекта содержит недопустимый путь.
            OSError: Если локальная запись завершилась ошибкой.
        """
        ...

    async def healthcheck(self) -> bool:
        """Проверяет доступность хранилища.

        Returns:
            bool: True, если хранилище доступно, иначе False.
        """
        ...


def normalize_object_key(object_key: str) -> str:
    """Проверяет и нормализует относительный ключ объекта.

    Args:
        object_key: Исходный относительный путь объекта.

    Returns:
        str: Безопасный POSIX-путь без начального слеша.

    Raises:
        ValueError: Если путь пустой или содержит traversal-сегменты.
    """
    path = PurePosixPath(object_key)
    if not object_key or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("Недопустимый ключ объекта.")
    return path.as_posix()


class LocalStorageBackend:
    """Реализует локальное файловое хранилище Data Lake."""

    def __init__(self, root: Path) -> None:
        """Инициализирует локальное хранилище.

        Args:
            root: Корневая директория Data Lake.

        Returns:
            None: Экземпляр готов к сохранению файлов.
        """
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    async def put(self, object_key: str, content: bytes) -> str:
        """Сохраняет объект в локальном Data Lake.

        Args:
            object_key: Относительный ключ объекта.
            content: Бинарное содержимое объекта.

        Returns:
            str: URI локального объекта.

        Raises:
            ValueError: Если ключ объекта содержит недопустимый путь.
            OSError: Если файл невозможно записать.
        """
        normalized_key = normalize_object_key(object_key)
        destination = self.root.joinpath(*PurePosixPath(normalized_key).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        return f"file://{destination.resolve()}"

    async def healthcheck(self) -> bool:
        """Проверяет доступность корневой директории Data Lake.

        Returns:
            bool: True, если директория существует и доступна для записи.
        """
        return self.root.exists() and self.root.is_dir()


class HDFSStorageBackend:
    """Реализует сохранение файлов через Hadoop WebHDFS REST API."""

    def __init__(self, namenode_url: str, hdfs_root: str) -> None:
        """Инициализирует WebHDFS-клиент.

        Args:
            namenode_url: HTTP-адрес NameNode с включённым WebHDFS.
            hdfs_root: Корневая директория проекта в HDFS.

        Returns:
            None: Экземпляр готов к выполнению WebHDFS-запросов.
        """
        self.namenode_url = namenode_url.rstrip("/")
        self.hdfs_root = "/" + hdfs_root.strip("/")

    def build_hdfs_path(self, object_key: str) -> str:
        """Формирует абсолютный путь объекта в HDFS.

        Args:
            object_key: Относительный ключ объекта.

        Returns:
            str: Абсолютный путь в HDFS.
        """
        normalized_key = normalize_object_key(object_key)
        return f"{self.hdfs_root}/{normalized_key}"

    async def put(self, object_key: str, content: bytes) -> str:
        """Создаёт директории и записывает объект через WebHDFS.

        Args:
            object_key: Относительный ключ объекта.
            content: Бинарное содержимое объекта.

        Returns:
            str: URI сохранённого объекта в HDFS.

        Raises:
            ValueError: Если ключ объекта содержит недопустимый путь.
            httpx.HTTPError: Если NameNode или DataNode вернул ошибку.
        """
        hdfs_path = self.build_hdfs_path(object_key)
        encoded_path = quote(hdfs_path, safe="/")
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            mkdir_response = await client.put(
                f"{self.namenode_url}/webhdfs/v1{quote(self.hdfs_root, safe='/')}",
                params={"op": "MKDIRS"},
            )
            mkdir_response.raise_for_status()
            parent_path = str(PurePosixPath(hdfs_path).parent)
            parent_response = await client.put(
                f"{self.namenode_url}/webhdfs/v1{quote(parent_path, safe='/')}",
                params={"op": "MKDIRS"},
            )
            parent_response.raise_for_status()
            create_response = await client.put(
                f"{self.namenode_url}/webhdfs/v1{encoded_path}",
                params={"op": "CREATE", "overwrite": "false"},
                content=content,
                headers={"Content-Type": "application/octet-stream"},
            )
            create_response.raise_for_status()
        return f"hdfs://{self.hdfs_root}/{normalize_object_key(object_key)}"

    async def healthcheck(self) -> bool:
        """Проверяет доступность NameNode через WebHDFS.

        Returns:
            bool: True, если NameNode отвечает успешным HTTP-статусом.
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self.namenode_url}/webhdfs/v1/",
                    params={"op": "GETFILESTATUS"},
                )
                return response.is_success
        except httpx.HTTPError:
            return False


def build_storage_backend(settings: Settings) -> StorageBackend:
    """Создаёт реализацию хранилища согласно настройкам приложения.

    Args:
        settings: Конфигурация backend и инфраструктуры.

    Returns:
        StorageBackend: Локальное хранилище или WebHDFS-адаптер.

    Raises:
        ValueError: Если указан неизвестный тип хранилища.
    """
    if settings.storage_backend == "local":
        return LocalStorageBackend(settings.local_lake_dir)
    if settings.storage_backend == "hdfs":
        return HDFSStorageBackend(settings.hdfs_namenode_url, settings.hdfs_root)
    raise ValueError(f"Неизвестный STORAGE_BACKEND: {settings.storage_backend}")
