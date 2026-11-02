from pathlib import Path
import uuid

from backend.infrastructure.storage import StorageBackend


async def store_uploaded_dataset(
    content: bytes,
    original_filename: str,
    storage: StorageBackend,
) -> dict[str, str]:
    """Сохраняет загруженный датасет в слой raw Data Lake.

    Args:
        content: Бинарное содержимое CSV-файла.
        original_filename: Имя файла, переданное пользователем.
        storage: Выбранная реализация объектного хранилища.

    Returns:
        dict[str, str]: Имя объекта, ключ Data Lake и URI хранилища.

    Raises:
        ValueError: Если исходное имя не содержит допустимого имени файла.
        OSError: Если локальное хранилище не смогло записать объект.
        Exception: Если инфраструктурное хранилище отклонило запись.
    """
    safe_filename = Path(original_filename).name
    if not safe_filename or safe_filename in {".", ".."}:
        raise ValueError("Некорректное имя файла.")
    stored_filename = f"{uuid.uuid4().hex[:12]}_{safe_filename}"
    object_key = f"raw/{stored_filename}"
    uri = await storage.put(object_key, content)
    return {
        "stored_filename": stored_filename,
        "object_key": object_key,
        "storage_uri": uri,
    }
