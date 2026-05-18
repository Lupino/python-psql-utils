from typing import Any, Callable, Protocol, TypeVar

from .types import TableName

TCrud = TypeVar("TCrud")


class CrudLike(Protocol):

    def save(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def get(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def get_list(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def count(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def remove(self, *args: Any, **kwargs: Any) -> Any:
        ...


class CrudConfigMixin:
    """Shared CRUD config holder for sync/async wrappers."""

    table: TableName
    keys: list[str]
    uniq_keys: list[str]
    json_keys: list[str]
    save_kwargs: dict[str, Any]
    get_kwargs: dict[str, Any]
    query_kwargs: dict[str, Any]

    def __init__(
        self,
        table: TableName,
        *,
        keys: list[str] | None = None,
        uniq_keys: list[str] | None = None,
        json_keys: list[str] | None = None,
        save_kwargs: dict[str, Any] | None = None,
        get_kwargs: dict[str, Any] | None = None,
        query_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.table = table
        self.keys = keys or []
        self.uniq_keys = uniq_keys or []
        self.json_keys = json_keys or []
        self.save_kwargs = save_kwargs or {}
        self.get_kwargs = get_kwargs or {}
        self.query_kwargs = query_kwargs or {}


def build_crud_instance(
    crud_cls: Callable[..., TCrud],
    table: TableName,
    *,
    keys: list[str] | None = None,
    uniq_keys: list[str] | None = None,
    json_keys: list[str] | None = None,
    save_kwargs: dict[str, Any] | None = None,
    get_kwargs: dict[str, Any] | None = None,
    query_kwargs: dict[str, Any] | None = None,
) -> TCrud:
    """Create a CRUD wrapper instance with shared kwargs plumbing."""
    return crud_cls(
        table,
        keys=keys,
        uniq_keys=uniq_keys,
        json_keys=json_keys,
        save_kwargs=save_kwargs,
        get_kwargs=get_kwargs,
        query_kwargs=query_kwargs,
    )


TCrudLike = TypeVar("TCrudLike", bound=CrudLike)


def build_crud_exports_tuple(
    crud: TCrudLike,
) -> tuple[
        TCrudLike,
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
]:
    """Return common bound methods tuple for CRUD wrappers."""
    return crud, crud.save, crud.get, crud.get_list, crud.count, crud.remove
