from typing import Any, Callable, Dict, List, TypeVar

from .types import TableName

TCrud = TypeVar("TCrud")


class CrudConfigMixin:
    """Shared CRUD config holder for sync/async wrappers."""

    table: TableName
    keys: List[str]
    uniq_keys: List[str]
    json_keys: List[str]
    save_kwargs: Dict[str, Any]
    get_kwargs: Dict[str, Any]
    query_kwargs: Dict[str, Any]

    def __init__(
        self,
        table: TableName,
        *,
        keys: List[str] | None = None,
        uniq_keys: List[str] | None = None,
        json_keys: List[str] | None = None,
        save_kwargs: Dict[str, Any] | None = None,
        get_kwargs: Dict[str, Any] | None = None,
        query_kwargs: Dict[str, Any] | None = None,
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
    keys: List[str] | None = None,
    uniq_keys: List[str] | None = None,
    json_keys: List[str] | None = None,
    save_kwargs: Dict[str, Any] | None = None,
    get_kwargs: Dict[str, Any] | None = None,
    query_kwargs: Dict[str, Any] | None = None,
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


def build_crud_exports_tuple(crud: Any) -> tuple[Any, Any, Any, Any, Any, Any]:
    """Return common bound methods tuple for CRUD wrappers."""
    return crud, crud.save, crud.get, crud.get_list, crud.count, crud.remove

