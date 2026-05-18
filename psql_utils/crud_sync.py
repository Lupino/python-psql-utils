from typing import Any, Callable, Dict, List, Optional

from . import record_sync
from ._crud_base import (
    CrudConfigMixin,
    build_crud_exports_tuple,
    build_crud_instance,
)
from .types import TableName


class CRUD(CrudConfigMixin):

    def save(self, *args: Any, **kwargs: Any) -> Any:
        return record_sync.save(
            self.table,
            *args,
            keys=self.keys,
            uniq_keys=self.uniq_keys,
            json_keys=self.json_keys,
            **self.save_kwargs,
            **kwargs,
        )

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return record_sync.get(
            self.table,
            *args,
            uniq_keys=self.uniq_keys,
            **self.get_kwargs,
            **kwargs,
        )

    def get_list(self, *args: Any, **kwargs: Any) -> Any:
        return record_sync.get_list(
            self.table,
            *args,
            json_keys=self.json_keys,
            **self.query_kwargs,
            **kwargs,
        )

    def count(self, *args: Any, **kwargs: Any) -> Any:
        return record_sync.count(
            self.table,
            *args,
            json_keys=self.json_keys,
            **self.query_kwargs,
            **kwargs,
        )

    def remove(self, *args: Any, **kwargs: Any) -> Any:
        return record_sync.remove(
            self.table,
            *args,
            uniq_keys=self.uniq_keys,
            **self.get_kwargs,
            **kwargs,
        )


CrudMethod = Callable[..., Any]
CrudExports = tuple[
    CRUD, CrudMethod, CrudMethod, CrudMethod, CrudMethod, CrudMethod
]


def build_crud(
    table: TableName,
    *,
    keys: Optional[List[str]] = None,
    uniq_keys: Optional[List[str]] = None,
    json_keys: Optional[List[str]] = None,
    save_kwargs: Optional[Dict[str, Any]] = None,
    get_kwargs: Optional[Dict[str, Any]] = None,
    query_kwargs: Optional[Dict[str, Any]] = None,
) -> CRUD:
    """Build a CRUD helper object for db modules."""
    return build_crud_instance(
        CRUD,
        table,
        keys=keys,
        uniq_keys=uniq_keys,
        json_keys=json_keys,
        save_kwargs=save_kwargs,
        get_kwargs=get_kwargs,
        query_kwargs=query_kwargs,
    )


def build_crud_exports(
    table: TableName,
    *,
    keys: Optional[List[str]] = None,
    uniq_keys: Optional[List[str]] = None,
    json_keys: Optional[List[str]] = None,
    save_kwargs: Optional[Dict[str, Any]] = None,
    get_kwargs: Optional[Dict[str, Any]] = None,
    query_kwargs: Optional[Dict[str, Any]] = None,
) -> CrudExports:
    """Build CRUD and return common bound methods as a tuple."""
    crud = build_crud(
        table,
        keys=keys,
        uniq_keys=uniq_keys,
        json_keys=json_keys,
        save_kwargs=save_kwargs,
        get_kwargs=get_kwargs,
        query_kwargs=query_kwargs,
    )
    return build_crud_exports_tuple(crud)
