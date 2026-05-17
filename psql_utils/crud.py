from typing import Any, Dict, List, Optional

from . import record
from .types import TableName


class CRUD:
    def __init__(
        self,
        table: TableName,
        *,
        keys: Optional[List[str]] = None,
        uniq_keys: Optional[List[str]] = None,
        json_keys: Optional[List[str]] = None,
        save_kwargs: Optional[Dict[str, Any]] = None,
        get_kwargs: Optional[Dict[str, Any]] = None,
        query_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.table = table
        self.keys = keys or []
        self.uniq_keys = uniq_keys or []
        self.json_keys = json_keys or []
        self.save_kwargs = save_kwargs or {}
        self.get_kwargs = get_kwargs or {}
        self.query_kwargs = query_kwargs or {}

    async def save(self, *args: Any, **kwargs: Any) -> Any:
        return await record.save(
            self.table,
            *args,
            keys=self.keys,
            uniq_keys=self.uniq_keys,
            json_keys=self.json_keys,
            **self.save_kwargs,
            **kwargs,
        )

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return await record.get(
            self.table,
            *args,
            uniq_keys=self.uniq_keys,
            **self.get_kwargs,
            **kwargs,
        )

    async def get_list(self, *args: Any, **kwargs: Any) -> Any:
        return await record.get_list(
            self.table,
            *args,
            json_keys=self.json_keys,
            **self.query_kwargs,
            **kwargs,
        )

    async def count(self, *args: Any, **kwargs: Any) -> Any:
        return await record.count(
            self.table,
            *args,
            json_keys=self.json_keys,
            **self.query_kwargs,
            **kwargs,
        )

    async def remove(self, *args: Any, **kwargs: Any) -> Any:
        return await record.remove(
            self.table,
            *args,
            uniq_keys=self.uniq_keys,
            **self.get_kwargs,
            **kwargs,
        )


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
    return CRUD(
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
) -> tuple[CRUD, Any, Any, Any, Any, Any]:
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
    return crud, crud.save, crud.get, crud.get_list, crud.count, crud.remove
