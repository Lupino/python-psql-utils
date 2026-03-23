import asyncio
from time import time
from typing import Optional, List, Any, Callable

from . import (select_one_only, select_one, select, count as pg_count, update,
               insert, delete)
from .types import TableName, c, cs
from .record_utils import (popup_data, EmptyRows, get_uniq_data, prepare_count,
                           prepare_get_list, prepare_save, prepare_get_by_uniq,
                           prepare_get_by_id)


async def get(
    table: TableName,
    *,
    id: Optional[int] = None,
    uniq_keys: Optional[List[str]] = None,
    optional_keys: Optional[List[str]] = None,
    required_uniq_keys: bool = True,
    ignore_extra_keys: bool = False,
    fields: Optional[List[str]] = None,
    popup: bool = False,
    **data: Any,
) -> Any:
    """
    Retrieves a single record from the database by ID or unique keys.
    """
    uniq_keys = uniq_keys or []
    optional_keys = optional_keys or []
    fields = fields or ['*']

    if id is not None:
        props = prepare_get_by_id(
            id=id,
            fields=fields,
            ignore_extra_keys=ignore_extra_keys,
            **data,
        )
    else:
        try:
            get_max_id, props = prepare_get_by_uniq(
                uniq_keys=uniq_keys,
                optional_keys=optional_keys,
                required_uniq_keys=required_uniq_keys,
                ignore_extra_keys=ignore_extra_keys,
                fields=fields,
                **data,
            )
        except EmptyRows:
            return None

        # Special case: if getting max(id), fetch it first to get the full row
        if get_max_id:
            id_val = await select_one_only(table, **props)
            if not id_val:
                return None

            props = prepare_get_by_id(
                id=id_val,
                fields=fields,
                ignore_extra_keys=ignore_extra_keys,
                **data,
            )

    ret = await select_one(table, **props)

    if popup:
        return popup_data(ret)

    return ret


async def save(
    table: TableName,
    *,
    id: Optional[int] = None,
    keys: Optional[List[str]] = None,
    uniq_keys: Optional[List[str]] = None,
    optional_keys: Optional[List[str]] = None,
    json_keys: Optional[List[str]] = None,
    sub_json_keys: Optional[List[str]] = None,
    replace_keys: Optional[List[str]] = None,
    exclude_data_keys: Optional[List[str]] = None,
    on_saved: Optional[Callable[[Any, int], Any]] = None,
    **data: Any,
) -> Any:
    """
    Inserts or updates a record.
    If 'id' is provided, it attempts to update.
    Otherwise, it checks unique keys to decide between insert or update.
    """
    keys = keys or []
    uniq_keys = uniq_keys or []
    optional_keys = optional_keys or []
    json_keys = json_keys or []
    sub_json_keys = sub_json_keys or []
    replace_keys = replace_keys or []
    exclude_data_keys = exclude_data_keys or []

    # Determine if we are updating an existing record
    if id is not None:
        old = await get(table, id=id)
        if not old:
            raise Exception(f'Update failed: record [{id}] does not exist')
    else:
        _, uniq_data = get_uniq_data(uniq_keys=uniq_keys, **data)
        old = await get(
            table,
            uniq_keys=uniq_keys,
            optional_keys=optional_keys,
            required_uniq_keys=True,
            ignore_extra_keys=True,
            **uniq_data,
        )

    # Prepare SQL columns and arguments
    rkeys, args = prepare_save(
        keys=keys,
        uniq_keys=uniq_keys,
        exclude_data_keys=exclude_data_keys,
        json_keys=json_keys,
        replace_keys=replace_keys,
        sub_json_keys=sub_json_keys,
        old_record=old,
        **data,
    )

    if old:
        # Check if updating unique keys causes a conflict with another record
        uniq_changed, uniq_full_data = get_uniq_data(
            uniq_keys=uniq_keys,
            old_record=old,
            **data,
        )

        if uniq_changed:
            existing_conflict = await get(
                table,
                uniq_keys=uniq_keys,
                optional_keys=optional_keys,
                required_uniq_keys=True,
                ignore_extra_keys=True,
                **uniq_full_data,
            )
            if existing_conflict:
                oid = existing_conflict['id']
                raise Exception(
                    f'Cannot update: unique value conflicts with record {oid}')

        # Optimization: If no fields changed, return existing ID immediately
        if len(args) == 0:
            return old['id']

        args.append(old['id'])
        await update(table, cs(rkeys), 'id=%s', tuple(args))

        if on_saved:
            ret = on_saved(old, old['id'])
            if asyncio.iscoroutine(ret):
                await ret
        return old['id']

    else:
        # Insert new record
        if 'created_at' in keys and data.get('created_at') is None:
            rkeys.append('created_at')
            args.append(int(time()))

        nid = await insert(table, cs(rkeys), tuple(args), c('id'))

        if on_saved:
            ret = on_saved(None, nid)
            if asyncio.iscoroutine(ret):
                await ret

        return nid


async def remove(
    table: TableName,
    *args: Any,
    on_removed: Optional[Callable[[Any], Any]] = None,
    **kwargs: Any,
) -> bool:
    """
    Removes a record.
    Fetches the record first to pass it to the on_removed callback.
    """
    fields = ['*'] if on_removed else ['id']

    old = await get(
        table,
        *args,
        fields=fields,
        ignore_extra_keys=True,
        **kwargs,
    )

    if old:
        await delete(table, 'id=%s', (old['id'], ))

        if on_removed:
            ret = on_removed(old)
            if asyncio.iscoroutine(ret):
                await ret

        return True
    return False


async def count(table: TableName, *args: Any, **kwargs: Any) -> int:
    """Returns the count of records matching the criteria."""
    props = prepare_count(*args, **kwargs)
    return int(await pg_count(table, **props))


async def get_list(
    table: TableName,
    *args: Any,
    offset: Optional[int] = None,
    size: Optional[int] = None,
    popup: bool = False,
    **kwargs: Any,
) -> Any:
    """
    Retrieves a list of records matching the criteria.
    Handles EmptyRows exceptions gracefully by returning an empty list.
    """
    try:
        props = prepare_get_list(*args, **kwargs)
    except EmptyRows:
        return []

    ret = await select(table, offset=offset, size=size, **props)

    if popup:
        return [popup_data(v) for v in ret]

    return ret
