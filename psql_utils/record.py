from . import select_one_only, select_one, select, count as pg_count, \
    update, insert, delete, c, cs
from . import TableName
from time import time
import asyncio
from typing import Optional, List, Any, Callable
from .record_utils import popup_data, EmptyRows, get_uniq_data, \
    prepare_count, prepare_get_list, prepare_save, \
    prepare_get_by_uniq, prepare_get_by_id


async def get(
    table: TableName,
    *,
    id: Optional[int] = None,
    uniq_keys: List[str] = [],
    optional_keys: List[str] = [],
    required_uniq_keys: bool = True,
    ignore_extra_keys: bool = False,
    fields: List[str] = ['*'],
    popup: bool = False,
    **data: Any,
) -> Any:

    if id:
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

        if get_max_id:
            id = await select_one_only(table, **props)
            if not id:
                return None

            props = prepare_get_by_id(
                id=id,
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
    keys: List[str] = [],
    uniq_keys: List[str] = [],
    optional_keys: List[str] = [],
    json_keys: List[str] = [],
    sub_json_keys: List[str] = [],
    replace_keys: List[str] = [],
    exclude_data_keys: List[str] = [],
    on_saved: Optional[Callable[[Any, int], Any]] = None,
    **data: Any,
) -> Any:

    if id:
        old = await get(table, id=id)
        if not old:
            raise Exception(f'update record[{id}], the record is not exists')
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
        uniq_changed, uniq_full_data = get_uniq_data(
            uniq_keys=uniq_keys,
            old_record=old,
            **data,
        )

        if uniq_changed:
            old1 = await get(
                table,
                uniq_keys=uniq_keys,
                optional_keys=optional_keys,
                required_uniq_keys=True,
                ignore_extra_keys=True,
                **uniq_full_data,
            )
            if old1:
                oid = old1['id']
                err = f'cant update record uniq value to exists value {oid}'
                raise Exception(err)

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

    try:
        props = prepare_get_list(*args, **kwargs)
    except EmptyRows:
        return []

    ret = await select(table, offset=offset, size=size, **props)

    if popup:
        return [popup_data(v) for v in ret]

    return ret
