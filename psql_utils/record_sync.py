from time import time
from typing import Optional, Any, Callable, SupportsInt, cast

from .sync import (select_one_only, select_one, select, count as pg_count,
                   update, insert, delete)
from ._record_shared import (
    normalize_get_inputs,
    normalize_save_inputs,
    prepare_get_props,
)
from .errors import RecordNotFoundError, UniqueConflictError
from .types import TableName, c, cs
from .record_utils import (popup_data, EmptyRows, get_uniq_data, prepare_count,
                           prepare_get_list, prepare_save, prepare_get_by_id)


def get(
    table: TableName,
    *,
    id: Optional[int] = None,
    uniq_keys: Optional[list[str]] = None,
    optional_keys: Optional[list[str]] = None,
    required_uniq_keys: bool = True,
    ignore_extra_keys: bool = False,
    fields: Optional[list[str]] = None,
    popup: bool = False,
    **data: Any,
) -> Optional[dict[str, object]]:
    """
    Retrieves a single record synchronously by ID or unique keys.
    """
    uniq_keys, optional_keys, fields = normalize_get_inputs(
        uniq_keys,
        optional_keys,
        fields,
    )
    prepared = prepare_get_props(
        id=id,
        uniq_keys=uniq_keys,
        optional_keys=optional_keys,
        required_uniq_keys=required_uniq_keys,
        ignore_extra_keys=ignore_extra_keys,
        fields=fields,
        data=data,
    )
    if prepared is None:
        return None
    get_max_id, props = prepared

    # Special case: if fetching max(id), resolve it to a specific ID first
    if get_max_id:
        id_val = select_one_only(table, **props)
        if not id_val:
            return None
        props = prepare_get_by_id(
            id=cast(int, id_val),
            fields=fields,
            ignore_extra_keys=ignore_extra_keys,
            **data,
        )

    ret = cast(Optional[dict[str, object]], select_one(table, **props))

    if popup:
        return cast(Optional[dict[str, object]], popup_data(ret))

    return ret


def save(
    table: TableName,
    *,
    id: Optional[int] = None,
    keys: Optional[list[str]] = None,
    uniq_keys: Optional[list[str]] = None,
    optional_keys: Optional[list[str]] = None,
    json_keys: Optional[list[str]] = None,
    sub_json_keys: Optional[list[str]] = None,
    replace_keys: Optional[list[str]] = None,
    exclude_data_keys: Optional[list[str]] = None,
    on_saved: Optional[Callable[[Optional[dict[str, object]], int],
                                Any]] = None,
    **data: Any,
) -> int:
    """
    Inserts or updates a record synchronously.
    - If 'id' is provided, attempts an update.
    - If not, uses unique keys to determine if it should insert or update.
    """
    (
        keys,
        uniq_keys,
        optional_keys,
        json_keys,
        sub_json_keys,
        replace_keys,
        exclude_data_keys,
    ) = normalize_save_inputs(
        keys,
        uniq_keys,
        optional_keys,
        json_keys,
        sub_json_keys,
        replace_keys,
        exclude_data_keys,
    )

    # Determine existing record for Update vs Insert logic
    if id is not None:
        old = get(table, id=id)
        if not old:
            raise RecordNotFoundError(
                f'Update failed: record [{id}] does not exist')
    else:
        _, uniq_data = get_uniq_data(uniq_keys=uniq_keys, **data)
        old = get(
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
        # Check for unique key conflicts on update
        uniq_changed, uniq_full_data = get_uniq_data(
            uniq_keys=uniq_keys,
            old_record=old,
            **data,
        )

        if uniq_changed:
            existing_conflict = get(
                table,
                uniq_keys=uniq_keys,
                optional_keys=optional_keys,
                required_uniq_keys=True,
                ignore_extra_keys=True,
                **uniq_full_data,
            )
            if existing_conflict:
                oid = cast(int, existing_conflict['id'])
                raise UniqueConflictError(
                    f'Cannot update: unique value conflicts with record {oid}')

        # Optimization: Return early if no data changed
        if len(args) == 0:
            return cast(int, old['id'])

        args.append(old['id'])
        update(table, cs(rkeys), 'id=%s', tuple(args))

        if on_saved:
            on_saved(old, cast(int, old['id']))
        return cast(int, old['id'])

    else:
        # Insert path
        if 'created_at' in keys and data.get('created_at') is None:
            rkeys.append('created_at')
            args.append(int(time()))

        nid = cast(int, insert(table, cs(rkeys), tuple(args), c('id')))
        if on_saved:
            on_saved(None, nid)

        return nid


def remove(
    table: TableName,
    *args: Any,
    on_removed: Optional[Callable[[dict[str, object]], Any]] = None,
    **kwargs: Any,
) -> bool:
    """
    Removes a record.
    Fetches it first to ensure existence and to pass to the callback.
    """
    fields = ['*'] if on_removed else ['id']

    old = get(table, *args, fields=fields, ignore_extra_keys=True, **kwargs)

    if old:
        delete(table, 'id=%s', (old['id'], ))
        if on_removed:
            on_removed(old)
        return True

    return False


def count(table: TableName, *args: Any, **kwargs: Any) -> int:
    """Returns the count of records matching criteria."""
    props = prepare_count(*args, **kwargs)
    return int(cast(SupportsInt, pg_count(table, **props)))


def get_list(
    table: TableName,
    *args: Any,
    offset: Optional[int] = None,
    size: Optional[int] = None,
    popup: bool = False,
    **kwargs: Any,
) -> list[dict[str, object]]:
    """
    Retrieves a list of records.
    Returns an empty list if criteria results in an EmptyRows exception.
    """
    try:
        props = prepare_get_list(*args, **kwargs)
    except EmptyRows:
        return []

    ret = cast(
        list[dict[str, object]],
        select(table, offset=offset, size=size, **props),
    )

    if popup:
        return [cast(dict[str, object], popup_data(v)) for v in ret]

    return ret
