from . import select_one_only, select_one, select, count as pg_count, \
    update, insert, delete, c, cs
from . import TableName
import json
from time import time
import re
import asyncio
from typing import Optional, List, Dict, Any, Callable

op_map = {
    'gt': '>',
    'lt': '<',
    'lte': '<=',
    'gte': '>=',
    'neq': '!=',
    'like': ' like ',
    'unlike': ' not like ',
    'in': ' in ',
    'match': ' ~* ',
    'unmatch': ' !~* ',
    'similar': ' similar to ',
    'unsimilar': ' not similar to ',
}

re_op = re.compile('_(' + '|'.join(op_map.keys()) + ')$')
re_num = re.compile(r'^\d+(.\d+)?$')


def merge_json(new: Any, old: Any) -> Any:
    if isinstance(new, dict) and isinstance(old, dict):
        old.update(new)
        return old

    return new


def merge_sub_json(data0: Any,
                   data1: Any,
                   replace_keys: List[str] = []) -> Any:
    for k, v in data1.items():
        if k not in replace_keys:
            new = data0.get(k)
            if new is None:
                data0[k] = v
            else:
                data0[k] = merge_json(data0[k], v)

    return data0


def append_extra(
    part_sql: List[str],
    args: List[Any],
    data: Dict[str, Any],
) -> None:
    for key, val in data.items():
        if val is None:
            continue
        part_sql.append(f'{key}=%s')
        args.append(val)


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

    part_sql = []
    args = []
    if id:
        part_sql.append('id=%s')
        args.append(id)
    else:
        if len(uniq_keys) == 0:
            if required_uniq_keys:
                if ignore_extra_keys:
                    return None
                else:
                    raise Exception('uniq_keys is required')

        get_max_id = False
        for key in uniq_keys:
            val = data.pop(key, None)
            if val is None:
                get_max_id = True
                if required_uniq_keys:
                    if key in optional_keys:
                        continue
                    raise Exception(f'{key} is required')

            part_sql.append(f'{key}=%s')
            args.append(val)

        if get_max_id:
            if not ignore_extra_keys:
                append_extra(part_sql, args, data)

            part_sql_s = ' AND '.join(part_sql)
            id = await select_one_only(table, c('max(id)'), part_sql_s,
                                       tuple(args))
            if not id:
                return None

            part_sql = ['id=%s']
            args = [id]

    if not ignore_extra_keys:
        append_extra(part_sql, args, data)

    part_sql_s = ' AND '.join(part_sql)
    ret = await select_one(table, cs(fields), part_sql_s, tuple(args))

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

    if len(exclude_data_keys) > 0:
        data = make_data(data.copy(), exclude_data_keys)

    uniq_data: Dict[str, Any] = {}

    for key in uniq_keys:
        uniq_data[key] = data.get(key)

    if id:
        old = await get(table, id=id)
        if not old:
            raise Exception(f'update record[{id}], the record is not exists')
    else:
        old = await get(table,
                        uniq_keys=uniq_keys,
                        optional_keys=optional_keys,
                        required_uniq_keys=True,
                        ignore_extra_keys=True,
                        **uniq_data)

    rkeys = []
    args = []

    for key in keys:
        val = data.get(key)
        if val is not None:
            if old:
                if old[key] == val:
                    continue

            rkeys.append(key)
            args.append(val)

    for key in json_keys:
        val = data.get(key)
        if val is not None:
            if old and key not in replace_keys:
                val = merge_json(val, old[key])
            rkeys.append(key)
            args.append(json.dumps(val))

    for key in sub_json_keys:
        val = data.get(key)
        if val is not None:
            if old:
                val = merge_sub_json(val, old[key], replace_keys)
            rkeys.append(key)
            args.append(json.dumps(val))

    if 'updated_at' in keys and data.get('updated_at') is None:
        rkeys.append('updated_at')
        args.append(int(time()))

    if old:
        uniq_changed = False
        if id:
            for key in uniq_keys:
                val = data.get(key)
                if val is not None:
                    if old[key] == val:
                        continue

                    rkeys.append(key)
                    args.append(val)
                    uniq_changed = True

        if uniq_changed:
            old1 = await get(table,
                             uniq_keys=uniq_keys,
                             optional_keys=optional_keys,
                             required_uniq_keys=True,
                             ignore_extra_keys=True,
                             **uniq_data)
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
        for key in uniq_keys:
            val = data.get(key)
            if val is not None:
                rkeys.append(key)
                args.append(val)

        if 'created_at' in keys and data.get('created_at') is None:
            rkeys.append('created_at')
            args.append(int(time()))

        nid = await insert(table, cs(rkeys), tuple(args), c('id'))
        if on_saved:
            ret = on_saved(None, nid)
            if asyncio.iscoroutine(ret):
                await ret

        return nid


async def remove(table: TableName,
                 *args: Any,
                 on_removed: Optional[Callable[[Any], Any]] = None,
                 **kwargs: Any) -> bool:
    fields = ['*'] if on_removed else ['id']

    old = await get(table,
                    *args,
                    fields=fields,
                    ignore_extra_keys=True,
                    **kwargs)
    if old:
        await delete(table, 'id=%s', (old['id'], ))
        if on_removed:
            ret = on_removed(old)
            if asyncio.iscoroutine(ret):
                await ret

        return True
    return False


def format_key(
    key: str,
    val: Any,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> str:
    if key.find('.') == -1:
        if len(keys) == 0:
            return key

        if key in keys:
            return key

        if 'data' not in json_keys:
            return key

        return format_key('data.' + key, val, json_keys=json_keys, keys=keys)

    keys = key.split('.')

    prefix = ''

    if keys[0] in json_keys:
        prefix = keys[0]
        keys = keys[1:]
    elif keys[1] in json_keys:
        prefix = keys[0] + '.' + keys[1]
        keys = keys[2:]
    else:
        return key

    out = prefix + "#>>'{" + ', '.join(keys) + "}'"

    tp = ''

    if isinstance(val, bytes):
        val = str(val, 'utf-8')

    if isinstance(val, str):
        if re_num.search(val):
            if val.isdigit():
                tp = 'int'
            else:
                tp = 'float'

        else:
            l_val = val.lower()
            if l_val == 'true' or l_val == 'false':
                tp = 'boolean'

    if isinstance(val, int):
        tp = 'int'

    if isinstance(val, float):
        tp = 'float'

    if isinstance(val, bool):
        tp = 'boolean'

    if tp:
        out = f'cast({out} as {tp})'

    return out


def append_query(
    query: List[tuple[str, str, Any]],
    key: str,
    val: Any,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> None:
    if val is None:
        return

    if isinstance(val, list):
        vs = ['%s' for x in val]
        fkey = format_key(key, val[0], json_keys=json_keys, keys=keys)
        query.append((key, f'{fkey} in (' + ', '.join(vs) + ')', val))
        return

    m = re_op.search(key)

    op = '='
    if m:
        op = m.group(1)
        key = key[:-len(op) - 1]
        op = op_map[op]

    if op.strip() == 'in':
        val = [x.strip() for x in val.split(',')]
        append_query(query, key, val, json_keys=json_keys, keys=keys)
    else:
        fkey = format_key(key, val, json_keys=json_keys, keys=keys)
        query.append((key, f'{fkey}{op}%s', val))


def sort_query(
    query: List[tuple[str, str, Any]],
    sort_keys: List[str],
) -> List[tuple[str, str, Any]]:
    ret = []
    for key in sort_keys:
        other = []
        for q in query:
            if q[0] == key:
                ret.append(q)
            else:
                other.append(q)

        query = other

    return ret + query


def record_query_to_sql(query: List[tuple[str, str, Any]],
                        part_sql: str = '',
                        args: Any = ()) -> tuple[str, Any]:
    new_part_sql = []
    new_args = []
    for q in query:
        new_part_sql.append(q[1])
        if isinstance(q[2], list):
            new_args += q[2]
        else:
            new_args.append(q[2])

    if part_sql:
        new_part_sql.append(part_sql)

    if len(args) > 0:
        for arg in args:
            new_args.append(arg)

    return ' AND '.join(new_part_sql), tuple(new_args)


def gen_query(
    *args: Any,
    sort_keys: List[str] = [],
    part_sql: str = '',
    json_keys: List[str] = [],
    keys: List[str] = [],
    **data: Any,
) -> tuple[str, Any]:
    query: List[tuple[str, str, Any]] = []
    for key, val in data.items():
        append_query(query, key, val, json_keys=json_keys, keys=keys)

    query = sort_query(query, sort_keys)

    return record_query_to_sql(query, part_sql, args)


async def count(
    table: TableName,
    *args: Any,
    field: str = '*',
    join_sql: str = '',
    groups: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    part_sql, args = gen_query(*args, **kwargs)
    return await pg_count(table,
                          part_sql,
                          args,
                          column=c(field),
                          join_sql=join_sql,
                          groups=groups)


async def get_list(table: TableName,
                   *args: Any,
                   offset: Optional[int] = None,
                   size: Optional[int] = None,
                   fields: List[str] = ['*'],
                   popup: bool = False,
                   join_sql: str = '',
                   groups: Optional[str] = None,
                   sorts: Optional[str] = 'id desc',
                   **kwargs: Any) -> Any:

    part_sql, args = gen_query(*args, **kwargs)
    ret = await select(table,
                       cs(fields),
                       part_sql,
                       args,
                       offset=offset,
                       size=size,
                       join_sql=join_sql,
                       groups=groups,
                       sorts=sorts)

    if popup:
        return [popup_data(v) for v in ret]

    return ret


def popup_data(ret: Any) -> Any:
    if isinstance(ret, dict):
        data = ret.pop('data', None)

        if isinstance(data, dict):
            data.update(ret)
            return data

    return ret


def make_data(data: Any, exclude_data_keys: List[str] = []) -> Any:
    new = {}

    for k in exclude_data_keys:
        v = data.pop(k, None)
        if v is not None:
            new[k] = v

    new['data'] = data
    return new
