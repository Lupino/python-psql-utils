from . import c, cs
import json
from time import time
import re
from typing import Optional, List, Dict, Any

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

IGNORE = '__IGNORE__'


class EmptyRows(Exception):
    pass


def merge_json(new: Any, old: Any) -> Any:
    if isinstance(new, dict) and isinstance(old, dict):
        old.update(new)
        return old

    return new


def merge_sub_json(
    data0: Any,
    data1: Any,
    replace_keys: List[str] = [],
) -> Any:
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


def prepare_save(
    keys: List[str] = [],
    uniq_keys: List[str] = [],
    exclude_data_keys: List[str] = [],
    json_keys: List[str] = [],
    replace_keys: List[str] = [],
    sub_json_keys: List[str] = [],
    old_record: Optional[Any] = None,
    **data: Any,
) -> tuple[List[str], List[Any]]:
    rkeys = []
    args = []

    if len(exclude_data_keys) > 0:
        data = make_data(data.copy(), exclude_data_keys)

    all_keys = keys + uniq_keys

    for key in all_keys:
        val = data.get(key)
        if val is not None:
            if old_record:
                if old_record[key] == val:
                    continue

            rkeys.append(key)
            args.append(val)

    for key in json_keys:
        val = data.get(key)
        if val is not None:
            if old_record and key not in replace_keys:
                val = merge_json(val, old_record[key])
            rkeys.append(key)
            args.append(json.dumps(val))

    for key in sub_json_keys:
        val = data.get(key)
        if val is not None:
            if old_record:
                val = merge_sub_json(val, old_record[key], replace_keys)
            rkeys.append(key)
            args.append(json.dumps(val))

    if 'updated_at' in keys and data.get('updated_at') is None:
        rkeys.append('updated_at')
        args.append(int(time()))

    return rkeys, args


def get_uniq_data(
    uniq_keys: List[str] = [],
    old_record: Optional[Any] = None,
    **data: Any,
) -> tuple[bool, Dict[str, Any]]:
    uniq_data: Dict[str, Any] = {}
    uniq_changed = False

    for key in uniq_keys:
        val = data.get(key)

        if val is None and old_record:
            val = old_record[key]

        uniq_data[key] = val

        if not old_record:
            continue

        if old_record[key] == val:
            continue

        uniq_changed = True

    return uniq_changed, uniq_data


def guess_type(val: Any) -> str:
    if isinstance(val, bytes):
        val = str(val, 'utf-8')

    if isinstance(val, str):
        l_val = val.lower()
        if l_val == 'true' or l_val == 'false':
            return 'boolean'

        if re_num.search(val):
            if val.isdigit():
                return 'int'
            return 'float'

    if isinstance(val, bool):
        return 'boolean'

    if isinstance(val, int):
        return 'int'

    if isinstance(val, float):
        return 'float'

    return ''


def format_key(
    key: str,
    val: Any,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> str:
    if key == '*':
        return key

    if key.find('.') == -1:
        if len(keys) == 0:
            return key

        if key in keys:
            return key

        if 'data' not in json_keys:
            return key

        return format_key('data.' + key, val, json_keys=json_keys, keys=keys)

    keys = key.split('.')
    json_types = ['int', 'float', 'boolean', 'text']

    prefix = ''

    if keys[0] in json_keys:
        prefix = keys[0]
        keys = keys[1:]
    elif keys[1] in json_keys:
        prefix = keys[0] + '.' + keys[1]
        keys = keys[2:]
    else:
        return key

    tp = ''
    if keys[-1] in json_types:
        tp = keys[-1]
        keys = keys[:-1]

    asName = ''

    if 'as' in keys:
        asName = keys[-1]
        keys = keys[:-2]

    out = prefix + "#>>'{" + ', '.join(keys) + "}'"

    if not tp:
        tp = guess_type(val)

    if tp:
        out = f'cast({out} as {tp})'

    if asName:
        out = f'{out} as {asName}'

    return out


def format_fields(
    fields: List[str],
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> List[str]:
    retval = []

    for key in fields:
        fkey = format_key(key.strip(), None, json_keys=json_keys, keys=keys)
        retval.append(fkey)

    return retval


def format_groups(
    groups: str | None,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> str | None:
    if not groups:
        return groups

    retval = format_fields(groups.split(','), json_keys=json_keys, keys=keys)

    return ','.join(retval)


def format_sorts_one(
    sorts: str | None,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> str | None:
    if not sorts:
        return sorts

    idx = sorts.rfind(' ')

    if idx == -1:
        return format_groups(sorts, json_keys=json_keys, keys=keys)

    fsorts = format_groups(sorts[:idx], json_keys=json_keys, keys=keys)
    if not fsorts:
        return fsorts
    return fsorts + sorts[idx:]


def format_sorts(
    sorts: str | None,
    json_keys: List[str] = [],
    keys: List[str] = [],
) -> str | None:
    if not sorts:
        return sorts

    items = sorts.split(',')

    retval = []
    for item in items:
        one = format_sorts_one(item.strip())
        if one:
            retval.append(one)

    return ', '.join(retval)


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
        if len(val) == 0:
            raise EmptyRows()
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
        if val.lower().find('select') > -1:
            query.append((key, f'{key}{op}({val})', IGNORE))
        else:
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


def record_query_to_sql(
        query: List[tuple[str, str, Any]],
        part_sql: str = '',
        args: Any = (),
) -> tuple[str, Any]:
    new_part_sql = []
    new_args = []
    for q in query:
        new_part_sql.append(q[1])
        if isinstance(q[2], list):
            new_args += q[2]
        else:
            if q[2] == IGNORE:
                continue
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


def prepare_count(
    *args: Any,
    field: str = '*',
    join_sql: str = '',
    groups: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    props: Dict[str, Any] = {}

    part_sql, args = gen_query(*args, **kwargs)
    props['part_sql'] = part_sql
    props['args'] = args

    json_keys = kwargs.get('json_keys', [])
    keys = kwargs.get('keys', [])

    groups = format_groups(groups, json_keys=json_keys, keys=keys)
    props['groups'] = groups
    props['join_sql'] = join_sql
    props['column'] = c(field)

    return props


def prepare_get_list(
    *args: Any,
    fields: List[str] = ['*'],
    join_sql: str = '',
    groups: Optional[str] = None,
    sorts: Optional[str] = 'id desc',
    **kwargs: Any,
) -> Dict[str, Any]:
    props: Dict[str, Any] = {}

    part_sql, args = gen_query(*args, **kwargs)
    props['part_sql'] = part_sql
    props['args'] = args

    json_keys = kwargs.get('json_keys', [])
    keys = kwargs.get('keys', [])

    sorts = format_sorts(sorts, json_keys=json_keys, keys=keys)
    groups = format_groups(groups, json_keys=json_keys, keys=keys)
    fields = format_fields(fields, json_keys=json_keys, keys=keys)

    props['sorts'] = sorts
    props['groups'] = groups
    props['columns'] = cs(fields)
    props['join_sql'] = join_sql

    return props


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


def prepare_get_by_uniq(
    uniq_keys: List[str] = [],
    optional_keys: List[str] = [],
    required_uniq_keys: bool = True,
    ignore_extra_keys: bool = False,
    fields: List[str] = ['*'],
    **data: Any,
) -> tuple[bool, Dict[str, Any]]:
    props: Dict[str, Any] = {}

    part_sql = []
    args = []

    if len(uniq_keys) == 0:
        if required_uniq_keys:
            if ignore_extra_keys:
                raise EmptyRows()
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

    if not ignore_extra_keys:
        append_extra(part_sql, args, data)

    if get_max_id:

        part_sql_s = ' AND '.join(part_sql)
        props['column'] = c('max(id)')
        props['part_sql'] = part_sql_s
        props['args'] = args
        return True, props

    part_sql_s = ' AND '.join(part_sql)
    props['columns'] = cs(fields)
    props['part_sql'] = part_sql_s
    props['args'] = args

    return False, props


def prepare_get_by_id(
    id: Optional[int] = None,
    fields: List[str] = ['*'],
    ignore_extra_keys: bool = False,
    **data: Any,
) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    part_sql = []
    args = []

    part_sql.append('id=%s')
    args.append(id)

    if not ignore_extra_keys:
        append_extra(part_sql, args, data)

    part_sql_s = ' AND '.join(part_sql)

    props['columns'] = cs(fields)
    props['part_sql'] = part_sql_s
    props['args'] = args

    return props
