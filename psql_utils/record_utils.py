import json
import re
from time import time
from typing import Optional, List, Dict, Any, Tuple

from .types import c, cs
from .errors import ValidationError

# Operator mapping for SQL generation (e.g., 'age_gt' -> 'age > %s')
OP_MAP = {
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

# Regex to detect operators at the end of a key (e.g., '_gt')
RE_OP = re.compile('_(' + '|'.join(OP_MAP.keys()) + ')$')
RE_NUM = re.compile(r'^\d+(\.\d+)?$')
RE_UNSAFE_SQL = re.compile(r';|--|/\*|\*/')

IGNORE = '__IGNORE__'


class EmptyRows(Exception):
    """Exception raised when a query should return no rows
    (e.g., empty IN clause)."""
    pass


def validate_sql_fragment(
    name: str,
    sql: str,
    *,
    must_start_select: bool = False,
) -> None:
    """Rejects obviously unsafe raw SQL fragments."""
    if '\x00' in sql:
        raise ValueError(f'{name} contains null byte')
    if RE_UNSAFE_SQL.search(sql):
        raise ValueError(f'{name} contains unsafe token')
    if must_start_select and not sql.strip().lower().startswith('select '):
        raise ValueError(f'{name} must start with SELECT')


def merge_json(new: Any, old: Any) -> Any:
    """Recursively updates a dictionary if both inputs are dicts."""
    if isinstance(new, dict) and isinstance(old, dict):
        old.update(new)
        return old
    return new


def merge_sub_json(
    data0: Any,
    data1: Any,
    replace_keys: Optional[List[str]] = None,
) -> Any:
    """Merges specific sub-dictionaries, skipping keys in replace_keys."""
    if not isinstance(data0, dict):
        return data0
    if not isinstance(data1, dict):
        return data0

    if replace_keys is None:
        replace_keys = []

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
    """Appends remaining dictionary items as standard equality checks."""
    for key, val in data.items():
        if val is not None:
            part_sql.append(f'{key}=%s')
            args.append(val)


def prepare_save(
    keys: Optional[List[str]] = None,
    uniq_keys: Optional[List[str]] = None,
    exclude_data_keys: Optional[List[str]] = None,
    json_keys: Optional[List[str]] = None,
    replace_keys: Optional[List[str]] = None,
    sub_json_keys: Optional[List[str]] = None,
    old_record: Optional[Any] = None,
    **data: Any,
) -> Tuple[List[str], List[Any]]:
    """
    Prepares columns and arguments for an INSERT or UPDATE statement.
    Handles JSON merging and modification checks against old_record.
    """
    keys = keys or []
    uniq_keys = uniq_keys or []
    exclude_data_keys = exclude_data_keys or []
    json_keys = json_keys or []
    replace_keys = replace_keys or []
    sub_json_keys = sub_json_keys or []

    rkeys = []
    args = []

    # Move specific keys from root to 'data' dict if requested
    if exclude_data_keys:
        data = make_data(data.copy(), exclude_data_keys)

    # Handle standard columns
    all_keys = keys + uniq_keys
    for key in all_keys:
        val = data.get(key)
        if val is not None:
            # Skip if value hasn't changed
            if old_record and old_record.get(key) == val:
                continue
            rkeys.append(key)
            args.append(val)

    # Handle JSON columns with merging
    for key in json_keys:
        val = data.get(key)
        if val is not None:
            if old_record and key not in replace_keys:
                val = merge_json(val, old_record.get(key))
            rkeys.append(key)
            args.append(json.dumps(val))

    # Handle Sub-JSON columns
    for key in sub_json_keys:
        val = data.get(key)
        if val is not None:
            if old_record:
                val = merge_sub_json(val, old_record.get(key), replace_keys)
            rkeys.append(key)
            args.append(json.dumps(val))

    # Auto-update timestamp
    if 'updated_at' in keys and data.get('updated_at') is None:
        rkeys.append('updated_at')
        args.append(int(time()))

    return rkeys, args


def get_uniq_data(
    uniq_keys: Optional[List[str]] = None,
    old_record: Optional[Any] = None,
    **data: Any,
) -> Tuple[bool, Dict[str, Any]]:
    """Determines if unique keys have changed compared to the old record."""
    uniq_keys = uniq_keys or []
    uniq_data: Dict[str, Any] = {}
    uniq_changed = False

    for key in uniq_keys:
        val = data.get(key)

        # Fallback to old record value if missing in new data
        if val is None and old_record:
            val = old_record.get(key)

        uniq_data[key] = val

        if not old_record:
            continue

        if old_record.get(key) != val:
            uniq_changed = True

    return uniq_changed, uniq_data


def guess_type(val: Any) -> str:
    """Attempts to infer the SQL type from a Python value."""
    if isinstance(val, bytes):
        val = str(val, 'utf-8')

    if isinstance(val, str):
        l_val = val.lower()
        if l_val in ('true', 'false'):
            return 'boolean'

        if RE_NUM.search(val):
            return 'int' if val.isdigit() else 'float'

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
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> str:
    """
    Formats a dictionary key into a SQL column or JSON path selector.
    Example: 'meta.score' -> "meta#>>'{score}'" (PostgreSQL syntax).
    """
    json_keys = json_keys or []
    keys = keys or []

    if key == '*':
        return key

    # Handle simple keys (no dots)
    if '.' not in key:
        if not keys or key in keys:
            return key
        # If not a known column, assume it's inside the 'data' JSON column
        if 'data' not in json_keys:
            return key
        return format_key(f'data.{key}', val, json_keys=json_keys, keys=keys)

    # Handle nested keys
    parts = key.split('.')
    json_types = {'int', 'float', 'boolean', 'text'}

    prefix = ''
    if parts[0] in json_keys:
        prefix = parts[0]
        parts = parts[1:]
    elif parts[1] in json_keys:
        # Example: table.json_col.field
        prefix = f'{parts[0]}.{parts[1]}'
        parts = parts[2:]
    else:
        return key

    # Check for type hint at the end (e.g., field.int)
    tp = ''
    if parts[-1] in json_types:
        tp = parts[-1]
        parts = parts[:-1]

    # Check for alias
    as_name = ''
    if 'as' in parts:
        # Assumes syntax like: field.as.alias
        as_idx = parts.index('as')
        if as_idx + 1 < len(parts):
            as_name = parts[as_idx + 1]
            parts = parts[:as_idx]

    # Construct PostgreSQL JSON path operator
    out = f"{prefix}#>>'{{{', '.join(parts)}}}'"

    if not tp:
        tp = guess_type(val)

    if tp:
        out = f'cast({out} as {tp})'

    if as_name:
        out = f'{out} as {as_name}'

    return out


def format_fields(
    fields: List[str],
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> List[str]:
    """Formats a list of field names."""
    return [
        format_key(f.strip(), None, json_keys=json_keys, keys=keys)
        for f in fields
    ]


def format_groups(
    groups: Optional[str],
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> Optional[str]:
    """Formats a comma-separated string of GROUP BY fields."""
    if not groups:
        return None

    formatted = format_fields(groups.split(','),
                              json_keys=json_keys,
                              keys=keys)
    return ','.join(formatted)


def format_sorts_one(
    sorts: Optional[str],
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> Optional[str]:
    """Formats a single ORDER BY clause."""
    if not sorts:
        return None

    # Split "column direction" (e.g., "age desc")
    idx = sorts.rfind(' ')
    if idx == -1:
        return format_groups(sorts, json_keys=json_keys, keys=keys)

    fsorts = format_groups(sorts[:idx], json_keys=json_keys, keys=keys)
    if not fsorts:
        return None

    return fsorts + sorts[idx:]


def format_sorts(
    sorts: Optional[str],
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> Optional[str]:
    """Formats a comma-separated string of ORDER BY clauses."""
    if not sorts:
        return sorts

    items = sorts.split(',')
    retval = []
    for item in items:
        one = format_sorts_one(item.strip(), json_keys=json_keys, keys=keys)
        if one:
            retval.append(one)

    return ', '.join(retval)


def append_query(
    query: List[Tuple[str, str, Any]],
    key: str,
    val: Any,
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
) -> None:
    """Parses a key/value pair and appends it to the query list."""
    if val is None:
        return

    # Handle suffix operators (e.g., age_gt)
    m = RE_OP.search(key)
    op = '='
    if m:
        op_suffix = m.group(1)
        key = key[:-len(op_suffix) - 1]
        op = OP_MAP[op_suffix]

    # Handle IN clause for lists
    if isinstance(val, list):
        if not val:
            raise EmptyRows()
        vs = ['%s' for _ in val]
        fkey = format_key(key, val[0], json_keys=json_keys, keys=keys)
        query.append((key, f'{fkey} in ({", ".join(vs)})', val))
        return

    # Handle IN clause for string-based values (subqueries or CSV)
    if op.strip() == 'in':
        if isinstance(val, str) and 'select' in val.lower():
            # It's a subquery
            validate_sql_fragment(
                f'subquery for "{key}_in"',
                val,
                must_start_select=True,
            )
            fkey = format_key(key, val, json_keys=json_keys, keys=keys)
            query.append((key, f'{fkey}{op}({val})', IGNORE))
        else:
            # It's a CSV string
            val_list = [x.strip() for x in str(val).split(',')]
            append_query(query, key, val_list, json_keys=json_keys, keys=keys)
    else:
        # Standard comparison
        fkey = format_key(key, val, json_keys=json_keys, keys=keys)
        query.append((key, f'{fkey}{op}%s', val))


def sort_query(
    query: List[Tuple[str, str, Any]],
    sort_keys: List[str],
) -> List[Tuple[str, str, Any]]:
    """Reorders query parameters based on a priority list."""
    ret = []
    other = []

    # Extract keys in preference order
    for key in sort_keys:
        for q in query:
            if q[0] == key:
                ret.append(q)

    # Collect remaining keys
    ret_keys = {q[0] for q in ret}
    for q in query:
        if q[0] not in ret_keys:
            other.append(q)

    return ret + other


def record_query_to_sql(
        query: List[Tuple[str, str, Any]],
        part_sql: str = '',
        args: Tuple[Any, ...] = (),
) -> Tuple[str, Tuple[Any, ...]]:
    """Compiles the query list into a final SQL WHERE string and args."""
    new_part_sql = []
    new_args = []

    for q in query:
        new_part_sql.append(q[1])
        val = q[2]
        if isinstance(val, list):
            new_args.extend(val)
        elif val != IGNORE:
            new_args.append(val)

    if part_sql:
        validate_sql_fragment('part_sql', part_sql)
        new_part_sql.append(part_sql)

    if args:
        new_args.extend(args)

    return ' AND '.join(new_part_sql), tuple(new_args)


def gen_query(
    *args: Any,
    sort_keys: Optional[List[str]] = None,
    part_sql: str = '',
    json_keys: Optional[List[str]] = None,
    keys: Optional[List[str]] = None,
    **data: Any,
) -> Tuple[str, Any]:
    """Main entry point to generate SQL WHERE clauses from dict data."""
    sort_keys = sort_keys or []
    json_keys = json_keys or []
    keys = keys or []

    query: List[Tuple[str, str, Any]] = []

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
    """Prepares arguments for a COUNT query."""
    props: Dict[str, Any] = {}

    if join_sql:
        validate_sql_fragment('join_sql', join_sql)
    if groups:
        validate_sql_fragment('groups', groups)

    part_sql, sql_args = gen_query(*args, **kwargs)
    props['part_sql'] = part_sql
    props['args'] = sql_args

    json_keys = kwargs.get('json_keys', [])
    keys = kwargs.get('keys', [])

    props['groups'] = format_groups(groups, json_keys=json_keys, keys=keys)
    props['join_sql'] = join_sql
    props['column'] = c(field)

    return props


def prepare_get_list(
    *args: Any,
    fields: Optional[List[str]] = None,
    join_sql: str = '',
    groups: Optional[str] = None,
    sorts: Optional[str] = 'id desc',
    **kwargs: Any,
) -> Dict[str, Any]:
    """Prepares arguments for a SELECT list query."""
    if join_sql:
        validate_sql_fragment('join_sql', join_sql)
    if groups:
        validate_sql_fragment('groups', groups)
    if sorts:
        validate_sql_fragment('sorts', sorts)

    if fields is None:
        fields = ['*']

    props: Dict[str, Any] = {}

    part_sql, sql_args = gen_query(*args, **kwargs)
    props['part_sql'] = part_sql
    props['args'] = sql_args

    json_keys = kwargs.get('json_keys', [])
    keys = kwargs.get('keys', [])

    props['sorts'] = format_sorts(sorts, json_keys=json_keys, keys=keys)
    props['groups'] = format_groups(groups, json_keys=json_keys, keys=keys)

    fmt_fields = format_fields(fields, json_keys=json_keys, keys=keys)
    props['columns'] = cs(fmt_fields)
    props['join_sql'] = join_sql

    return props


def popup_data(ret: Any) -> Any:
    """Promotes keys inside a 'data' sub-dictionary to the top level."""
    if isinstance(ret, dict):
        data = ret.pop('data', None)
        if isinstance(data, dict):
            data.update(ret)
            return data
    return ret


def make_data(data: Dict[str, Any],
              exclude_data_keys: Optional[List[str]] = None) -> Dict[str, Any]:
    """Moves non-excluded keys into a nested 'data' dictionary."""
    exclude_data_keys = exclude_data_keys or []
    new = {}

    for k in exclude_data_keys:
        v = data.pop(k, None)
        if v is not None:
            new[k] = v

    new['data'] = data
    return new


def prepare_get_by_uniq(
    uniq_keys: Optional[List[str]] = None,
    optional_keys: Optional[List[str]] = None,
    required_uniq_keys: bool = True,
    ignore_extra_keys: bool = False,
    fields: Optional[List[str]] = None,
    **data: Any,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Prepares arguments to fetch a record by unique keys.
    Returns (get_max_id_mode, props).
    """
    uniq_keys = uniq_keys or []
    optional_keys = optional_keys or []
    fields = fields or ['*']

    props: Dict[str, Any] = {}
    part_sql = []
    args = []

    if not uniq_keys:
        if required_uniq_keys:
            if ignore_extra_keys:
                raise EmptyRows()
            raise ValidationError('uniq_keys is required')

    get_max_id = False
    for key in uniq_keys:
        val = data.pop(key, None)
        if val is None:
            get_max_id = True
            if required_uniq_keys and key not in optional_keys:
                raise ValidationError(f'{key} is required')
            # Missing optional unique keys should not force "key = NULL"
            # in WHERE clause; they are intentionally omitted.
            continue

        part_sql.append(f'{key}=%s')
        args.append(val)

    if not ignore_extra_keys:
        append_extra(part_sql, args, data)

    props['part_sql'] = ' AND '.join(part_sql)
    props['args'] = args

    if get_max_id:
        props['column'] = c('max(id)')
        return True, props

    props['columns'] = cs(fields)
    return False, props


def prepare_get_by_id(
    id: Optional[int] = None,
    fields: Optional[List[str]] = None,
    ignore_extra_keys: bool = False,
    **data: Any,
) -> Dict[str, Any]:
    """Prepares arguments to fetch a record by primary key (id)."""
    fields = fields or ['*']
    props: Dict[str, Any] = {}
    part_sql = []
    args = []

    part_sql.append('id=%s')
    args.append(id)

    if not ignore_extra_keys:
        append_extra(part_sql, args, data)

    props['columns'] = cs(fields)
    props['part_sql'] = ' AND '.join(part_sql)
    props['args'] = args

    return props
