from . import select_one_only, select_one, select, count as pg_count, \
    update, insert, delete, c, cs
import json
from time import time
import re
import asyncio

re_op = re.compile('_(gt|lt|gte|lte)$')


def merge_json(new, old):
    if isinstance(new, dict) and isinstance(old, dict):
        old.update(new)
        return old

    return new


def merge_sub_json(data0, data1, replace_keys=[]):
    for k, v in data1.items():
        new = data0.get(k)
        if new is None:
            data0[k] = v
        else:
            if k not in replace_keys:
                data0[k] = merge_json(data0[k], v)

    return data0


async def get(table,
              *,
              id=None,
              uniq_keys=[],
              fields=['*'],
              popup=False,
              **data):
    part_sql = ''
    args = ()
    if id:
        part_sql = 'id=%s'
        args = (id, )
    else:
        if len(uniq_keys) == 0:
            return None

        part_sql = []
        args = []
        get_max_id = False
        for key in uniq_keys:
            val = data.get(key)
            if not val:
                get_max_id = True
                continue

            part_sql.append(f'{key}=%s')
            args.append(val)

        part_sql = ' AND '.join(part_sql)
        args = tuple(args)

        if get_max_id:
            id = await select_one_only(table, c('max(id)'), part_sql, args)
            if not id:
                return None

            part_sql = 'id=%s'
            args = (id, )

    ret = await select_one(table, cs(fields), part_sql, args)

    if popup:
        return popup_data(ret)

    return ret


async def save(table,
               *,
               id=None,
               keys=[],
               uniq_keys=[],
               json_keys=[],
               sub_json_keys=[],
               replace_keys=[],
               exclude_data_keys=[],
               on_saved=None,
               **data):

    if len(exclude_data_keys) > 0:
        data = make_data(data.copy(), exclude_data_keys)

    uniq_data = {}

    for key in uniq_keys:
        uniq_data[key] = data.get(key)

    if id:
        old = await get(table, id=id)
        if not old:
            raise Exception(f'update record[{id}], the record is not exists')
    else:
        old = await get(table, uniq_keys=uniq_keys, **uniq_data)

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

    if old:
        uniq_changed = False
        for key in uniq_keys:
            val = data.get(key)
            if val is not None:
                if old[key] == val:
                    continue

                rkeys.append(key)
                args.append(val)
                uniq_changed = True

        if uniq_changed:
            old1 = await get(table, uniq_keys=uniq_keys, **uniq_data)
            if old1:
                oid = old1['id']
                err = f'cant update record uniq value to exists value {oid}'
                raise Exception(err)

        if len(args) == 0:
            return old['id']

        args.append(old['id'])
        await update(table, cs(rkeys), 'id=%s', tuple(args))

        if on_saved:
            new = await get(table, id=old['id'])
            ret = on_saved(old, new)
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
            new = await get(table, id=nid)
            ret = on_saved(None, new)
            if asyncio.iscoroutine(ret):
                await ret

        return nid


async def remove(table, *args, **kwargs):
    old = await get(table, *args, fields=['id'], **kwargs)
    if old:
        await delete(table, 'id=%s', (old['id'], ))
        return True
    return False


op_map = {'gt': '>', 'lt': '<', 'lte': '<=', 'gte': '>='}


def append_query(query, key, val):
    if val is None:
        return

    if isinstance(val, list):
        vs = ['%s' for x in val]
        query.append((key, f'{key} in (' + ', '.join(vs) + ')', val))
        return

    m = re_op.search(key)

    op = '='
    if m:
        op = m.group(1)
        key = key[:-len(op) - 1]
        op = op_map[op]

    query.append((key, f'{key}{op}%s', val))


def sort_query(query, sort_keys):
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


def record_query_to_sql(query, part_sql='', args=()):
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


def gen_query(*args, sort_keys=[], part_sql='', **data):
    query = []
    for key, val in data.items():
        append_query(query, key, val)

    query = sort_query(query, sort_keys)

    return record_query_to_sql(query, part_sql, args)


async def count(table, *args, field='*', join_sql='', other_sql='', **kwargs):
    part_sql, args = gen_query(*args, **kwargs)
    return await pg_count(table,
                          part_sql,
                          args,
                          column=c(field),
                          join_sql=join_sql,
                          other_sql=other_sql)


async def get_list(table,
                   *args,
                   offset=None,
                   size=None,
                   fields=['*'],
                   popup=False,
                   join_sql='',
                   other_sql='order by id desc',
                   **kwargs):

    part_sql, args = gen_query(*args, **kwargs)
    ret = await select(table,
                       cs(fields),
                       part_sql,
                       args,
                       offset=offset,
                       size=size,
                       join_sql=join_sql,
                       other_sql=other_sql)

    if popup:
        return [popup_data(v) for v in ret]

    return ret


def popup_data(ret):
    if isinstance(ret, dict):
        data = ret.pop('data', None)

        if isinstance(data, dict):
            data.update(ret)
            return data

    return ret


def make_data(data, exclude_data_keys=[]):
    new = {}

    for k in exclude_data_keys:
        v = data.pop(k, None)
        if v is not None:
            new[k] = v

    new['data'] = data
    return new
