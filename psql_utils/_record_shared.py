from typing import Any, Dict, List, Optional, Tuple

from .record_utils import EmptyRows, prepare_get_by_id, prepare_get_by_uniq


def normalize_get_inputs(
    uniq_keys: Optional[List[str]],
    optional_keys: Optional[List[str]],
    fields: Optional[List[str]],
) -> Tuple[List[str], List[str], List[str]]:
    """Normalize get() list-like inputs."""
    return uniq_keys or [], optional_keys or [], fields or ['*']


def normalize_save_inputs(
    keys: Optional[List[str]],
    uniq_keys: Optional[List[str]],
    optional_keys: Optional[List[str]],
    json_keys: Optional[List[str]],
    sub_json_keys: Optional[List[str]],
    replace_keys: Optional[List[str]],
    exclude_data_keys: Optional[List[str]],
) -> Tuple[List[str], List[str], List[str], List[str], List[str], List[str],
           List[str]]:
    """Normalize save() list-like inputs."""
    return (
        keys or [],
        uniq_keys or [],
        optional_keys or [],
        json_keys or [],
        sub_json_keys or [],
        replace_keys or [],
        exclude_data_keys or [],
    )


def prepare_get_props(
    *,
    id: Optional[int],
    uniq_keys: List[str],
    optional_keys: List[str],
    required_uniq_keys: bool,
    ignore_extra_keys: bool,
    fields: List[str],
    data: Dict[str, Any],
) -> Optional[Tuple[bool, Dict[str, Any]]]:
    """
    Prepare get/select props.
    Returns None when conditions imply empty rows.
    """
    query_data = dict(data)
    if id is not None:
        return False, prepare_get_by_id(
            id=id,
            fields=fields,
            ignore_extra_keys=ignore_extra_keys,
            **query_data,
        )
    try:
        return prepare_get_by_uniq(
            uniq_keys=uniq_keys,
            optional_keys=optional_keys,
            required_uniq_keys=required_uniq_keys,
            ignore_extra_keys=ignore_extra_keys,
            fields=fields,
            **query_data,
        )
    except EmptyRows:
        return None
