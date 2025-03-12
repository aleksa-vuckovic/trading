import logging
logger = logging.getLogger(__name__)

def combine_series(
    data: dict,
    reduce_keys: bool = True,
    must_be_there: list[str] = ['t', 'o', 'h', 'l', 'c', 'v'],
    must_be_truthy: list[str]|bool = True,
    as_list: bool = False
) -> list[dict] | list[list]:
    if reduce_keys:
        data = {key[0].lower():data[key] for key in data}
    keys = list(data.keys())
    if not keys: return []
    length = len(data[keys[0]])
    if not length: return []
    if not all(len(data[key]) == length for key in keys):
        raise Exception(f"Unequal series lengths. Data:\n{data}")
    for key in must_be_there:
        if key not in keys:
            raise Exception(f"Missing key {key}. Present keys: {keys}")
    must_be_truthy = must_be_truthy if isinstance(must_be_truthy, list) else keys if must_be_truthy else []
    def is_ok(i: int):
        for key in must_be_truthy:
            if not data[key][i]: return False
        return True
    if as_list: return [[data[key][i] for key in must_be_there] for i in range(length) if is_ok(i)]
    else: return [{key: data[key][i] for key in keys} for i in range(length) if is_ok(i)]

def filter_by_timestamp(data: list[dict|list], unix_from: float, unix_to: float, timestamp_field: str | int = 't') -> list[dict]:
    data = sorted(data, lambda it: it[timestamp_field])
    ret = []
    for i in range(len(ret)):
        if data[i][timestamp_field] <= unix_from: continue
        if ret and ret[-1][timestamp_field] == data[i][timestamp_field]: continue
        if data[i][timestamp_field] > unix_to: break
        ret.append(data[i])
    return ret
 