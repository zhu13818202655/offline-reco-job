import functools
import logging
import time
from typing import Callable, Dict, Iterable, List, Optional, Union

import requests
from pydantic import BaseModel

from .model import ApiError, ApiReturnType, DictValueType_, LowCodeRecord, RecordValueType_

logger = logging.getLogger('lowcodeapi')


class VarSet(BaseModel):
    token: Optional[str] = None
    update_time: int = 0
    base_url: str = ''
    auth_url: str = ''
    secret: str = ''
    client_id: str = ''
    max_retry: int = 10  # max retry times
    max_retry_interval: int = 10
    # retry interval = min(retry_time+retry_time**2,max_retry_interval)


def check_base_url(base_url):
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    return base_url


class VarSetSingleton:
    var_set_dict = {}

    def __new__(cls, var_set_key: str = 'base', var_set: VarSet = None):
        if var_set is None:
            v = cls.var_set_dict.get(var_set_key, None)
            if v is None:
                raise KeyError('var set have not been set yet')
            return v
        var_set.base_url = check_base_url(var_set.base_url)
        cls.var_set_dict[var_set_key] = var_set
        return var_set


def check_response_impl(r: requests.Response, error_msg: str = None) -> requests.Response:
    try:
        r.raise_for_status()
        if 'error' in r.json():
            logger.error(r.text)
            logger.error(f'{r.request.url},{r.request.body}')
            if error_msg:
                logger.error(str(error_msg))
            raise ApiError(str(r.text))
        return r
    except (requests.HTTPError, requests.JSONDecodeError) as e:
        logger.error(str(error_msg))
        raise ApiError(str(e))


def check_response(error_msg='request failed'):
    def check_response_wrapper(
        send_request_fn: Callable[..., requests.Response],
    ) -> Callable[..., requests.Response]:
        @functools.wraps(send_request_fn)
        def f(*args, **kwargs) -> requests.Response:
            return check_response_impl(send_request_fn(*args, **kwargs), error_msg=error_msg)

        return f

    return check_response_wrapper


def get_token_impl(auth_url, client_id, secret):
    r = requests.post(
        auth_url,
        headers={'Content-Type': 'application/x-www-form-urlencoded',},
        data={'grant_type': 'client_credentials', 'scope': 'api'},
        auth=(client_id, secret),
    )
    return r


def get_token(var_set_key='base'):
    var_set = VarSetSingleton(var_set_key)
    r = check_response(error_msg='failed to get token')(
        functools.partial(get_token_impl, var_set.auth_url, var_set.client_id, var_set.secret)
    )()
    resp = r.json()
    token = resp['access_token']
    return token


def check_token(var_set_key='base'):
    var_set = VarSetSingleton(var_set_key)
    if var_set.token is None or time.time() - var_set.update_time > 60 * 60 * 1.9:
        var_set.token = get_token(var_set_key)
        var_set.update_time = time.time()
        logger.info(f'Token update {var_set.token}')


def get_header_token(var_set_key='base'):
    check_token(var_set_key)
    var_set = VarSetSingleton(var_set_key)
    return f'Bearer {var_set.token}'


def json_resp(request_fn: Callable[..., requests.Response], *args, **kwargs):
    return request_fn(*args, **kwargs).json()


def as_dict(request_fn: Callable[..., requests.Response], *args, **kwargs) -> ApiReturnType:
    return json_resp(request_fn, *args, **kwargs)['response']


def as_obj(request_fn: Callable[..., requests.Response], *args, **kwargs) -> LowCodeRecord:
    return LowCodeRecord.parse_obj(as_dict(request_fn, *args, **kwargs))


def of_namespace(namespace_id):
    def fn(f, *args, **kwargs):
        return functools.update_wrapper(
            functools.partial(f, namespace_id=namespace_id, *args, **kwargs), f
        )

    return fn


def of_module(module_id):
    def fn(f, *args, **kwargs):
        return functools.update_wrapper(
            functools.partial(f, module_id=module_id, *args, **kwargs), f
        )

    return fn


def of_record(record_id):
    def fn(f, *args, **kwargs):
        return functools.update_wrapper(
            functools.partial(f, record_id=record_id, *args, **kwargs), f
        )

    return fn


def compose_ns_md_rc(namespace_id, module_id, record_id):
    return of_namespace(namespace_id)(of_module(module_id)(of_record(record_id)))


def compose_ns_md(namespace_id, module_id):
    return of_namespace(namespace_id)(of_module(module_id))


def get_record(*, record_id, module_id, namespace_id, var_set_key='base',) -> requests.Response:
    @check_response(error_msg=f'failed to get record {record_id}')
    def get_record_request(base_url, header_token):
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/{record_id}'
        return requests.get(url, headers={'Authorization': header_token})

    var_set = VarSetSingleton(var_set_key)
    return get_record_request(var_set.base_url, get_header_token(var_set_key),)


def get_record_list(
    *,
    namespace_id,
    module_id,
    query: Optional[str] = None,
    sort: Optional[str] = None,
    limit: Optional[str] = None,
    var_set_key='base',
) -> requests.Response:
    @check_response(error_msg=f'failed to get record list {module_id}')
    def get_record_list_request(
        namespace_id, module_id, query, sort, limit, base_url, header_token
    ):
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/'
        has_arg = False
        if query is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            query: Optional[str] = query
            url += f'query=({query})'
        if sort is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            else:
                url += '&'
            sort: str = sort
            url += f'sort={sort}'
        if limit is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            else:
                url += '&'
            limit: int = limit
            url += f'limit={limit}'
        return requests.get(url, headers={'Authorization': header_token})

    var_set = VarSetSingleton(var_set_key)

    return get_record_list_request(
        namespace_id,
        module_id,
        query,
        sort,
        limit,
        var_set.base_url,
        get_header_token(var_set_key),
    )


def update_values(d: Dict[str, DictValueType_]) -> Dict[str, List[Dict[str, RecordValueType_]]]:
    """
    Convert { k: v } to { name: k, value: v }
    """
    values = []
    for k, v in d.items():
        if not v:
            values.append({'name': str(k), 'value': None})
            continue
        if isinstance(v, list):
            for _ in v:
                values.append({'name': str(k), 'value': str(_)})
        else:
            values.append({'name': str(k), 'value': str(v)})
    return {'values': values}


def update_value_list(
    ori_values: Dict[str, List[Dict[str, RecordValueType_]]],
    new_values: Dict[str, List[Dict[str, RecordValueType_]]],
) -> Dict[str, List[Dict[str, RecordValueType_]]]:
    old_v: List[Dict] = ori_values['values']
    out_v: List[Dict] = new_values['values']
    new_keys = set([v['name'] for v in out_v])
    for v in old_v:
        if v['name'] not in new_keys:
            out_v.append(v)
    return {'values': out_v}


def update_record(*, namespace_id, module_id, record_id, new_values, var_set_key='base'):
    """
    new_values: Dict[str, Any]  { k1: v1, k2: [v2,v3,v4] }
    """

    @check_response(error_msg=f'failed to update record {record_id}')
    def update_record_request(base_url, header_token):
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/{record_id}'

        ori_record = as_dict(
            get_record,
            namespace_id=namespace_id,
            module_id=module_id,
            record_id=record_id,
            var_set_key=var_set_key,
        )
        values = update_value_list({'values': ori_record['values']}, update_values(new_values))
        values['ownedBy'] = ori_record['ownedBy']
        r = requests.post(
            url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': header_token,
            },
            json=values,
        )
        logger.debug(f'update record {record_id} -> {values}')
        return r

    var_set = VarSetSingleton(var_set_key)
    return update_record_request(var_set.base_url, get_header_token(var_set_key),)


def create_record(*, namespace_id, module_id, new_values, var_set_key='base'):
    @check_response(error_msg=f'failed to create record')
    def create_record_request(base_url, header_token) -> requests.Response:
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/'
        valuse = update_values(new_values)
        r = requests.post(
            url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': header_token,
            },
            json=valuse,
        )
        return r

    var_set = VarSetSingleton(var_set_key)
    return create_record_request(
        base_url=var_set.base_url, header_token=get_header_token(var_set_key)
    )


def delete_record(*, namespace_id, module_id, record_id, var_set_key='base'):
    @check_response(error_msg=f'failed to create record')
    def delete_record_request(base_url, header_token) -> requests.Response:
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/{record_id}'
        r = requests.delete(
            url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': header_token,
            },
        )
        return r

    var_set = VarSetSingleton(var_set_key)
    return delete_record_request(var_set.base_url, get_header_token(var_set_key),)


def get_file_details(*, namespace_id, attachment_id, var_set_key='base'):
    @check_response(error_msg='failed to get attachment info')
    def get_file_details_request(base_url, header_token):
        url = f'{base_url}/namespace/{namespace_id}/attachment/record/{attachment_id}'
        return requests.get(url, headers={'Authorization': header_token})

    var_set = VarSetSingleton(var_set_key)
    return get_file_details_request(var_set.base_url, get_header_token(var_set_key))


def delete_file(*, namespace_id, attachment_id, var_set_key='base'):
    @check_response(error_msg='failed to delete attachment')
    def delete_file_details_request(base_url, header_token):
        url = f'{base_url}/namespace/{namespace_id}/attachment/record/{attachment_id}'
        return requests.delete(url, headers={'Authorization': header_token})

    var_set = VarSetSingleton(var_set_key)
    return delete_file_details_request(var_set.base_url, get_header_token(var_set_key))


def download_file(*, namespace_id, attachment_id, save_path, var_set_key='base'):
    def download_file_request(file_url, save_path, base_url, header_token):
        url = f'{base_url}{file_url}'
        r = requests.get(url, headers={'Authorization': header_token})
        r.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(r.content)
        return r

    var_set = VarSetSingleton(var_set_key)
    return download_file_request(
        get_file_details(
            namespace_id=namespace_id, attachment_id=attachment_id, var_set_key=var_set_key,
        ).json()['response']['url'],
        save_path,
        var_set.base_url,
        get_header_token(var_set_key),
    )


def upload_file(
    *,
    namespace_id,
    module_id,
    record_id,
    upload_field: str,
    upload_file_path: Union[str, Iterable[str]],
    var_set_key='base',
):
    @check_response(error_msg='failed to create file attachment')
    def create_attachment(upload_file, base_url, header_token):
        filename = upload_file
        upload_fp = open(filename, 'rb')
        filename = filename.split('/')[-1]
        url = f'{base_url}/namespace/{namespace_id}/module/{module_id}/record/attachment'
        r = requests.post(
            url,
            headers={'Authorization': header_token,},
            data={'fieldName': upload_field, 'recordID': record_id},
            files={'upload': (filename, upload_fp)},
        )
        return r

    def upload_file_request(base_url, header_token):
        if isinstance(upload_file_path, str):
            attachment_id = as_dict(create_attachment, upload_file_path, base_url, header_token)[
                'attachmentID'
            ]
        elif isinstance(upload_file_path, list):
            attachment_id = [
                as_dict(create_attachment, file_path, base_url, header_token)['attachmentID']
                for file_path in upload_file_path
            ]
        else:
            raise TypeError(
                f'upload_file_path must be a string or a list, got {type(upload_file_path)}'
            )
        return update_record(
            namespace_id=namespace_id,
            module_id=module_id,
            record_id=record_id,
            new_values={upload_field: attachment_id},
            var_set_key=var_set_key,
        )

    var_set = VarSetSingleton(var_set_key)
    return upload_file_request(var_set.base_url, get_header_token(var_set_key))
