import functools
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

import requests

from core.logger import tr_logger

from .model import (
    ApiError,
    ApiReturnType,
    DictValueType_,
    LowCodeModule,
    LowCodeNamespace,
    LowCodeRecord,
    RecordValueType_,
)


def require_args(require_args: List[str]):
    """
    This decorator is to constraint arguments for functions in LowCodeClient::work_chain
        to garentee that kwargs has the arguments needed

    @require_args(['bar'])
    def foo(self,rtn,**kwargs):
        print(kwargs['bar'])

    foo(bar='hello world') -> success

    foo() -> assert error. argument 'bar' is missing

    """

    def require_args_check(func):
        @wraps(func)
        def arg_checker(self, *args, **kwargs):
            for k in require_args:
                if k not in kwargs.keys():
                    msg = (
                        f'{self.__class__.__module__}.{self.__class__.__qualname__}.{func}({", ".join(require_args)}),'
                        f' got {", ".join(kwargs.keys())}. missing args: {k}'
                    )
                    self.logger.critical(msg)
                    raise AssertionError(msg)
            return func(self, *args, **kwargs)

        return arg_checker

    return require_args_check


class LowCodeClient:
    """
    :param base_url: something end with "/api/compose"
    :param auth_url: something end with "/auth/oauth2/token"

    to carry multiple set of configs, just change this class var and use cls_var_hub with another key
    """

    cls_var_hub = {
        'base': {
            'token': None,
            'update_time': 0,
            'base_url': '',
            'auth_url': '',
            'secret': '',
            'client_id': '',
            'max_retry': 10,  # max retry times
            'max_retry_interval': 10,  # retry interval = min(retry_time+retry_time**2,max_retry_interval)
        }
    }

    def __init__(self, url: str, var_set='base'):
        """
        garuntee url not end with '/'
        """

        self.var_set = var_set
        if 'token' not in self.cls_var_hub[self.var_set]:
            self.cls_var_hub[self.var_set]['token'] = None
        if 'update_time' not in self.cls_var_hub[self.var_set]:
            self.cls_var_hub[self.var_set]['update_time'] = 0

        if self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
        if not url.startswith('/'):
            url = '/' + url
        if url.endswith('/'):
            url = url[:-1]
        self.url = self.base_url + url
        self.logger = self.setup_logger()

        self.max_retry = self.cls_var_hub[self.var_set]['max_retry']
        self.max_retry_interval = self.cls_var_hub[self.var_set]['max_retry_interval']

        self.work_chain: List[Callable] = [self.check_token]

    def check_token(self, rtn: Dict, **kwargs) -> Any:
        if self.token is None or time.time() - self.token_time > 60 * 60 * 1.9:
            self.token = self.get_token()
            self.token_time = time.time()
            self.logger.info(f'Token update {self.token}')

    @property
    def base_url(self):
        return self.cls_var_hub[self.var_set]['base_url']

    @base_url.setter
    def base_url(self, url: str):
        self.cls_var_hub[self.var_set]['base_url'] = url

    @property
    def auth_url(self):
        return self.cls_var_hub[self.var_set]['auth_url']

    @property
    def secret(self):
        return self.cls_var_hub[self.var_set]['secret']

    @property
    def client_id(self):
        return self.cls_var_hub[self.var_set]['client_id']

    @property
    def token(self):
        return self.cls_var_hub[self.var_set]['token']

    @token.setter
    def token(self, value):
        self.cls_var_hub[self.var_set]['token'] = value

    @property
    def token_time(self):
        return self.cls_var_hub[self.var_set]['update_time']

    @token_time.setter
    def token_time(self, value):
        self.cls_var_hub[self.var_set]['update_time'] = value

    @property
    def header_token(self):
        return f"Bearer {self.cls_var_hub[self.var_set]['token']}"

    def setup_logger(self):
        self.logger = tr_logger.getChild('lowcode_api')
        return self.logger

    def __call__(self, **kwargs) -> ApiReturnType:
        """
        NOTE:

            * all functions of self.work_chain is called in order

            * the function must have arguments: (self, rtn_data:dict, **kwargs)

            * rtn_data is a dictionary containing all data modified by functions in work_chain

            * all functions return value are added to rtn_data.

            * you should update rtn_data dict and return this whold dict as the next function's input
        """
        rtn_data = {}
        retry = self.max_retry
        max_interval = self.max_retry_interval
        for fn in self.work_chain:
            exception = None
            for i in range(retry):
                try:
                    rtn = fn(rtn_data, **kwargs)
                    break
                except ApiError as e:
                    exception = e
                    self.logger.error(f'error in trying {i + 1}/{retry} {fn} args:{kwargs}.')
                    time.sleep(min(1 + i + i * i, max_interval))
            if exception is not None:
                raise exception
            if rtn is not None:
                rtn_data = rtn
        return rtn_data

    def check_response(self, r: requests.Response, error_msg: str = None):
        try:
            if r.status_code != 200 or 'error' in r.json():
                self.logger.error(r.text)
                self.logger.error(f'{r.request.url},{r.request.body}')
                if error_msg:
                    self.logger.error(str(error_msg))
                raise ApiError(str(r.text))
        except requests.JSONDecodeError as e:
            self.logger.error(str(error_msg))
            raise ApiError(str(e))

    def get_token(self):
        r = requests.post(
            self.auth_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded',},
            data={'grant_type': 'client_credentials', 'scope': 'api'},
            auth=(self.client_id, self.secret),
        )
        self.check_response(r, 'Failed to get token')
        resp = r.json()
        token = resp['access_token']
        return token


class GetRecordList(LowCodeClient):
    """
    Call args:
        module_id: str
        query: Optional[str]
                SQL query string, e.g.:
                    key='value' AND (key1 = 'value1' OR (key2 != 'value2' OR key3 is null))
                notice: must use '' not "" in string.
        sort: Optional[str]
                SQL sort, e.g.:
                    name DESC
                    createdAt DESC, id ASC

    .. code-block:: python
        >>> get_record_list_fn = GetRecordList(namespace_id='123456789')
        >>> record_list:List[LowCodeRecord] = get_record_list_fn(
        >>>     module_id='123123123',
        >>>     query="status='ok'",
        >>>     sort="name DESC, id ASC",
        >>>     limit=10
        >>> )['record_list_obj']

    return:
        record_list_obj:List[LowCodeRecord]
        record_list_json:List[Dict]
    """

    def __init__(self, namespace_id, var_set='base'):
        url = f'/namespace/{namespace_id}/module'
        super().__init__(url, var_set=var_set)
        self.work_chain.append(self.get_record_list)

    @require_args(['module_id'])
    def get_record_list(self, rtn_data: Dict, **kwargs):
        url = self.url + f"/{kwargs['module_id']}/record/"
        has_arg = False
        if 'query' in kwargs and kwargs['query'] is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            query: Optional[str] = kwargs['query']
            url += f'query=({query})'
        if 'sort' in kwargs and kwargs['sort'] is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            else:
                url += '&'
            sort: str = kwargs['sort']
            url += f'sort={sort}'
        if 'limit' in kwargs and kwargs['limit'] is not None:
            if not has_arg:
                url += '?'
                has_arg = True
            else:
                url += '&'
            limit: int = kwargs['limit']
            url += f'limit={limit}'

        r = requests.get(url, headers={'Authorization': self.header_token})
        self.check_response(r, 'failed to get record list')
        record_set: List[Dict[str, Any]] = r.json()['response']['set']
        record_list: List[LowCodeRecord] = [LowCodeRecord.parse_obj(rec) for rec in record_set]
        rtn_data['record_list_obj'] = record_list
        rtn_data['record_list_json'] = record_set
        return rtn_data


class ListNamespace(LowCodeClient):
    def __init__(self, var_set='base'):
        url = f'/namespace'
        super().__init__(url, var_set=var_set)

    @functools.lru_cache(1)
    def list_namespace(self) -> List[LowCodeNamespace]:
        self.check_token({})
        self.logger.info(f'url: {self.url}')
        r = requests.get(self.url + '/', headers={'Authorization': self.header_token})
        assert r.status_code == 200, f'{r.__dict__}'
        nss: List[LowCodeNamespace] = [
            LowCodeNamespace.parse_obj(i) for i in r.json()['response']['set']
        ]
        return nss


class ListModule(LowCodeClient):
    def __init__(self, namespace_id, var_set='base'):
        url = f'/namespace/{namespace_id}/module'
        super().__init__(url, var_set=var_set)
        self.work_chain.append(self.list_module)

    @functools.lru_cache(1)
    def list_module(self) -> List[LowCodeModule]:
        self.check_token({})
        self.logger.info(f'url: {self.url}')
        r = requests.get(self.url + '/', headers={'Authorization': self.header_token})
        assert r.status_code == 200, f'{r.__dict__}'
        ms: List[LowCodeModule] = [LowCodeModule.parse_obj(i) for i in r.json()['response']['set']]
        return ms


class RecordAPI(LowCodeClient):
    def __init__(self, namespace_id, module_id, var_set='base'):
        url = f'/namespace/{namespace_id}/module/{module_id}/record'
        super().__init__(url, var_set=var_set)

    @staticmethod
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

    @staticmethod
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


class GetRecord(RecordAPI):
    """
    Call args:
        record_id
    return:
        record:Dict :record dict
        record_obj: LowCodeRecord object
    """

    def __init__(self, namespace_id, module_id, var_set='base'):
        super().__init__(namespace_id, module_id, var_set=var_set)
        self.work_chain.append(self.get_record)

    @require_args(['record_id'])
    def get_record(self, rtn_data: Dict, **kwargs):
        url = self.url + '/' + str(kwargs['record_id'])
        r = requests.get(url, headers={'Authorization': self.header_token})
        self.check_response(r, f"Failed to get record {kwargs['record_id']}")
        rtn_data['record'] = r.json()['response']
        record: LowCodeRecord = LowCodeRecord.parse_obj(rtn_data['record'])
        rtn_data['record_obj'] = record
        return rtn_data


class UpdateRecord(GetRecord):
    """
    Call args:
        record_id
        new_values: Dict[str, Any]  { k1: v1, k2: [v2,v3,v4] }

    return:
        new_record: Dict
        new_record_obj: LowCodeRecord
        return by GetRecord:
            record:Dict: original record dict
            record_obj: original LowCodeRecord object
    """

    def __init__(self, namespace_id, module_id, var_set='base'):
        super().__init__(namespace_id, module_id, var_set=var_set)
        self.work_chain.append(self.update_record)

    @require_args(['record_id', 'new_values'])
    def update_record(self: 'UpdateRecord', rtn_data: Dict, **kwargs) -> Dict:
        values: Dict[str, List[Dict]] = {'values': rtn_data['record']['values']}
        new_values: Dict[str, List[Dict]] = self.update_values(kwargs['new_values'])
        values = UpdateRecord.update_value_list(values, new_values)
        values['ownedBy'] = rtn_data['record']['ownedBy']
        r = requests.post(
            self.url + '/' + kwargs['record_id'],
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': self.header_token,
            },
            json=values,
        )
        self.check_response(r)
        rtn_data['new_record'] = r.json()['response']
        self.logger.debug(f'update record {kwargs["record_id"]} -> {values}')
        record: LowCodeRecord = LowCodeRecord.parse_obj(rtn_data['record'])
        rtn_data['new_record_obj'] = record
        return rtn_data


class CreateRecord(RecordAPI):
    """
    return:
        new_record: Dict
        new_record_obj: LowCodeRecord
    """

    def __init__(self, namespace_id, module_id, var_set='base'):
        super().__init__(namespace_id, module_id, var_set=var_set)
        self.url = self.url + '/'
        self.work_chain.append(self.create_record)

    @require_args(['new_values'])
    def create_record(self, rtn_data: Dict, **kwargs):
        values = UpdateRecord.update_values(kwargs['new_values'])
        r = requests.post(
            self.url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': self.header_token,
            },
            json=values,
        )
        self.check_response(r)
        rtn_data['new_record'] = r.json()['response']
        rtn_data['new_record_obj'] = LowCodeRecord.parse_obj(rtn_data['new_record'])
        return rtn_data


class DeleteRecord(GetRecord):
    """
    return:
        same as GetRecord
    """

    def __init__(self, namespace, module, var_set='base'):
        super().__init__(namespace, module, var_set=var_set)
        self.work_chain.append(self.delete_record)

    @require_args(['record_id'])
    def delete_record(self, rtn_data: Dict, **kwargs):
        url = self.url + '/' + str(kwargs['record_id'])
        r = requests.delete(
            url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': self.header_token,
            },
        )
        self.check_response(r)
        return rtn_data


class UploadFile(LowCodeClient):
    """
    Call args:
        record_id:        str
        upload_field:     str
        upload_file_path: Union[str,List[str]]
    return:
        one of:
            new_attachment: Dict  if upload only one file
            new_attachment_list: List[Dict]  if upload list of file
    """

    def __init__(self, namespace_id, module_id, var_set='base'):
        url = f'/namespace/{namespace_id}/module/{module_id}/record/attachment'
        super().__init__(url, var_set=var_set)
        self.update_record = UpdateRecord(namespace_id, module_id)
        self.work_chain.append(self.upload_file)

    def upload_file_request(self, upload_field, upload_file_path, record_id):
        filename = upload_file_path
        upload_fp = open(filename, 'rb')
        filename = filename.split('/')[-1]
        r = requests.post(
            self.url,
            headers={'Authorization': self.header_token,},
            data={'fieldName': upload_field, 'recordID': record_id},
            files={'upload': (filename, upload_fp)},
        )
        self.check_response(r)
        return r.json()['response']

    @require_args(['record_id', 'upload_field', 'upload_file_path'])
    def upload_file(self, rtn_data, **kwargs):
        upload_to_field = kwargs['upload_field']
        record_id = kwargs['record_id']
        upload_file_path = kwargs['upload_file_path']
        if isinstance(upload_file_path, str):
            rtn_data['new_attachment'] = self.upload_file_request(
                upload_to_field, upload_file_path, record_id
            )
        elif isinstance(upload_file_path, list):
            rtn_data['new_attachment_list'] = []
            for file_path in upload_file_path:
                rtn_data['new_attachment_list'].append(
                    self.upload_file_request(upload_to_field, file_path, record_id)
                )
        else:
            raise TypeError(
                f'upload_file_path must be a string or a list, got {type(upload_file_path)}'
            )
        if 'new_attachment' in rtn_data:
            attachment_id = rtn_data['new_attachment']['attachmentID']
        else:
            attachment_id = [
                attachment['attachmentID'] for attachment in rtn_data['new_attachment_list']
            ]
        rtn = self.update_record(
            record_id=kwargs['record_id'], new_values={upload_to_field: attachment_id},
        )
        rtn_data.update(rtn)
        return rtn_data


class GetFile(LowCodeClient):
    """
    Call args:
        attachment_id
    return
        attachment_detail
    """

    def __init__(self, namespace_id, var_set='base'):
        url = f'/namespace/{namespace_id}/attachment/record'
        super().__init__(url, var_set=var_set)
        self.work_chain.append(self.get_file_details)

    @require_args(['attachment_id'])
    def get_file_details(self: 'DownloadFile', rtn_data: Dict[str, Any], **kwargs):
        url = f"{self.url}/{str(kwargs['attachment_id'])}"
        r = requests.get(url, headers={'Authorization': self.header_token})
        self.check_response(r, f"Failed to get attachment details {kwargs['attachment_id']}")
        rtn_data['attachment_detail'] = r.json()['response']
        return rtn_data


class DownloadFile(GetFile):
    """
    Call args:
        attachment_id
    return
        attachment_detail
    """

    def __init__(self, namespace_id, var_set='base'):
        super().__init__(namespace_id, var_set=var_set)
        self.work_chain.append(self.download_file)

    @require_args(['attachment_id', 'save_path'])
    def download_file(self: 'DownloadFile', rtn_data: Dict[str, Any], **kwargs):
        url = self.base_url + rtn_data['attachment_detail']['url']
        r = requests.get(url, headers={'Authorization': self.header_token})
        if r.status_code != 200:
            self.logger.error('Failed to download attachment')
            self.logger.error(r.text)
        try:
            if r.json().get('error', None) is not None:
                self.logger.error(r.text)
                raise ApiError('error to download_file')
        except requests.JSONDecodeError:
            pass

        with open(kwargs['save_path'], 'wb') as f:
            f.write(r.content)
        return rtn_data


class DeleteFile(GetFile):
    """
    Call args:
        attachment_id
    return
        attachment_detail
    """

    def __init__(self, namespace_id, var_set='base'):
        super().__init__(namespace_id, var_set=var_set)
        self.work_chain.append(self.delete_file)

    @require_args(['attachment_id'])
    def delete_file(self, rtn_data: Dict[str, Any], **kwargs):
        url = f"{self.url}/{str(kwargs['attachment_id'])}"
        r = requests.delete(url, headers={'Authorization': self.header_token})
        self.check_response(r, f"Failed to get attachment details {kwargs['attachment_id']}")
        return rtn_data


# High level API
class LowCode:
    def __init__(
        self,
        base_url,
        auth_url,
        secret,
        client_id,
        max_retry=10,
        max_retry_interval=10,
        var_set='base',
    ):
        """
        :param base_url: something end with "/api/compose"
        :param auth_url: something end with "/auth/oauth2/token"
        :param secret:
        :param client_id:
        :param namespace_id:
        :param var_set:
        """
        LowCodeClient.cls_var_hub[var_set] = {
            'token': None,
            'update_time': 0,
            'base_url': base_url,
            'auth_url': auth_url,
            'secret': secret,
            'client_id': client_id,
            'max_retry': max_retry,
            'max_retry_interval': max_retry_interval,
        }
        self.var_set = var_set
        self.namespace_id = None
        self.get_record_list_fn = None
        self.get_record_list_fn = None

    def with_ns(self, ns: LowCodeNamespace):
        self.namespace_id = ns.namespace_id

        self.get_record_list_fn = GetRecordList(self.namespace_id, var_set=self.var_set)
        self.download_file_fn = DownloadFile(self.namespace_id, var_set=self.var_set)

        return self

    def with_ns_by_handle(self, handle: str):
        ns = self.get_namespace_by_handle(handle)
        self.namespace_id = ns.namespace_id

        self.get_record_list_fn = GetRecordList(self.namespace_id, var_set=self.var_set)
        self.download_file_fn = DownloadFile(self.namespace_id, var_set=self.var_set)

        return self

    def get_namespace_by_handle(self, handle: str) -> LowCodeNamespace:
        nss: List[LowCodeNamespace] = ListNamespace().list_namespace()
        for ns in nss:
            if ns.handle == handle:
                return ns
        raise KeyError

    @functools.lru_cache()
    def get_modules(self) -> List[LowCodeModule]:
        ms: List[LowCodeModule] = ListModule(self.namespace_id).list_module()
        return ms

    def get_module_by_handle(self, handle: str) -> LowCodeModule:
        ms = self.get_modules()
        for m in ms:
            if m.handle == handle:
                return m
        raise KeyError

    def get_record_list(self, module_id, **kwargs) -> List[LowCodeRecord]:
        if self.get_record_list_fn:
            return self.get_record_list_fn(module_id=module_id, **kwargs)['record_list_obj']
        else:
            raise ValueError

    def get_record(self, module_id, record_id):
        return GetRecord(self.namespace_id, module_id, var_set=self.var_set)(record_id=record_id)

    def update_record(self, module_id, record_id, new_values):
        return UpdateRecord(self.namespace_id, module_id, var_set=self.var_set)(
            record_id=record_id, new_values=new_values
        )

    def create_record(self, module_id, new_values):
        return CreateRecord(self.namespace_id, module_id, var_set=self.var_set)(
            new_values=new_values
        )

    def delete_record(self, module_id, record_id):
        return DeleteRecord(self.namespace_id, module_id, var_set=self.var_set)(record_id=record_id)

    def upload_file(self, module_id, record_id, upload_field, upload_file_path):
        return UploadFile(self.namespace_id, module_id, var_set=self.var_set)(
            record_id=record_id, upload_field=upload_field, upload_file_path=upload_file_path,
        )

    def download_file(self, attachment_id, save_path):
        if self.download_file_fn:
            return self.download_file_fn(attachment_id=attachment_id, save_path=save_path)
        else:
            raise ValueError
