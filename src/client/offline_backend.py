# -*- coding: utf-8 -*-
# @File : offline_backend.py
# @Author : r.yang
# @Date : Sun Jan 16 23:25:30 2022
# @Description : offline rec backend svc

from typing import Any, Union

import requests

from core.utils import try_invoke


class OfflineBackendClient:

    _headers = {'Content-Type': 'application/json'}

    def __init__(self, addr: str, prefix: str = 'reco'):
        self.addr = f'http://{addr.replace("http://", "")}/{prefix}'

    def _address(self, route: str):
        return self.addr + '/' + route.lstrip('/')

    @try_invoke
    def post(self, route: str, body: Union[dict, list]) -> Any:
        return requests.post(self._address(route), json=body, headers=self._headers)

    @try_invoke
    def get(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.get(self._address(route), params=params, headers=self._headers)

    @try_invoke
    def delete(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.delete(self._address(route), params=params, headers=self._headers)

    @try_invoke
    def patch(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.patch(self._address(route), json=params, headers=self._headers)

    def raw_post(self, route: str, body: Union[dict, list]) -> Any:
        return requests.post(self._address(route), json=body, headers=self._headers)

    def raw_get(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.get(self._address(route), params=params, headers=self._headers)

    def raw_delete(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.delete(self._address(route), params=params, headers=self._headers)

    def raw_patch(self, route: str, params: Union[dict, list] = None) -> Any:
        return requests.patch(self._address(route), json=params, headers=self._headers)
