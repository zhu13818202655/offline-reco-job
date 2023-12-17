# -*- coding: utf-8 -*-
# @File : sv_mocker.py
# @Author : r.yang
# @Date : Fri May 13 16:45:11 2022
# @Description : mock offline reco servie

from typing import Any, Callable, Union

import requests


class MockClient:
    def __init__(self):
        self.session = requests.Session()
        # self.adapter = requests_mock.Adapter()
        # self.session.mount('http://', self.adapter)  # deprecated
        self.rtns = {}
        self.check_cb = {}

    def register_rtn(self, method: str, route: str, rtn: Any):
        self.rtns[(method, route.lstrip('/'))] = rtn

    def register_check_callback(self, method: str, route: str, cb: Callable[[Any], None]):
        self.check_cb[(method, route.lstrip('/'))] = cb

    def _request(self, method, route, body):
        cb = self.check_cb.get((method, route.lstrip('/')))
        if cb:
            cb(body)
        return self.rtns[(method, route.lstrip('/'))]

    def post(
        self,
        route: str,
        body: Union[dict, list, None] = None,
        json: Union[dict, None] = None,
        **kwargs,
    ) -> Any:
        if not body and json:
            body = json
        return self._request('post', route, body)

    def get(self, route: str, params: Union[dict, list, None] = None) -> Any:
        return self._request('get', route, params)

    def delete(self, route: str, params: Union[dict, list, None] = None) -> Any:
        return self._request('delete', route, params)

    def patch(self, route: str, params: Union[dict, list, None] = None) -> Any:
        return self._request('patch', route, params)
