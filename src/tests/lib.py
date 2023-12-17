# -*- coding: utf-8 -*-
# @File : lib.py
# @Author : r.yang
# @Date : Tue Feb 22 15:11:40 2022
# @Description : test job libs


from typing import Callable, List, Type, Union

from configs import Context
from core.logger import tr_logger
from jobs.base import ConfiglessJob, ConfigurableJob, IDestructHook

test_logger = tr_logger.getChild('tests')


class JobTestRunner:
    _hooks: List[Callable[[], None]] = []

    def __init__(self, job: Type[Union[ConfigurableJob, ConfiglessJob]]):
        self._job = job

    def run(self, dag_id, batch_id, cb: Callable[[Context], Context] = None):
        test_logger.info(
            f'JobRunner::run(job={self._job.__name__}, dag_id={dag_id}, batch_id={batch_id})'
        )

        ctx = Context.default(dag_id, batch_id, self._job.__name__)
        if cb is not None:
            ctx = cb(ctx)
        job_inst = self._job(ctx)
        job_inst.run()
        job_inst.show()

        if isinstance(job_inst, IDestructHook):
            self.hook(job_inst.fixture_destruct_hook)

        test_logger.info(
            f'JobRunner::end(job={self._job.__name__}, dag_id={dag_id}, batch_id={batch_id})'
        )
        return job_inst

    @classmethod
    def hook(cls, cb: Callable[[], None], pos=None):
        if pos is not None:
            cls._hooks.insert(pos, cb)
        else:
            cls._hooks.append(cb)

    @classmethod
    def destroy(cls):
        for h in cls._hooks:
            h()
