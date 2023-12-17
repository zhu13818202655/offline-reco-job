from typing import Any, List

from core.logger import tr_logger

logger = tr_logger.getChild('core.job')


class IOutputTable:
    @staticmethod
    def output_table_info_list(ctx):
        logger.warning(f'job has no output table')
        return []


class WithContext:
    _ctx: Any

    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def ctx(self):
        return self._ctx


class DagRuntimeConfigDAO:
    def get(self, ctx):
        raise NotImplementedError

    def create(self, ctx, config):
        raise NotImplementedError

    def list_batch_id(self, ctx) -> List[str]:
        raise NotImplementedError


class Job:
    def run(self):
        raise NotImplementedError

    def show(self):
        pass
