# -*- coding: utf-8 -*-

import argparse
import importlib
import json
import traceback
from typing import List, Type, Union

import configs
from configs.utils import Env
from core.job.registry import JobNode, JobRegistry
from core.tr_logging.hive import HiveLoggerHandler
from core.utils import TimeMonitor
from jobs.base import ConfiglessJob, ConfigurableJob
from jobs.spark.prepare import PrepareTable

_exec_dir = '/'.join(configs.__file__.split('/')[:-3])
print('exec_dir: ', _exec_dir)

from configs import Context
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder

logger = tr_logger.getChild('entrypoint')


class Entrypoint:
    _module_job_tpl = '{module}.{job}'

    def __init__(self, dag_id, batch_id, job: JobNode, config_path):
        self.dag_id = dag_id
        self.batch_id = batch_id
        self.job = job

        cfg = json.load(open(config_path))

        self.ctx = Context(
            dag_id=dag_id,
            batch_id=batch_id,
            job_name=job.clz.__name__,
            module_name=job.clz.__module__,
            runtime_config=cfg,
            config_path=config_path,
        )

    def build_jobs(self):
        import_module_path = self._module_job_tpl.format(
            module=self.ctx.module_name, job=self.ctx.job_name
        )
        dot_index = import_module_path.rindex('.')
        module = importlib.import_module(import_module_path[:dot_index])

        instance = getattr(module, import_module_path[dot_index + 1 :])(self.ctx)
        return [instance]

    def run(self, jobs: List[Union[ConfigurableJob, ConfiglessJob]]):
        for j in jobs:
            j.run()
            j.show()


class SparkEntrypoint(Entrypoint):

    _jobs_without_hive_log = [PrepareTable]

    def __init__(self, *args):
        super().__init__(*args)

        self._ensure_tables()
        if self._job_in(self._jobs_without_hive_log):
            TRLogging.once(ColorLoggerHanlder())
        else:
            TRLogging.once(ColorLoggerHanlder(), HiveLoggerHandler(self.ctx))

    def _job_in(self, jobs: List[Type]):
        for job in jobs:
            if self.ctx.job_name == job.__name__:
                return True
        return False

    def _ensure_tables(self):
        # 本地测试时，多任务无法共享warehouse，所以warehouse分离，每个都得重新建表
        if Env.is_local and not self._job_in([PrepareTable]):
            PrepareTable(self.ctx).run()


class LocalEntrypoint(Entrypoint):
    def __init__(self, *args):
        super().__init__(*args)
        TRLogging.once(ColorLoggerHanlder())


class EntrypointFactory:

    _ENTRYPOINT_MAP = {
        'local': LocalEntrypoint,
        'spark': SparkEntrypoint,
    }

    @staticmethod
    def create(args) -> Entrypoint:
        job_node: JobNode = JobRegistry.lookup(args.job_name)
        entrypoint = EntrypointFactory._ENTRYPOINT_MAP[job_node.clz.__module__.split('.')[-2]]
        return entrypoint(args.dag_id, args.batch_id, job_node, args.config_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dag_id', type=str)
    parser.add_argument('--batch_id', type=str)
    parser.add_argument('--job_name', type=str)
    parser.add_argument('--config_path', type=str, default=_exec_dir + '/runtime_config.json')
    args = parser.parse_args()
    TimeMonitor.enable = True

    entrypoint = EntrypointFactory.create(args)
    try:
        entrypoint.run(entrypoint.build_jobs())
    except BaseException as e:
        err_str = traceback.format_exc()
        logger.error(f'job runtime error, error: {e}, traceback: {err_str}')
        raise e
    finally:
        TimeMonitor.report()
