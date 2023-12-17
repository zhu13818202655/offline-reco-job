# -*- coding: utf-8 -*-
# @File : init_dag_config.py
# @Author : r.yang
# @Date : Sat Jan 15 19:45:16 2022
# @Description :

import argparse
import json
import os
from datetime import datetime

import yaml

from client.offline_backend import OfflineBackendClient
from configs import merge_cfg
from configs.model import AllInOneConfig, PostProcessDagConfig, RecoDagConfig
from configs.model.config import BaseDagConfig
from configs.utils import ConfigRegistry, DagType
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder

logger = tr_logger.getChild('jobs.init')
_MAGIC_FUNC = open  # ADHOC for traversal check


class MysqlFetcher:
    __doc__ = """未来对接mysql配置源
    """

    def __init__(self, args) -> None:
        self.client = OfflineBackendClient(args.offline_backend_addr, 'reco')

    def get_dag_type(self, batch_id, dag_id) -> DagType:
        dags = self.client.get('/dags', {'batch_id': batch_id})
        dag_meta_map = {i['dag_id']: i for i in dags}
        try:
            return DagType(dag_meta_map[dag_id]['dag_type'])
        except:
            return None

    def get_dag_config(self, batch_id, dag_id) -> dict:
        dag_configs = self.client.get('/dag_configs', {'batch_id': batch_id})
        dag_config_map = {i['dag_id']: i for i in dag_configs}
        try:
            return dag_config_map[dag_id]['content']
        except:
            return {}


class LocalFetcher:
    def __init__(self, args) -> None:
        self.aio_cfg = AllInOneConfig(**yaml.safe_load(_MAGIC_FUNC(args.config_path)))
        logger.info(f'init AllInOneConfig from local: {args.config_path}')
        logger.info(f'config: {json.dumps(self.aio_cfg.dict(), indent=4, ensure_ascii=False)}')

    def get_dag_type(self, batch_id, dag_id) -> DagType:
        for dag in self.aio_cfg.reco_dags + self.aio_cfg.post_proc_dags + [self.aio_cfg.base_dag]:
            if dag.dag_id == dag_id:
                if isinstance(dag, RecoDagConfig):
                    return DagType.Reco
                elif isinstance(dag, PostProcessDagConfig):
                    return DagType.PostProc
                return DagType.Base

        return None

    def get_dag_config(self, batch_id, dag_id) -> dict:
        for dag in self.aio_cfg.reco_dags + self.aio_cfg.post_proc_dags + [self.aio_cfg.base_dag]:
            if dag.dag_id == dag_id:
                return dag.dict()
        return {}


def gen_dag_config(fetcher, dag_id, batch_id):

    dag_type = fetcher.get_dag_type(batch_id, dag_id)
    dag_config = fetcher.get_dag_config(batch_id, dag_id)

    if dag_type is None or not dag_config:
        logger.error(f'dag: {dag_id}, batch_id: {batch_id} not exist')
        raise IndexError(f'dag: {dag_id}, batch_id: {batch_id} not exist')

    if dag_type != DagType.Base:
        base_config = fetcher.get_dag_config(batch_id, 'base')['base']
        if dag_type == DagType.PostProc:
            return ConfigRegistry.wrap_cfg(
                merge_cfg(dag_config, base_config=base_config,), PostProcessDagConfig,
            )
        elif dag_type == DagType.Reco:
            return ConfigRegistry.wrap_cfg(
                merge_cfg(dag_config, base_config=base_config,), RecoDagConfig,
            )
    return ConfigRegistry.wrap_cfg(dag_config, BaseDagConfig)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--dag_id', type=str)
    parser.add_argument('--batch_id', default=datetime.today().strftime('%Y%m%d'), type=str)
    parser.add_argument('--output_dir', type=str, default='./output')

    # mysql
    parser.add_argument('--offline_backend_addr', type=str)

    # local
    parser.add_argument('--config_path', type=str)
    args = parser.parse_args()

    TRLogging.once(ColorLoggerHanlder())
    if args.config_path:
        fetcher = LocalFetcher(args)
    elif args.offline_backend_addr:
        fetcher = MysqlFetcher(args)
    else:
        raise ValueError

    dag_config = gen_dag_config(fetcher, args.dag_id, args.batch_id)
    if not dag_config:
        return

    output_dir = os.path.join(args.output_dir, args.batch_id)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'{args.dag_id}.json')

    with _MAGIC_FUNC(output_path, 'w') as f:
        json.dump(dag_config, f, indent=4, ensure_ascii=False)
    logger.info('config json is dumped to %s', output_path)


if __name__ == '__main__':
    main()
