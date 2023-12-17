# -*- coding: utf-8 -*-
# @File : startup.py
# @Author : r.yang
# @Date : Tue Jan 11 15:48:37 2022
# @Description : 导入场景、场景支持的渠道、场景默认配置

import argparse
import os
from datetime import datetime
from typing import List, Optional

from client.offline_backend import OfflineBackendClient
from configs import load_or_init_dag_cfg
from configs.init import base_config
from configs.model import AllInOneConfig, PostProcessDagConfig, RecoDagConfig
from configs.model.base import BaseConfig, ItemInfo
from configs.model.config import BaseDagConfig, SceneConfig
from configs.utils import DagType, TagSelector
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder
from core.utils import DateHelper, TimeMonitor, timeit

logger = tr_logger.getChild('jobs.init')


@timeit()
def save_dags(batch_id, client, reco_dags, post_proc_dags, base_dag):
    dag_scenes = []
    for dag in reco_dags + post_proc_dags + [base_dag]:
        data = {
            'dag_id': dag.dag_id,
            'dag_name': dag.dag_name,
            'scene_id': dag.scene.scene_id if isinstance(dag, RecoDagConfig) else None,
            'batch_id': batch_id,
        }
        if isinstance(dag, RecoDagConfig):
            data['dag_type'] = DagType.Reco.value
        elif isinstance(dag, PostProcessDagConfig):
            data['dag_type'] = DagType.PostProc.value
        else:
            data['dag_type'] = DagType.Base.value

        dag_scenes.append(data)

    if dag_scenes:
        client.post('/dags', dag_scenes)


@timeit()
def save_activity(
    batch_id,
    client,
    dags: List[PostProcessDagConfig],
    scenes: List[SceneConfig],
    base_cfg: Optional[BaseConfig],
):
    if base_cfg is None:
        raise ValueError('base config is None')

    latest_actvs = client.get('/activities')
    actv_id_to_cnt = {}
    if latest_actvs:
        actv_id_to_cnt = {i['actv_id']: i['user_count'] for i in latest_actvs}

    scene_id_to_name = {s.scene_id: s.scene_name for s in scenes}
    activities = []
    for dag in dags:
        for actv in dag.actvs:
            if not actv.to_BI:
                continue
            channel = base_cfg.get_channel(dag.channel_id, actv.banner_id)
            activities.append(
                {
                    'actv_id': actv.actv_id,
                    'scene_id': actv.scene_id,
                    'scene_name': scene_id_to_name[actv.scene_id]
                    + ('(测试)' if actv.test_user_item else ''),
                    'channel_id': channel.channel_id,
                    'channel_name': channel.channel_name,
                    'banner_id': channel.banner_id,
                    'user_count': actv_id_to_cnt.get(actv.actv_id, 0),
                    'batch_id': batch_id,
                }
            )
    for i, banner_id in enumerate(base_cfg.test_banners):
        activities.append(
            {
                'actv_id': f'T9{i:03d}',
                'scene_id': 'scene_credit_card_ca',
                'scene_name': f'{banner_id}测试',
                'channel_id': 'PMBS_IRE',
                'channel_name': '手机银行',
                'banner_id': banner_id,
                'user_count': 1,
                'batch_id': batch_id,
            }
        )

    client.post('/activities', activities)
    return activities


@timeit()
def save_products(batch_id, client, all_items):

    products = []
    for scene in base_config().scenes:
        items: List[ItemInfo] = TagSelector(all_items).query(**scene.item_selector)
        for item in items:
            for item_id in item.item_ids:
                products.append(
                    {
                        'scene_id': scene.scene_id,
                        'prod_id': item_id,
                        'prod_name': item.desc,
                        'stg_id': '',
                        'batch_id': batch_id,
                    }
                )

    client.post('/products', products)
    return products


@timeit()
def save_configs(
    batch_id,
    client,
    reco_dags: List[RecoDagConfig],
    post_proc_dags: List[PostProcessDagConfig],
    base_dag: BaseDagConfig,
):
    configs = []
    for config in reco_dags + post_proc_dags + [base_dag]:
        configs.append({'dag_id': config.dag_id, 'content': config.dict(), 'batch_id': batch_id})

    client.post('/dag_configs', configs)


def ensure_dir(aio_cfg: AllInOneConfig, batch_id):
    if not aio_cfg.base_dag.base:
        return

    if not os.path.exists(aio_cfg.base_dag.base.nas_output_dir):
        return

    date_helper = DateHelper(batch_id, aio_cfg.base_dag.base.react_delay)
    push_date = date_helper.date_after_react_delay.str
    dir_path = os.path.join(aio_cfg.base_dag.base.nas_output_dir, 'output', push_date)

    os.makedirs(dir_path, exist_ok=True)
    try:
        os.chmod(dir_path, 0o775)
        logger.warning(f'dir: {os.path.dirname(dir_path)} maked and chmod to 0o775')
    except:
        logger.warning(f'dir: {os.path.dirname(dir_path)} already exists')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--offline_backend_addr', type=str)
    parser.add_argument('--config_path', type=str, default='./dag_configs.yaml')
    parser.add_argument('--batch_id', type=str, default=datetime.today().strftime('%Y%m%d'))
    args = parser.parse_args()
    client = OfflineBackendClient(args.offline_backend_addr, 'reco')

    TRLogging.once(ColorLoggerHanlder())
    TimeMonitor.enable = True

    logger.info(f'init aio config from yaml: {args.config_path}')
    aio_cfg = load_or_init_dag_cfg(args.config_path)

    ensure_dir(aio_cfg, args.batch_id)

    save_activity(
        args.batch_id,
        client,
        aio_cfg.post_proc_dags,
        [i.scene for i in aio_cfg.reco_dags],
        aio_cfg.base_dag.base,
    )
    # save_products(
    #     args.batch_id, client, aio_cfg.base_dag.base.items if aio_cfg.base_dag.base else []
    # )

    # # 暂时不需要 dag 和 dag_config
    # save_dags(args.batch_id, client, aio_cfg.reco_dags, aio_cfg.post_proc_dags, aio_cfg.base_dag)
    # save_configs(args.batch_id, client, aio_cfg.reco_dags, aio_cfg.post_proc_dags, aio_cfg.base_dag)

    TimeMonitor.report()


if __name__ == '__main__':
    main()
