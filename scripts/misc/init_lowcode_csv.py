# -*- coding: utf-8 -*-
# @File : init_lowcode_provision.py
# @Author : r.yang
# @Date : Thu Nov 24 19:28:08 2022
# @Description : init csv for import


import argparse
import os

import pandas as pd

from core.job.base import IOutputTable
from core.job.registry import JobRegistry
from core.tr_logging.hive import HiveLoggerHandler
from jobs.base import SparkRecoJob  # noqa

parser = argparse.ArgumentParser()
parser.add_argument('--output_dir', default='./output')
parser.add_argument('--env', type=str, choices=['prod', 'uat', 'pi', 'local'], default='uat')
parser.add_argument('--type', default='dag')
args = parser.parse_args()

os.environ['RECO_ENVIRONMENT'] = args.env


def to_csv(records: list):
    output_path = os.path.join(args.output_dir, f'{records[0].__class__.__name__}.csv')
    df = pd.DataFrame([i.dict() for i in records])
    del df['id']
    df.to_csv(output_path, index=False, header=True)


def export_config_items():
    from client.modules import ConfigItem
    from configs.init import aio_config

    aio = aio_config()

    os.makedirs(args.output_dir, exist_ok=True)
    items = ConfigItem.from_aio_cfg(aio)
    to_csv(items)


def export_config_dags():
    from client.modules import ConfigDag
    from scheduler.config.init import EnvSetttings, get_scheduler_config

    cfg = get_scheduler_config(EnvSetttings.construct())

    os.makedirs(args.output_dir, exist_ok=True)
    dags = ConfigDag.from_cfg(cfg)
    to_csv(dags)


def export_table_schema():
    from configs.init import aio_config

    aio = aio_config()
    if not aio.base_dag.base:
        return

    jobs = [i[1].clz for i in JobRegistry.iter_all_jobs()]
    res = []
    for outputTableJob in [HiveLoggerHandler,] + jobs:
        if not issubclass(outputTableJob, IOutputTable):
            continue
        for table_info in outputTableJob.output_table_info_list(aio.base_dag.base):
            for f in table_info['field_list']:
                db, table = table_info['name'].split('.', 2)
                res.append(
                    {
                        'task': outputTableJob.__name__,
                        'database': db,
                        'table_name': table,
                        'table_comment': table_info['comment'],
                        'field_name': f['name'],
                        'field_type': f['type'],
                        'field_comment': f['comment'],
                        'is_partition': False,
                    }
                )
            for f in table_info['partition_field_list']:
                db, table = table_info['name'].split('.', 2)
                res.append(
                    {
                        'task': outputTableJob.__name__,
                        'database': db,
                        'table_name': table,
                        'table_comment': table_info['comment'],
                        'field_name': f['name'],
                        'field_type': f['type'],
                        'field_comment': f['comment'],
                        'is_partition': True,
                    }
                )
    df = pd.DataFrame([i for i in res])
    df.to_csv(os.path.join(args.output_dir, 'table_schema.csv'), index=False, header=True)


def main():
    {'dag': export_config_dags, 'item': export_config_items, 'table': export_table_schema,}[
        args.type
    ]()


if __name__ == '__main__':
    main()
