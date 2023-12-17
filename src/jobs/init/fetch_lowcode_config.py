# -*- coding: utf-8 -*-
# @File : fetch_lowcode_config.py
# @Author : r.yang
# @Date : Fri Nov 25 14:43:43 2022
# @Description :

import argparse

import yaml
from pydantic.main import BaseModel

from client.lowcode.v1 import LowCode
from client.modules import (
    ConfigChannel,
    ConfigDag,
    ConfigFeatEngine,
    ConfigGroup,
    ConfigItem,
    ConfigModel,
    ConfigPostProc,
    ConfigReco,
)
from configs.init import aio_config
from configs.model import AllInOneConfig
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder
from scheduler.config.init import get_env_settings, get_scheduler_config
from scheduler.config.model import SchedulerConfig

logger = tr_logger.getChild('jobs.init')
__MAGIC_FUNC = open  # ADHOC for traversal check


def log_diff(a: BaseModel, b: BaseModel):
    import difflib
    from io import StringIO

    def get_lines(x):
        f = StringIO('')
        yaml.dump(x.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
        f.seek(0)
        lines = f.read().splitlines()
        return lines

    alines = get_lines(a)
    blines = get_lines(b)
    logger.info('=' * 16 + ' diff start ' + '=' * 16)
    for diffs in difflib.unified_diff(alines, blines, fromfile='old', tofile='new'):
        print(diffs)
    logger.info('=' * 16 + ' diff end ' + '=' * 16)


def fetch_aio_cfg(lowcode: LowCode) -> AllInOneConfig:
    item = ConfigItem.from_lowcode(lowcode)
    group = ConfigGroup.from_lowcode(lowcode)
    model = ConfigModel.from_lowcode(lowcode)
    reco = ConfigReco.from_lowcode(lowcode)
    feat = ConfigFeatEngine.from_lowcode(lowcode)
    post = ConfigPostProc.from_lowcode(lowcode)
    channel = ConfigChannel.from_lowcode(lowcode)

    # item.pretty_log()
    # group.pretty_log()
    # model.pretty_log()
    # reco.pretty_log()
    # feat.pretty_log()
    # post.pretty_log()
    # channel.pretty_log()

    aio_cfg = aio_config()
    aio_cfg = item.merge_aio_cfg(aio_cfg)
    aio_cfg = channel.merge_aio_cfg(aio_cfg)
    aio_cfg = feat.merge_aio_cfg(aio_cfg)
    aio_cfg = model.merge_aio_cfg(aio_cfg)
    aio_cfg = group.merge_aio_cfg(aio_cfg)
    aio_cfg = reco.merge_aio_cfg(aio_cfg)
    aio_cfg = post.merge_aio_cfg(aio_cfg)

    # logger.info(f'{aio_cfg.json(indent=4, ensure_ascii=False)}')

    log_diff(aio_config(), aio_cfg)
    return aio_cfg


def fetch_scheduler_cfg(lowcode: LowCode, mock=False) -> SchedulerConfig:

    settings = get_env_settings(mock)
    scheduler_cfg = get_scheduler_config(settings)

    cfgs = ConfigDag.from_lowcode(lowcode)
    cfgs.pretty_log()

    scheduler_cfg = cfgs.merge_scheduler_cfg(scheduler_cfg)
    log_diff(get_scheduler_config(settings), scheduler_cfg)
    return scheduler_cfg


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--lowcode_url', type=str)
    parser.add_argument('--auth_secret', type=str)
    parser.add_argument('--auth_client_id', type=str)
    parser.add_argument('--dag_config_path', type=str, help='输出的配置目录')
    parser.add_argument('--scheduler_config_path', type=str, help='输出的配置目录')
    args = parser.parse_args()

    TRLogging.once(ColorLoggerHanlder())

    lowcode = LowCode(
        base_url=args.lowcode_url.rstrip('/') + '/api/compose',
        auth_url=args.lowcode_url.rstrip('/') + '/auth/oauth2/token',
        secret=args.auth_secret,
        client_id=args.auth_client_id,
    ).with_ns_by_handle('bos')

    aio_cfg = fetch_aio_cfg(lowcode)
    with __MAGIC_FUNC(args.dag_config_path, 'w') as f:
        yaml.dump(aio_cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
    logger.info(f'dag config dumped to {args.dag_config_path}')

    scheduler_cfg = fetch_scheduler_cfg(lowcode)
    with __MAGIC_FUNC(args.scheduler_config_path, 'w') as f:
        yaml.dump(scheduler_cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
    logger.info(f'scheduler config dumped to {args.scheduler_config_path}')


if __name__ == '__main__':
    main()
