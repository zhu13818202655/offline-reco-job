# -*- coding: utf-8 -*-
# @File : init_lowcode_record.py
# @Author : r.yang
# @Date : Tue Nov 22 17:46:39 2022
# @Description :


import argparse
import logging
import os

import yaml

parser = argparse.ArgumentParser()
parser.add_argument('--env', type=str, choices=['prod', 'uat', 'pi', 'local'], default='prod')
parser.add_argument('--type', type=str, choices=['dag', 'scheduler'], default='dag')
parser.add_argument('--output_path', type=str)
args = parser.parse_args()

os.environ['RECO_ENVIRONMENT'] = args.env


from client.lowcode.v1 import LowCode
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder
from core.utils import timeit
from jobs.init.fetch_lowcode_config import fetch_aio_cfg, fetch_scheduler_cfg

TRLogging.once(ColorLoggerHanlder())


lowcode = LowCode(
    base_url='http://172.18.192.89:38073/api/compose',
    auth_url='http://172.18.192.89:38073/auth/oauth2/token',
    secret='wNqSXlY2nTOdJZnFLgWDnGkp81rEwteanucbxOiLfVpwer0yURECrRmIFzlS9uZM',
    client_id='311858455747009744',
).with_ns_by_handle('bos')
logger = tr_logger.getChild('lowcode')


@timeit(level=logging.INFO)
def main():

    if args.type == 'dag':
        cfg = fetch_aio_cfg(lowcode)
        with open(args.output_path, 'w') as f:
            yaml.dump(cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
    else:
        cfg = fetch_scheduler_cfg(lowcode, True)
        with open(args.output_path, 'w') as f:
            yaml.dump(cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)


if __name__ == '__main__':
    main()
