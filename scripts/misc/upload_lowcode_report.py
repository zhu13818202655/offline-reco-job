# -*- coding: utf-8 -*-
# @File : upload_lowcode_report.py
# @Author : r.yang
# @Date : Mon Nov 28 10:52:00 2022
# @Description :


import argparse
import os
from datetime import datetime, timedelta

from client.modules import ReportAgg, ReportAggList
from configs.init.common import S_DEBIT
from configs.utils import ProdType
from core.utils import Datetime

parser = argparse.ArgumentParser()
parser.add_argument('--env', type=str, choices=['prod', 'uat', 'pi', 'local'], default='prod')
parser.add_argument('--mode', type=str, choices=['s', 'c'], default='s')
parser.add_argument('--output_path', type=str)
args = parser.parse_args()

os.environ['RECO_ENVIRONMENT'] = args.env


from client.lowcode.v1 import LowCode
from core.logger import tr_logger
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder

TRLogging.once(ColorLoggerHanlder())


lowcode = LowCode(
    base_url='http://172.18.192.89:38073/api/compose',
    auth_url='http://172.18.192.89:38073/auth/oauth2/token',
    secret='wNqSXlY2nTOdJZnFLgWDnGkp81rEwteanucbxOiLfVpwer0yURECrRmIFzlS9uZM',
    client_id='311858455747009744',
).with_ns_by_handle('bos')
logger = tr_logger.getChild('lowcode')


def cross_data_insert():
    import numpy as np

    base_dt = Datetime.from_str('20220101')
    for d in range(1):
        cur_dt = (base_dt + timedelta(days=d)).strftime('%Y-%m-%d')
        for channel_id in ['PSMS_IRE', 'PMBS_IRE']:
            res = []
            banner_ids = [''] if channel_id == 'PSMS_IRE' else ['71_bbcreditCard_tjbk']
            for banner_id in banner_ids:
                for user_type in ['exp', 'exp1', 'ctl']:

                    send = np.random.randint(600000, 1000000)
                    click = np.random.randint(send // 10, send // 5)
                    convert = np.random.randint(click // 10, click // 5)

                    res.append(
                        ReportAgg(
                            dt=cur_dt,
                            user_type=user_type,
                            prod_type=ProdType.DEBIT,
                            scene_id=S_DEBIT.scene_id,
                            operate=0,
                            send=send,
                            click=click,
                            click_ratio=click / max(send, 1),
                            convert=convert,
                            convert_ratio=convert / max(send, 1),
                            during='...',
                            channel_id=channel_id,
                            banner_id=banner_id,
                        )
                    )
            ReportAggList(res).to_lowcode(lowcode, dt=cur_dt, channel_id=channel_id)


def single_data_insert():
    dt = '20221128'
    dt = datetime.strptime(dt, '%Y%m%d').strftime('%Y-%m-%d')
    reports = ReportAggList(
        [
            ReportAgg(
                dt=dt,
                user_type='exp',
                prod_type='debitCard',
                scene_id='test',
                operate=1,
                click=1,
                click_ratio=0.01,
                send=1,
                convert=1,
                convert_ratio=0.003,
                during='weekly',
                channel_id='test',
                banner_id='test',
            )
        ]
    )
    reports.to_lowcode(lowcode, dt=dt, channel_id='test')


def main():
    return {'c': cross_data_insert, 's': single_data_insert,}[args.mode]()


if __name__ == '__main__':
    main()
