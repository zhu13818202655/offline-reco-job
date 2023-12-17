# -*- coding: utf-8 -*-
# @File : op_user_test.py
# @Author : r.yang
# @Date : Mon Mar 14 19:16:46 2022
# @Description : format string


import os
from datetime import datetime, timedelta

import pandas as pd


def mock_user_pool(output_dir, base_dt='20211230'):
    user_id = ['{:03d}'.format(i) for i in range(100)]
    user_type = ['ctl' if i.endswith('9') else 'exp' for i in user_id]
    df = pd.DataFrame(data=list(zip(user_id, user_type)), columns=['user_id', 'user_type'])

    base_dt = datetime.strptime(base_dt, '%Y%m%d')
    df['score'] = 1.0
    df['batch_id'] = base_dt.strftime('%Y%m%d')
    df['dt'] = base_dt.strftime('%Y%m%d')
    df['dag_id'] = 'reco_credit_card_ca_0000'

    dfs = [df]
    for i in range(1, 7):
        tmp_df = df.copy(deep=True)
        tmp_df['dt'] = (base_dt + timedelta(days=i)).strftime('%Y%m%d')
        tmp_df['batch_id'] = tmp_df['dt']
        dfs.append(tmp_df)

    df = pd.concat(dfs)
    path = os.path.join(output_dir, './user_pool.csv')
    df.to_csv(path, sep='\x01', header=True, index=False)
    print(f'mock data saved to {path}')


def mock_crowd_package(output_dir, base_dt='20211230'):
    dfs = []
    base_dt = datetime.strptime(base_dt, '%Y%m%d')
    for d in range(7):
        user_id = ['{:03d}'.format(i % 100) for i in range(d * 20, (d + 1) * 20)]
        user_type = ['ctl' if i.endswith('9') else 'exp' for i in user_id]
        df = pd.DataFrame(data=list(zip(user_id, user_type)), columns=['user_id', 'user_type'])
        df['item_id'] = 'XXX'
        df['channel_id'] = 'PSMS_IRE'
        df['banner_id'] = ''
        df['score'] = 1.0
        df['model_version'] = '1.0'
        df['scene_id'] = 'scene_credit_card_ca'
        df['rank'] = 1
        df['batch_id'] = (base_dt + timedelta(days=d)).strftime('%Y%m%d')
        df['dt'] = (base_dt + timedelta(days=d)).strftime('%Y%m%d')
        df['dag_id'] = 'reco_credit_card_ca_0000'
        dfs.append(df)

    for d in range(7):
        user_id = ['{:03d}'.format(i % 100) for i in range(d * 20, (d + 1) * 20)]
        user_type = ['ctl' if i.endswith('9') else 'exp' for i in user_id]
        df = pd.DataFrame(data=list(zip(user_id, user_type)), columns=['user_id', 'user_type'])
        df['item_id'] = 'XXX'
        df['channel_id'] = 'PMBS_IRE'
        df['banner_id'] = 'WHITELIST'
        df['score'] = 1.0
        df['model_version'] = '1.0'
        df['scene_id'] = 'scene_credit_card_ca'
        df['rank'] = 1
        df['batch_id'] = (base_dt + timedelta(days=d)).strftime('%Y%m%d')
        df['dt'] = (base_dt + timedelta(days=d)).strftime('%Y%m%d')
        df['dag_id'] = 'reco_credit_card_ca_0000'
        dfs.append(df)

    df = pd.concat(dfs)
    path = os.path.join(output_dir, './crowd_package.csv')
    df.to_csv(path, sep='\x01', header=True, index=False)
    print(f'mock data saved to {path}')


def mock_crowd_feedback(output_dir, base_dt='20211230'):
    dfs = []
    base_dt = datetime.strptime(base_dt, '%Y%m%d')
    # 第一天跑的时候还没有当天的
    for d in range(7):
        user_id = ['{:03d}'.format(i % 100) for i in range((d) * 20, (d + 1) * 20)]
        user_type = ['ctl' if i.endswith('9') else 'exp' for i in user_id]
        df = pd.DataFrame(data=list(zip(user_id, user_type)), columns=['user_id', 'user_type'])
        df['item_id'] = 'XXX'
        df['channel_id'] = 'PSMS_IRE'
        df['banner_id'] = ''
        df['send'] = df['user_id'].map(lambda x: 0 if x.endswith('0') else 1)
        df['click'] = ''
        df['convert'] = ''
        df['batch_id'] = (base_dt + timedelta(days=d + 2 + 3)).strftime('%Y%m%d')
        df['dt'] = (base_dt + timedelta(days=d)).strftime('%Y%m%d')
        df['dag_id'] = 'reco_credit_card_ca_0000'
        dfs.append(df)

    df = pd.concat(dfs)
    path = os.path.join(output_dir, './crowd_feedback.csv')
    df.to_csv(path, sep='\x01', header=True, index=False)
    print(f'mock data saved to {path}')
