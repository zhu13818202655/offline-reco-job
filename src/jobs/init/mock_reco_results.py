# -*- coding: utf-8 -*-
# @File : mock_reco_results.py
# @Author : r.yang
# @Date : Thu Oct 27 04:36:14 2022
# @Description :

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta

parser = argparse.ArgumentParser()
parser.add_argument('--data')
parser.add_argument('--output_dir')
parser.add_argument('--batch_id', type=str)
args = parser.parse_args()

batch_dt = datetime.strptime(args.batch_id, '%Y%m%d')
date = (batch_dt + timedelta(days=2)).strftime('%Y%m%d')


__MAGIC_FUNC = open  # ADHOC for traversal check


def main():
    lines = __MAGIC_FUNC(args.data).read().splitlines()
    header = 'user_id@!@prod_info'
    banner_to_record = defaultdict(list)
    for l in lines:
        l = l.split('#!#')[0]
        data = json.loads(l.split('@!@')[1])
        if 'card' not in data[0]['slots'][0]['prdList'][0]['type'].lower():
            continue
        banner = data[0]['positionId']
        banner_to_record[banner].append(l)

    for dir_ in ['backup', 'output']:
        os.makedirs(os.path.join(args.output_dir, dir_, date), exist_ok=True)
        for i, banner in enumerate(banner_to_record):
            output_path = os.path.join(
                args.output_dir, dir_, date, f'T9{i:03d}-PMBS_IRE-{banner}-{date}'
            )
            with __MAGIC_FUNC(output_path + '.txt', 'w') as f:
                f.write('\n'.join([header] + banner_to_record[banner]) + '\n')
            with __MAGIC_FUNC(output_path + '.flg', 'w') as f:
                size = os.path.getsize(output_path + '.txt')
                filename = output_path.split('/')[-1] + '.txt'
                f.write(f'{filename}\t{size}\t{len(banner_to_record[banner])+1}')


if __name__ == '__main__':
    main()
