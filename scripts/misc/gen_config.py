# -*- coding: utf-8 -*-
# @File : gen_config.py
# @Author : r.yang
# @Date : Mon Oct 24 02:25:54 2022
# @Description : format string


import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--env', type=str, choices=['prod', 'uat', 'pi', 'local'], default='prod')
parser.add_argument('--output_path', type=str)
args = parser.parse_args()

os.environ['RECO_ENVIRONMENT'] = args.env


def main():
    import yaml

    from configs.init import aio_config

    aio_cfg = aio_config()
    with open(args.output_path, 'w') as f:
        yaml.dump(aio_cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f'aio config is dumped to {args.output_path}')


if __name__ == '__main__':
    main()
