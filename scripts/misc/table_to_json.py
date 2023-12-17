# -*- coding: utf-8 -*-
# @File : table_to_json.py
# @Author : r.yang
# @Date : Sun Mar 20 18:32:27 2022
# @Description : format string


import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--table_txt')
parser.add_argument('--output', default='tmp.json')
args = parser.parse_args()


def main():
    lines = open(args.table_txt).read().splitlines()
    data = {
        'name': '',
        'comment': '',
        'field_list': [],
        'partition_field_list': [],
    }
    for line in lines:
        eles = line.split()
        assert len(eles) == 3, eles
        data['field_list'].append(
            {'name': eles[0], 'type': eles[1], 'comment': eles[2],}
        )
    with open(args.output, 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    main()
