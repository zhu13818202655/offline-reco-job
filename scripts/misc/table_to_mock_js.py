# -*- coding: utf-8 -*-
# @File : table_to_json.py
# @Author : r.yang
# @Date : Sun Mar 20 18:32:27 2022
# @Description : format string


import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--table_txt')
parser.add_argument('--output_dir', default='./mocker/data')
args = parser.parse_args()

TPL = """
/* global $Ctx */

$Ctx.Spec = {{
    template: {{
{mock_list}
        dt: '@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    }},
    fields: [{fields_list}]
}}

"""
MOCK_INDENT = 8


def main():
    lines = open(args.table_txt).read().splitlines()

    cols = []
    for line in lines:
        eles = line.split()
        assert len(eles) == 3, eles
        cols.append(eles[0])

    fields_list = ','.join(cols)
    mock_list = [f'{" " *MOCK_INDENT}"{col}|1": [0, 1, 2, 3],' for col in cols]

    with open(
        os.path.join(
            args.output_dir,
            os.path.basename(args.table_txt).replace('.', '_').replace('_txt', '.js'),
        ),
        'w',
    ) as f:
        f.write(TPL.format(fields_list=fields_list, mock_list='\n'.join(mock_list)))


if __name__ == '__main__':
    main()
