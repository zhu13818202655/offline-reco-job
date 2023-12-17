# -*- coding: utf-8 -*-
# @File : prodile_demo.py
# @Author : r.yang
# @Date : Tue Jun 21 10:38:30 2022
# @Description : format string

import os

import numpy as np

os.environ['RECO_ENVIRONMENT'] = 'local'  # noqa


from pyspark.sql.session import SparkSession

from algorithm.classifer import ClassifierModelFactory, LGBIncrClassifierModel
from configs.model.base import ClassifierConfig
from core.utils import numpy_feature_to_df


def main():
    spark = (
        SparkSession.builder.enableHiveSupport()
        .config('spark.sql.crossJoin.enabled', 'true')
        .getOrCreate()
    )

    sample_num = 1000000
    X = np.random.randn(sample_num, 20)
    y = np.random.randint(2, size=sample_num)
    df = numpy_feature_to_df(spark, X, y)
    del X, y

    model_cfg = ClassifierConfig(model_version='test', model_clz=LGBIncrClassifierModel.__name__,)
    model_cfg.custom_train_args['batch_num'] = 10
    model = ClassifierModelFactory.create(model_cfg)
    model.fit_df(df)


if __name__ == '__main__':
    main()
