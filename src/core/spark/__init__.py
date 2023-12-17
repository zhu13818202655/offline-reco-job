import pyspark.sql.types as T
from pyspark.sql import SparkSession

from core.logger import tr_logger
from core.utils import log_upper

logger = tr_logger.getChild('core.spark')


class WithSpark:

    # final name Mangling
    __spark = None

    @staticmethod
    def _init_spark():
        spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        spark_session.sql('set hive.support.quoted.identifiers=none')
        spark_session.sql('set spark.sql.parser.quotedRegexColumnNames=true')
        spark_session.sql('set hive.exec.dynamic.partition=true')
        spark_session.sql('set hive.exec.dynamic.partition.mode=nonstrict')
        spark_session.sql('set spark.sql.adaptive.enabled=true')
        spark_session.sql('set spark.sql.crossJoin.enabled=true')
        spark_session.sql('set spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000')

        def clean_ad(ad: str) -> str:
            import re

            ad = re.sub(r'\${(.+?)}', '', ad)
            ad = re.sub(
                r'[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)',
                '',
                ad,
            )
            ad = re.sub(r'(尊敬|亲爱)的.*(先生|女士)(：|，)', '', ad)
            return ad

        def sim(x, y):
            import numpy as np
            from scipy import spatial

            if not x or not y:
                return 0.0
            dist = spatial.distance.cosine(x, y)
            return float(1 - dist) if not np.isnan(dist) else 0.0

        import json

        spark_session.udf.register('udf_clean_ad', clean_ad, T.StringType())
        spark_session.udf.register('udf_sim', sim, T.FloatType())
        spark_session.udf.register('construct_json', lambda x: json.dumps(x.asDict()))

        return spark_session

    @property
    def spark(self):
        if self.__spark is None:
            self.__spark = self._init_spark()
        return self.__spark

    def run_spark_sql(self, sql):
        with log_upper(0):
            logger.info('running sql: %s', sql)
        return self.spark.sql(sql)
