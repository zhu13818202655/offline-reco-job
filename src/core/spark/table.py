import re
import secrets
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException

from core.logger import tr_logger
from core.spark import WithSpark
from core.utils import log_upper

logger = tr_logger.getChild('core.spark.table')


class SchemaCreator(WithSpark):
    def __init__(self):
        pass

    def create_database(self, database_name):
        self.run_spark_sql('create database if not exists {}'.format(database_name))

    def drop_database(self, database_name):
        self.run_spark_sql('drop database if exists {} CASCADE'.format(database_name))

    def drop_table(self, table_name):
        self.run_spark_sql('drop table if exists {}'.format(table_name))

    def use_database(self, database_name):
        self.run_spark_sql('use {}'.format(database_name))

    def is_schema_changed(self, table_info):
        table_name = table_info['name']
        if self.spark._jsparkSession.catalog().tableExists(*table_name.split('.', 1)):
            old_columns = self.run_spark_sql(f'select * from {table_name}').columns
            new_columns = [
                i['name'] for i in table_info['field_list'] + table_info['partition_field_list']
            ]
            return old_columns != new_columns
        return False

    def create_table(self, table_info):
        table_name = table_info['name']
        if self.is_schema_changed(table_info):
            logger.info(f'table: {table_name} schema changed, drop the old table')
            self.drop_table(table_name)

        table_comment = table_info['comment']
        field_list = table_info['field_list']
        field_statement_list = ', \n               '.join(
            "{name} {type} comment '{comment}'".format(
                name=field['name'], type=field['type'], comment=field['comment'],
            )
            for field in field_list
        )
        partition_field_list = table_info['partition_field_list']
        partition_field_statement_list = ', \n'.join(
            "{name} {type} comment '{comment}'".format(
                name=field['name'], type=field['type'], comment=field['comment'],
            )
            for field in partition_field_list
        )
        self.run_spark_sql(
            """
           CREATE TABLE IF NOT EXISTS {table_name}
           (
               {field_statement_list}
           ) comment '{table_comment}'
               {partion_statement}
               stored as parquet
           """.format(
                table_name=table_name,
                table_comment=table_comment,
                field_statement_list=field_statement_list,
                partion_statement="""
                partitioned by (
                   {partition_field_statement_list}
                )
            """.format(
                    partition_field_statement_list=partition_field_statement_list
                )
                if partition_field_statement_list
                else '',
            )
        )


class TableUtils:
    @staticmethod
    def truncate_middle(s, n=512, ellipsis='...<TRUNCATED>...'):
        if len(s) <= n:
            # string is already short-enough
            return s
        # half of the size, minus the 3 .'s
        n_2 = int(n) // 2 - len(ellipsis)
        # whatever's left
        n_1 = n - n_2 - len(ellipsis)
        return ''.join((s[:n_1], ellipsis, s[-n_2:]))

    @staticmethod
    def get_columns(spark, table) -> List[str]:
        return spark.sql(f'select * from {table} limit 0').columns


class TableNameConstructor(object):
    @staticmethod
    def format_output_table_by_class_name(database, class_name, prefix='aaqh_a_rl_'):
        return TableNameConstructor.format_output_table(
            database=database,
            core_name=TableNameConstructor.camel_case_to_snake_case(class_name),
            prefix=prefix,
        )

    @staticmethod
    def format_output_table(database, core_name, prefix='aaqh_a_rl_'):
        return '{database}.{prefix}{core_name}'.format(
            database=database, prefix=prefix, core_name=core_name,
        )

    @staticmethod
    def camel_case_to_snake_case(string):
        return '{}{}'.format(
            string[0].lower(),
            re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), string[1:]),
        )


class TableInserter(WithSpark):

    _field_type_dict = {
        'string': T.StringType(),
        'int': T.IntegerType(),
        'float': T.FloatType(),
        'bool': T.BooleanType(),
        'double': T.DoubleType(),
    }

    def __init__(self, table_info: dict):
        self._partition_fields: List[str] = []
        self._schema = self._infer_schema(table_info)
        self._table_info = table_info

    @property
    def table_name(self):
        return self._table_info['name']

    @staticmethod
    def _lookup_field_type(type_: str):
        is_map = re.match(r'map<(.+?)>', type_)
        if is_map:
            key_t, value_t = is_map.groups()[0].split(',')
            return T.MapType(
                TableInserter._lookup_field_type(key_t.strip()),
                TableInserter._lookup_field_type(value_t.strip()),
            )

        is_array = re.match(r'array<(.+?)>', type_)
        if is_array:
            item_t = is_array.groups()[0].strip()
            return T.ArrayType(TableInserter._lookup_field_type(item_t.strip()))

        if type_ not in TableInserter._field_type_dict:
            raise KeyError('hive field type: {type_} is not supported')
        return TableInserter._field_type_dict[type_]

    def _infer_schema(self, table_info):

        schemas = []
        for field in table_info['field_list']:
            schemas.append(T.StructField(field['name'], self._lookup_field_type(field['type'])))

        for field in table_info['partition_field_list']:
            assert field['type'] == 'string'
            self._partition_fields.append(field['name'])

        return T.StructType(schemas)

    def insert_list(self, data: list, overwrite=True, **partition_kwargs) -> DataFrame:
        df = self.spark.createDataFrame(data, self._schema)
        return self.insert_df(df, overwrite=overwrite, **partition_kwargs)

    def insert_df(self, df, overwrite=True, **partition_kwargs) -> DataFrame:

        secret_generator = secrets.SystemRandom()

        table_tmp = f'temp_view_{secret_generator.randint(1,1000)}'

        assert set(partition_kwargs.keys()) == set(
            self._partition_fields
        ), f'{partition_kwargs.keys()} vs {self._partition_fields}'

        df.createOrReplaceTempView(table_tmp)
        try:
            self.run_spark_sql(
                """
                INSERT {overwrite} table {table_name}
                    partition({partition})
                SELECT
                    {fields}
                FROM {table_tmp}
            """.format(
                    overwrite='overwrite' if overwrite else 'into',
                    partition=', '.join([f"{k}='{v}'" for k, v in partition_kwargs.items()]),
                    table_name=self._table_info['name'],
                    table_tmp=table_tmp,
                    fields=','.join([i['name'] for i in self._table_info['field_list']]),
                ),
            )
        except AnalysisException as e:
            raise e
        finally:
            self.spark.catalog.dropTempView(table_tmp)

        with log_upper(1):
            logger.info(
                '{} records is inserted to table: {}@prt={}'.format(
                    df.count(), self._table_info['name'], partition_kwargs
                )
            )
        return df

    def insert_line(self, line: list, **partition_kwargs):
        assert set(partition_kwargs.keys()) == set(
            self._partition_fields
        ), f'{partition_kwargs.keys()} vs {self._partition_fields}'

        data = []
        for i, field in enumerate(self._table_info['field_list']):
            if field['type'] == 'string':
                data.append("'{}'".format(line[i]))
            else:
                data.append('{}'.format(line[i]))

        sql = """
                insert into table {table_name}
                    partition({partition})
                    values(
                        {data}
                    )
                """.format(
            partition=', '.join([f"{k}='{v}'" for k, v in partition_kwargs.items()]),
            table_name=self._table_info['name'],
            data=', '.join(data),
        )
        self.run_spark_sql(sql)

    def drop_partitions(self, **partition_kwargs):
        sql = """
            ALTER TABLE {table} DROP IF EXISTS PARTITION({partition})
        """.format(
            table=self._table_info['name'],
            partition=', '.join([f"{k}='{v}'" for k, v in partition_kwargs.items()]),
        )
        self.run_spark_sql(sql)

    def has_parition(self, **kwargs):
        partitions = [
            row.partition
            for row in self.run_spark_sql(f'show partitions {self.table_name}').collect()
        ]
        partitions = [{i.split('=')[0]: i.split('=')[1] for i in p.split('/')} for p in partitions]

        res = kwargs in partitions
        logger.info(f'{kwargs} exists: {res}')
        return res


HEADER_SUFFIX = '.header'


def dump_df_with_header(spark: SparkSession, df: DataFrame, output_path, sep='\x01'):
    column_df = spark.createDataFrame([(0,) * len(df.columns)], df.columns)
    df.write.mode('overwrite').option('header', False).option('delimiter', sep).option(
        'quote', ''
    ).csv(output_path)

    column_df.coalesce(1).write.mode('overwrite').option('header', True).option(
        'delimiter', sep
    ).csv(output_path + HEADER_SUFFIX)


def load_df(spark: SparkSession, path, sep='\x01'):
    return spark.read.option('header', True).option('delimiter', sep).csv(path)
