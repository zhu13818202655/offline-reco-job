# -*- coding: utf-8 -*-
# @File : load_data.py
# @Author : r.yang
# @Date : Tue Mar  1 10:29:06 2022
# @Description : load mock data


import multiprocessing
import os
import threading

from pydantic.env_settings import BaseSettings
from pydantic.fields import Field

from configs.model.base import BaseConfig
from core.job.base import IOutputTable
from core.job.registry import JobRegistry
from core.logger import tr_logger
from core.spark.table import SchemaCreator
from jobs.base import SparkConfiglessJob

logger = tr_logger.getChild('load_data')


class TestConfig(BaseSettings):
    mock_data_path: str = Field(
        env='RECO_MOCK_DATA_PATH',
        default_factory=lambda: os.path.join(
            os.path.dirname(__file__), '../../../mocker/.runtime/data'
        ),
    )
    data_load_thread: int = Field(
        env='RECO_DATA_LOAD_THREAD', default_factory=lambda: multiprocessing.cpu_count()
    )


@JobRegistry.register(JobRegistry.T.BASE)
class LoadData(SparkConfiglessJob, IOutputTable):
    def __init__(self, ctx):
        super().__init__(ctx)
        self._test_cfg = TestConfig()
        self._schema_creator = SchemaCreator()

    @staticmethod
    def list_job_output_tables(ctx):
        tables = []
        name_set = set()
        for _, job in JobRegistry.iter_all_jobs():
            if not issubclass(job.clz, IOutputTable):
                continue
            for table in job.clz.output_table_info_list(ctx.base):
                if table['name'] not in name_set:
                    tables.append(table)
                    name_set.add(table['name'])

        return tables

    def run(self):

        table_info_list = self.list_job_output_tables(self._ctx)
        from multiprocessing.dummy import Pool

        if self._test_cfg.data_load_thread:
            pool = Pool(self._test_cfg.data_load_thread)
            for table_info in table_info_list:
                pool.apply_async(self._load_table, (table_info, self._test_cfg.mock_data_path))
            pool.close()
            pool.join()
        else:
            for table_info in table_info_list:
                self._load_table(table_info, self._test_cfg.mock_data_path)

    def _load_table(self, table_info, base_directory):
        self._insert_into_test_common_data(
            base_directory=base_directory, table_name=table_info['name']
        )

    def _insert_into_test_common_data(
        self, base_directory, table_name, header='true', mode='DROPMALFORMED'
    ):
        csv_path = '{}/{}.csv'.format(
            base_directory, table_name.lower().split('.')[1].replace('bdasire_card_', '')
        )

        temp_view_name = (
            self.__class__.__name__ + '_temp_view_' + str(threading.current_thread().ident)
        )
        if not os.path.exists(csv_path):
            logger.warning(f'csv path: {csv_path} not exist!')
            return

        logger.info(f'csv of {table_name} found: {csv_path}')
        try:
            df = (
                self.spark.read.option('header', header)
                .option('sep', '\x01')
                .option('mode', mode)
                .csv(csv_path)
            )
            df.createOrReplaceTempView(temp_view_name)
            self.spark.sql(
                """
                    insert into table {table_name}
                    select * from {temp_view_name}
                    """.format(
                    table_name=table_name, temp_view_name=temp_view_name
                )
            )
            self.spark.catalog.dropTempView(temp_view_name)
        except BaseException as e:
            logger.error(f'load table from csv error: {e}')

    @staticmethod
    def output_table_info_list(base: BaseConfig):
        return [
            {
                'name': base.external_table.cre_crd_user,
                'comment': '信用卡持卡',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'if_valid_hold_crd', 'type': 'string', 'comment': ''},
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'last_apply_dt', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.deb_crd_user,
                'comment': '借记卡持卡',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'if_hold_card', 'type': 'string', 'comment': ''},
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'firsr_apply_dt', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.user_tel_number,
                'comment': '用户手机号',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'phone_no', 'type': 'string', 'comment': '联系电话'},
                    {'name': 'credit_crd_phone_no', 'type': 'string', 'comment': '信用卡电话'},
                    {'name': 's06_phone_no', 'type': 'string', 'comment': '信贷电话'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.sms_feedback,
                'comment': '短信反馈',
                'field_list': [
                    {'name': 'mblno', 'type': 'string', 'comment': '手机号'},
                    {'name': 'failreason', 'type': 'string', 'comment': '失败原因'},
                    {'name': 'msgst', 'type': 'int', 'comment': '状态报告'},
                    {'name': 'msgcntnt', 'type': 'string', 'comment': '短信正文'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.user_feat_ls_crd,
                'comment': '用户基本信息',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户号'},
                    {'name': 'age', 'type': 'decimal(26,0)', 'comment': '年龄'},
                    {'name': 'sex', 'type': 'string', 'comment': '性别'},
                    {'name': 'phone_valid_ind', 'type': 'string', 'comment': '是否有有效手机号'},
                    {'name': 'zxyh_cust_ind', 'type': 'string', 'comment': '是否直销银行客户'},
                    {'name': 'risk_fund_valid_ind', 'type': 'string', 'comment': '是否有基金有效风评'},
                    {'name': 'risk_fin_valid_ind', 'type': 'string', 'comment': '是否有理财有效风评'},
                    {'name': 'mngr_ind', 'type': 'string', 'comment': '是否管户'},
                    {'name': 'salary_ind', 'type': 'string', 'comment': '是否代发工资'},
                    {'name': 'pension_ind', 'type': 'string', 'comment': '是否代发养老'},
                    {'name': 'debcard_ind', 'type': 'string', 'comment': '是否持有借记卡'},
                    {'name': 'debcard_sbk_ind', 'type': 'string', 'comment': '是否持有社保卡'},
                    {'name': 'loan_person_ind', 'type': 'string', 'comment': '是否持有个贷'},
                    {'name': 'loan_house_ind', 'type': 'string', 'comment': '是否持有房贷'},
                    {'name': 'loan_car_ind', 'type': 'string', 'comment': '是否持有车贷'},
                    {'name': 'both_card_ind', 'type': 'string', 'comment': '是否双卡客户'},
                    {'name': 'credit_card_only_ind', 'type': 'string', 'comment': '是否裸信用卡客户'},
                    {'name': 'loan_car_only_ind', 'type': 'string', 'comment': '是否裸车贷客户'},
                    {'name': 'credit_card_hold_ind', 'type': 'string', 'comment': '是否持有有效信用卡'},
                    {'name': 'debit_cust_ind', 'type': 'string', 'comment': '是否借记账户客户'},
                    {'name': 'debit_cust_n_daifa_ind', 'type': 'string', 'comment': '是否借记账户非重点客户'},
                    {'name': 'mt_card_ind', 'type': 'string', 'comment': '是否持有有效美团卡'},
                    {'name': 'crd_card_ind', 'type': 'string', 'comment': '是否持有有效非美团卡'},
                    {'name': 'over_star_card_ind', 'type': 'string', 'comment': '是否星钻卡及以上客户'},
                    {'name': 'huitong_ind', 'type': 'string', 'comment': '是否慧通卡客户'},
                    {'name': 'daixiao_cust_ind', 'type': 'string', 'comment': '是否其他理财代销客户'},
                    {'name': 'newcust_ls_year_ind', 'type': 'string', 'comment': '是否当年新增零售客户'},
                    {'name': 'newcust_ls_month_ind', 'type': 'string', 'comment': '是否当月新增零售客户'},
                    {'name': 'newcust_crd_year_ind', 'type': 'string', 'comment': '是否当年新增信用卡客户'},
                    {'name': 'newcust_crd_month_ind', 'type': 'string', 'comment': '是否当月新增信用卡客户'},
                    {'name': 'deposit_cur_cust_ind', 'type': 'string', 'comment': '是否持有活期'},
                    {'name': 'deposit_fix_cust_ind', 'type': 'string', 'comment': '是否持有定存'},
                    {'name': 'fin_bb_cust_ind', 'type': 'string', 'comment': '是否持有保本'},
                    {'name': 'fin_nbb_cust_ind', 'type': 'string', 'comment': '是否持有非保本'},
                    {'name': 'stru_cust_ind', 'type': 'string', 'comment': '是否持有结存'},
                    {'name': 'statbond_cust_ind', 'type': 'string', 'comment': '是否持有国债'},
                    {'name': 'fund_cust_ind', 'type': 'string', 'comment': '是否持有基金'},
                    {'name': 'insu_cust_ind', 'type': 'string', 'comment': '是否持有保险'},
                    {'name': 'trust_cust_ind', 'type': 'string', 'comment': '是否持有信托'},
                    {'name': 'gold_cust_ind', 'type': 'string', 'comment': '是否持有贵金属'},
                    {'name': 'fund_non_cur_cust_ind', 'type': 'string', 'comment': '是否持有非货币公募基金'},
                    {'name': 'fund_spe_cust_ind', 'type': 'string', 'comment': '是否持有专户基金'},
                    {'name': 'instal_cust_ind', 'type': 'string', 'comment': '是否持有信用卡分期'},
                    {'name': 'loan_cust_ind', 'type': 'string', 'comment': '是否持有信贷类产品'},
                    {'name': 'caifu_fuzai_cust_ind', 'type': 'string', 'comment': '是否持有财富负债产品'},
                    {'name': 'deposit_p_cust_ind', 'type': 'string', 'comment': '是否持有个人存款产品'},
                    {'name': 'fin_daixiao_cust_ind', 'type': 'string', 'comment': '是否持有理财代销产品'},
                    {'name': 'fuzhai_prd_cnt', 'type': 'decimal(26,0)', 'comment': '财富负债产品持有个数'},
                    {'name': 'deposit_p_bal', 'type': 'decimal(38,6)', 'comment': '个人存款余额'},
                    {'name': 'fin_daixiao_bal', 'type': 'decimal(38,6)', 'comment': '理财代销余额'},
                    {'name': 'deposit_cur_bal', 'type': 'decimal(38,6)', 'comment': '活期余额合计'},
                    {'name': 'deposit_fix_bal', 'type': 'decimal(38,6)', 'comment': '定存余额合计'},
                    {'name': 'fin_bb_bal', 'type': 'decimal(38,6)', 'comment': '保本余额合计'},
                    {'name': 'fin_nbb_bal', 'type': 'decimal(38,6)', 'comment': '非保本余额合计'},
                    {'name': 'stru_bal', 'type': 'decimal(38,6)', 'comment': '结存余额合计'},
                    {'name': 'statebond_bal', 'type': 'decimal(38,6)', 'comment': '国债余额合计'},
                    {'name': 'fund_bal', 'type': 'decimal(38,6)', 'comment': '基金余额合计'},
                    {'name': 'insu_bal', 'type': 'decimal(38,6)', 'comment': '保险余额合计'},
                    {'name': 'trust_bal', 'type': 'decimal(38,6)', 'comment': '信托余额合计'},
                    {'name': 'gold_bal', 'type': 'decimal(38,6)', 'comment': '贵金属余额合计'},
                    {
                        'name': 'fund_non_cur_cust_bal',
                        'type': 'decimal(38,6)',
                        'comment': '非货币公募基金余额合计',
                    },
                    {'name': 'fund_spe_cust_bal', 'type': 'decimal(38,6)', 'comment': '专户基金余额合计'},
                    {
                        'name': 'tot_asset_n_zhixiao_bal',
                        'type': 'decimal(38,6)',
                        'comment': 'AUM合计_不含直销',
                    },
                    {
                        'name': 'tot_asset_n_zhixiao_month_avg_bal',
                        'type': 'decimal(38,6)',
                        'comment': '月日均aum_不含直销',
                    },
                    {'name': 'tot_asset_bal', 'type': 'decimal(38,6)', 'comment': 'AUM合计_含直销'},
                    {
                        'name': 'tot_asset_month_avg_bal',
                        'type': 'decimal(38,6)',
                        'comment': '月日均aum_含直销',
                    },
                    {'name': 'loan_bal', 'type': 'decimal(38,6)', 'comment': '个贷余额合计'},
                    {'name': 'loan_house_bal', 'type': 'decimal(38,6)', 'comment': '房贷余额合计'},
                    {'name': 'loan_car_bal', 'type': 'decimal(38,6)', 'comment': '车贷余额合计'},
                    {'name': 'cust_bal', 'type': 'decimal(38,6)', 'comment': '信用卡余额合计'},
                    {'name': 'pim_bal', 'type': 'decimal(38,6)', 'comment': '信用卡分期余额合计'},
                    {'name': 'cust_limit', 'type': 'decimal(38,6)', 'comment': '信用卡额度合计'},
                    {'name': 'lum_bal', 'type': 'decimal(38,6)', 'comment': 'LUM合计'},
                    {'name': 'pmbs_cust_ind', 'type': 'string', 'comment': '手机银行签约'},
                    {'name': 'pwec_cust_ind', 'type': 'string', 'comment': '微信银行签约'},
                    {'name': 'pibs_cust_ind', 'type': 'string', 'comment': '个人网银签约'},
                    {'name': 'bind_alipay_debit_ind', 'type': 'string', 'comment': '支付宝绑定借记卡签约'},
                    {'name': 'bind_alipay_crd_ind', 'type': 'string', 'comment': '支付宝绑定信用卡签约'},
                    {'name': 'bind_wechat_debit_ind', 'type': 'string', 'comment': '微信绑定借记卡签约'},
                    {'name': 'bind_wechat_crd_ind', 'type': 'string', 'comment': '微信绑定信用卡签约'},
                    {'name': 'mob_pay_cust_ind', 'type': 'string', 'comment': '移动支付行为'},
                    {'name': 'auto_repay_cust_ind', 'type': 'string', 'comment': '信用卡自动还款签约'},
                    {'name': 'thd_pty_sec_hold_cust_ind', 'type': 'string', 'comment': '第三方存管签约'},
                    {'name': 'pub_ser_fee_hold_cust_ind', 'type': 'string', 'comment': '公用事业费签约'},
                    {'name': 'pwec_act_ind', 'type': 'string', 'comment': '微信银行当月有登录'},
                    {'name': 'pmbs_act_ind', 'type': 'string', 'comment': '手机银行当月有登录'},
                    {'name': 'counter_act_ind', 'type': 'string', 'comment': '线下网点当月活跃'},
                    {'name': 'channel_cnt', 'type': 'decimal(26,0)', 'comment': '签约渠道个数'},
                    {'name': 'deb_txn_m_ind', 'type': 'string', 'comment': '借记卡当月借记交易'},
                    {'name': 'crd_txn_m_ind', 'type': 'string', 'comment': '借记卡当月贷记交易'},
                    {'name': 'cum_cash_txn_m_ind', 'type': 'string', 'comment': '借记卡当月消费取款'},
                    {'name': 'deb_txn_m_cnt', 'type': 'decimal(26,0)', 'comment': '借记卡当月借记次数'},
                    {'name': 'deb_txn_m_amt', 'type': 'decimal(38,6)', 'comment': '借记卡当月借记金额'},
                    {'name': 'crd_txn_m_cnt', 'type': 'decimal(26,0)', 'comment': '借记卡当月贷记次数'},
                    {'name': 'crd_txn_m_amt', 'type': 'decimal(38,6)', 'comment': '借记卡当月贷记金额'},
                    {'name': 'cum_cash_m_cnt', 'type': 'decimal(26,0)', 'comment': '借记卡当月消费取款次数'},
                    {'name': 'cum_cash_m_amt', 'type': 'decimal(38,6)', 'comment': '借记卡当月消费取款金额'},
                    {'name': 'crd_card_txn_m_ind', 'type': 'string', 'comment': '信用卡当月交易'},
                    {'name': 'repay_m_ind', 'type': 'string', 'comment': '信用卡当月还款'},
                    {'name': 'crd_card_txn_m_cnt', 'type': 'decimal(26,0)', 'comment': '信用卡当月交易次数'},
                    {'name': 'crd_card_txn_m_amt', 'type': 'decimal(38,6)', 'comment': '信用卡当月交易金额'},
                    {'name': 'repay_amt', 'type': 'decimal(38,6)', 'comment': '信用卡当月还款次数'},
                    {'name': 'repay_cnt', 'type': 'decimal(26,0)', 'comment': '信用卡当月还款金额'},
                    {'name': 'mov_ind', 'type': 'string', 'comment': '是否当月有动账'},
                    {'name': 'mov_cnt', 'type': 'decimal(26,0)', 'comment': '当月动账次数'},
                    {'name': 'mov_amt', 'type': 'decimal(38,6)', 'comment': '当月动账金额'},
                    {'name': 'xfqx_cnt', 'type': 'decimal(26,0)', 'comment': '当月消费取款次数'},
                    {'name': 'xfqx_amt', 'type': 'decimal(38,6)', 'comment': '当月消费取款金额'},
                    {'name': 'fin_bb_txn_cnt', 'type': 'decimal(26,0)', 'comment': '保本理财购买笔数'},
                    {'name': 'fin_nbb_txn_cnt', 'type': 'decimal(26,0)', 'comment': '非保本理财购买笔数'},
                    {
                        'name': 'fund_non_cur_txn_cnt',
                        'type': 'decimal(26,0)',
                        'comment': '非货币公募基金购买笔数',
                    },
                    {'name': 'fund_spe_txn_cnt', 'type': 'decimal(26,0)', 'comment': '专户基金购买笔数'},
                    {'name': 'trust_txn_cnt', 'type': 'decimal(26,0)', 'comment': '信托购买笔数'},
                    {'name': 'insu_txn_cnt', 'type': 'decimal(26,0)', 'comment': '保险购买笔数'},
                    {'name': 'fin_bb_txn_amt', 'type': 'decimal(38,6)', 'comment': '保本理财购买金额'},
                    {'name': 'fin_nbb_txn_amt', 'type': 'decimal(38,6)', 'comment': '非保本理财购买金额'},
                    {
                        'name': 'fund_non_cur_txn_amt',
                        'type': 'decimal(38,6)',
                        'comment': '非货币公募基金购买金额',
                    },
                    {'name': 'fund_spe_txn_amt', 'type': 'decimal(38,6)', 'comment': '专户基金购买金额'},
                    {'name': 'trust_txn_amt', 'type': 'decimal(38,6)', 'comment': '信托购买金额'},
                    {'name': 'insu_txn_amt', 'type': 'decimal(38,6)', 'comment': '保险购买金额'},
                    {'name': 'fin_nbb_income', 'type': 'decimal(38,6)', 'comment': '非保本理财中收'},
                    {
                        'name': 'fund_non_cur_income',
                        'type': 'decimal(38,6)',
                        'comment': '非货币公募基金中收',
                    },
                    {'name': 'insu_income', 'type': 'decimal(38,6)', 'comment': '保险中收'},
                    {'name': 'last_tran_date', 'type': 'string', 'comment': '日期'},
                    {'name': 'fenhang', 'type': 'string', 'comment': '分行号'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'}],
            },
            {
                'name': base.external_table.cust_act_label,
                'comment': '用户行为',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '用户号'},
                    {'name': 'pmbs_cust_ind', 'type': 'string', 'comment': '手机银行客户标识'},
                ],
                'partition_field_list': [{'name': 'data_dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.app_feedback,
                'comment': '手机银行反馈',
                'field_list': [
                    {'name': 'useridentifier', 'type': 'string', 'comment': '用户标识（手机号+用户号）'},
                    {'name': 'event_identifier', 'type': 'string', 'comment': '事件标识'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.app_action,
                'comment': '手机银行事件',
                'field_list': [
                    {'name': 'act_nm', 'type': 'string', 'comment': '名称'},
                    {'name': 'channel_type', 'type': 'string', 'comment': '渠道类型'},
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户编号'},
                ],
                'partition_field_list': [{'name': 'pt_td', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.custom_data,
                'comment': '',
                'field_list': [
                    {'name': 'useridentifier', 'type': 'string', 'comment': ''},
                    {'name': 'custom_data', 'type': 'string', 'comment': ''},
                    {'name': 'event_identifier', 'type': 'string', 'comment': ''},
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户编号'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.s21_ecusrdevice,
                'comment': '',
                'field_list': [
                    {'name': 'deviceno', 'type': 'string', 'comment': ''},
                    {'name': 'userseq', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.s21_ecusr_h,
                'comment': '',
                'field_list': [
                    {'name': 'userseq', 'type': 'string', 'comment': ''},
                    {'name': 'cifseq', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.s21_ecextcifno_h,
                'comment': '',
                'field_list': [
                    {'name': 'cifno', 'type': 'string', 'comment': ''},
                    {'name': 'cifnotype', 'type': 'string', 'comment': ''},
                    {'name': 'cifseq', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.user_stat_mt,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'camp_stat', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.hx_cancel_log,
                'comment': '操作日志表',
                'field_list': [
                    {'name': 'bm_cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'bm_opr_id', 'type': 'string', 'comment': ''},
                    {'name': 'bm_new_contents', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.hx_cancel_user,
                'comment': '卡核心销户用户',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'cust_cancel_ind', 'type': 'string', 'comment': ''},
                    {'name': 'cust_anniv_dt', 'type': 'string', 'comment': ''},
                    {'name': 'cust_mobile_no', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.mt_cancel_user,
                'comment': '美团销户用户',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'card_ccl_ind', 'type': 'string', 'comment': ''},
                    {'name': 'card_distory_dt', 'type': 'string', 'comment': ''},
                    {'name': 'card_atv_dt', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.can_acct_type,
                'comment': '在线挽留',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'if_logout_online_value_cust', 'type': 'int', 'comment': ''},
                    {'name': 'if_logout_online_nvalue_cust', 'type': 'int', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.dim_jydm,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'if_use', 'type': 'string', 'comment': ''},
                    {'name': 'in_acct_type', 'type': 'string', 'comment': ''},
                    {'name': 'trx_type1', 'type': 'string', 'comment': ''},
                    {'name': 'id_jydm', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.ath_txn_info_mt,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'acct_id_dt', 'type': 'string', 'comment': ''},
                    {'name': 'in_acct_amt', 'type': 'decimal(26,8)', 'comment': ''},
                    {'name': 'in_acct_ccy_cd', 'type': 'string', 'comment': ''},
                    {'name': 'crd_card_trans_cd', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.ath_txn_info,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'acct_id_dt', 'type': 'string', 'comment': ''},
                    {'name': 'in_acct_amt', 'type': 'decimal(26,8)', 'comment': ''},
                    {'name': 'in_acct_ccy_cd', 'type': 'string', 'comment': ''},
                    {'name': 'crd_card_trans_cd', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.card_pim_info,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'card_pim_dsc', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.cstbsc_info_mt,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'cust_anniv_dt', 'type': 'string', 'comment': ''},
                    {'name': 'cust_mobile_no', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.rtl_asset_bal,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'aum_tm', 'type': 'float', 'comment': ''},
                    {'name': 'dpst_tm', 'type': 'float', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.flag_month,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '客户ID'},
                    {'name': 'age', 'type': 'int', 'comment': ''},
                    {'name': 'activate_card_flag', 'type': 'int', 'comment': ''},
                    {'name': 'first_bin_wx', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_zfb', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_unionpay', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_mt', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_jd', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_sn', 'type': 'string', 'comment': ''},
                    {'name': 'first_bin_wp', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.s70_exposure,
                'comment': '',
                'field_list': [
                    {'name': 'advice_id', 'type': 'int', 'comment': ''},
                    {'name': 'customer_id', 'type': 'string', 'comment': ''},
                    {'name': 'strategy_id', 'type': 'string', 'comment': ''},
                    {'name': 'resource_id', 'type': 'string', 'comment': ''},
                    {'name': 'strategy_category', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'data_dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.s38_behavior,
                'comment': '',
                'field_list': [
                    {'name': 'adviceid', 'type': 'string', 'comment': ''},
                    {'name': 'seed', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.epm_activity,
                'comment': '',
                'field_list': [
                    {'name': 'activity_code', 'type': 'string', 'comment': ''},
                    {'name': 'activity_name', 'type': 'string', 'comment': ''},
                    {'name': 'activity_start_date', 'type': 'string', 'comment': ''},
                    {'name': 'activity_end_date', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.s70_t2_resource,
                'comment': '',
                'field_list': [
                    {'name': 'resource_id', 'type': 'int', 'comment': ''},
                    {'name': 'content', 'type': 'string', 'comment': ''},
                    {'name': 'material_key', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.fin_tbproduct,
                'comment': '',
                'field_list': [
                    {'name': 'prd_code', 'type': 'string', 'comment': ''},
                    {'name': 'channels', 'type': 'string', 'comment': ''},
                    {'name': 'status', 'type': 'string', 'comment': ''},
                    {'name': 'reserve1', 'type': 'string', 'comment': ''},
                    {'name': 'cycle_tradable', 'type': 'string', 'comment': ''},
                    {'name': 'per_allow', 'type': 'string', 'comment': ''},
                    {'name': 'ipo_end_date_outer', 'type': 'int', 'comment': ''},
                    {'name': 'list_type', 'type': 'string', 'comment': ''},
                    {'name': 'is_new_client_prd', 'type': 'string', 'comment': ''},
                    {'name': 'diamond_card_flag', 'type': 'string', 'comment': ''},
                    {'name': 'salary_card_flags', 'type': 'string', 'comment': ''},
                    {'name': 'pension_funds_flag', 'type': 'string', 'comment': ''},
                    {'name': 'service_level', 'type': 'string', 'comment': ''},
                    {'name': 'ipo_type', 'type': 'string', 'comment': ''},
                    {'name': 'risk_level', 'type': 'int', 'comment': ''},
                    {'name': 'prd_name', 'type': 'string', 'comment': ''},
                    {'name': 'nav', 'type': 'string', 'comment': ''},
                    {'name': 'nav_date', 'type': 'int', 'comment': ''},
                    {'name': 'model_comment', 'type': 'string', 'comment': ''},
                    {'name': 'prd_manager', 'type': 'string', 'comment': ''},
                    {'name': 'ipo_start_date', 'type': 'int', 'comment': ''},
                    {'name': 'ipo_end_date', 'type': 'int', 'comment': ''},
                    {'name': 'income_date', 'type': 'int', 'comment': ''},
                    {'name': 'benchmark', 'type': 'string', 'comment': ''},
                    {'name': 'modelcomment_desc', 'type': 'string', 'comment': ''},
                    {'name': 'guest_type', 'type': 'string', 'comment': ''},
                    {'name': 'estab_ratio', 'type': 'decimal(16,4)', 'comment': ''},
                    {'name': 'day_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'week_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'two_week_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'month_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'two_month_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'three_month_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'half_year_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'year_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'rate1', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'twoyear_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'threeyear_yearrate', 'type': 'decimal(18,8)', 'comment': ''},
                    {'name': 'benchmark_show', 'type': 'string', 'comment': ''},
                    {'name': 'pfirst_amt', 'type': 'int', 'comment': ''},
                    {'name': 'prd_invest_type', 'type': 'string', 'comment': ''},
                    {'name': 'prd_time_limit', 'type': 'int', 'comment': ''},
                    {'name': 'prd_name2', 'type': 'string', 'comment': ''},
                    {'name': 'ta_code', 'type': 'string', 'comment': ''},
                    {'name': 'estab_date', 'type': 'int', 'comment': ''},
                    {'name': 'end_date', 'type': 'int', 'comment': ''},
                    {'name': 'ta_name', 'type': 'string', 'comment': ''},
                    {'name': 'ta_shortname', 'type': 'string', 'comment': ''},
                    {'name': 'nav_ratio', 'type': 'decimal(16,4)', 'comment': ''},
                    {'name': 'cycle_days', 'type': 'int', 'comment': ''},
                    {'name': 'lock_days', 'type': 'int', 'comment': ''},
                    {'name': 'term_enjoy_type', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.pro_ind_chrem,
                'comment': '',
                'field_list': [
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'prod_type', 'type': 'string', 'comment': ''},
                    {'name': 'risk_level', 'type': 'string', 'comment': ''},
                    {'name': 'invest_term', 'type': 'string', 'comment': ''},
                    {'name': 'mini_sale_amt', 'type': 'int', 'comment': ''},
                    {'name': 'inds_comp_base', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.rbw_09_01_HD,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_09_01_002_hd', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.rbw_04_27,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_04_27_006', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.rbw_07,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_07_00_002', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.rbw_04_10,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_04_10_054', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_04_10_055', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.rbw_08_hd,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_08_00_001', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.cstpro_ind_chrem,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'rct7d_rdmpt_amt', 'type': 'float', 'comment': ''},
                    {'name': 'rct1y_subsc_amt', 'type': 'float', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.rbw_06_06,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_06_06_007', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.rbw_01,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_01_00_001', 'type': 'string', 'comment': '客户姓名'},
                    {'name': 'lbl_01_00_023', 'type': 'string', 'comment': '客户手机号'},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.s14_tbproduct_h,
                'comment': '',
                'field_list': [
                    {'name': 'prd_code', 'type': 'string', 'comment': ''},
                    {'name': 'prd_name', 'type': 'string', 'comment': ''},
                    {'name': 'prd_type', 'type': 'string', 'comment': ''},
                    {'name': 'pfirst_amt', 'type': 'decimal(26,8)', 'comment': ''},
                    {'name': 'prd_attr', 'type': 'string', 'comment': ''},
                    {'name': 'risk_level', 'type': 'int', 'comment': ''},
                    {'name': 'channels', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.pro_ind_fund,
                'comment': '',
                'field_list': [
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'rct1y_profit_rate', 'type': 'decimal(16,4)', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.cstpro_ind_fund,
                'comment': '',
                'field_list': [
                    {'name': 'prod_id', 'type': 'string', 'comment': ''},
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'rct1w_mbank_subsc_amt', 'type': 'decimal(26,6)', 'comment': ''},
                    {'name': 'rct7d_rdmpt_amt', 'type': 'float', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.rbw_08,
                'comment': '',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'lbl_08_00_038', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'partitionbid', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.s14_tbthemeprd_h,
                'comment': '',
                'field_list': [
                    {'name': 'prd_code', 'type': 'string', 'comment': ''},
                    {'name': 'theme_id', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': base.external_table.zj_reco_result,
                'comment': '',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': ''},
                    {'name': 'item_id', 'type': 'string', 'comment': ''},
                    {'name': 'item_type', 'type': 'string', 'comment': ''},
                    {'name': 'channel_id', 'type': 'string', 'comment': ''},
                    {'name': 'banner_id', 'type': 'string', 'comment': ''},
                    {'name': 'score', 'type': 'int', 'comment': ''},
                    {'name': 'prediction', 'type': 'double', 'comment': ''},
                    {'name': 'version', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景ID'},
                    {'name': 'pipeline_id', 'type': 'string', 'comment': ''},
                ],
            },
            {
                'name': base.external_table.zj_reco_result_delivery,
                'comment': '',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': ''},
                    {'name': 'item_id', 'type': 'string', 'comment': ''},
                    {'name': 'item_type', 'type': 'string', 'comment': ''},
                    {'name': 'channel_id', 'type': 'string', 'comment': ''},
                    {'name': 'banner_id', 'type': 'string', 'comment': ''},
                    {'name': 'score', 'type': 'int', 'comment': ''},
                    {'name': 'prediction', 'type': 'double', 'comment': ''},
                    {'name': 'version', 'type': 'string', 'comment': ''},
                    {'name': 'delivery_status', 'type': 'int', 'comment': ''},
                    {'name': 'pipeline_id', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '日期分区'},
                ],
            },
            {
                'name': base.external_table.fin_whiteblacklist,
                'comment': '',
                'field_list': [
                    {'name': 'prd_code', 'type': 'string', 'comment': ''},
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
        ]
