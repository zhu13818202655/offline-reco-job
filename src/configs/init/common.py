# -*- coding: utf-8 -*-
# @File : common.py
# @Author : r.yang
# @Date : Thu Nov 24 13:57:51 2022
# @Description : format string


from typing import List

from configs.model.base import ChannelInfo, ItemInfo, SceneInfo, TrainConfig, UserInfo
from configs.model.config import AdhocSceneConfig, ChannelOperateConfig, GroupInfo, SceneConfig
from configs.model.table import ExternalTable, OutputTable
from configs.utils import Env, ProdType, UserType


def get_output_table(table_name_builder) -> OutputTable:
    return OutputTable(
        runtime_config=table_name_builder('runtime_config'),
        log=table_name_builder('spark_job_log'),
        user_feature=table_name_builder('user_feature'),
        model_sample=table_name_builder('model_sample'),
        model_user_feature=table_name_builder('model_user_feature'),
        user_pool=table_name_builder('user_pool'),
        operating_user=table_name_builder('operating_user'),
        crowd_package=table_name_builder('crowd_package'),
        cancel_user=table_name_builder('cancel_user'),
        crowd_feedback=table_name_builder('crowd_feedback'),
        weixin_feedback=table_name_builder('weixin_feedback'),
        attr_feedback=table_name_builder('attr_feedback'),
        report_app=table_name_builder('report_app'),
        report_sms=table_name_builder('report_sms'),
        report_agg=table_name_builder('report_agg'),
        sms_reco_results=table_name_builder('sms_reco_results_v2'),
        app_reco_results=table_name_builder('app_reco_results'),
        push_reco_results=table_name_builder('push_reco_results'),
        whitelist_results=table_name_builder('crowd_whitelist'),
        s1106_results=table_name_builder('s1106_results'),
        s1108_results=table_name_builder('s1108_results'),
        s1109_results=table_name_builder('s1109_results'),
        ire_model_result=table_name_builder('ire_model_result'),
        user_percentile=table_name_builder('user_percentile'),
        ire_user_pred_result=table_name_builder('ire_user_pred_result'),
    )


def get_external_table(table_name_builder) -> ExternalTable:
    return ExternalTable(
        cre_crd_user=table_name_builder('apprbw_rbw_cstpro_ind_cre_crd'),
        deb_crd_user=table_name_builder('apprbw_rbw_cstpro_ind_deb_crd'),
        cust_act_label=table_name_builder('appcha_cha_gj_pmbs_cust_act_label'),
        hx_cancel_log=table_name_builder('appmis_mis_f_card_bscmtn_info'),  # 操作日志表, 卡核心销户用户
        hx_cancel_user=table_name_builder('appmis_mis_f_cstbsc_info'),  # 卡核心未销户用户
        mt_cancel_user=table_name_builder('appmis_mis_f_crdbsc_info_mt'),  # 美团卡(未),销户用户
        can_acct_type=table_name_builder('appmis_e_mis_can_acct_type_to_bi'),  # 是否可在线挽留
        ath_txn_info=table_name_builder('appmis_mis_f_card_ath_txn_info'),  # 交易信息表
        dim_jydm=table_name_builder('appmis.mis_dim_jydm'),
        cstbsc_info_mt=table_name_builder('appmis_mis_f_cstbsc_info_mt'),
        ath_txn_info_mt=table_name_builder('appmis_mis_f_card_ath_txn_info_mt'),
        flag_month=table_name_builder('appmis_mis_flg_bas_sum_month'),  # 信用卡用户标签
        card_pim_info=table_name_builder('appmis_mis_f_card_pim_info'),  # 分期记录
        rtl_asset_bal=table_name_builder('appmlm.mlm_rtl_asset_bal'),  # v1.2
        user_tel_number=table_name_builder('appims_ims_cust_phoneno'),  # 手机号
        user_stat_mt=table_name_builder('apprbw_e_rbw_cust_camp_popupw_stat_mt'),  # 营销授权
        sms_feedback=table_name_builder('shdata_s35_edb_sms_outslog'),  # 短信反馈
        app_feedback=table_name_builder('shdata_s70_event'),  # 手机银行反馈
        app_action=table_name_builder('appcha.p_bh_logon_contact_act'),  # 手机银行打开
        custom_data=table_name_builder('shdata_s70_custom_data_json'),
        s21_ecusrdevice=table_name_builder('shdata_s21_ecusrdevice'),
        s21_ecusr_h=table_name_builder('shdata_s21_ecusr_h'),
        s21_ecextcifno_h=table_name_builder('shdata_s21_ecextcifno_h'),
        s70_exposure=table_name_builder('shdata_s70_exposure_log'),
        s38_behavior=table_name_builder('shdata_s38_behavior_pmbs'),
        s70_t2_resource=table_name_builder('shdata_s70_t2_resource_material'),
        epm_activity=table_name_builder('appims_ims_f_epm_activity_info'),
        # 特征工程输入表，前缀必须是 user_feat 或 item_feat，后续会自动加载
        user_feat_ls_crd=table_name_builder('apprbw_rbw_lsyb_ls_crd'),
        fin_tbproduct=table_name_builder('xianzhi.fin_tbproduct'),
        s14_tbproduct_h=table_name_builder('shdata_s14_tbproduct_h'),
        s14_tbthemeprd_h=table_name_builder('shdata_s14_tbthemeprd_h'),
        pro_ind_chrem=table_name_builder('apprbw_rbw_pro_ind_chrem'),
        pro_ind_fund=table_name_builder('apprbw_rbw_pro_ind_fund'),
        rbw_09_01_HD=table_name_builder('apprbw_rbw_09_01_HD '),
        rbw_04_27=table_name_builder('apprbw_rbw_04_27'),
        rbw_07=table_name_builder('apprbw_rbw_07'),
        rbw_04_10=table_name_builder('apprbw_rbw_04_10'),
        rbw_08_hd=table_name_builder('apprbw_rbw_08_hd'),
        rbw_08=table_name_builder('apprbw_rbw_08'),
        cstpro_ind_chrem=table_name_builder('apprbw_rbw_cstpro_ind_chrem'),
        cstpro_ind_fund=table_name_builder('apprbw_rbw_cstpro_ind_fund'),
        rbw_06_06=table_name_builder('apprbw_rbw_06_06'),
        rbw_01=table_name_builder('apprbw_rbw_01'),
        zj_reco_result=table_name_builder('xianzhi.zj_reco_result'),
        zj_reco_result_delivery=table_name_builder('xianzhi.zj_reco_result_delivery'),
        fin_whiteblacklist=table_name_builder('xianzhi.fin_whiteblacklist'),
    )


CNAME_APP = 'PMBS_IRE'
CNAME_SMS = 'PSMS_IRE'

C_SMS = ChannelInfo(
    channel_id=CNAME_SMS,
    channel_name='智能短信渠道',
    feedback_delay=3,
    push_delay=0,
    max_push=30,
    allow_repeat=True,
    num_items=1,
)

C_APP_WHITELIST = ChannelInfo(
    channel_id=CNAME_APP,
    channel_name='手机银行白名单弹窗渠道',
    banner_id='WHITELIST',
    feedback_delay=1,
    push_delay=1,
    max_push=30,
    allow_repeat=True,
    num_items=1,
)


def get_reco_db() -> str:
    if Env.is_prod or Env.is_stage:
        return 'xianzhi'
    return 'recodb'


def get_channels() -> List[ChannelInfo]:
    return [
        C_SMS,
        C_APP_WHITELIST,
        ChannelInfo(
            channel_id=CNAME_APP,
            channel_name='手机银行首页弹窗',
            banner_id='71_sy_navigation_tab_popup',
            feedback_delay=1,
            push_delay=1,
            max_push=30,
            allow_repeat=True,
            num_items=1,
        ),
        ChannelInfo(
            channel_id=CNAME_APP,
            channel_name='手机银行信用卡标版首页主banner',
            banner_id='71_bbsy_banner',
            feedback_delay=1,
            push_delay=1,
            max_push=30,
            allow_repeat=True,
            num_items=1,
        ),
        ChannelInfo(
            channel_id=CNAME_APP,
            channel_name='手机银行7.0_尊享版_信用卡_轮播banner',
            banner_id='71_zxcreditCard_Banner',
            feedback_delay=1,
            push_delay=1,
            max_push=30,
            allow_repeat=True,
            num_items=1,
        ),
        ChannelInfo(
            channel_id=CNAME_APP,
            channel_name='手机银行7.0_标版_信用卡_推荐办卡banner',
            banner_id='71_bbcreditCard_tjbk',
            feedback_delay=1,
            push_delay=1,
            max_push=30,
            allow_repeat=True,
            num_items=1,
        ),
        ChannelInfo(
            channel_id=CNAME_APP,
            channel_name='手机银行7.0_标版_首页_每日权益广告banner',
            banner_id='71_bbsy_mrhdtj',
            feedback_delay=1,
            push_delay=1,
            max_push=30,
            allow_repeat=True,
            num_items=1,
        ),
    ]


S_CREDIT = SceneInfo(
    scene_id='scene_credit_card_ca',
    scene_name='信用卡获客场景',
    prod_type=ProdType.CREDIT,
    item_selector={'type': ProdType.CREDIT},
)
S_DEBIT = SceneInfo(
    scene_id='scene_debit_card_ca',
    scene_name='借记卡获客场景',
    prod_type=ProdType.DEBIT,
    item_selector={'type': ProdType.DEBIT},
)
S_CREDIT_CANCEL = SceneInfo(
    scene_id='scene_cancel',
    scene_name='信用卡销户场景',
    prod_type=ProdType.CREDIT,
    item_selector={'type': ProdType.CREDIT},
)


def get_scenes() -> List[SceneInfo]:
    return [
        S_CREDIT,
        S_DEBIT,
        S_CREDIT_CANCEL,
    ]


def get_items() -> List[ItemInfo]:
    return [
        ItemInfo(item_ids=['6524'], label=0, desc='006524银联标准金卡IC卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['1101'], label=1, desc='001101VISA双币种金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6053'], label=2, desc='006053银联标准白金信用卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6158', '6315'], label=3, desc='006158银联年轻无界金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6152'], label=4, desc='006152乐乐茶联名银联金卡(温变卡)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6153'], label=5, desc='006153乐乐茶联名银联金卡(透明卡)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6047'], label=6, desc='006047小刘鸭银联白金卡(精致版)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6139'], label=7, desc='006139小刘鸭银联金卡', type=ProdType.CREDIT),
        ItemInfo(
            item_ids=['6127'], label=8, desc='006127招财猫主题信用卡银联金卡(蓝色黑猫版)', type=ProdType.CREDIT
        ),
        ItemInfo(
            item_ids=['6128'], label=9, desc='006128招财猫主题信用卡银联金卡(红色白猫版)', type=ProdType.CREDIT
        ),
        ItemInfo(
            item_ids=['6129'], label=10, desc='006129招财猫主题信用卡银联金卡(金色黑猫版)', type=ProdType.CREDIT
        ),
        ItemInfo(
            item_ids=['6130'], label=11, desc='006130招财猫主题信用卡银联金卡(粉色白猫版)', type=ProdType.CREDIT
        ),
        ItemInfo(item_ids=['6324'], label=12, desc='006324招财猫主题信用卡银联金卡(透明版)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6330'], label=13, desc='006330招财猫主题信用卡银联金卡(花卉版)', type=ProdType.CREDIT),
        ItemInfo(
            item_ids=['4045'], label=14, desc='004045酷MA萌(熊本熊)JCB白金卡(精致版)', type=ProdType.CREDIT
        ),
        ItemInfo(item_ids=['4046'], label=15, desc='004046酷MA萌JCB白金卡(精致版)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['4112'], label=16, desc='004112酷MA萌JCB金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['4120'], label=17, desc='004120酷MA萌(熊本熊)JCB金卡', type=ProdType.CREDIT),
        ItemInfo(
            item_ids=['6045'], label=18, desc='006045酷MA萌(熊本熊)银联白金卡(精致版)', type=ProdType.CREDIT
        ),
        ItemInfo(item_ids=['6046'], label=19, desc='006046酷MA萌银联白金卡(精致版)', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6103'], label=20, desc='006103酷MA萌(熊本熊)银联金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6112'], label=21, desc='006112酷MA萌银联金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6331'], label=22, desc='006331光明随心订银联金卡', type=ProdType.CREDIT),
        ItemInfo(item_ids=['6304'], label=23, desc='006304上海银行哈啰出行联名信用卡银联金卡', type=ProdType.CREDIT),
        ItemInfo(
            item_ids=['6055', '6048'], label=24, desc='006055银联年轻无界白金卡(精致版)', type=ProdType.CREDIT
        ),
        ItemInfo(
            item_ids=['CCLN'],
            desc='威萌虎生肖卡',
            type=ProdType.DEBIT,
            label=0,
            sms_content='上海银行威萌虎生肖借记卡，做工精美，祝您的生活如虎添翼！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—生肖卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGM'],
            desc='招财猫主题借记卡',
            type=ProdType.DEBIT,
            label=1,
            sms_content='上海银行招财猫主题借记卡，做工精美，祝您好运滚滚来！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡-招财猫主题借记卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGN'],
            desc='招财猫主题借记卡（樱花限量版）',
            type=ProdType.DEBIT,
            label=2,
            sms_content='上海银行招财猫主题借记卡，做工精美，祝您好运滚滚来！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡-招财猫主题借记卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGH'],
            desc='许愿卡（健康BOY）',
            type=ProdType.DEBIT,
            label=3,
            sms_content='上海银行许愿卡（借记卡），做工精美，祝您拥有浪漫的日子，不一样的心愿！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCKP'],
            desc='许愿卡（无忧）',
            type=ProdType.DEBIT,
            label=4,
            sms_content='上海银行许愿卡（借记卡），做工精美，祝您拥有浪漫的日子，不一样的心愿！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGI'],
            desc='许愿卡（旺运）',
            type=ProdType.DEBIT,
            label=5,
            sms_content='上海银行许愿卡（借记卡），做工精美，祝您拥有浪漫的日子，不一样的心愿！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGG'],
            desc='许愿卡（桃花GIRL）',
            type=ProdType.DEBIT,
            label=6,
            sms_content='上海银行许愿卡（借记卡），做工精美，祝您拥有浪漫的日子，不一样的心愿！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCGJ'],
            desc='许愿卡（生财）',
            type=ProdType.DEBIT,
            label=7,
            sms_content='上海银行许愿卡（借记卡），做工精美，祝您拥有浪漫的日子，不一样的心愿！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
        ItemInfo(
            item_ids=['CCKO'],
            desc='许愿卡（锦鲤）',
            type=ProdType.DEBIT,
            label=8,
            sms_content='上海银行许愿系列锦鲤卡（借记卡），做工精美，祝您的生活锦鲤上升、梦想成真！关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡—许愿卡”即可申请，详询95594。[回复TD退订]',
        ),
    ]


def get_test_users() -> List[UserInfo]:
    return [
        UserInfo(user_id='3127852100', scope='all', phone_no='18201902482'),  # 秦老师
        UserInfo(user_id='3095681436', scope='all', phone_no='18818206097'),
        UserInfo(user_id='3127199832', scope='all', phone_no='16622808625'),
        UserInfo(user_id='3032284975', scope='all', phone_no='15967367594'),
        UserInfo(user_id='3126953062', scope='all', phone_no='18269799386'),
        UserInfo(user_id='3001078054', scope='debit', phone_no='13564426102'),  # 关老师
        UserInfo(user_id='6011259147', scope='credit', phone_no='15800340557'),  # 张亚光老师
        UserInfo(user_id='3001076536', scope='credit', phone_no='13917454306'),  # 任松龄老师
        # 手机银行测试
        UserInfo(user_id='3133523681', scope='all', phone_no='18399984474'),
        UserInfo(user_id='3133523682', scope='all', phone_no='15718419806'),
        UserInfo(user_id='3133523683', scope='all', phone_no='15156037136'),
        UserInfo(user_id='3133523684', scope='all', phone_no='18648071640'),
        UserInfo(user_id='3133523685', scope='all', phone_no='13847433688'),
        UserInfo(user_id='3133523686', scope='all', phone_no='17151316333'),
    ]


def get_group_cfg(channel_id, banner_id, items=[]) -> ChannelOperateConfig:
    return ChannelOperateConfig(
        channel_id=channel_id,
        banner_id=banner_id,
        group_users={
            UserType.CTL.value: GroupInfo(num=25000, minimum=True),  # 对照组（随机圈人，随机投产品）
            UserType.EXP.value: GroupInfo(num=600000),  # 实验组（算法圈人，算法投产品）
            UserType.EXP1.value: GroupInfo(num=25000),  # 实验组1 （算法圈人，随机投产品）
            UserType.CTL1.value: GroupInfo(num=25000),  # 对照组1 （算法圈人，不投放）
            UserType.NOOP.value: GroupInfo(num=25000, minimum=True),  # 空白组（不参与投放）
        },
        groups_no_push=[UserType.CTL1, UserType.NOOP],
        items_specified=items,
    )


def get_feature_map(is_local: bool) -> dict:
    if is_local:
        return {
            'default': [],
            'standardScaler': [],
            'quantileDiscretizer': [],
            'bucketizer': [['age', [0.0, 11.0, 21.0, 31.0, 41.0, 51.0, 61.0]]],
            'stringIndex': ['phone_valid_ind'],
            'onehot': ['sex'],
            'log1p': ['mov_cnt'],
            'dateDiff': ['last_tran_date'],
        }
    return {
        'default': [],
        'standardScaler': [],
        'quantileDiscretizer': [],
        'bucketizer': [['age', [0.0, 11.0, 21.0, 31.0, 41.0, 51.0, 61.0]]],
        'stringIndex': [
            'card_lvl_crdit',
            'mov_cnt_groups',
            'aum_n_zhixiao_m_avg_level',
            'lum_bal_level',
            'phone_valid_ind',
            'zxyh_cust_ind',
            'risk_fund_valid_ind',
            'risk_fin_valid_ind',
            'mngr_ind',
            'salary_ind',
            'pension_ind',
            'debcard_ind',
            'debcard_sbk_ind',
            'loan_person_ind',
            'loan_house_ind',
            'loan_car_ind',
            'both_card_ind',
            'credit_card_only_ind',
            'loan_car_only_ind',
            'credit_card_hold_ind',
            'debit_cust_ind',
            'debit_cust_n_daifa_ind',
            'mt_card_ind',
            'crd_card_ind',
            'over_star_card_ind',
            'huitong_ind',
            'daixiao_cust_ind',
            'newcust_ls_year_ind',
            'newcust_ls_month_ind',
            'newcust_crd_year_ind',
            'newcust_crd_month_ind',
            'deposit_cur_cust_ind',
            'deposit_fix_cust_ind',
            'fin_bb_cust_ind',
            'fin_nbb_cust_ind',
            'stru_cust_ind',
            'statbond_cust_ind',
            'fund_cust_ind',
            'insu_cust_ind',
            'trust_cust_ind',
            'gold_cust_ind',
            'fund_non_cur_cust_ind',
            'fund_spe_cust_ind',
            'instal_cust_ind',
            'loan_cust_ind',
            'caifu_fuzai_cust_ind',
            'deposit_p_cust_ind',
            'fin_daixiao_cust_ind',
            'pmbs_cust_ind',
            'pwec_cust_ind',
            'pibs_cust_ind',
            'bind_alipay_debit_ind',
            'bind_alipay_crd_ind',
            'bind_wechat_debit_ind',
            'bind_wechat_crd_ind',
            'mob_pay_cust_ind',
            'auto_repay_cust_ind',
            'thd_pty_sec_hold_cust_ind',
            'pub_ser_fee_hold_cust_ind',
            'pwec_act_ind',
            'pmbs_act_ind',
            'counter_act_ind',
            'deb_txn_m_ind',
            'crd_txn_m_ind',
            'cum_cash_txn_m_ind',
            'crd_card_txn_m_ind',
            'repay_m_ind',
            'mov_ind',
        ],
        'onehot': ['sex'],
        'log1p': [
            'mov_cnt',
            'mov_amt',
            'xfqx_cnt',
            'xfqx_amt',
            'deb_txn_m_cnt',
            'deb_txn_m_amt',
            'crd_txn_m_cnt',
            'crd_txn_m_amt',
            'cum_cash_m_cnt',
            'cum_cash_m_amt',
            'crd_card_txn_m_cnt',
            'crd_card_txn_m_amt',
            'repay_amt',
            'tot_asset_n_zhixiao_bal',
            'tot_asset_n_zhixiao_month_avg_bal',
            'tot_asset_bal',
            'tot_asset_month_avg_bal',
            'loan_bal',
            'loan_house_bal',
            'loan_car_bal',
            'cust_bal',
            'pim_bal',
            'cust_limit',
            'lum_bal',
            'fin_bb_txn_cnt',
            'fin_nbb_txn_cnt',
            'fund_non_cur_txn_cnt',
            'fund_spe_txn_cnt',
            'trust_txn_cnt',
            'insu_txn_cnt',
            'fin_bb_txn_amt',
            'fin_nbb_txn_amt',
            'fund_non_cur_txn_amt',
            'fund_spe_txn_amt',
            'trust_txn_amt',
            'insu_txn_amt',
            'fin_nbb_income',
            'fund_non_cur_income',
            'insu_income',
            'channel_cnt',
            'fuzhai_prd_cnt',
            'deposit_p_bal',
            'fin_daixiao_bal',
            'deposit_cur_bal',
            'deposit_fix_bal',
            'fin_bb_bal',
            'fin_nbb_bal',
            'stru_bal',
            'statebond_bal',
            'fund_bal',
            'insu_bal',
            'trust_bal',
            'gold_bal',
            'fund_non_cur_cust_bal',
            'fund_spe_cust_bal',
        ],
        'dateDiff': ['last_tran_date', 'crd_txn_last_dt', 'deb_txn_last_dt',],
    }


def get_adhoc_scene_config() -> AdhocSceneConfig:
    return AdhocSceneConfig(
        s1106_base_product='9B310115',
        s1108_base_product='9B310115',
        s1106_max_prod_num=5,
        s1108_max_prod_num=5,
        s1109_max_prod_num=5,
    )


def adhoc_scene() -> SceneConfig:
    return SceneConfig(
        scene_id='adhoc',
        scene_name='adhoc',
        adhoc=get_adhoc_scene_config(),
        item_selector={},
        train=TrainConfig(sample_dt='20220501', train_sample_month_num=4, eval_sample_month_num=1,),
    )
