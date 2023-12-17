# -*- coding: utf-8 -*-
# @File : table.py
# @Author : r.yang
# @Date : Fri Feb 11 14:42:10 2022
# @Description : table config


from pydantic import BaseModel


class OutputTable(BaseModel):

    runtime_config: str
    log: str

    user_feature: str
    model_sample: str
    model_user_feature: str

    user_pool: str
    operating_user: str
    crowd_package: str
    cancel_user: str
    user_percentile: str

    crowd_feedback: str
    weixin_feedback: str
    attr_feedback: str
    report_app: str
    report_sms: str
    report_agg: str

    # post process result
    sms_reco_results: str
    app_reco_results: str
    push_reco_results: str
    whitelist_results: str

    # model result to dw
    ire_model_result: str
    ire_user_pred_result: str

    s1106_results: str
    s1108_results: str
    s1109_results: str


class ExternalTable(BaseModel):

    cre_crd_user: str
    deb_crd_user: str
    cust_act_label: str

    hx_cancel_log: str
    hx_cancel_user: str
    mt_cancel_user: str
    can_acct_type: str
    ath_txn_info: str
    dim_jydm: str
    cstbsc_info_mt: str
    ath_txn_info_mt: str
    flag_month: str
    card_pim_info: str
    rtl_asset_bal: str

    user_tel_number: str
    user_stat_mt: str
    sms_feedback: str
    app_feedback: str
    app_action: str
    custom_data: str

    s21_ecusrdevice: str
    s21_ecusr_h: str
    s21_ecextcifno_h: str
    s70_exposure: str
    s38_behavior: str
    s70_t2_resource: str

    epm_activity: str

    # 特征工程输入表，前缀必须是 user_feat 或 item_feat，后续会自动加载
    user_feat_ls_crd: str
    # user_feat_basic_info: str = ExtDBField('appmlm_basic_info_tmp1')
    # user_feat_txn_info: str = ExtDBField('appmlm_mlm_rtl_finc_loss_base_txn_info')
    # user_feat_risk_info: str = ExtDBField('appmlm_mlm_rtl_risk_eval_highest_lvl')

    fin_tbproduct: str
    s14_tbproduct_h: str
    s14_tbthemeprd_h: str
    pro_ind_chrem: str
    pro_ind_fund: str

    rbw_07: str
    rbw_04_10: str
    rbw_09_01_HD: str
    rbw_04_27: str
    rbw_08_hd: str
    rbw_08: str
    cstpro_ind_chrem: str
    cstpro_ind_fund: str
    rbw_06_06: str
    rbw_01: str

    zj_reco_result: str
    zj_reco_result_delivery: str
    fin_whiteblacklist: str
