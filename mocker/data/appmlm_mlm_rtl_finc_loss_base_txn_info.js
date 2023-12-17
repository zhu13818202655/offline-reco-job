
/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "if_cust_mngr|1": ["0", "1", ""],
        "cur_month_cust_new|1": ["0", "1", ""],
        "nold_prod_num|1": [0, 1, 2, 3],
        "str_dpst_mature_cnt|1": [0, 1, 2, 3],
        "trust_mature_cnt|1": [0, 1, 2, 3],
        "fix_dpst_mature_cnt|1": [0, 1, 2, 3],
        "tbond_mature_cnt|1": [0, 1, 2, 3],
        "if_nong_mature_next_month|1": ["0", "1", ""],
        "if_finc_mature_next_month|1": ["0", "1", ""],
        "fin_mature_cnt_1|1": [0, 1, 2, 3],
        "nong_fin_mature_cnt|1": [0, 1, 2, 3],
        "ropen_fin_mature_cnt|1": [0, 1, 2, 3],
        "copen_fin_mature_cnt|1": [0, 1, 2, 3],
        "close_fin_mature_cnt|1": [0, 1, 2, 3],
        "ombs_month_browse_finc_num_1|1": [0, 1, 2, 3],
        "ombs_month_browse_finc_num_2|1": [0, 1, 2, 3],
        "owec_month_browse_finc_num_1|1": [0, 1, 2, 3],
        "owec_month_browse_finc_num_2|1": [0, 1, 2, 3],
        "oibs_month_browse_finc_num_1|1": [0, 1, 2, 3],
        "pibs_month_browse_finc_num_2|1": [0, 1, 2, 3],
        "follw_fund_prd_num|1": [0, 1, 2, 3],
        "follow_finc_prd_num|1": [0, 1, 2, 3],
        "if_follow_yjl_prd|1": ["0", "1", ""],
        "if_follow_flp_prd|1": ["0", "1", ""],
        "pmbs_recent_browse_finc_prod_tm": "@float(0, 100)",
        "pwec_month_browse_finc_num_3|1": [0, 1, 2, 3],
        "pibs_month_browse_finc_num_3|1": [0, 1, 2, 3],
        dt: '@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        "cust_id",
        "if_cust_mngr",
        "cur_month_cust_new",
        "nold_prod_num",
        "str_dpst_mature_cnt",
        "trust_mature_cnt",
        "fix_dpst_mature_cnt",
        "tbond_mature_cnt",
        "if_nong_mature_next_month",
        "if_finc_mature_next_month",
        "fin_mature_cnt_1",
        "nong_fin_mature_cnt",
        "ropen_fin_mature_cnt",
        "copen_fin_mature_cnt",
        "close_fin_mature_cnt",
        "ombs_month_browse_finc_num_1",
        "ombs_month_browse_finc_num_2",
        "owec_month_browse_finc_num_1",
        "owec_month_browse_finc_num_2",
        "oibs_month_browse_finc_num_1",
        "pibs_month_browse_finc_num_2",
        "follw_fund_prd_num",
        "follow_finc_prd_num",
        "if_follow_yjl_prd",
        "if_follow_flp_prd",
        "pmbs_recent_browse_finc_prod_tm",
        "pwec_month_browse_finc_num_3",
        "pibs_month_browse_finc_num_3",
        "dt",
    ]
}

