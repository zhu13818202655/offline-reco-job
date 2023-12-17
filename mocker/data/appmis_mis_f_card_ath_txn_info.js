/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        acct_id_dt: '@datetimeRange("2020-01-01", "2021-01-04", "yyyyMMdd")',
        in_acct_amt: 100,
        in_acct_ccy_cd: 'CNY',
        crd_card_trans_cd: 'id_j',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'acct_id_dt',
        'in_acct_amt',
        'in_acct_ccy_cd',
        'crd_card_trans_cd',
        'dt'
    ]
}
