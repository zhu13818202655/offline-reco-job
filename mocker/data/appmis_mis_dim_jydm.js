/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        if_use: '1',
        in_acct_type: '01主账户',
        trx_type1: '消费',
        id_jydm: 'id_j',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'if_use',
        'in_acct_type',
        'trx_type1',
        'id_jydm',
        'dt'
    ]
}
