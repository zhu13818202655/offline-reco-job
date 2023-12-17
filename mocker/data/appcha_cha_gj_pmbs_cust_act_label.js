/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        pmbs_cust_ind: $Ctx.Mock.Random.weightedPick({
            '1': 9,
            '0': 1,
        }),
        data_dt: '@datetimeRange("2022-01-01", "2022-01-01", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'pmbs_cust_ind',
        'data_dt'
    ]
}
