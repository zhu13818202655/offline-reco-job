/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        card_pim_dsc: '交易分期1',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'card_pim_dsc',
        'dt'
    ]
}
