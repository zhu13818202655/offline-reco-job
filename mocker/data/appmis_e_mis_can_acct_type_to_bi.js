/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        if_logout_online_value_cust: 1,
        if_logout_online_nvalue_cust: 1,
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'if_logout_online_value_cust',
        'if_logout_online_nvalue_cust',
        'dt'
    ]
}
