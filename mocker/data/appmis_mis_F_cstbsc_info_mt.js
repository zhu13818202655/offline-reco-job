/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        cust_anniv_dt: '@datetimeRange("2020-01-01", "2022-01-04", "yyyyMMdd")',
        cust_mobile_no: '18686868686',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'cust_anniv_dt',
        'cust_mobile_no',
        'dt'
    ]
}
