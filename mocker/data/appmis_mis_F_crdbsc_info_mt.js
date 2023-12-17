/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        "card_ccl_ind|1": [
            '0',
            '1',
        ],
        card_distory_dt: '@datetimeRange("2021-03-01", "2022-03-01", "yyyyMMdd")',
        card_atv_dt: '@datetimeRange("2020-01-01", "2021-01-04", "yyyyMMdd")',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'card_ccl_ind',
        'card_distory_dt',
        'card_atv_dt',
        'dt'
    ]
}
