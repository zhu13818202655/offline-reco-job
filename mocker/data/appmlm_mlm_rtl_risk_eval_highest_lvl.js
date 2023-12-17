
/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        risk_lvl_high: /\d{1}/,
        dt: '@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        "cust_id",
        "risk_lvl_high",
        "dt",
    ]
}

