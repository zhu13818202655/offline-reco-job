/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "lbl_09_01_002_hd":'1',
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'lbl_09_01_002_hd',
        'dt',
    ]
}
