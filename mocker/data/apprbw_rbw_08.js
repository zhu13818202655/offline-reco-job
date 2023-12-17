/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "lbl_08_00_038|1":['0', '1', '2', '3', '4', '5'],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'lbl_08_00_038',
        'dt',
    ]
}
