/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "lbl_07_00_002|1":['53','54','10','20','00'],
        partitionbid: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'lbl_07_00_002',
        'partitionbid',
    ]
}
