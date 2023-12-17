/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "lbl_04_10_054":'1',
        "lbl_04_10_055":'1',
        partitionbid: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'lbl_04_10_055',
        'lbl_04_10_054',
        'partitionbid',
    ]
}
