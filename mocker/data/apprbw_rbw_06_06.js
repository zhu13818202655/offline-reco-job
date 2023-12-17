/* global $Ctx */

$Ctx.Spec = {
    template: {
        "cust_id": /\d{3}/,
        "lbl_06_06_007|1":['20221201','20220101','20230329'],
        partitionbid: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'lbl_06_06_007',
        'partitionbid',
    ]
}
