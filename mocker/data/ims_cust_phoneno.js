/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        phone_no: /18818881\d{3}/,
        credit_crd_phone_no: /18818881\d{3}/,
        s60_phone_no: /18818881\d{3}/,
        dt: '@datetimeRange("2022-01-01", "2022-01-01", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'phone_no',
        'credit_crd_phone_no',
        's60_phone_no',
        'dt'
    ]
}
