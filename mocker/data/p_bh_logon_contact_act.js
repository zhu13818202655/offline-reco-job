/* global $Ctx */

$Ctx.Spec = {
    template: {
        'act_nm|1': ['登录', '指纹登录', '人脸登录balabala'],
        'channel_type|1': ['PWEC','PITM','PMBS','PIBS'],
        cust_id: /\d{3}/,
        pt_td: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'act_nm',
        'channel_type',
        'cust_id',
        'pt_td'
    ]
}
