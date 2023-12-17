/* global $Ctx */

$Ctx.Spec = {
    template: {
        bm_cust_id: /\d{3}/,
        "bm_opr_id|1": [
            'topcardbat1',
            'topcardweb1',
            'TOPCARD11',
        ],
        bm_new_contents: 'C',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'bm_cust_id',
        'bm_opr_id',
        'bm_new_contents',
        'dt'
    ]
}
