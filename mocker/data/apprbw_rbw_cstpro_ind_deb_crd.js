/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        if_hold_card: $Ctx.Mock.Random.weightedPick({
            '1': 5,
            '0': 5,
        }),
        "prod_id|1": [
            'CCLI',
            'CCLN',
            'CCGM',
            'CCGN',
            'CCGH',
            'CCKP',
            'CCGI',
            'CCGG',
            'CCGJ',
            'CCKO',
            'CCGY',
        ],
        firsr_apply_dt: '@datetimeRange("2022-01-01", "2022-02-01", "yyyyMMdd")',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'if_hold_card',
        'prod_id',
        'firsr_apply_dt',
        'dt'
    ]
}
