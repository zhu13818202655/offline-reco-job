/* global $Ctx */

$Ctx.Spec = {
    template: {
        cust_id: /\d{3}/,
        if_valid_hold_crd: $Ctx.Mock.Random.weightedPick({
            '1': 5,
            '0': 5,
        }),
        "prod_id|1": [
            '6524',
            '1101',
            '6053',
            '6158',
            '6315',
            '6152',
            '6153',
            '6047',
            '6139',
            '6127',
            '6128',
            '6129',
            '6130',
            '6324',
            '6330',
            '4045',
            '4046',
            '4112',
            '4120',
            '6045',
            '6046',
            '6103',
            '6112',
            '6331',
            '6304',
            '6048',
            '6055',
        ],
        last_apply_dt: '@datetimeRange("2021-12-31", "2022-02-28", "yyyyMMdd")',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'if_valid_hold_crd',
        'prod_id',
        'last_apply_dt',
        'dt'
    ]
}
