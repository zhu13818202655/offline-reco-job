/* global $Ctx */

$Ctx.Spec = {
    template: {
        'channel_id|1': [
            'PSMS_IRE',
        ],
        'banner_id': '',
        'scene_id': 'scene_test',
        'user_type|1': [
            'exp',
            'exp1',
            'ctl',
            'ctl1',
            'noop',
        ],
        'prod_type|1': [
            'creditCard',
            'debitCard',
        ],
        operate: /\d{3}/,
        send: /\d{3}/,
        click: /\d{2}/,
        convert: /\d{1}/,
        'during|1': [
            'week',
            'day',
            'month',
        ],
        'dag_id|1': [
            'reco_debit_card_ca_0000',
            'reco_debit_card_ca_0001',
            'reco_credit_card_ca_0000',
        ],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'channel_id',
        'banner_id',
        'scene_id',
        'user_type',
        'prod_type',
        'operate',
        'send',
        'click',
        'convert',
        'during',
        'dag_id',
        'dt'
    ]
}
