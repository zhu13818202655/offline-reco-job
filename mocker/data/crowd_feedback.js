/* global $Ctx */

$Ctx.Spec = {
    template: {
        user_id: /\d{3}/,
        user_type: $Ctx.Mock.Random.weightedPick({
            'exp': 9,
            'ctl': 1,
        }),
        'item_id|1': [
            '0000',
            '0001',
            '0002',
            '0003',
            '0004',
        ],
        channel_id: 'PSMS_IRE',
        banner_id: '',
        'send|0-1': 1,
        'click|0-1': 1,
        convert: '',
        'dag_id|1': [
            'reco_credit_card_ca_0000',
            'reco_credit_card_ca_0001',
        ],
        batch_id: function () {
            return this.dt;
        },
        dt: '@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'user_id',
        'user_type',
        'item_id',
        'channel_id',
        'banner_id',
        'send',
        'click',
        'convert',
        'batch_id',
        'dt',
        'dag_id'
    ]
}
