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
        "channel_id|1": ['PSMS_IRE', 'PMBS_IRE'],
        "banner_id": function () {
            if (this.channel_id == 'PMBS_IRE'){
                return 'WHITELIST';
            }
            return '';
        },
        'score|0.4': 1,
        'rank': /\d{1}/,
        model_version: 'v0',
        'dag_id|1': [
            'reco_credit_card_ca_0000',
            'reco_credit_card_ca_0001',
        ],
        'scene_id': 'scene_credit_card_ca',
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
        batch_id: function () {
            return this.dt;
        }
    },
    fields: [
        'user_id',
        'user_type',
        'item_id',
        'channel_id',
        'banner_id',
        'score',
        'rank',
        'model_version',
        'scene_id',
        'batch_id',
        'dt',
        'dag_id',
    ]
}
