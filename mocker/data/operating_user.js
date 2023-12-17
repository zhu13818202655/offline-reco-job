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
        max_push: 4,
        plan_push: 1,
        'try_push|0-2': 1,
        'actual_push|0-1': 1,
        'tag|1': ['all', 'ready'],
        'dag_id|1': [
            'reco_credit_card_ca_0000',
            'reco_credit_card_ca_0001',
        ],
        batch_id: function () {
            return this.dt;
        },
        'dt|1': ["20211030", "20211230", "20220101", "20220102", '20220103', '20220104', '20220105'] //'@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'user_id',
        'user_type',
        'channel_id',
        'banner_id',
        'max_push',
        'plan_push',
        'try_push',
        'actual_push',
        'tag',
        'batch_id',
        'dt',
        'dag_id',
    ]
}
