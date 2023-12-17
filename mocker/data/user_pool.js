/* global $Ctx */

$Ctx.Spec = {
    template: {
        user_id: /\d{3}/,
        user_type: $Ctx.Mock.Random.weightedPick({
            'exp': 9,
            'ctl': 1,
        }),
        'dag_id|1': [
            'reco_credit_card_ca_0000',
            'reco_credit_card_ca_0001',
        ],
        'score|0.4': 1,
        batch_id: function () {
            return this.dt;
        },
        dt: '@datetimeRange("2021-12-30", "2022-01-05", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'user_id',
        'user_type',
        'score',
        'batch_id',
        'dt',
        'dag_id',
    ]
}
