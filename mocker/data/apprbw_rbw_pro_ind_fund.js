/* global $Ctx */

$Ctx.Spec = {
    template: {
        'prod_id|1':['WPZF22Y01011','WPTK22D1401A','AF213030G','17517S','AF223200B','WPXK22D0701A','WPTK22D02A','W2016901B','5811321005','WPJK18M1227Y'],
        'rct1y_profit_rate|1':[0.001,0.0415],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'prod_id',
        'rct1y_profit_rate',
        'dt',
    ]
}
