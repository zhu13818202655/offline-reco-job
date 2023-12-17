/* global $Ctx */

$Ctx.Spec = {
    template: {
        'cust_id': /\d{3}/,
        'prod_id|1':['WPZF22Y01011','WPTK22D1401A','AF213030G','17517S','AF223200B','WPXK22D0701A','WPTK22D02A','W2016901B','5811321005','WPJK18M1227Y'],
        'rct7d_rdmpt_amt|1':[0,1,2,3,4,5],
        'rct1y_subsc_amt|1': [0,1],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'cust_id',
        'prod_id',
        'rct7d_rdmpt_amt',
        'rct1y_subsc_amt',
        'dt',
    ]
}
