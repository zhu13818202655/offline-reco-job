/* global $Ctx */

$Ctx.Spec = {
    template: {
        'prd_code|1':['WPZF22Y01011','WPTK22D1401A','AF213030G','17517S','AF223200B','WPXK22D0701A','WPTK22D02A','W2016901B','5811321005','WPJK18M1227Y'],
        'prd_name|1': ['test'],
        'prod_type|1': ['0', '1', ''],
        'pfirst_amt|1': [1000, 2000],
        'prd_attr|1': ['0', '1'],
        'risk_level|1': [0,1,2,3,4,5],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'prd_code',
        'prd_name',
        'prod_type',
        'pfirst_amt',
        'prd_attr',
        'risk_level',
        'dt',
    ]
}
