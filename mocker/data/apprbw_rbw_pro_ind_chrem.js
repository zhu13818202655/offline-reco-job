/* global $Ctx */

$Ctx.Spec = {
    template: {
        'prod_id|1':['WPZF22Y01011','WPTK22D1401A','AF213030G','17517S','AF223200B','WPXK22D0701A','WPTK22D02A','W2016901B','5811321005','WPJK18M1227Y'],
        'prod_type|1':['35','5','32','1','37'],
        'risk_level:1':['01','03','02'],
        'inds_comp_base|1':['4.09%','0.0415'],
        'invest_term|1':['100','1000','1500','500'],
        'mini_sale_amt|1':[100,20000,50000,1000000],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
    },
    fields: [
        'prod_id',
        'prod_type',
        'risk_level',
        'inds_comp_base',
        'invest_term',
        'mini_sale_amt',
        'dt',
    ]
}
