/* global $Ctx */

$Ctx.Spec = {
    template: {
        bm_cust_id: /\d{3}/,
        age: 23,
        "activate_card_flag|1": [0, 1,],
        first_bin_wx: "20220202",
        first_bin_zfb: "20220202",
        first_bin_unionpay: "20220202",
        first_bin_mt: "20220202",
        first_bin_jd: "20220202",
        first_bin_sn: "20220202",
        first_bin_wp: "20220202",
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'cust_id',
        'age',
        'activate_card_flag',
        'first_bin_wx',
        'first_bin_zfb',
        'first_bin_unionpay',
        'first_bin_mt',
        'first_bin_jd',
        'first_bin_sn',
        'first_bin_wp',
        'dt'
    ]
}
