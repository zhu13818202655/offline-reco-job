/* global $Ctx */

$Ctx.Spec = {
    template: {
        'user_id|1': ['0', '1', '6', '7', '8', '9'],
        'item_id|1': ['WPZF22Y01011', 'WPTK22D1401A', 'AF213030G', '17517S', 'AF223200B', 'WPXK22D0701A', 'WPTK22D02A', 'W2016901B', '5811321005', 'WPJK18M1227Y'],
        'item_type|1': ['04', '05'],
        'channel_id|1': ['PMBS_IRE', 'PSMS_IRE'],
        'banner_id|1': ['71_bbsy_banner', '71_zxcreditCard_Banner', '71_bbcreditCard_tjbk'],
        'score|1': ['1', '2', '3'],
        'prediction|1': ['1.1', '0.2', '3.3'],
        'version|1': ['version1', 'version2', 'version3'],
        'delivery_status|1': ['0', '1'],
        'pipeline_id|1': ['pipeline_id1', 'pipeline_id2'],
        dt: '@datetimeRange("2021-12-28", "2022-01-05", "yyyyMMdd")', // 数据分区日期
        'scene_id|1': ['S2101', 'S2102', 'S2103'],
    },
    fields: [
        'user_id',
        'item_id',
        'item_type',
        'channel_id',
        'banner_id',
        'score',
        'prediction',
        'version',
        'delivery_status',
        'pipeline_id',
        'dt',
        'scene_id',
    ]
}
