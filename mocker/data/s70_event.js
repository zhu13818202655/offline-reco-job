/* global $Ctx */

$Ctx.Spec = {
    template: {
        useridentifier: /\d{11}_\d{10}/,
        event_identifier: 'TODO',
        dt: '@datetimeRange("2022-01-01", "2022-01-04", "yyyyMMdd")' // 数据分区日期
    },
    fields: [
        'useridentifier',
        'event_identifier',
        'dt'
    ]
}
