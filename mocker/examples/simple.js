/* global $Ctx */

$Ctx.Spec = {
  template: {
    builtin: '@id',
    date: '@datetimeRange("2020-01-01", "2021-01-01", "yyyy-MM-dd")',
    datetime: '@datetimeRange("2020-01-01", "2021-01-01", "yyyy-MM-dd hh:mm:ss")',
    double: '@float(100, 1000)',
    doubleWithAccuracy: '@float(100, 1000, 2, 4)',
    long: '@integer(100, 1000)',
    word: '@word(6)',
    uniqueWord: '@uniqueWord(6)',
    'enum|1': [
      '12480788',
      '15420416'
    ],
    plain: '755'
  }
}
