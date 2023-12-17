/* global $Ctx */

const genRandomHex = size => [...Array(size)].map(() => Math.floor(Math.random() * 16).toString(16)).join('')

$Ctx.Mock.Random.extend({
  myid: function (len) {
    if (typeof len === 'undefined') {
      len = 6
    }
    return genRandomHex(len)
  }
})

const mapping = {
  310000: '上海市',
  320000: '江苏省',
  330000: '浙江省',
  340000: '安徽省'
}

const weighted = {
  A: 1,
  B: 1,
  C: 10
}

$Ctx.Spec = {
  template: {
    custom: '@myid',
    customWithParams: '@myid(8)',
    'ser|1': Object.keys(mapping),
    name: function () {
      return mapping[this.ser]
    },
    weighted: $Ctx.Mock.Random.weightedPick(weighted),
    date: '@datetimeRange("2020-01-01", "2021-01-01", "yyyy-MM-dd")',
    datetime: '@datetimeRange("2020-01-01", "2021-01-01", "yyyy-MM-dd hh:mm:ss")'
  }
}
