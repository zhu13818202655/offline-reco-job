# Mock

## Installation

Run `npm install`.

## CLI

```shell
$ ./lib/cli.js mock --help
cli.js mock [spec-file]

generate mock data

Options:
      --help           Show help                                       [boolean]
      --version        Show version number                             [boolean]
  -v, --verbose        Run with verbose logging                        [boolean]
      --count          number of records                         [default: 5000]
      --stdin          read from stdin                [boolean] [default: false]
      --format         spec file's format
                  [string] [choices: "detect", "js", "json"] [default: "detect"]
      --exporter       exporter       [string] [choices: "csv"] [default: "csv"]
      --csv-header     include header in csv output   [boolean] [default: false]
      --csv-quote      csv quote                          [string] [default: ""]
      --csv-delimiter  csv delimiter                     [string] [default: ","]
```

## Spec File

### JSON

TBD

### Script

Script context:

```javascript
$Ctx = {
  Mock: { ... },  // Mock.js
  Data: { ... },  // Data loading utils
  Spec: {}        // Output spec
}
```

Example:

```javascript
/* global $Ctx */

$Ctx.Spec = {
  template: {
    hello: 'world'
  }
}
```

## Output Formats

### CSV

Specify fields order using `fields` property:

```javascript
/* global $Ctx */

$Ctx.Spec = {
  template: {
    hello: 'world'
  },
  fields: ['hello']
}
```

## Placeholders

Builtin placeholders: [Mock.js 数据占位符定义](http://mockjs.com/examples.html#DPD).

Custom placeholders:

* `@datetimeRange(start, end, fmt)`
* `@uniqueWord(len)`

Custom helpers:

* `$Ctx.Mock.Random.weightedPick(choices)`

## Example

### Simple(JSON)

```json
{
  "template": {
    "builtin": "@id",
    "date": "@datetimeRange('2020-01-01', '2021-01-01', 'yyyy-MM-dd')",
    "double": "@float(100, 1000)",
    "doubleWithAccuracy": "@float(100, 1000, 2, 4)",
    "long": "@integer(100, 1000)",
    "word": "@word(6)",
    "uniqueWord": "@uniqueWord(6)",
    "enum|1": [
      "12480788",
      "15420416"
    ],
    "plain": "755"
  }
}
```

```shell
$ ./lib/cli.js mock --count 2 --csv-header examples/simple.json
builtin,date,double,doubleWithAccuracy,long,word,uniqueWord,enum,plain
370000200611098554,2020-04-27,357.56,507.0612,671,xnhjil,prcmnw,15420416,755
230000197808140917,2020-01-23,838,189.1718,692,ozvuca,xkbomc,12480788,755
```

### Simple(Script)

```javascript
/* global $Ctx */

$Ctx.Spec = {
  template: {
    builtin: '@id',
    date: '@datetimeRange("2020-01-01", "2021-01-01", "yyyy-MM-dd")',
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
```

```shell
$ ./lib/cli.js mock --count 2 --csv-header examples/simple.js
builtin,date,double,doubleWithAccuracy,long,word,uniqueWord,enum,plain
370000198704235386,2020-03-27,255.24611624458,544.537,451,yyoxqq,cwwqpl,15420416,755
640000199511115134,2020-10-13,739.2,371.2892,735,cirtcc,vvooyx,12480788,755
```

### Complex

```javascript
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
```

```shell
$ ./lib/cli.js mock --count 2 --csv-header examples/complex.js
custom,customWithParams,ser,date,datetime,name,weighted
3308c0,2937a900,310000,2020-02-28,2020-07-08 10:31:08,上海市,C
f0df8d,5ef16960,320000,2020-09-05,2020-03-31 08:32:34,江苏省,C
```

### Data File

```javascript
/* global $Ctx */

const data = $Ctx.Data.yaml('data.yaml')

$Ctx.Spec = {
  template: {
    'uid|1': Object.keys(data.users),
    name: function () {
      return data.users[this.uid].name
    }
  }
}
```

## Migration

Migrate from old mock file:

```shell
$ npm run -s translate ../onebox/data/l_itc_990067.arqp_p_recommend_user_pool_whitelist.json
/* global $Ctx */

$Ctx.Spec = {
  template: {
    ...
  },
  fields: {
    ...
  }
}
```

Generate using old mock file:

```shell
$ npm run -s translate ../onebox/data/l_itc_990067.arqp_p_recommend_user_pool_whitelist.json | ./lib/cli.js mock --count=2 --csv-header --stdin
user_id,user_type,sync_dt,bbk,bk1
86566882,experimental,2020-10-31,755,755
95185408,control,2020-10-31,755,755
```
