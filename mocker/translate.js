const fs = require('fs')
const standard = require('standard')

function _ (obj) {
  return JSON.stringify(obj, null, '\t')
}

const translators = {}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.EnumGenerator'] = function (column, spec) {
  const { candidateWithWeightMap, name, comment } = column
  spec.fields.push(name)

  const values = []
  const weights = {}
  for (const [value, weight] of Object.entries(candidateWithWeightMap)) {
    values.push(value)
    weights[weight] = 1
  }

  if (Object.keys(weights).length === 1) {
    let key = _(name)
    key = key.substr(0, key.length - 1) + '|1"'
    return [`${key}: ${_(values)},  // ${comment}`]
  }

  const choices = _(candidateWithWeightMap)
  return [`${_(name)}: $Ctx.Mock.Random.weightedPick(${choices}),  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.DateGenerator'] = function (column, spec) {
  const { name, comment, startDate, endDate } = column
  spec.fields.push(name)
  return [`${_(name)}: '@datetimeRange(${_(startDate)}, ${_(endDate)}, "yyyy-MM-dd")',  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.DoubleGenerator'] = function (column, spec) {
  const { name, comment, min, max } = column
  spec.fields.push(name)
  return [`${_(name)}: '@float(${_(min)}, ${_(max)})',  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.DoubleWithAccuracyGenerator'] = function (column, spec) {
  const { name, comment, min, max, accuracy } = column
  spec.fields.push(name)
  return [`${_(name)}: '@float(${_(min)}, ${_(max)}, 0, ${_(accuracy)})',  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.LongGenerator'] = function (column, spec) {
  const { name, comment, min, max } = column
  spec.fields.push(name)
  return [`${_(name)}: '@integer(${_(min)}, ${_(max)})',  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.UniqueWordGenerator'] = function (column, spec) {
  const { name, comment, length } = column
  spec.fields.push(name)
  return [`${_(name)}: '@uniqueWord(${_(length)})',  // ${comment}`]
}

translators['com.tianrang.platform.data.cdh.data_generation.generator.column.WordGenerator'] = function (column, spec) {
  const { name, comment, length } = column
  spec.fields.push(name)
  return [`${_(name)}: '@word(${_(length)})',  // ${comment}`]
}

function processFile (file) {
  const data = JSON.parse(fs.readFileSync(file, { encoding: 'utf8' }))
  let lines = [
    '/* global $Ctx */',
    '',
    '$Ctx.Spec = {'
  ]
  const spec = {
    fields: []
  }

  // Generate template
  let templateLines = []
  for (const column of data.columnGeneratorList) {
    const translator = translators[column.className]
    if (!translator) {
      console.error('missing translator for column type: ' + column.className)
      console.error(column)
      process.exit(-1)
    }
    templateLines.push(translator(column, spec))
  }
  templateLines = Array.prototype.concat.apply([], templateLines)
  lines.push('template: {')
  lines = lines.concat(templateLines)
  lines.push('},')

  // Generate fields
  lines.push(`fields: ${_(spec.fields)},`)

  lines.push('}')
  return lines
}

const args = process.argv.slice(2)

if (args.length !== 1) {
  console.error('missing input file')
  process.exit(-1)
}
const inputFile = args[0]

const spec = processFile(inputFile).join('\n')
standard.lintText(spec, { fix: true, globals: ['$Ctx'] }, (err, results) => {
  if (err) {
    console.error(spec)
    console.error(err)
    process.exit(-1)
  }
  if (results.errorCount > 0) {
    console.error(spec)
    console.error(_(results))
    process.exit(-1)
  }
  process.stdout.write(results.results[0].output)
})
