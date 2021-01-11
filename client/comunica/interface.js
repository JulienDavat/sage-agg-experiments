const newEngine = require('@comunica/actor-init-sparql').newEngine
const program = require('commander')
const fs = require('fs')

// Command line interface to execute queries
program
  .description('Execute a SPARQL query using the Comunica client')
  .usage('<server> [options]')
  .option('-q, --query <query>', 'evaluates the given SPARQL query')
  .option('-f, --file <file>', 'evaluates the SPARQL query stored in the given file')
  .option('-m, --measure <output>', 'evaluates the query and writes the query execution statistics in the given file', __dirname + '/result.csv')
  .option('-o, --output <output>', 'evaluates the query and writes the query output in the given file', __dirname + '/stats.xml')
  .option('--format <format>', 'the format [xml,json] of the query output', "xml")
  .parse(process.argv)

if (program.args.length !== 1) {
  process.stderr.write('Error: you must specify exactly one TPF server to use.\nSee ./comunica.js --help for more details.\n')
  process.exit(1)
}

if (program.format && !['xml', 'json'].includes(program.format)) {
  process.stderr.write('Error: you specified a wrong format. The output format can be "json" or "xml"\n')
  process.exit(1)
}

// fetch the SPARQL query to execute
let query = null
if (program.file) {
  query = fs.readFileSync(program.file).toString()
} else if (program.query) {
  query = program.query
} 
if (query == null) {
  process.stderr.write('Error: you must specify a SPARQL query to execute.\nSee ./comunica.js --help for more details.\n')
  process.exit(1)
}

start(program.args[0], query)


async function start(server, query) {
  replace()
  // then load
  let engine = newEngine()
  let startTime = Date.now()
  engine.query(query, { sources: [server] }).then((result) => {
    engine.resultToString(result, `application/sparql-results+${program.format}`, result.context).then((stream) => {
      res = ''
      stream.data.on('data', (data) => {
        res += data
      })
      stream.data.on('end', () => {
        // parseXmlToCsv(res)
        const ActorHttpNative = require('@comunica/actor-http-native').ActorHttpNative
        let stats = ActorHttpNative.stats
        if (program.measure) {
          let endTime = Date.now()
          let time = endTime - startTime
          fs.appendFileSync(program.measure, `${time / 1000},${stats.calls},${stats.bytes}`)
        }
        if (program.output) {
          fs.appendFileSync(program.output, res)
        }
      })
    })
  }).catch(e => {
    console.error(e)
  })
}

function replace () {
  // replaces Requester.js in actor-http-native manually in order to compute query execution metrics
  fs.writeFileSync(__dirname + '/../../node_modules/@comunica/actor-http-native/lib/Requester.js', fs.readFileSync(__dirname + '/Requester.js', 'utf-8'))
  fs.writeFileSync(__dirname + '/../../node_modules/@comunica/actor-http-native/lib/ActorHttpNative.js', fs.readFileSync(__dirname + '/ActorHttpNative.js', 'utf-8'))
}
