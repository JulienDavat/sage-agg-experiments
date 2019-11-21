const newEngine = require('@comunica/actor-init-sparql').newEngine;
const Dom = require('dom-parser')
const fs = require('fs')
const path = require('path')
const program = require('commander')

// Command line interface to execute queries
program
    .description('Execute a SPARQL query using the Comunica client')
    .usage('<server> [options]')
    .option('-q, --query <query>', 'evaluates the given SPARQL query')
    .option('-t, --timeout <timeout>', 'set SPARQL query timeout in milliseconds (default: 30mn)', 30 * 60 * 1000)
    .option('-m, --measure <output>', 'measure the query execution time (in seconds) & append it to a file', __dirname + '/result.csv')
    .parse(process.argv)

let timeoutId = undefined
// get servers
if (program.args.length !== 1) {
    process.stderr.write('Error: you must specify exactly one TPF server to use.\nSee ./comunica.js --help for more details.\n')
    process.exit(1)
}
// fetch SPARQL query to execute
let query = null
let timeout = null
if (program.query) {
    query = program.query
} else {
    process.stderr.write('Error: you must specify a SPARQL query to execute.\nSee ./comunica.js --help for more details.\n')
    process.exit(1)
}
start(program.args[0], program.query, program.timeout)


async function start(server, query, timeout) {
  replace()
  // then load
  // set query timeout
  timeoutId = setTimeout(() => {
    fs.appendFileSync(program.measure, `0,0,0,1`)
    process.stderr.write(`TIMEOUT EXCEEDED (${timeout}ms) - shutting down query processing...\n`)
  }, timeout)
  const engine = newEngine()
  const startTime = Date.now()
  engine.query(query, {
    sources: [server]
  }).then((result) => {
    engine.resultToString(result, 'application/sparql-results+xml', result.context).then((d) => {
      res = ''
      d.data.on('data', (a) => {
        res += a
      })
      d.data.on('end', () => {
        clearTimeout(timeoutId)
        parseXmlToCsv(res)
        const A = require('@comunica/actor-http-native').ActorHttpNative
        const stats = A.stats
        if (program.measure) {
          const endTime = Date.now()
          const time = endTime - startTime
          fs.appendFileSync(program.measure, `${time / 1000},${stats.calls},${stats.bytes},0`)
        }
      })
    })
  }).catch(e => {
    console.error(e)
  })
}

function parseXmlToCsv(xml) {
  const parser = new Dom()
  const bind = new Map()
  const document = parser.parseFromString(xml)
  const head = document.getElementsByTagName('head')[0].childNodes
  const results = document.getElementsByTagName('results')[0].childNodes
  headers = []
  head.forEach(h => {
    if (!h.text) {
      headers.push("" + h.attributes[0].value)
    }
  })
  process.stdout.write(headers.join(',') + '\n')
  results.forEach(e => {
    if(!e.text) {
      const result = e
      arr = []
      e.getElementsByTagName('binding').forEach(b => {
        attr = b.attributes[0].value
        b.childNodes.forEach(ch => {
          if(!ch.text) {
            arr.push("" + ch.textContent)
          }
        })
      })
      process.stdout.write(arr.join(',')  + '\n')
    }
  })
}

function replace () {
  // (NODE ONLY) before all replace manually Requester.js in actor-http-native
  fs.writeFileSync(__dirname + '/node_modules/@comunica/actor-http-native/lib/Requester.js', fs.readFileSync(__dirname + '/Requester.js', 'utf-8'))
  fs.writeFileSync(__dirname + '/node_modules/@comunica/actor-http-native/lib/ActorHttpNative.js', fs.readFileSync(__dirname + '/ActorHttpNative.js', 'utf-8'))
}
