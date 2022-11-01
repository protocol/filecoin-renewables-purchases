import fs from 'fs'
import axios from 'axios'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/parse-estuary-${(new Date()).toISOString()}.log`)
process.stdout.write = process.stderr.write = access.write.bind(access)

// Check for provided parameters
const args = process.argv.slice(2)
const folder = args[0]

await parseEstuary()

await new Promise(resolve => setTimeout(resolve, 1000))
process.exit()

async function parseEstuary(){
  let minersList = (await axios("https://api.estuary.tech/public/miners", {
    method: 'get'
  })).data
  //.filter((m) => {return m.suspended == false})   // suspended category is temporary, they did use the energy to seal and store data so include them
    .map((m) => {return m.addr})

  minersList = minersList.filter(_onlyUnique)

  const path = `${folder}/estuary-miners.json`
  await fs.promises.writeFile(`${path}`, JSON.stringify(minersList))
}

function _onlyUnique(value, index, self) {
  return self.indexOf(value) === index;
}
