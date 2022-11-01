import fs from 'fs'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/parse-synthetic-locations-${(new Date()).toISOString()}.log`)
process.stdout.write = process.stderr.write = access.write.bind(access)

// Check for provided parameters
const args = process.argv.slice(2)
const syntheticLocationFile = args[0]
const syntheticLocationListProperty = args[1]

if(syntheticLocationFile == null || syntheticLocationListProperty == null) {
  console.error(`Error! Bad argument provided. Synthetic locations file path and property name containing Miner IDs are required parameters.`)
  await new Promise(resolve => setTimeout(resolve, 100))
  process.exit()
}

await parseSyntheticLocations()

await new Promise(resolve => setTimeout(resolve, 1000))
process.exit()

async function parseSyntheticLocations(){
  const minersData = JSON.parse(fs.readFileSync(syntheticLocationFile, {encoding:'utf8', flag:'r'}))
  let minersList = minersData[syntheticLocationListProperty].map((m) => {return m.provider})
  minersList = minersList.filter(_onlyUnique)
  const pathChunks = syntheticLocationFile.split("/")
  pathChunks.splice(pathChunks.length-1, 1)
  const pathFolder = pathChunks.join("/")
  const path = `${pathFolder}/synthetic-locations-miners.json`
  await fs.promises.writeFile(`${path}`, JSON.stringify(minersList))
}

function _onlyUnique(value, index, self) {
  return self.indexOf(value) === index;
}
