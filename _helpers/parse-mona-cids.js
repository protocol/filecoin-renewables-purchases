import fs from 'fs'
import axios from 'axios'
import { parse } from 'csv-parse/sync'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/parse-mona-cids-${(new Date()).toISOString()}.log`)
process.stdout.write = process.stderr.write = access.write.bind(access)

// Check for provided parameters
const args = process.argv.slice(2)
const mona_file = args[0]
const cid_columns = args[1]

if(mona_file == null || cid_columns == null) {
  console.error(`Error! Bad argument provided. Mona CSV file path and comma separated CID columns list are required parameters.`)
  await new Promise(resolve => setTimeout(resolve, 100))
  process.exit()
}

await parseMonaCids()

await new Promise(resolve => setTimeout(resolve, 1000))
process.exit()

async function parseMonaCids(){
  // load mona csv containing cids
  const monaData = parse(fs.readFileSync(mona_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true})
  let peersData = {}, minersData = {}, miners = {}, minersList = []
  let totalCids = 0, totalIndexedCids = 0, totalCidsMatchedWithMinerIds = 0
  const columns = cid_columns.split(",")

  for (const line of monaData) {
    for (const column of columns) {
      const cid = line[column.trim()]
      if(cid == null || cid == "null")
        continue
      totalCids++
      const cidContactUri = 'https://cid.contact/cid/' + cid
      try {
        peersData[cid] = (await axios(cidContactUri, {
          method: 'get'
        })).data
        totalIndexedCids++

        for (const multihashResult of peersData[cid].MultihashResults) {
          for (const providerResult of multihashResult.ProviderResults) {
            const peerId = providerResult.Provider.ID
            const cidMinerMatchUri = 'https://green.filecoin.space/minerid-peerid/api/v1/miner-id?peer_id=' + peerId
            try {
              minersData[cid] = (await axios(cidMinerMatchUri, {
                method: 'get'
              })).data

              for (const miner of minersData[cid]) {
                miners[cid] = miner.MinerId
                if(minersList.indexOf(miner.MinerId) == -1)
                  minersList.push(miner.MinerId)
                totalCidsMatchedWithMinerIds++
                console.log(`${cid} with provided peer Id ${peerId} is matched with ${miner.MinerId}.`)
              }
            }
            catch (error) {
              console.log(`${cid} is indexed but can not find miner Id based on provided peer Id ${peerId}.`)
              minersData[cid] = null
            }
          }
        }
      }
      catch (error) {
        console.log(`${cid} is not being indexed so far.`)
        peersData[cid] = null
      }
    }
  }
  console.log(`Total CIDs in file: ${totalCids}; Total indexed CIDs: ${totalIndexedCids}; Total matched CIDs: ${totalCidsMatchedWithMinerIds}; Total miners found ${minersList.length}.`)

  minersList = minersList.filter(_onlyUnique)
  console.dir(miners, { depth: null })
  console.dir(minersList, { depth: null })

  const pathChunks = mona_file.split("/")
  pathChunks.splice(pathChunks.length-1, 1)
  const pathFolder = pathChunks.join("/")
  const path = `${pathFolder}/mona-miners.json`
  await fs.promises.writeFile(`${path}`, JSON.stringify(minersList))
}

function _onlyUnique(value, index, self) {
  return self.indexOf(value) === index;
}
