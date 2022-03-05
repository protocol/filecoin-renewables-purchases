let axios = require("axios")

// Plan is for this API to be public ASAP = )
let api_header = require('./zl_api_header.json')

async function main() {

  // requestString = `https://api.filecoin.energy/models/export?&id=4&limit=100&offset=0&miner=f01234`
  requestString = 'https://proofs-api.zerolabs.green/api/partners/filecoin/nodes'
  console.log(requestString)
  var total_energy = await axios.get(
    requestString, {
      headers:api_header
    })
  console.log(total_energy)

}


main()
