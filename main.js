let axios = require("axios")
let api_header = require('./zl_api_header.json') // Plan is for this API to be public ASAP = )
let getEnergy = require("./getEnergy")
const json2csvparse = require('json2csv')
const fs = require('fs')

start = '2020-07-01'
end = '2022-03-04' // ie today's date

async function main() {

  // First get the list of nodes that Zero Labs has records for
  requestString = 'https://proofs-api.zerolabs.green/api/partners/filecoin/nodes'
  var nodesResult = await axios.get(
    requestString, {
      headers:api_header
    })

  outData = nodesResult.data.map(x => { return {"minerID":x.id}})
  // console.log(outData)

  // For each node, examine energy purchases
  for (i=0; i<outData.length; i++){

    console.log(outData[i])

    // Get Transaction data
    requestString = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${outData[i].minerID}/transactions`
    var transactionsResult = await axios.get(
      requestString, {
        headers:api_header
      })

    // Record transaction data
    outData[i].pageUrl = transactionsResult.data.pageUrl
    outData[i].deliveredEACs_MWh = transactionsResult.data.recsTotal/1e6

    // Get Contracts data
    requestString = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${outData[i].minerID}/contracts`
    var contractsResult = await axios.get(
      requestString, {
        headers:api_header
      })

    // Sum and record volume under contract
    openVol = contractsResult.data.contracts.reduce((previousValue, elem) => {
      newVol = Number.parseInt(elem.openVolume)
      return previousValue + newVol
    },0)
    outData[i].openEACContracts_MWh = openVol/1e6
    outData[i].AllEACs_MWh = outData[i].deliveredEACs_MWh + outData[i].openEACContracts_MWh

    // Compare to the volume of energy consumed by this node over the history of the network
    enResult = await getEnergy.get_total_energy_data(start, end, outData[i].minerID)
    outData[i].EnergyConsumed_Upper_MWh = enResult.total_energy_upper_MWh_recalc
    outData[i].Renewable_Consumed_Ratio = outData[i].AllEACs_MWh / outData[i].EnergyConsumed_Upper_MWh

  }

console.log(outData)

fs.writeFileSync('EAC_purchase_summary.csv', json2csvparse.parse(outData))

}


main()
