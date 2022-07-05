let fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');


function gen_step_5_auto(transaction_folder, contracts, smart_contract_id, attestation_folder){

  // Load data for steps 2 and 3, and the manual info for step5
  const step2_file=transaction_folder+'_step2_orderSupply.csv'
  const step3_file=transaction_folder+'_step3_match.csv'

  const step2_data = parse(fs.readFileSync(transaction_folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  const step3_data = parse(fs.readFileSync(transaction_folder+'/'+step3_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});

  // console.log(step2_data[0])
  // console.log(step3_data[0])
  // console.log(step5_manual_data[0])

  // We need to know the batch number that's already been allocated.
  // Do this by first loading all of the other allocations
  // For this import, have to turn cast off so address reads as a string
  const transaction_array = parse(fs.readFileSync('all_transactions.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  var all_allocations = []
  transaction_array.forEach(function(transaction, idx){
    // console.log(transaction.transaction_folder)
    try{
      newData = parse(fs.readFileSync(transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_manual.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      // console.log(`Found ${transaction.transaction_folder+'_step5_redemption_information_manual.csv'}`)
      all_allocations = all_allocations.concat(newData)}catch{}
    try{
      newData = parse(fs.readFileSync(transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_automatic.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      // console.log(`Found ${transaction.transaction_folder+'_step5_redemption_information_automatic.csv'}`)
      // If this is the file we are making (ie we ran this script multiple times), ignore it
      if (!(transaction.transaction_folder == transaction_folder)){all_allocations = all_allocations.concat(newData)}
    }catch{}
  })
  allocations_same_smart_contract_id = all_allocations.filter(x=> x.smart_contract_address == smart_contract_id)
  allocation_numbers = allocations_same_smart_contract_id.map(x=> Number(x.batchID))
  batch_previous_match = allocation_numbers.reduce((prev, elem)=> {if (elem>prev){return elem} else{return prev}}, 0)
  console.log(`Maximum previous batch number found: ${batch_previous_match}`)


  // Find the previous max attestation number
  // We are assuming that this function generates a new automatic step 5
  // Previous attestations for this transaction are only in the corresponding manual attestation file for this transaction
  var prevAttestation_max = 0
  try{
    this_transaction_manual = parse(fs.readFileSync(transaction_folder+'/'+transaction_folder+'_step5_redemption_information_manual.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
    // console.log(`Found ${transaction_folder+'_step5_redemption_information_manual.csv'}`)
    this_transaction_manual_attestation_numbers = this_transaction_manual.map(x=> Number(x.attestation_id.replace(transaction_folder+'_attestation_', '')))
    prevAttestation_max = this_transaction_manual_attestation_numbers.reduce((prev, elem)=> {if(elem>prev){return elem} else{return prev}}, 0)
  }catch{}
  console.log(`Maximum previous attestation number found for this transaction: ${prevAttestation_max}`)

  // We were passed a list of contracts to generate auto redemptions for. Loop through them
  new_allocations = []
  var batch_new = batch_previous_match
  var attest_new = prevAttestation_max
  contracts.forEach(function (contract, index) {

    // console.log(contract)

    // Find step 2 and step 3 data corresponding to this contract and make sure the volumes match
    step2_thisContract = step2_data.filter(x=>x.contract_id == contract)
    step2_thisContract = step2_thisContract[0]
    step3_thisContract = step3_data.filter(x=>x.contract_id == contract)
    step3_thisContract_totalVol = step3_thisContract.reduce((prev, elem) => {return prev + elem.volume_MWh}, 0)
    if (! step3_thisContract_totalVol == step2_thisContract.volume_MWh){error('Step 3 volume doesnt match step 2')}

    location = step2_thisContract.country
    if (step2_thisContract.region.length >0){location = step2_thisContract.country + '-' +step2_thisContract.region}
    // Make an entry for each step3 allocation
    this_contract_allocations = step3_thisContract.map(elem => {
      attest_new += 1
      batch_new += 1
      return {
        'attestation_id': `${transaction_folder}_attestation_${attest_new}`,
        'redemption_process': 'automatic',
        'contract_id': contract,
        'allocation_id': elem.allocation_id,
        'smart_contract_address': smart_contract_id,
        'batchID':batch_new,
        'network': 246,
        'zl_protocol_version': 'ZL 1.0.0',
        'minerID': elem.minerID,
        'beneficiary':`Blockchain Network ID: 246 - Tokenization Protocol: ZL 1.0.0 - Smart Contract Address: ${smart_contract_id} - Batch ID: ${batch_new} - Filecoin minerID ${elem.minerID}`,
        'beneficiary_country':step2_thisContract.country,
        'beneficiary_location':location,
        'supply_country':step2_thisContract.country,
        'volume_required':elem.volume_MWh,
        'start_date':step2_thisContract.reportingStart,
        'end_date':step2_thisContract.reportingEnd,
        'redemption_purpose':`The certificates are redeemed (= assigned to the beneficiary) for the purpose of tokenization and bridging to the Blockchain: Energy Web Chain with the Network ID 246. The smart contract address is ${smart_contract_id} and the specific certificate batch ID is ${batch_new}. The certificates will be created as tokens of type ERC1888-Topic1 This redemption is matched to Filecoin minerID ${elem.minerID}`,
        'attestation_folder': attestation_folder
      }
    })

    new_allocations = new_allocations.concat(this_contract_allocations)

  })

  console.log(new_allocations.length)
  fs.writeFileSync(transaction_folder+'/'+transaction_folder+'_step5_redemption_information_automatic.csv', json2csvparse.parse(new_allocations));
}


module.exports = {gen_step_5_auto}
