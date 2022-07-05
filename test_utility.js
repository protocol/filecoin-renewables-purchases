let fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');



function test_step_7(delivery_folder, step_7_filename){

  // Load the data we are checking
  const step7 = parse(
      fs.readFileSync(delivery_folder+'/'+step_7_filename,
      {
          encoding:'utf8',
          flag:'r'
      }
  ), {
      columns: true,
      cast: true
  });

  // Load the generation records for this delivery, which we are using to cross-reference
  const genRecords = parse(
      fs.readFileSync(delivery_folder+'/'+delivery_folder+'_step6_generationRecords.csv',
      {
          encoding:'utf8',
          flag:'r'
      }
  ), {
      columns: true,
      cast: true
  });

  for (idj = 0; idj<step7.length; idj++){
    // console.log('')
    // console.log('------')


    // Find this certificate from the order
    thisCert = genRecords.filter(elem => elem.certificate == step7[idj].certificate)[0]
    // console.log(thisCert)


    // Load step2, which has information for this contract
    // console.log(step7[idj].order_folder)
    const order = parse(
        fs.readFileSync(step7[idj].order_folder+'/'+step7[idj].order_folder+'_step2_orderSupply.csv',
        {
            encoding:'utf8',
            flag:'r'
        }
    ), {
        columns: true,
        cast: true
    });
    thisContract = order.filter(elem => elem.contract_id == step7[idj].contract)[0]

    // console.log(thisContract)

    // Compare the info from contract and certificate
    step7[idj].contract_country = thisContract.country
    step7[idj].certificate_country = thisCert.country
    step7[idj].country_match = thisContract.country == thisCert.country

    step7[idj].contract_region = thisContract.region
    step7[idj].certificate_region = thisCert.region
    step7[idj].region_match = thisContract.region == thisCert.region

    step7[idj].contract_start = thisContract.reportingStart
    step7[idj].certificate_start = thisCert.generationStart
    contract_start_date = new Date(thisContract.reportingStart).getTime()
    certificate_start_date = new Date(thisCert.generationStart).getTime()
    step7[idj].start_match = contract_start_date == certificate_start_date

    step7[idj].contract_end = thisContract.reportingEnd
    step7[idj].certificate_end = thisCert.generationEnd
    contract_end_date = new Date(thisContract.reportingEnd).getTime()
    certificate_end_date = new Date(thisCert.generationEnd).getTime()
    step7[idj].end_match = contract_end_date == certificate_end_date

    step7[idj].cert_within_contract = (contract_start_date <= certificate_start_date) && (contract_end_date >= certificate_end_date)
    step7[idj].country_region_and_certWithinContract = step7[idj].country_match && step7[idj].region_match && step7[idj].cert_within_contract

    // console.log(step7[idj])
  }

  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_check_step_7.csv", json2csvparse.parse(step7));

}


async function test_step_3(folder){

  step2_file=folder+'_step2_orderSupply.csv',
  step3_file=folder+'_step3_match.csv',
  step5_file=folder+'_step5_redemption_information.csv'

  const step2_data = parse(fs.readFileSync(folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  const step3_data = parse(fs.readFileSync(folder+'/'+step3_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  const step5_data = parse(fs.readFileSync(folder+'/'+step5_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});


  locs = require('./'+folder+'/_assets/synthetic-country-state-province-locations-latest.json')
  // console.log(locs.providerLocations[1])

  step3_data_wLocs = step3_data.map(elem => {
    minerID_locs_dict = locs.providerLocations.filter(x => x.provider == elem.minerID)
    minerID_locs = minerID_locs_dict.reduce((result, x) => result.concat(x.region), [])
    minerID_country = minerID_locs_dict.reduce((result, x) => result.concat(x.country), [])
    // console.log(minerID_locs)
    elem['location']=minerID_locs
    elem['country']=minerID_country
    return elem
  })

  // Check that the contract totals match between step 2 and 3
  // Each line in step 2 maps to one or more lines in step 3
  console.log('')
  step2_data.forEach(function (contractLine, index) {

    // console.log(contractLine)

    // Check that the totals add up for each contract
    console.log(`Checking ${contractLine.contract_id}...`)
    correspondingAllocations = step3_data_wLocs.filter(elem => elem.contract_id == contractLine.contract_id)
    allocations_thisLine = correspondingAllocations.reduce((prev, elem) => prev+Number(elem.volume_MWh), 0)
    if (!(allocations_thisLine == contractLine.volume_MWh)){
      console.log(`   Warning: ${contractLine.contract_id} step 3 allocations of ${allocations_thisLine} MWh don't match step 2 contract volume of ${contractLine.volume_MWh} MWh`)
    } else {console.log(`   Total allocation of ${allocations_thisLine} MWh agrees between steps 2 and 3`)}

    correspondingAllocations.forEach(function (allocation, allocidx) {
      // console.log(allocation)
      if (allocation.country.length == 0) {console.log(`   No country for ${allocation.allocation_id}`)}

      if (!(allocation.country.includes(contractLine.country))){
        console.log(`   Warning: for ${allocation.allocation_id}, ${allocation.minerID} location is ${allocation.country} but contract is ${contractLine.country}`)
      }

      // allocation.country.forEach(function(alloc_country, alloc_country_idx){
      //   if (!(alloc_country == contractLine.country)){
      //     console.log(`   Warning: for ${allocation.allocation_id},  ${allocation.minerID} location is ${allocation.country} but contract is ${contractLine.country}`)
      //   }
      //   // console.log(alloc_country)
      // })

      if (!(contractLine.region == '')){
        console.log(`   Check region for ${contractLine.contract_id}: ${allocation.minerID} region is ${allocation.location} and contract is ${contractLine.region}`)
      }

    })

    console.log('')
  });

}

// Test all step5 files
// For each transaction, make sure the total volume for step5 equals the contract volume
// For automatic redemptions, make sure volume in step5 matches volume in step3
// For each smart contract, make sure batch numbers are sequential (no repeats or gaps)
async function test_step_5(){

  // Look for all the step5 files we can find
  step5_filenames = []
  step5_to_folder = {}
  folder_to_step5 = {}
  const transaction_array = parse(fs.readFileSync('all_transactions.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  var all_allocations = []
  transaction_array.forEach(function(transaction, idx){
    folder_to_step5[transaction.transaction_folder] = []
    try{
      newData = parse(fs.readFileSync(transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_manual.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      console.log(`Found ${transaction.transaction_folder+'_step5_redemption_information_manual.csv'}`)
      step5_filenames.push(transaction.transaction_folder+'_step5_redemption_information_manual.csv')
      step5_to_folder[transaction.transaction_folder+'_step5_redemption_information_manual.csv'] = transaction.transaction_folder
      folder_to_step5[transaction.transaction_folder].push(transaction.transaction_folder+'_step5_redemption_information_manual.csv')
      all_allocations = all_allocations.concat(newData)}catch{}
    try{
      newData = parse(fs.readFileSync(transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_automatic.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      console.log(`Found ${transaction.transaction_folder+'_step5_redemption_information_automatic.csv'}`)
      step5_filenames.push(transaction.transaction_folder+'_step5_redemption_information_automatic.csv')
      folder_to_step5[transaction.transaction_folder].push(transaction.transaction_folder+'_step5_redemption_information_automatic.csv')
      step5_to_folder[transaction.transaction_folder+'_step5_redemption_information_automatic.csv'] = transaction.transaction_folder
      all_allocations = all_allocations.concat(newData)
    }catch{}
  })

  console.log(folder_to_step5)

  // Compare the step5 files we found to step 2 contracts
  folders = Object.keys(folder_to_step5)
  folders.forEach(function(folder, idx){
    folder_contracts_fully_allocated = true
    console.log('')
    console.log(`Comparing step2 to step5 in folder ${folder}`)

    // Load data for the step5 file(s)
    step5_filenames = folder_to_step5[folder]
    step5_data = step5_filenames.reduce((prev, elem)=>{
      newData = parse(fs.readFileSync(folder+'/'+elem, {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      return prev.concat(newData)
    },[])

    // Check whether we have volumes.
    step5_data_keys = Object.keys(step5_data[0])
    if(!(step5_data_keys.includes('volume_required'))){console.log('  > no volume key (old csv version?), cannot compare to step 2'); folder_contracts_fully_allocated = false} else{
      if(step5_data[0].volume_required == ''){console.log('  > no volume listed, cannot compare to step 2'); folder_contracts_fully_allocated = false} else{


        // Load data for the step2 file
        step2_file = folder+'_step2_orderSupply.csv'
        step2_data = parse(fs.readFileSync(folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});

        // For every contract, see whether the volume matches
        step2_data.forEach(function(contract, contractidx){
          corresponding_vol = step5_data.filter(x=> x.contract_id == contract.contract_id)
          if(corresponding_vol.length == 0){console.log(`  >${contract.contract_id} not yet allocated`); folder_contracts_fully_allocated = false} else{
            corresponding_total = corresponding_vol.reduce((prev, elem)=> {
              return prev + Number(elem.volume_required)},0)
            if(!(corresponding_total==contract.volume_MWh)){
              console.log(`  > Mismatch: ${contract.contract_id}: step 5 has ${corresponding_total} MWh, step2 has ${contract.volume_MWh} MWh`)
              folder_contracts_fully_allocated = false
            }
          }
        })



      }
    }
    if(folder_contracts_fully_allocated){console.log(`   Compared step 2 and 5: all ${folder} contracts appear fully allocated!`)}
  })

  // // Test each step5 file by comparing it to steps 2 and 3
  // step5_filenames.forEach(function(filename, idx){
  //
  //   console.log('')
  //   console.log(`Testing file ${filename}`)
  //   folder = step5_to_folder[filename]
  //
  //   // Load data. Check whether we have volumes.
  //   step5_data = parse(fs.readFileSync(folder+'/'+filename, {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
  //   if(step5_data[0].volume_required == ''){console.log('  > no volume listed, cannot compare to steps 2 and 3')} else{
  //
  //     // Otherwise, load steps 2 and 3 for comparison
  //     step2_file = folder+'_step2_orderSupply.csv'
  //     step2_data = parse(fs.readFileSync(folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  //     step3_file = folder+'_step3_match.csv'
  //     step3_data = parse(fs.readFileSync(folder+'/'+step3_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  //
  //     // Check whether matches step2 contracts
  //     step2_match = true
  //     step2_data.forEach(function(contract, contract_idx){
  //       // console.log(`  ${contract.contract_id}`)
  //       contract_match = step5_data.filter(x=> x.contract_id == contract.contract_id)
  //       corresponding_total = contract_match.reduce((prev, elem)=> {
  //         return prev + Number(elem.volume_required)}, 0)
  //       // console.log(corresponding_total)
  //     })
  //
  //   }
  // })

}

module.exports = {test_step_7, test_step_3, test_step_5}

// test_step_7('20210831_delivery', '20210831_delivery_step7_certificate_to_contract.csv')
//test_step_7('20220429_SP_delivery', '20220429_SP_delivery_step7_certificate_to_contract.csv')
// test_step_7('20211231_3D_delivery', '20211231_3D_delivery_step7_certificate_to_contract.csv')
