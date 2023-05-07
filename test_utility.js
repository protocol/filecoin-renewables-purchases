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


async function compare_order_to_delivery(path, transaction_folder, verbosity){

  console.log(' ')
  console.log('Examining order and delivery for '+transaction_folder)

  // Load order data
  step2_file=transaction_folder+'_step2_orderSupply.csv',
  step2_data = parse(fs.readFileSync(path+'/'+transaction_folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  ordered_Volume = step2_data.reduce((prev, elem) => prev+Number(elem.volume_MWh), 0);
  console.log('  Ordered ' + ordered_Volume + ' MWh')

  // Load delivered data
  step6_data = await load_step6_data(path, transaction_folder)
  delivered_Volume = step6_data.reduce((prev, elem) => {
    return prev+(Number(elem.volume_Wh)/1e6)
  }, 0);
  console.log('  Delivered ' + delivered_Volume + ' MWh ('+(Math.round(delivered_Volume/ordered_Volume*100))+'%)')

  // Compare order to delivery
  // verbose flag -> output itemized list of orders that don't match
  // load_step_2
    //Load step 2 info
 
  // Print amount ordered
  // Print amount delivered
  // Go down step 2 and compare to step 6
    // compare_order_to_delivery_line
      // Calculate score up to 6 by comparing fields other than volume:
      // step 2: productType, energySources, reportingStart, reportingEnd, country, region, volume_MWh
      // step 6: reportingStart, reportingEnd, country, region, volume_Wh, productType, energySource
    // If score is perfect 6
      // If volume is equal, remove both from queue
      // If volumes are not equal, remove smaller and subtract volume from larger
    // If verbose, print left over volumes
    // Print undelivered amount from order
    // Print surplus delivery

}

// For a given transaction, load all of the delivery data
async function load_step6_data(path, transaction_folder){
  
  // Load step5 information, which points us to the delivery folder(s)
  step5_file=transaction_folder+'_step5_redemption_information.csv'
  try{
    var step5_data = parse(fs.readFileSync(path+'/'+transaction_folder+'/'+step5_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  } catch{
    console.log('  No file '+step5_file)
    return []
  }

  delivery_folders = step5_data.reduce((prev, elem) => {
    if (!(prev.includes(elem.attestation_folder))){
      return prev.concat(elem.attestation_folder)
    } else {
      return prev
    }
  }, []);

  if (delivery_folders.length > 1){console.log('  > Multiple delivery folders not tested.')}

  toReturn = delivery_folders.reduce((prev, elem) => {
    step6_file=elem+'_step6_generationRecords.csv'
    try{
      var new_step6 = parse(fs.readFileSync(path+'/'+elem+'/'+step6_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
      return prev.concat(new_step6)
    }catch{
      console.log('  No delivered volume '+step6_file)
      return prev
    }
    
  }, []);

  // This should be an array of all delivered volumes for this order, even if they are in multiple folders
  return toReturn

}




// Test all step5 files
// For each transaction, make sure the total volume for step5 equals the contract volume
// For automatic redemptions, make sure volume in step5 matches volume in step3
// For each smart contract, make sure batch numbers are sequential (no repeats or gaps)
// pass the path to filecoin-renewables-purchases
async function test_step_5(path){

  // Look for all the step5 files we can find
  step5_filenames = []
  step5_to_folder = {}
  folder_to_step5 = {}
  const transaction_array = parse(fs.readFileSync(path+'/all_transactions.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  // const transaction_array = parse(fs.readFileSync([path+'all_transactions.csv'], {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  var all_allocations = []
  console.log('')
  console.log('---------')
  console.log('Searching for step5 files')
  transaction_array.forEach(function(transaction, idx){
    folder_to_step5[transaction.transaction_folder] = []
    // newData = parse(fs.readFileSync(path+'/'+transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_manual.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
    try{
      newData = parse(fs.readFileSync(path+'/'+transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_manual.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      console.log(`   ...Found ${transaction.transaction_folder+'_step5_redemption_information_manual.csv'}`)
      step5_filenames.push(transaction.transaction_folder+'_step5_redemption_information_manual.csv')
      step5_to_folder[transaction.transaction_folder+'_step5_redemption_information_manual.csv'] = transaction.transaction_folder
      folder_to_step5[transaction.transaction_folder].push(transaction.transaction_folder+'_step5_redemption_information_manual.csv')
      all_allocations = all_allocations.concat(newData)}catch{}
    try{
      newData = parse(fs.readFileSync(path+'/'+transaction.transaction_folder+'/'+transaction.transaction_folder+'_step5_redemption_information_automatic.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      console.log(`   ...Found ${transaction.transaction_folder+'_step5_redemption_information_automatic.csv'}`)
      step5_filenames.push(transaction.transaction_folder+'_step5_redemption_information_automatic.csv')
      folder_to_step5[transaction.transaction_folder].push(transaction.transaction_folder+'_step5_redemption_information_automatic.csv')
      step5_to_folder[transaction.transaction_folder+'_step5_redemption_information_automatic.csv'] = transaction.transaction_folder
      all_allocations = all_allocations.concat(newData)
    }catch{}
  })

  console.log('')
  console.log('---------')

  // Compare the step5 files we found to step 2 contracts
  folders = Object.keys(folder_to_step5)
  folders.forEach(function(folder, idx){
    folder_contracts_fully_allocated = true
    console.log('')
    console.log(`Comparing step2 to step5 in folder ${folder}`)

    // Load data for the step5 file(s)
    step5_filenames_this_folder = folder_to_step5[folder]
    // console.log()
    // console.log(folder_to_step5)
    step5_data = step5_filenames_this_folder.reduce((prev, elem)=>{
      newData = parse(fs.readFileSync(path+'/'+folder+'/'+elem, {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      return prev.concat(newData)
    },[])

    // Check whether we have volumes.
    if(step5_filenames_this_folder.length == 0){console.log('   > No allocation files found')} else{
      step5_data_keys = Object.keys(step5_data[0])
      if(!(step5_data_keys.includes('volume_required'))){console.log('  > no volume key (old csv version?), cannot compare to step 2'); folder_contracts_fully_allocated = false} else{
        if(step5_data[0].volume_required == ''){console.log('  > no volume listed, cannot compare to step 2'); folder_contracts_fully_allocated = false} else{


          // Load data for the step2 file
          step2_file = folder+'_step2_orderSupply.csv'
          step2_data = parse(fs.readFileSync(path+'/'+folder+'/'+step2_file, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});

          // For every contract, see whether the volume matches
          step2_data.forEach(function(contract, contractidx){
            corresponding_vol = step5_data.filter(x=> x.contract_id == contract.contract_id)
            if(corresponding_vol.length == 0){console.log(`   > ${contract.contract_id} not yet allocated`); folder_contracts_fully_allocated = false} else{
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
    }
    if(folder_contracts_fully_allocated && (step5_filenames_this_folder.length > 0)){console.log(`   Compared step 2 and 5: all ${folder} contracts match allocated volumes!`)}
  })

  // For step5 files with 'automatic' in the filename, ensure they match to a step3 allocation
  console.log('')
  console.log('---------')
  console.log("Checking that every 'automatic' step5 redemption corresponds to a step3 allocation")
  step5_filenames.forEach(function(filename, idx){
    if(filename.includes('automatic')){
      foldername = filename.replace('_step5_redemption_information_automatic.csv', '')
      console.log('')
      console.log(filename)
      step5_data = parse(fs.readFileSync(path+'/'+foldername+'/'+filename, {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      step3_data = parse(fs.readFileSync(path+'/'+foldername+'/'+foldername+'_step3_match.csv', {encoding:'utf8', flag:'r'}), {columns: true, cast: false});
      if(!(step3_data.length >= step5_data.length)){console.log(`   > Warning: step 5 has ${step5_data.length} allocations and step 3 has ${step3_data.length}. Step 5 should not have more than step3.`)}
      if(!(Object.keys(step5_data[0]).includes('allocation_id'))){console.log(`   > No allocation_id key (old step5 format?), cannot run check.`)}else{
        step5_corresponds_to_step3 = true
        step5_data.forEach(function(step5_elem, idx){
          corresponding_step3 = step3_data.filter(x=> x.allocation_id == step5_elem.allocation_id)

          // console.log(step5_elem)
          // console.log(corresponding_step3)

          if (corresponding_step3.length >1){console.log(`   > Warning: ${step5_elem.allocation_id} matches multiple step3 allocations. It should not.`); step5_corresponds_to_step3 = false} else{
            corresponding_step3 = corresponding_step3[0]
          }
          if (corresponding_step3.length >1){console.log(`   > Warning: ${step5_elem.allocation_id} in step 5 does not match an allocation in step3`); step5_corresponds_to_step3 = false}

          if(!(step5_elem.minerID == corresponding_step3.minerID)){
            console.log(`   > Warning! For ${step5_elem.allocation_id}, step5 minerID is ${step5_elem.minerID} vs step3 minerID is ${corresponding_step3.minerID}`)
            step5_corresponds_to_step3 = false
          }

          if(!(step5_elem.volume_required == corresponding_step3.volume_MWh)){
            console.log(`   > Warning! For ${step5_elem.allocation_id}, step5 volume is ${step5_elem.volume_required} MWh vs step3 minerID is ${corresponding_step3.volume_MWh} MWh`)
            step5_corresponds_to_step3 = false
          }

        })
        if (step5_corresponds_to_step3){console.log(`   Success: each step5 attestaion corresponds to one step3 allocation!`)}
      }
    }
  })

  // Check that the batchIDs match across folders
  // Use all_allocations which has data from every step5
  console.log('')
  console.log('---------')
  console.log("Checking that batchIDs are consistent between transaction folders")

  all_smartContract_addresses = []

  // Find smart contract addresses across all allocations
  all_allocations.forEach(function(alloc, idx){if(!(all_smartContract_addresses.includes(alloc.smart_contract_address))){
    all_smartContract_addresses.push(alloc.smart_contract_address)
  }})

  all_smartContract_addresses.forEach(function(address, idx){
    if(!(address == '')){
      console.log(`Checking smart contract address ${address}:`)
      batches_correct = true
      allocs_with_this_address = all_allocations.filter(x=>x.smart_contract_address == address)
      batchIDs = allocs_with_this_address.reduce((prev, curr)=>{return prev.concat(Number(curr.batchID))},[])

      // Check that there aren't gaps
      batchID_max = batchIDs.reduce((prev, curr)=>{if (curr>prev){return curr} else{return prev}}, 0)
      batchID_min = batchIDs.reduce((prev, curr)=>{if (curr<prev){return curr} else{return prev}}, 1000000)
      if(!((batchID_max-batchID_min) == (batchIDs.length-1))){console.log(`   > Warning: gap or repeat detected: min is ${batchID_min}, max is ${batchID_max}, number of batchIDs is ${batchIDs.length}`); batches_correct = false}

      // Check that there aren't repeats
      batchIDs.forEach(function(thisID, idx){
        // console.log(thisID)
        matching_elems = batchIDs.filter(x=>x == thisID)
        if(!(matching_elems.length == 1)){console.log(`   > Warning: found batch ${thisID} showed up ${matching_elems.length} times. Must not repeat.`); batches_correct = false}
      })
      if(batches_correct){console.log(`   Success: no gaps or repeats in batch numbers detected!`)}
    }
  })

}

module.exports = {test_step_7, test_step_3, test_step_5, compare_order_to_delivery}

// test_step_7('20210831_delivery', '20210831_delivery_step7_certificate_to_contract.csv')
//test_step_7('20220429_SP_delivery', '20220429_SP_delivery_step7_certificate_to_contract.csv')
// test_step_7('20211231_3D_delivery', '20211231_3D_delivery_step7_certificate_to_contract.csv')
