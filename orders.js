let fs = require('fs')
const { parse } = require('csv-parse/sync')
let getEnergy = require('./getEnergy')
let axios = require('axios')
const json2csvparse = require('json2csv')

// Do:
  // Prioritize based on Estuary
    // https://api.estuary.tech/public/miners
  // Prioritize based on filrep score
    // https://api.filrep.io/api/v1/miners?limit=4296
  // Prioritize based on 1 IP vs synthetic (doing this already)
  // Support renewables purchases that are ordered by not issued yet
  // Subtract existing renewable energy (don't just ignore if in ZL)
  // For US and Chinese providers, build more granular matching than country

  // Locations priority now:
    // Demo for mona (find NERC for US)
    // Estuary (find NERC for US)
    // Jim's non-synth
    // Synthetic


// New Beneficiaries Output:Name, Country Name, Location
  // Name: minerID
  // Country Name: code is fine?
  // Location: give description?

// Redemptions Output:
  // ID: blank
  // Name: minerID
  // Country: country code
  // Location: provide description?
  // Redemption Account: get from input
  // Volume required: allocation
  // Start date: assume is REC vintage
  // End date: assume is REC vintage
  // Redemption Purpose: Renewable energy purchase for node on the Filecoin network with minerID ${minerID}

// Walk through supply, line by line. For each supply tranche:
  // Get providers from Jim's V1 list (no synthetic locations). For each minerID:
    // Match country (or skip)
    // Find the amount of electricity used during this period from filecoin.energy
    // See whether this SP is listed on ZL. If so, skip (for now)
    // Find margin * electricity use as max allocation
    // allocation = ceil(max allocation, remaining supply)
    // Add supply and subtract from allocation

// See whether two date ranges, with all dates written as strings, partially overlap
// For each date range, start must be before end
function date_ranges_partially_overlap(r1start, r1end, r2start, r2end){

  // console.log("")
  // console.log(`${r1start} - ${r1end} and ${r2start} - ${r2end}`)

  // Assume true, test for cases where partial overlap is false
  partialOverlap = true

  // If an end date does not have a full timestamp (ie if it is in the form
  // 2021-07-31 instead of 2021-07-31T23:59:59.999Z), we need to append the timestamp
  if (! r1end.includes(':')){
    r1end += 'T23:59:59.999Z'
  }
  if (! r2end.includes(':')){
    r2end += 'T23:59:59.999Z'
  }

  // Convert dates into timestamps so we can compare them
  r1start_timestamp = new Date(r1start).getTime()
  r1end_timestamp = new Date(r1end).getTime()
  r2start_timestamp = new Date(r2start).getTime()
  r2end_timestamp = new Date(r2end).getTime()

  // Test that the dates are in the correct order
  if ( (r1start_timestamp > r1end_timestamp) || (r2start_timestamp > r2end_timestamp)){
    throw new Error('Bad date order')
  }

  // First false case: all of r1 is after r2
  if( (r2start_timestamp <= r1start_timestamp) && (r2end_timestamp <= r1start_timestamp)){
    partialOverlap = false
  }

  // Second false case: r1 is within r2
  if( (r2start_timestamp <= r1start_timestamp) && (r1end_timestamp <= r2end_timestamp)){
    partialOverlap = false
  }

  // Third false case: r2 is within r1
  if( (r1start_timestamp <= r2start_timestamp) && (r2end_timestamp <= r1end_timestamp)){
    partialOverlap = false
  }

  // Fourth false case: all of r1 is before r2
  if( (r1end_timestamp <= r2start_timestamp) && (r1end_timestamp <= r2end_timestamp)){
    partialOverlap = false
  }

  return partialOverlap

}

// See whether date range r2 is entirely contained in r1
function r2_in_r1(r1start, r1end, r2start, r2end){

  // If an end date does not have a full timestamp (ie if it is in the form
  // 2021-07-31 instead of 2021-07-31T23:59:59.999Z), we need to append the timestamp
  if (! r1end.includes(':')){
    r1end += 'T23:59:59.999Z'
  }
  if (! r2end.includes(':')){
    r2end += 'T23:59:59.999Z'
  }

  // Convert dates into timestamps so we can compare them
  r1start_timestamp = new Date(r1start).getTime()
  r1end_timestamp = new Date(r1end).getTime()
  r2start_timestamp = new Date(r2start).getTime()
  r2end_timestamp = new Date(r2end).getTime()

  // Test that the dates are in the correct order
  if ( (r1start_timestamp > r1end_timestamp) || (r2start_timestamp > r2end_timestamp)){
    throw new Error('Bad date order')
  }

  // Make the comparison
  if ( (r1start <= r2start) && (r2end <= r1end)){return true} else{
    return false
  }
}

async function match_to_SP_list(supplyRecord, supply_remaining, locations, ZL_nodes, new_beneficiaries, redemptions, orderFolder){

  console.log(supplyRecord)

  // To match this supply, walk through SPs from the locations file.
  SP_idx = 0
  while ((supply_remaining>0) && (SP_idx < locations.length)){
    console.log(SP_idx)
    SP_location = locations[SP_idx]
    console.log(supply_remaining)
    console.log(" " + SP_location.country + " - " + supplyRecord.country)

    // Match country with supply, otherwise skip this SP
    if (! (SP_location.country == supplyRecord.country)){
      console.log(' ...skipping')
      SP_idx ++
      continue
    }

    // If we've gotten here, the countries match for supply[i] and SP_location

    // There are two situations where we might skip
    skip_this_minerID = false

    // Find previous renewable energy records
    prev_renewables = {"purchases":{}, "contracts":{}}
    prev_renewable_summary = []
    ZL_match = ZL_nodes.data.filter(x => x.id == locations[SP_idx].miner)
    if (ZL_match.length > 0){

      prev_renewables = await getEnergy.get_previous_renewables(locations[SP_idx].miner)

      // Summarize purchase records for existing allocations
      for (idx=0; idx<prev_renewables.purchases.transactions.length; idx++){
        // console.log(prev_renewables.purchases.transactions[idx])
        prev_renewable_summary.push({
          "volume_MWh": prev_renewables.purchases.transactions[idx].generation.energyWh/1e6,
          "start_date": prev_renewables.purchases.transactions[idx].reportingStart,
          "end_date": prev_renewables.purchases.transactions[idx].reportingEnd
        })
      }

      // Summarize contract records for existing allocations
      for (idx=0; idx<prev_renewables.contracts.contracts.length; idx++){
        // console.log(prev_renewables.contracts.contracts[idx])
        prev_renewable_summary.push({
          "volume_MWh": prev_renewables.contracts.contracts[idx].openVolume/1e6,
          "start_date": prev_renewables.contracts.contracts[idx].reportingStart,
          "end_date": prev_renewables.contracts.contracts[idx].reportingEnd
        })
      }

    }

    // console.log(prev_renewable_summary)


    // Have we already allocated for this minerID in this date range (ie if supply dates overlap)?
    prevAllocation_thisMinerID = redemptions.filter(x => x.name == locations[SP_idx].miner)

    // Combine previous renewables purchases with previous allocation
    prevAllocation_thisMinerID = prevAllocation_thisMinerID.concat(prev_renewable_summary)

    // console.log(prevAllocation_thisMinerID)

    // Check whether a previous allocation partially overlaps. If so, skip
    for (j=0; j<prevAllocation_thisMinerID.length; j++){
      if (date_ranges_partially_overlap(supplyRecord.start, supplyRecord.end, prevAllocation_thisMinerID[j].start_date, prevAllocation_thisMinerID[j].end_date)){
        console.log('Skipping on account of overlap with record:')
        console.log(prevAllocation_thisMinerID[j])
        skip_this_minerID = true
      }
    }

    // If allocating to this SP will overlap with another allocation, then skip
    if (skip_this_minerID){
      SP_idx ++
      continue
    }

    // Find the amount of energy used by this SP over the supply time period
    energy_use = null
    try{
      energy_use = await getEnergy.get_total_energy_data(supplyRecord.start, supplyRecord.end, locations[SP_idx].miner)
    } catch {
      console.log("Couldn't find energy use (API error?)")
      SP_idx ++
      continue
    }

    console.log(energy_use)
    margin = 1.5 // Overbuying EACs
    max_allocation = Math.ceil(energy_use.total_energy_upper_MWh * margin)
    console.log(`Max allocation from energy use: ${max_allocation} MWh`)

    // If a previous allocation is entirely within this one, adjust max_allocation down
    prevWithin = prevAllocation_thisMinerID.filter(x => r2_in_r1(supplyRecord.start, supplyRecord.end, x.start_date, x.end_date))
    console.log("Found records within this time range:")
    console.log(prevWithin)
    sumWithin = prevWithin.reduce((prev, elem) => prev + elem.volume_MWh, 0)
    max_allocation = Math.max((max_allocation-sumWithin), 0)
    console.log(`Adjusted max allocation: ${max_allocation} MWh`)

    // If this is entirely within a previous allocation and that time range already has
    // enough renewable energy, adjust max_allocation down
    insidePrev = prevAllocation_thisMinerID.filter(x => r2_in_r1(x.start_date, x.end_date, supplyRecord.start, supplyRecord.end))
    console.log("Found supply time range entirely within record:")
    console.log(insidePrev)

    // Limit actual allocation by available supply
    allocation = Math.min(max_allocation, supply_remaining)

    // Advance if allocation is zero
    if (allocation <= 0){
      SP_idx ++
      continue
    }

    // Now that we're done adjusting allocations, calculate remaining supply
    supply_remaining = supply_remaining - allocation

    // Add to new beneficiaries if necessary
    existingEntry = new_beneficiaries.filter(x => x.name == locations[SP_idx].miner)
    if (existingEntry.length == 0){
      new_beneficiaries.push({
        "name": locations[SP_idx].miner,
        "country_name": SP_location.country,
        "location": SP_location.region
      })
    }

    // console.log(supply[i])


    // Add allocation to redemptions
    redemptions.push({
      "ID":"",
      "name":locations[SP_idx].miner,
      "country":SP_location.country,
      "location": SP_location.region,
      "redemption_account": supplyRecord["redemption_account"],
      "volume_required": allocation,
      "start_date": supplyRecord.start,
      "end_date": supplyRecord.end,
      "redemption_purpose": `Renewable energy purchase for node on the Filecoin network with minerID ${locations[SP_idx].miner}`,
      "supply_remaining":supply_remaining,
      "totalSealed_GiB": energy_use.totalSealed_GiB,
      "total_time_hours" : energy_use.total_time_hours,
      "initial_capacity_GiB" : energy_use.initial_capacity_GiB,
      "final_capacity_GiB": energy_use.final_capacity_GiB,
      "time_average_capacity_GiB": energy_use.time_average_capacity_GiB,
      "total_energy_upper_MWh": energy_use.total_energy_upper_MWh
    })

    // Record output files after new allocation
    fs.writeFileSync(folder+'/new_beneficiaries.csv', json2csvparse.parse(new_beneficiaries))
    fs.writeFileSync(folder+'/redemptions.csv', json2csvparse.parse(redemptions))


    SP_idx ++
  }

  return [supply_remaining, new_beneficiaries, redemptions];

}

async function new_EAC_redemption_order(orderFolder, locationFilename, supplyFilename){

  // Load locations from file
  settings = require("./"+orderFolder+"/orderSettings.json")
  locations = require("./"+orderFolder+"/"+settings.locationFilename)

  // The data schema changed, so rename if necessary
  if ('providerLocations' in locations){
    locations ={
      "date":locations.date,
      "epoch":locations.epoch,
      "minerLocations": locations.providerLocations
    } }

  // Here, only use providers with a single location and prioritize non-synthetic locations
  locations = locations.minerLocations.filter(x => x.numLocations == 1)

  // The data schema changed, so rename if necessary
  if ('provider' in locations[0]){
    locations = locations.map(x => {
      x["miner"] = x.provider
      return x
    })}
  locations_nonSynth = locations.filter( x=> !('delegate' in x))
  locations_synth = locations.filter( x=> ('delegate' in x))

  // Account for HK - CN power grid interconnection
  locations = locations.map(x=> {
    if (x.country == 'HK'){x.country = 'CN'}
    return x
  })

  // Load supply, which are EACs under contract we will allocate
  const supply = parse(fs.readFileSync("./"+orderFolder+"/"+settings.supplyFilename, {encoding:'utf8', flag:'r'}), {"columns":true})

  // These arrays will be output orders for I-REC
  new_beneficiaries = []
  redemptions = []

  // Find the list of nodes in the ZL system
  requestString = 'https://proofs-api.zerolabs.green/api/partners/filecoin/nodes'
  var ZL_nodes = await axios.get(requestString)

  // Walk through supply, line by line, to allocate to SPs
  for (i=0; i<supply.length; i++){

    supplyRecord = supply[i]

    // Load all supply from this line
    supply_remaining = Number(supply[i].volume_MWh)

    // Take the remaining supply and match to SPs, going down the list by location
    // Try first with non-synthetic locations
    var [supply_remaining, new_beneficiaries, redemptions] = await match_to_SP_list(supply[i], supply_remaining, locations_nonSynth, ZL_nodes, new_beneficiaries, redemptions, orderFolder)

    // If there is leftover supply, use synthetic locations
    if (!(supply_remaining == 0)){
      [supply_remaining, new_beneficiaries, redemptions] = await match_to_SP_list(supply[i], supply_remaining, locations_synth, ZL_nodes, new_beneficiaries, redemptions, orderFolder)
    }

  }

  // console.log(new_beneficiaries)
  // console.log(redemptions)

  // fs.writeFileSync(folder+'/new_beneficiaries.csv', json2csvparse.parse(new_beneficiaries))
  // fs.writeFileSync(folder+'/redemptions.csv', json2csvparse.parse(redemptions))


}

async function checkOrder(folder){

  settings = require("./"+folder+"/orderSettings.json")
  const supply = parse(fs.readFileSync("./"+folder+"/"+settings.supplyFilename, {encoding:'utf8', flag:'r'}), {"columns":true})
  const redemptions = parse(fs.readFileSync("./"+folder+"/redemptions_ewc.csv", {encoding:'utf8', flag:'r'}), {"columns":true})

  // Check that supply adds correctly
  for (i=0; i<supply.length; i++){

    console.log('')
    console.log(`${supply[i].redemption_account} from ${supply[i].start} to ${supply[i].end}:`)

    redemptionsThisLine = redemptions.filter(x => {
      return (x.redemption_account == supply[i].redemption_account) && (x.start_date == supply[i].start) && (x.end_date == supply[i].end)
    })

    totalRedemptions = redemptionsThisLine.reduce((prev, elem) => prev+Number(elem.volume_required), 0)

    if (totalRedemptions == Number(supply[i].volume_MWh)){
      console.log(`  Supply of ${supply[i].volume_MWh} matches redemption total of ${totalRedemptions}`)
    } else {
      console.log(`  >>>>WARNING: Supply of ${supply[i].volume_MWh} DOESN'T match redemption total of ${totalRedemptions}`)
    }

  }
}

module.exports = {new_EAC_redemption_order, checkOrder}
