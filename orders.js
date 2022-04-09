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

async function match_to_SP_list(supplyRecord, supply_remaining, locations, ZL_nodes, new_beneficiaries, redemptions, orderFolder){

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

    // Account for previous renewables purchases, by skipping if this minerID is in the ZL interface
    // (In the future, subtract the amount)
    ZL_match = ZL_nodes.data.filter(x => x.id == locations[SP_idx].miner)
    if (ZL_match.length > 0){
      console.log('Skipping on account of previous renewable energy purchases')
      skip_this_minerID = true
    }


    // Have we already allocated for this minerID in this date range (ie if supply dates overlap)?
    prevAllocation_thisMinerID = redemptions.filter(x => x.name == locations[SP_idx].miner)
    for (j=0; j<prevAllocation_thisMinerID.length; j++){

      // Convert dates into timestamps so we can compare them
      vintage_start_timestamp = new Date(supplyRecord.start).getTime()
      vintage_end_timestamp = new Date(supplyRecord.end).getTime()
      previous_start_timestamp = new Date(prevAllocation_thisMinerID[j].start_date).getTime()
      previous_end_timestamp = new Date(prevAllocation_thisMinerID[j].end_date).getTime()

      // This allocation overlaps if either of the supply start and end times
      // falls within the previous time range
      if ((
        (previous_start_timestamp <= vintage_start_timestamp)&&(vintage_start_timestamp < previous_end_timestamp))
      ||(
        (previous_start_timestamp < vintage_end_timestamp)&&(vintage_end_timestamp<=previous_end_timestamp)
      )){
        console.log('Skipping on account of overlap')
        skip_this_minerID = true
      }
    }

    // If allocating to this SP will overlap with another allocation, then skip
    if (skip_this_minerID){
      SP_idx ++
      continue
    }


    // Find the amount of energy used by this SP
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
    console.log(max_allocation)

    // Determine number of RECs to allocate (before adjusting for existing allocations below)
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
    console.log(supply[i])

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
