let axios = require("axios")
const fs = require('fs')
const {performance} = require('perf_hooks')

// Take whatever total energy data is given, returns sum of upper lim
function sum_total_energy_upper(data){

  data_with_time_difference = data.map((elem, index, array) => {
    prevTime = elem.timestamp
    if (index > 0) {prevTime = data[index - 1].timestamp}
    prevTime_stamp = new Date(prevTime)
    currentTime_stamp = new Date(elem.timestamp)
    difference_ms = currentTime_stamp - prevTime_stamp
    difference_hours = difference_ms/1000/3600
    toReturn = elem
    toReturn['timeDiff_hours'] = difference_hours
    toReturn['total_energy_kWh_upper'] = Number(elem.total_energy_kW_upper) * difference_hours
    return toReturn
  })

  totalEnergy = data_with_time_difference.reduce((previousValue, elem) => {
    return previousValue + elem.total_energy_kWh_upper
  }, 0)

  return totalEnergy
}

async function get_previous_renewables(minerID){

  toReturn = {}

  // Find purchased renewable energy
  requestString = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${minerID}/transactions`
  var purchases = await axios.get(requestString)
  toReturn.purchases = purchases.data

  // Find open contracts
  requestString = `https://proofs-api.zerolabs.green/api/partners/filecoin/nodes/${minerID}/contracts`
  var contracts = await axios.get(requestString)
  toReturn.contracts = contracts.data

  return toReturn
}

// Requests data from the dashboard API
// If we hit the limit, makes multiple requests and combines the data
// Start and end points are limits, output is not inclusive of end day
async function request_data(start, end, code_name, minerID, outputOn){

  limit = 1000 // This is set to 1000 by the API; will give incorrect results if you use >1000

  start = new Date(start)
  requestString = `https://api.filecoin.energy/models/export?end=${end}&code_name=${code_name}&limit=${limit}&offset=0&start=${start.toISOString()}&miner=${minerID}`
  if (outputOn) {console.log(requestString)}

  var returned_records = await axios.get(requestString)
  var returned_records_data = returned_records.data.data

  // If the array length equals the limit, we need to recursively append to this array
  if (returned_records_data.length == limit){

    // Find timestamp between this block and the next
    last_time = returned_records_data[returned_records_data.length - 1].timestamp
    time_unix = new Date(last_time).getTime()
    next_time_unix = time_unix + 15000 // Advance by 15s, which is 0.5 block time
    next_start_time = new Date(next_time_unix)

    // Request next segment
    var new_data = await request_data(next_start_time, end, code_name, minerID, outputOn)
    return [...returned_records_data, ...new_data]

  }else{

    // If the request doesn't equal the limit, assume the limit is large enough
    // to cover the entire time range
    return returned_records_data
  }
}

// Date format example: '2020-07-01', or ISO 8601 timestamp
async function get_total_energy_data(start, end, minerID, outputOn){

  sealing_records_data = await request_data(start, end, 'SealedModel', minerID, outputOn)

  totalSealed_GiB = sealing_records_data.reduce((previousValue, elem) => {
    return previousValue + Number(elem.sealed_this_epoch_GiB)
  }, 0)



  // Storage request
  storage_records_data = await request_data(start, end, 'CapacityModel', minerID, outputOn)

  if(storage_records_data.length == 0){
    return {
      'total_energy_upper_MWh':0,
      'total_energy_records_found' : 0
    }
  }

  // Errors related to number of datapoints in request
  if(sealing_records_data.length == limit){throw new Error('Limit too short for sealing request')}
  if(storage_records_data.length == limit){throw new Error('Limit too short for storage request')}


  // Add in records at the beginning and end, for the storage array
  // If we don't do this, the energy used to store files between the end points (of the request)
  // and the first/last data points returned will be zero
  // console.log('')
  requestStartTime = new Date(start)
  firstBlockTime = new Date(storage_records_data[0].timestamp)
  // console.log(`(Storage) Start time difference: ${(firstBlockTime - requestStartTime)/1000/3600} hours`)
  // console.log(`(Storage)Array length from storage request: ${storage_records_data.length}`)

  if (!(firstBlockTime - requestStartTime) == 0){
    newFirstRecord = {
      epoch: null,
      miner: storage_records_data[0].miner,
      capacity_GiB: storage_records_data[0].capacity_GiB,
      timestamp: start+'T00:00:00.000Z'
    }

    storage_records_data.unshift(newFirstRecord)
  }

  // console.log(`(Storage) Array length after adjusting first block: ${storage_records_data.length}`)

  requestEndTime = new Date(end+'T23:59:30.000Z')
  lastBlockTime = new Date(storage_records_data[storage_records_data.length - 1].timestamp)
  // console.log(`(Storage) End time difference: ${(requestEndTime - lastBlockTime)/1000/3600} hours`)

  if (!(requestEndTime - lastBlockTime) == 0){
    newLastRecord = {
      epoch: null,
      miner: storage_records_data[storage_records_data.length - 1].miner,
      capacity_GiB: storage_records_data[storage_records_data.length - 1].capacity_GiB,
      timestamp: end+'T23:59:30.000Z'
    }

    storage_records_data.push(newLastRecord)
  }
  // console.log(`(Storage) Array length after adjusting last block: ${storage_records_data.length}`)
  // console.log(storage_records_data)

  // Find actual storage energy from API data
  storage_with_time_difference = storage_records_data.map((elem, index, array) => {
    prevTime = elem.timestamp
    if (index > 0) {prevTime = storage_records_data[index - 1].timestamp}
    prevTime_stamp = new Date(prevTime)
    currentTime_stamp = new Date(elem.timestamp)
    difference_ms = currentTime_stamp - prevTime_stamp
    difference_hours = difference_ms/1000/3600
    toReturn = elem
    toReturn['timeDiff_hours'] = difference_hours
    toReturn['GiB_hours'] = Number(elem.capacity_GiB) * difference_hours
    return toReturn
  })
  integrated_GiB_hr = storage_with_time_difference.reduce((previousValue, elem) => {
    return previousValue + elem.GiB_hours
  }, 0)

  PUE_upper = 1.93

  sealingEnergy_upper_MWh = totalSealed_GiB*5.60E-8*1024**3/1E6
  storage_upper_integrated_MWh = integrated_GiB_hr* 8.1E-12*1024**3/1E6
  total_energy_upper_MWh_recalc = (sealingEnergy_upper_MWh + storage_upper_integrated_MWh)*PUE_upper
  margin = 1 // If we want to increase REC purchase

  // Calculate the whole time period of request, in hours
  // difference_totalperiod_hours = (requestEndTime.getTime() - requestStartTime.getTime())/1000/3600
  starting_time = new Date(storage_with_time_difference[0].timestamp).getTime()
  ending_time = new Date(storage_with_time_difference[storage_with_time_difference.length - 1].timestamp).getTime()
  difference_totalperiod_hours = (ending_time - starting_time)/1000/3600

  to_return = {
    'minerID': minerID,
    'start' : start,
    'end' : end,
    // 'total_energy_upper_MWh':result_kWh/1000,
    // 'total_energy_records_found' : total_energy_records_found,
    'totalSealed_GiB': totalSealed_GiB,
    // 'sealingEnergy_upper_MWh': sealingEnergy_upper_MWh,
    // 'datapointAverageCapacity_GiB' : datapointAverageCapacity_GiB,
    'total_time_hours' : difference_totalperiod_hours,
    // 'datapointAvgStorageTime_GiB_hours' : datapointAvgStorageTime_GiB_hours,
    // 'datapointAvg_Storage_energy_MWh' : datapointAvg_Storage_energy_MWh,
    'initial_capacity_GiB': storage_records_data[0].capacity_GiB,
    'final_capacity_GiB': storage_records_data[storage_records_data.length - 1].capacity_GiB,
    'time_average_capacity_GiB' : integrated_GiB_hr / difference_totalperiod_hours,
    // 'storage_upper_integrated_MWh' : storage_upper_integrated_MWh,
    'total_energy_upper_MWh' : total_energy_upper_MWh_recalc,
    // 'REC_purchase_with_margin' : Math.ceil(total_energy_upper_MWh_recalc * margin)
  }

  return to_return
}

module.exports = {get_total_energy_data, get_previous_renewables}
