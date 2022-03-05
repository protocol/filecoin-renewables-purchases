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

async function get_total_energy_data(start, end, minerID){

  // Major questions:
  // (1) what if the limit (of datapoints returned from energy API) is too small?
    // Checked this with timers (below), we can make the limit high enough for the entire chain for a quarter.
    // Individual minerIDs have fewer datapoints so should be fine for much longer
    // If you are looking at the total chain for more than three months, data may be cut off.
    // Will throw an error below if you hit the limit
  // (2) what if there isn't a block exactly at the time start and end?
    // For individual miners, this is expected to happen frequently
 // (3) How close is the output to previous estimates?

 // To-do list:
  // Give upper, est, and lower
  // Allow user to request model version
  // Possibly allow user more visibility into and control over request limit

  // The filecoin green api can only use timestamps at one day resolution
  // Limit should therefore be multiple of 24*60*2=2880 (number of blocks in one day)
  one_day_blocks = 2880
  limit = one_day_blocks*120

  // console.log(`Calculating sum of upper limit for minerID ${minerID} from ${start} to ${end}`)

  // Sealing request
  requestString = `https://api.filecoin.energy/models/export?end=${end}&id=4&limit=${limit}&offset=0&start=${start}&miner=${minerID}`
  var sealing_records = await axios.get(requestString)
  sealing_records_data = sealing_records.data.data
  // console.log(`Array length from sealing request: ${sealing_records_data.length}`)
  totalSealed_GiB = sealing_records_data.reduce((previousValue, elem) => {
    return previousValue + Number(elem.sealed_this_epoch_GiB)
  }, 0)
  // console.log(totalSealed_GiB)

  // Storage request
  requestString = `https://api.filecoin.energy/models/export?end=${end}&id=3&limit=${limit}&offset=0&start=${start}&miner=${minerID}`
  var storage_records = await axios.get(requestString)
  storage_records_data = storage_records.data.data
  // console.log(storage_records_data)

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

  requestEndTime = new Date(end)
  lastBlockTime = new Date(storage_records_data[storage_records_data.length - 1].timestamp)
  // console.log(`(Storage) End time difference: ${(requestEndTime - lastBlockTime)/1000/3600} hours`)

  if (!(requestEndTime - lastBlockTime) == 0){
    newLastRecord = {
      epoch: null,
      miner: storage_records_data[storage_records_data.length - 1].miner,
      capacity_GiB: storage_records_data[storage_records_data.length - 1].capacity_GiB,
      timestamp: end+'T00:00:00.000Z'
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

  to_return = {
    'minerID': minerID,
    'start' : start,
    'end' : end,
    // 'total_energy_upper_MWh':result_kWh/1000,
    // 'total_energy_records_found' : total_energy_records_found,
    'totalSealed_GiB': totalSealed_GiB,
    'sealingEnergy_upper_MWh': sealingEnergy_upper_MWh,
    // 'datapointAverageCapacity_GiB' : datapointAverageCapacity_GiB,
    // 'difference_totalperiod_hours' : difference_totalperiod_hours,
    // 'datapointAvgStorageTime_GiB_hours' : datapointAvgStorageTime_GiB_hours,
    // 'datapointAvg_Storage_energy_MWh' : datapointAvg_Storage_energy_MWh,
    'integrated_GiB_hr' : integrated_GiB_hr,
    'storage_upper_integrated_MWh' : storage_upper_integrated_MWh,
    'total_energy_upper_MWh_recalc' : total_energy_upper_MWh_recalc,
    'REC_purchase_with_margin' : Math.ceil(total_energy_upper_MWh_recalc * margin)
  }

  return to_return
}

module.exports = {get_total_energy_data}
