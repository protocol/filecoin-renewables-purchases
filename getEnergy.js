let axios = require("axios")
const fs = require('fs')
const {performance} = require('perf_hooks')
// const { parse } = require('csv-parse/sync')
// const json2csvparse = require('json2csv');

// Take whatever total energy data is given, returns sum of upper lim
function sum_total_energy_upper(data){
  // console.log(data)

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

  // console.log(data_with_time_difference)

  totalEnergy = data_with_time_difference.reduce((previousValue, elem) => {
    return previousValue + elem.total_energy_kWh_upper
  }, 0)

  return totalEnergy
}

async function get_total_energy_data(start, end, minerID){

  // Major questions:
  // (1) what if the limit is too small?
    // Checked this with timers (below), we can make the limit high enough for a quarter
  // (2) what if there isn't a block exactly at the time start and end?
    // For individual miners, this is expected to happen frequently
 // (3) Does the output actually match what I calculated previously?

  // The filecoin green api can only use timestamps at one day resolution
  // Limit should therefore be multiple of 24*60*2=2880 (number of blocks in one day)
  one_day_blocks = 2880
  limit = one_day_blocks*120

  // Total energy request
  // requestString = `https://api.filecoin.energy/models/export?end=${end}&id=0&limit=${limit}&offset=0&start=${start}&miner=${minerID}`
  // requestString = `https://api.filecoin.energy/models/export?end=${end}&id=0&limit=${limit}&offset=0&start=${start}`
  // preRequest = performance.now()
  // var total_energy = await axios.get(requestString)
  // postRequest = performance.now()
  // total_energy_data = total_energy.data.data
  console.log(`Calculating sum of upper limit for minerID ${minerID} from ${start} to ${end}`)
  // console.log(`Array length from request: ${total_energy_data.length}`)
  // var total_energy_records_found = total_energy_data.length
  // console.log(`Request limit: ${limit}`)
  // console.log(`Request time: ${postRequest - preRequest} ms`)

  // Sealing request
  requestString = `https://api.filecoin.energy/models/export?end=${end}&id=4&limit=${limit}&offset=0&start=${start}&miner=${minerID}`
  var sealing_records = await axios.get(requestString)
  sealing_records_data = sealing_records.data.data
  console.log(`Array length from sealing request: ${sealing_records_data.length}`)
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

  // Errors related to request
  // if(total_energy_data.length == limit){throw new Error('Limit too short for request')}
  // if(total_energy_data.length == 0){
  //   // throw new Error('Found no records')
  //   return {
  //     'total_energy_upper_MWh':0,
  //     'total_energy_records_found' : 0
  //   }
  // }

  // Add in records at the beginning and end, for the energy array
  // console.log('')
  // console.log('First record:')
  // console.log(total_energy_data[0])
  // requestStartTime = new Date(start)
  // firstBlockTime = new Date(total_energy_data[0].timestamp)
  // console.log(`(Energy) Start time difference: ${(firstBlockTime - requestStartTime)/1000/3600} hours`)
  //
  // if (!(firstBlockTime - requestStartTime) == 0){
  //   newFirstRecord = {
  //     epoch: null,
  //     miner: total_energy_data[0].miner,
  //     total_energy_kW_lower: total_energy_data[0].total_energy_kW_lower,
  //     total_energy_kW_estimate: total_energy_data[0].total_energy_kW_estimate,
  //     total_energy_kW_upper: total_energy_data[0].total_energy_kW_upper,
  //     timestamp: start+'T00:00:00.000Z'
  //   }
  //
  //   total_energy_data.unshift(newFirstRecord)
  // }
  //
  // console.log(`(Energy) Array length after adjusting first block: ${total_energy_data.length}`)
  // // console.log(total_energy_data[0])
  //
  // // console.log('Last record:')
  // // console.log(total_energy_data[total_energy_data.length - 1])
  // requestEndTime = new Date(end)
  // lastBlockTime = new Date(total_energy_data[total_energy_data.length - 1].timestamp)
  // console.log(`(Energy) End time difference: ${(requestEndTime - lastBlockTime)/1000/3600} hours`)
  //
  // if (!(requestEndTime - lastBlockTime) == 0){
  //   newLastRecord = {
  //     epoch: null,
  //     miner: total_energy_data[total_energy_data.length - 1].miner,
  //     total_energy_kW_lower: total_energy_data[total_energy_data.length - 1].total_energy_kW_lower,
  //     total_energy_kW_estimate: total_energy_data[total_energy_data.length - 1].total_energy_kW_estimate,
  //     total_energy_kW_upper: total_energy_data[total_energy_data.length - 1].total_energy_kW_upper,
  //     timestamp: end+'T00:00:00.000Z'
  //   }
  //
  //   total_energy_data.push(newLastRecord)
  // }
  // console.log(`(Energy) Array length after adjusting last block: ${total_energy_data.length}`)
  // // console.log('Last record:')
  // // console.log(total_energy_data[total_energy_data.length - 1])

  //2021-10-01T00:00:00.000Z


  // Add in records at the beginning and end, for the storage array
  console.log('')
  // console.log('First storage record:')
  // console.log(storage_records_data[0])
  requestStartTime = new Date(start)
  firstBlockTime = new Date(storage_records_data[0].timestamp)
  console.log(`(Storage) Start time difference: ${(firstBlockTime - requestStartTime)/1000/3600} hours`)
  console.log(`(Storage)Array length from storage request: ${storage_records_data.length}`)

  if (!(firstBlockTime - requestStartTime) == 0){
    newFirstRecord = {
      epoch: null,
      miner: storage_records_data[0].miner,
      capacity_GiB: storage_records_data[0].capacity_GiB,
      timestamp: start+'T00:00:00.000Z'
    }

    storage_records_data.unshift(newFirstRecord)
  }

  console.log(`(Storage) Array length after adjusting first block: ${storage_records_data.length}`)

  requestEndTime = new Date(end)
  lastBlockTime = new Date(storage_records_data[storage_records_data.length - 1].timestamp)
  console.log(`(Storage) End time difference: ${(requestEndTime - lastBlockTime)/1000/3600} hours`)

  if (!(requestEndTime - lastBlockTime) == 0){
    newLastRecord = {
      epoch: null,
      miner: storage_records_data[storage_records_data.length - 1].miner,
      capacity_GiB: storage_records_data[storage_records_data.length - 1].capacity_GiB,
      timestamp: end+'T00:00:00.000Z'
    }

    storage_records_data.push(newLastRecord)
  }
  console.log(`(Storage) Array length after adjusting last block: ${storage_records_data.length}`)
  // console.log(storage_records_data)


  // // Sum energy
  // preCalc = performance.now()
  // result_kWh = sum_total_energy_upper(total_energy_data)
  // postCalc = performance.now()
  // console.log(`Sum time: ${postCalc - preCalc} ms`)

  // Estimate storage energy
  datapointSum_GiB = storage_records_data.reduce((previousValue, elem) => {
    return previousValue + Number(elem.capacity_GiB)
  }, 0)
  datapointAverageCapacity_GiB = datapointSum_GiB/storage_records_data.length
  startTime_stamp = new Date(start)
  endTime_stamp = new Date(end)
  difference_totalperiod_ms = endTime_stamp - startTime_stamp
  difference_totalperiod_hours = difference_totalperiod_ms/1000/3600
  datapointAvgStorageTime_GiB_hours = difference_totalperiod_hours * datapointAverageCapacity_GiB
  datapointAvg_Storage_energy_MWh = datapointAvgStorageTime_GiB_hours * 8.1E-12*1024**3/1E6

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

  // console.log(storage_with_time_difference)

  integrated_GiB_hr = storage_with_time_difference.reduce((previousValue, elem) => {
    return previousValue + elem.GiB_hours
  }, 0)

  PUE_upper = 1.93

  sealingEnergy_upper_MWh = totalSealed_GiB*5.60E-8*1024**3/1E6
  storage_upper_integrated_MWh = integrated_GiB_hr* 8.1E-12*1024**3/1E6
  total_energy_upper_MWh_recalc = (sealingEnergy_upper_MWh + storage_upper_integrated_MWh)*PUE_upper
  margin = 1

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
