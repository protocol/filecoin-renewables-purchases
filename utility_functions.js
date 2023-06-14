import fs from 'fs';
import { parse } from 'csv-parse/sync';
import json2csvparse from 'json2csv';


// Replacing the contract, minerID in step7 with an allocation name
function reafactor_step_7(delivery_folder, step_7_filename){

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

  // console.log(step7)

  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_step7_certificate_to_contract_BACKUP.csv", json2csvparse.parse(step7));


  newStep7 = []

  for (idj = 0; idj<step7.length; idj++){

    console.log('')
    console.log('------')
    console.log(step7[idj])

    // Load the allocation records for this delivery, which we are using to cross-reference
    const allocationRecords = parse(
        fs.readFileSync(step7[idj].order_folder+'/'+step7[idj].order_folder+'_step3_match.csv',
        {
            encoding:'utf8',
            flag:'r'
        }
    ), {
        columns: true,
        cast: true
    });

    // console.log(allocationRecords)


    // Find this allocation from step7 info
    thisAlloc = allocationRecords.filter(elem => (elem.minerID == step7[idj].minerID) && (elem.contract_id == step7[idj].contract))
    if(thisAlloc.length > 1) {error()}
    thisAlloc = thisAlloc[0]
    console.log(thisAlloc)

    allocation_name = thisAlloc.allocation_id
    console.log(allocation_name)

    // Reconstruct record for step 7
    newElem = {
      "certificate":step7[idj].certificate,
      "volume_MWh":step7[idj].volume_MWh,
      "order_folder":step7[idj].order_folder,
      "allocation":allocation_name
    }
    console.log(newElem)
    newStep7.push(newElem)


  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_step7_certificate_to_contract.csv", json2csvparse.parse(newStep7));
  }

}

function calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, winter){

  if(
    (mmFwd < Number(mm) && Number(mm) < mmBwd) ||
    (mmFwd == Number(mm) && ddFwd <= Number(dd)) ||
    (mmBwd == Number(mm) && Number(dd) < ddBwd)
    ){return winter + 60}
  return winter

}

// This ignores the fact that on different years daylight savings changes on different days
function getTimezoneOffset(countryCode, timeStamp, generatorName){
  
  // get yyyy-mm-dd and make sure we were given the right format
  var rx = /\d\d\d\d-\d\d-\d\d/
  var yyyymmddSubstring = rx.exec(timeStamp);
  try{
    yyyymmddSubstring[0]
  }catch{error()}

  var yyyy = yyyymmddSubstring[0].substring(0,4)
  var mm = yyyymmddSubstring[0].substring(5,7)
  var dd = yyyymmddSubstring[0].substring(8,10)

  if(countryCode == 'CN'){
      return 480
  }

  if(
    (countryCode == 'DE')||
    (countryCode == 'FR')||
    (countryCode == 'BE')||
    (countryCode == 'NL')||
    (countryCode == 'NO')||
    (countryCode == 'PL')||
    (countryCode == 'DK')
    ){
    var mmFwd = 3
    var ddFwd = 26
    var mmBwd = 10
    var ddBwd = 29
    return calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, 60)
  }

  if(
    (countryCode == 'GB')
    ){
    var mmFwd = 3
    var ddFwd = 26
    var mmBwd = 10
    var ddBwd = 29
    return calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, 0)
  }

  if(
    (countryCode == 'BG')
    ){
    var mmFwd = 3
    var ddFwd = 26
    var mmBwd = 10
    var ddBwd = 29
    return calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, 60*2)
  }

  if(countryCode == 'JP'){
    return 60*9
  }

  if(countryCode == 'US'){
  var mmFwd = 3
  var ddFwd = 12
  var mmBwd = 11
  var ddBwd = 5
    if(generatorName == 'Buckeye Wind Farm'){
      return calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, -60*6)
    }
    if(generatorName == 'Spring Canyon II Wind Energy Center'){
      return calcDaylight(mmFwd, ddFwd, mmBwd, ddBwd, mm, dd, -60*8)
    }
      return ''
  }
}

function fillTimezones(path, inputFile, outputName){
  var data = parse(fs.readFileSync(path+'/'+inputFile, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
  data.forEach(elem =>{
    elem.reportingStartTimezoneOffset = getTimezoneOffset(elem.country, elem.reportingStart, elem.generatorName)
    elem.reportingEndTimezoneOffset = getTimezoneOffset(elem.country, elem.reportingEnd, elem.generatorName)
    elem.generationStartTimezoneOffset = getTimezoneOffset(elem.country, elem.generationStart, elem.generatorName)
    elem.generationEndTimezoneOffset = getTimezoneOffset(elem.country, elem.generationEnd, elem.generatorName)
  })
  // console.log(data)

  fs.writeFileSync(path+"/"+outputName, json2csvparse.parse(data));

}

function fillCerts(path, certPrefix, inputFile, outputName){
  var data = parse(fs.readFileSync(path+'/'+inputFile, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});

  const certNums = data.reduce((prev, current) => prev.concat(Number(current.certificate.replace(certPrefix, ''))), [])
  var largestCert = Math.max(...certNums)

  data.forEach(elem =>{
    
    var thisCertNum = Number(elem.certificate.replace(certPrefix, ''))
    if (thisCertNum == 0){
      largestCert = largestCert + 1
      var newCert = largestCert
      // console.log(certPrefix+largestCert)
      elem.certificate = certPrefix+largestCert
    }
    
  })
  // console.log(data)

  fs.writeFileSync(path+"/"+outputName, json2csvparse.parse(data));

}


export {reafactor_step_7, fillTimezones, fillCerts};

// reafactor_step_7('20210831_delivery', '20210831_delivery_step7_certificate_to_contract.csv')
// reafactor_step_7('20211231_3D_delivery', '20211231_3D_delivery_step7_certificate_to_contract.csv')
// reafactor_step_7('20220429_SP_delivery', '20220429_SP_delivery_step7_certificate_to_contract.csv')
