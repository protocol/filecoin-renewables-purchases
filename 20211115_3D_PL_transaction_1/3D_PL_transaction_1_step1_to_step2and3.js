const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const order = parse(
    fs.readFileSync("20211115_3D_PL_transaction_1_step1_fromContract.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

// Extract the product productType
// If product field includes key, productType is value
product_key = {
  "I-REC":"IREC",
  "GO -":"GO",
  "REC - Green-e":"REC",
  "LGC - ":"LGC"
}

label_key = {
  "REC - Green-e":"GREEN_E_ENERGY"
}

source_key = {
  "Solar and/or Wind": "wind, solar",
  "Wind and/or Solar": "wind, solar",
  "Biogas": "biogas"
}

contractDate = '2021-11-15'

deliveryDate = '2022-04-15'

dates_key = {
  "January" : {"month_start":'01', "month_end":"01"},
  "February" : {"month_start":'02', "month_end":"02"},
  "March" : {"month_start":'03', "month_end":"03"},
  "April" : {"month_start":'04', "month_end":"04"},
  "May" : {"month_start":'05', "month_end":"05"},
  "June" : {"month_start":'06', "month_end":"06"},
  "July" : {"month_start":'07', "month_end":"07"},
  "August" : {"month_start":'08', "month_end":"08"},
  "September" : {"month_start":'09', "month_end":"09"},
  "October" : {"month_start":'10', "month_end":"10"},
  "November" : {"month_start":'11', "month_end":"11"},
  "December" : {"month_start":'12', "month_end":"12"},
  "1/1/" : {"month_start":'01'},
  "2/1/" : {"month_start":'02'},
  "3/1/" : {"month_start":'03'},
  "4/1/" : {"month_start":'04'},
  "5/1/" : {"month_start":'05'},
  "6/1/" : {"month_start":'06'},
  "7/1/" : {"month_start":'07'},
  "8/1/" : {"month_start":'08'},
  "9/1/" : {"month_start":'09'},
  "10/1/" : {"month_start":'10'},
  "11/1/" : {"month_start":'11'},
  "12/1/" : {"month_start":'12'},
  "1/31/" : {"month_end":'01'},
  "2/31/" : {"month_end":'02'},
  "3/31/" : {"month_end":'03'},
  "4/31/" : {"month_end":'04'},
  "5/31/" : {"month_end":'05'},
  "6/31/" : {"month_end":'06'},
  "7/31/" : {"month_end":'07'},
  "8/31/" : {"month_end":'08'},
  "9/31/" : {"month_end":'09'},
  "10/31/" : {"month_end":'10'},
  "11/31/" : {"month_end":'11'},
  "12/31/" : {"month_end":'12'},
  "Calendar Year" : {"month_start":'01', "month_end":"12"},
  " 2020" : {"year_start":"2020", "year_end":"2020"},
  " 2021" : {"year_start":"2021", "year_end":"2021"},
  "/20 -" : {"year_start":"2020"},
  "/21 -" : {"year_start":"2021"},
  "31/20" : {"year_end":"2020"},
  "31/21" : {"year_end":"2021"}
}

country_codes_irec = parse(
    fs.readFileSync("country_codes_irec.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

country_region_key = {}
for (idj = 0; idj<country_codes_irec.length; idj++){
  country_region_key[country_codes_irec[idj].Name] = {"country":country_codes_irec[idj].ID}
}
country_region_key["California"] = {"country":"US", "region":"WECC"}
country_region_key["WECC"] = {"country":"US", "region":"WECC"}
country_region_key["NPCC"] = {"country":"US", "region":"NPCC"}
country_region_key["MRO"] = {"country":"US", "region":"MRO"}
country_region_key["SPP"] = {"country":"US", "region":"SPP"}
country_region_key["TRE"] = {"country":"US", "region":"TRE"}
country_region_key["SERC"] = {"country":"US", "region":"SERC"}
country_region_key["FRCC"] = {"country":"US", "region":"FRCC"}
country_region_key["RFC"] = {"country":"US", "region":"RFC"}
country_region_key["ASCC"] = {"country":"US", "region":"ASCC"}

// console.log(country_region_key)

step3_out = []

step2_out = order.map((elem, idx) => {

  productType = ''
  for (const key in product_key){
    if(elem['Environmental Product'].includes(key)){productType = product_key[key]}
  }

  label = ''
  for (const key in label_key){
    if(elem['Environmental Product'].includes(key)){label = label_key[key]}
  }

  energySources = ''
  for (const key in source_key){
    if(elem['Environmental Product'].includes(key)){energySources = source_key[key]}
  }

  vintage_dates = {
    "year_start" : '',
    "year_end" : '',
    "month_start" : '',
    "month_end" : ''
  }
  for (const key in dates_key){
    if(elem['Reporting Year / Vintage'].includes(key)){
      dateInfo = dates_key[key]
      for (const infoKey in dateInfo){
        vintage_dates[infoKey] = dateInfo[infoKey]
      }
    }
  }

  reportingStart = vintage_dates["year_start"]+'-'+vintage_dates["month_start"]+'-'+'01'
  reportingEnd = vintage_dates["year_end"]+'-'+vintage_dates["month_end"]+'-'+'31'


  location = {"country" : '',
    "region" : ''}
  for (const key in country_region_key){
    if(elem['Environmental Product'].includes(key)){
      regionInfo = country_region_key[key]
      for (const locKey in regionInfo){
        location[locKey] = regionInfo[locKey]
      }
    }
  }

  step2_out_elem = {
    "contract_id": `20211115_3D_PL_transaction_1_contract_${idx+1}`,
    "productType":productType,
    "label":label,
    "energySources":energySources,
    "contractDate":contractDate,
    "deliveryDate":deliveryDate,
    "reportingStart":reportingStart,
    "reportingEnd":reportingEnd,
    "sellerName":"3Degrees Group, Inc",
    "sellerAddress":"235 Montgomery St Suite 320 CA94104 San Francisco US",
    "country":location.country,
    "region":location.region,
    "volume_MWh":elem[' Quantity '],
    "step3_match_complete":1,
    "step4_ZL_contract_complete":0,
    "step5_redemption_data_complete":0,
    "step6_attestation_info_complete":0,
    "step7_certificates_matched_to_supply":0,
    "step8_IPLDrecord_complete":0,
    "step9_transaction_complete":0,
    "step10_volta_complete":0,
    "step11_finalRecord_complete":0
  }

  step3_out_elem = {
    "allocation_id":`20210831_EW_PL_transaction_1_allocation_${idx+1}`,
    "UUID":"",
    "contract_id": `20211115_3D_PL_transaction_1_contract_${idx+1}`,
    "minerID":elem['Retirement Notes'].split('Filecoin ID:: ')[1],
    "volume_MWh":elem[' Quantity '],
    "defaulted":0,
    "step4_ZL_contract_complete":0,
    "step5_redemption_data_complete":0,
    "step6_attestation_info_complete":0,
    "step7_certificates_matched_to_supply":0,
    "step8_IPLDrecord_complete":0,
    "step9_transaction_complete":0,
    "step10_volta_complete":0,
    "step11_finalRecord_complete":0
  }
  step3_out.push(step3_out_elem)

  return step2_out_elem
})

// console.log(step2_out)

fs.writeFileSync("20211115_3D_PL_transaction_1_step2_orderSupply.csv", json2csvparse.parse(step2_out));
fs.writeFileSync("20211115_3D_PL_transaction_1_step3_match.csv", json2csvparse.parse(step3_out));
