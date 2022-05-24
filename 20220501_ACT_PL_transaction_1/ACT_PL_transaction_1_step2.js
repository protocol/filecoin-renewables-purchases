const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const input = parse(
    fs.readFileSync("20220501_ACT_PL_transaction_1_step1_fromContract.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});



const countries = parse(
    fs.readFileSync("../country_codes_irec.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

countriesKey={}
for (i=0; i<countries.length; i++){
  // console.log(countries[i])
  countriesKey[countries[i].Name] = countries[i].ID
}

// console.log(countriesKey)

// console.log(input)

productKey = {
  "GoO": "GO",
  "REGO":"REGO",
  "I-REC":"IREC",
  "NFC":"NFC",
  "Green-e":"REC",
  "LGC":"LGC"
}

labelKey = {
  "GoO": "",
  "REGO":"",
  "I-REC":"",
  "NFC":"",
  "Green-e":"GREEN_E_ENERGY",
  "LGC":"",
}

sourceKey = {
  "Wind/Solar":"wind, solar",
  "Hydro":"hydro",
  "Wind":"wind"
}

step2 = input.map((elem, idx) => {
  country = ""
  if (elem.Location.includes("Netherlands")){country = countriesKey["Netherlands"]}
  if (elem.Location.includes("Germany")){country = countriesKey["Germany"]}
  if (elem.Location.includes("France")){country = countriesKey["France"]}
  if (elem.Location.includes("Belgium")){country = countriesKey["Belgium"]}
  if (elem.Location.includes("UK")){country = countriesKey["United Kingdom of Great Britain and Northern Ireland"]}
  if (elem.Location.includes("Norway")){country = countriesKey["Norway"]}
  if (elem.Location.includes("Denmark")){country = countriesKey["Denmark"]}
  if (elem.Location.includes("Bulgaria")){country = countriesKey["Bulgaria"]}
  if (elem.Location.includes("Poland")){country = countriesKey["Poland"]}
  if (elem.Location.includes("China")){country = countriesKey["China"]}
  if (elem.Location.includes("Japan")){country = countriesKey["Japan"]}
  if (elem.Location.includes("USA")){country = countriesKey["United States of America"]}
  if (elem.Location.includes("MRO")){country = countriesKey["United States of America"]}
  if (elem.Location.includes("WECC")){country = countriesKey["United States of America"]}

  region = ""
  if (elem.Location.includes("MRO")){region = "MRO"}
  if (elem.Location.includes("WECC")){region = "WECC"}


  return {
    "contract_id": `20220501_ACT_PL_transaction_1_contract_${idx+1}`,
    "productType": productKey[elem.Product],
    "label": labelKey[elem.Product],
    "energySources": sourceKey[elem.Tech],
    "contractDate": "20220501",
    "deliveryDate": "20220630",
    "reportingStart": elem.start,
    "reportingEnd": elem.end,
    "sellerName": "ACT Commodities Inc.",
    "sellerAddress": "437 Madison Avenue, Suite 17A, New York, NY, 10022, USA",
    "country": country,
    "region": region,
    "volume_MWh": elem.MWh,
    "step3_match_complete": 0,
    "step4_ZL_contract_complete": 0,
    "step5_redemption_data_complete": 0,
    "step6_attestation_info_complete": 0,
    "step7_certificates_matched_to_supply": 0,
    "step8_IPLDrecord_complete": 0,
    "step9_transaction_complete": 0,
    "step10_volta_complete": 0,
    "step11_finalRecord_complete": 0
  }
})

// console.log(step2)
//
fs.writeFileSync("20220501_ACT_PL_transaction_1_step2_orderSupply.csv", json2csvparse.parse(step2));
