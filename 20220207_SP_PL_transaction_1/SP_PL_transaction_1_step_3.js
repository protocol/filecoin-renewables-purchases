const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const input = parse(
    fs.readFileSync("SP_PL_step3_prelim.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

// console.log(input)

match_contracts = {
  "Longli Jia Tuo Mountain Windfarm": "20220207_SP_PL_transaction_1_contract_1",
  "Tuokexun Windfarm Phase 1 and 2":"20220207_SP_PL_transaction_1_contract_4",
  "Sanjian Windfarm":"20220207_SP_PL_transaction_1_contract_5",
  "Yuxian Lihuajian Windfarm":"20220207_SP_PL_transaction_1_contract_6"
}

// if "Guedin Windfarm"
start_contract = {
  "2021-04-01":"20220207_SP_PL_transaction_1_contract_2",
  "2021-05-01":"20220207_SP_PL_transaction_1_contract_3"
}




step3 = input.map((elem, idx) => {
  // find the contract
  contract_id = ''
  if (elem.source == "Guedin Windfarm"){contract_id = start_contract[elem.start_date]}
  else {contract_id = match_contracts[elem.source]}

  // find the minerID
  split = elem.beneficiary.split("MinerID: ")

  return {
    "contract_id": contract_id,
    "minerID": split[1],
    "volume_MWh": elem.volume_MWh,
    "attestation_id": `20220207_SP_PL_transaction_1_attestation_${idx+1}`,
    "defaulted": 0,
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

console.log(step3)

fs.writeFileSync("20220207_SP_PL_transaction_1_step_3_match.csv", json2csvparse.parse(step3));
