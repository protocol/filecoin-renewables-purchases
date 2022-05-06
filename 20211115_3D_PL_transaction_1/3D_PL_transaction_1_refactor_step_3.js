const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const input = parse(
    fs.readFileSync("prelim_20211115_3D_PL_transaction_1_step3_match.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

step3 = input.map((elem, idx) => {
  return {
    "contract_id" : elem.contract_id.replace("line","contract"),
    "minerID" : elem.minerID,
    "volume_MWh": elem.volume_MWh,
    "attestation_id":`20211115_3D_PL_transaction_1_attestation_${idx+1}`,
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
})

console.log(step3)

fs.writeFileSync("20211115_3D_PL_transaction_1_step3_match.csv", json2csvparse.parse(step3));
