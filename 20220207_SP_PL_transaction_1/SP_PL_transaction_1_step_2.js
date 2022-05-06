const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const input = parse(
    fs.readFileSync("SP_PL_step2_prelim.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

// console.log(input)


step2 = input.map((elem, idx) => {
  return {
    "contract_id": `20220207_SP_PL_transaction_1_contract_${idx+1}`,
    "productType": elem.productType,
    "label": elem.label,
    "energySources": elem.energySources,
    "contractDate": elem.contractDate,
    "deliveryDate": elem.deliveryDate,
    "reportingStart": elem.reportingStart,
    "reportingEnd": elem.reportingEnd,
    "sellerName": elem.sellerName,
    "sellerAddress": elem.sellerAddress,
    "country": elem.country,
    "region": elem.region,
    "volume_MWh": elem.volume_MWh,
    "step2_order_complete": elem.step2_order_complete,
    "step3_match_complete": elem.step3_match_complete,
    "step4_ZL_contract_complete": elem.step4_ZL_contract_complete,
    "step5_redemption_data_complete": elem.step5_redemption_data_complete,
    "step6_attestation_info_complete": elem.step6_attestation_info_complete,
    "step7_certificates_matched_to_supply": elem.step7_certificates_matched_to_supply,
    "step8_IPLDrecord_complete": elem.step8_IPLDrecord_complete,
    "step9_transaction_complete": elem.step9_transaction_complete,
    "step10_volta_complete": elem.step10_volta_complete,
    "step11_finalRecord_complete": elem.step11_finalRecord_complete
  }
})

console.log(step2)

fs.writeFileSync("20220207_SP_PL_transaction_1_step2_orderSupply.csv", json2csvparse.parse(step2));
