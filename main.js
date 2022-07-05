// let catalog_renewables_purchases = require("./catalog_renewables_purchases").catalog_renewables_purchases
// let orders = require("./orders")
// let getEnergy = require('./getEnergy')
let test_utilities = require('./test_utility.js')
let gen5 = require('./_helpers/generate_step5.js').gen_step_5_auto

// Run this to assess renewable energy purchases that have been associated with nodes
// catalog_renewables_purchases()

// Take supply and match to SP electricity to produce a redemption order
// folder = '20220407_fractionalize_contract_supply'
// orders.new_EAC_redemption_order(folder)
// folder = 'test'
// orders.new_EAC_redemption_order(folder)

// tests = ['20220420_SP_FF_transaction_1', '20220501_ACT_PL_transaction_1', '20220607_SP_FF_transaction_2']
//
// test_utilities.test_step_3(tests[2])


// 20220704: generate step 5 automatic redemptions
const smart_contract_id = '0x2248a8E53c8cf533AEEf2369FfF9Dc8C036c8900'
attestation_folder = '20220510_ACT_PL_transaction_1_delivery_1'

ACT_PL_transaction_1_folder = '20220501_ACT_PL_transaction_1'
ACT_PL_transaction_1_IREC_contracts = ['20220501_ACT_PL_transaction_1_contract_14', '20220501_ACT_PL_transaction_1_contract_15', '20220501_ACT_PL_transaction_1_contract_16', '20220501_ACT_PL_transaction_1_contract_17', '20220501_ACT_PL_transaction_1_contract_18']

SP_FF_transaction_1_folder = '20220420_SP_FF_transaction_1'
SP_FF_transaction_1_IREC_contracts = ['20220420_SP_FF_transaction_1_contract_1']

SP_FF_transaction_2_folder = '20220607_SP_FF_transaction_2'
SP_FF_transaction_2_IREC_contracts = ['20220607_SP_FF_transaction_2_contract_1', '20220607_SP_FF_transaction_2_contract_2', '20220607_SP_FF_transaction_2_contract_3']

// gen5(ACT_PL_transaction_1_folder, ACT_PL_transaction_1_IREC_contracts, smart_contract_id, attestation_folder)
// gen5(SP_FF_transaction_1_folder, SP_FF_transaction_1_IREC_contracts, smart_contract_id, attestation_folder)
gen5(SP_FF_transaction_2_folder, SP_FF_transaction_2_IREC_contracts, smart_contract_id, attestation_folder)


// Run checks
// folder = '20220318_order'
// orders.checkOrder(folder)

// async function main(){
//   result = await getEnergy.get_total_energy_data('2020-08-01', '2022-03-20', 'f066596')
//   console.log(result)
// }
//
// main()
