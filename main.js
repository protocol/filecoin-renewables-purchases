// let catalog_renewables_purchases = require("./catalog_renewables_purchases").catalog_renewables_purchases
// let orders = require("./orders")
// let getEnergy = require('./getEnergy')
let test_utilities = require('./test_utility.js')

// Run this to assess renewable energy purchases that have been associated with nodes
// catalog_renewables_purchases()

// Take supply and match to SP electricity to produce a redemption order
// folder = '20220407_fractionalize_contract_supply'
// orders.new_EAC_redemption_order(folder)
// folder = 'test'
// orders.new_EAC_redemption_order(folder)

tests = ['20220420_SP_FF_transaction_1', '20220501_ACT_PL_transaction_1', '20220607_SP_FF_transaction_2']

test_utilities.test_step_3(tests[2])


// Run checks
// folder = '20220318_order'
// orders.checkOrder(folder)

// async function main(){
//   result = await getEnergy.get_total_energy_data('2020-08-01', '2022-03-20', 'f066596')
//   console.log(result)
// }
//
// main()
