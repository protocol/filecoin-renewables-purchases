let catalog_renewables_purchases = require("./catalog_renewables_purchases").catalog_renewables_purchases
let orders = require("./orders")
let getEnergy = require('./getEnergy')

// Run this to assess renewable energy purchases that have been associated with nodes
// catalog_renewables_purchases()

// Take supply and match to SP electricity to produce a redemption order
// folder = '20220309_order'
// orders.new_EAC_redemption_order(folder)

// Run checks
folder = '20220318_order'
orders.checkOrder(folder)

// async function main(){
//   result = await getEnergy.get_total_energy_data('2020-08-01', '2022-03-20', 'f066596')
//   console.log(result)
// }
//
// main()
