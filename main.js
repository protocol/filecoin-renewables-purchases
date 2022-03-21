let catalog_renewables_purchases = require("./catalog_renewables_purchases").catalog_renewables_purchases
let orders = require("./orders")

// Run this to assess renewable energy purchases that have been associated with nodes
// catalog_renewables_purchases()

// Take supply and match to SP electricity to produce a redemption order
folder = '20220318_order'
locationFilename = '20220317-synthetic-country-state-province-locations-latest.json' // For this order, using a version before synthetic locations (so all are tied to an IP location directly)
supplyFilename = '20210309_supply.csv'
orders.new_EAC_redemption_order(folder, locationFilename, supplyFilename)
