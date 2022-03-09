let catalog_renewables_purchases = require("./catalog_renewables_purchases").catalog_renewables_purchases
let orders = require("./orders")

// Run this to assess renewable energy purchases that have been associated with nodes
// catalog_renewables_purchases()

// Take supply and match to SP electricity to produce a redemption order
folder = '20210308_order'
locationFilename = '20210108_minerLocationsReport.json' // For this order, using a version before synthetic locations (so all are tied to an IP location directly)
supplyFilename = '20210308_supply.csv'
orders.new_EAC_redemption_order(folder, locationFilename, supplyFilename)
