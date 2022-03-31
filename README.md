# filecoin-renewables-purchases
Renewable energy purchases for the Filecoin network


**catalog_renewables_purchases** Examines EAC purchases by SP, using the public Zero Labs and Filecoin Energy Dashboard APIs.

**orders.new_EAC_redemption_order** produces a registry order matching available EACs to Filecoin SPs based on the upper bound of estimated energy use.

**add-ewc-redemption-statement.js** adds metadata mapping an EAC order to a Zero Labs smart contract.

**orders.checkOrder** compares the output of an EAC order to supply in order to double check that the totals match.

## EAC Tracking
Files showing Energy Attribute Certificate (EAC) renewable energy purchases on the Filecoin network, matched to total energy use of the given node. This may not reflect all existing renewable energy purchase contracts with brokers. Archive of output files:
- [20220304_EAC_purchase_summary.csv](https://github.com/redransil/filecoin-renewables-purchases/blob/main/EAC_Purchase_Summary_Archive/20220304_EAC_purchase_summary.csv)
- [20220308_EAC_purchase_summary.csv](https://github.com/redransil/filecoin-renewables-purchases/blob/main/EAC_Purchase_Summary_Archive/20220308_EAC_purchase_summary.csv)
