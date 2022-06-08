# filecoin-renewables-purchases
Renewable energy purchases for the Filecoin network


**catalog_renewables_purchases** Examines EAC purchases by SP, using the public Zero Labs and Filecoin Energy Dashboard APIs.

**orders.new_EAC_redemption_order** produces a registry order matching available EACs to Filecoin SPs based on the upper bound of estimated energy use.

**add-ewc-redemption-statement.js** adds metadata mapping an EAC order to a Zero Labs smart contract.

**orders.checkOrder** compares the output of an EAC order to supply in order to double check that the totals match.

## REC Outputs
RECs from this repo are recorded in the [Zero Labs](https://zerolabs.green/) system and the [Filecoin Energy Dashboard](https://filecoin.energy). 

For IPLD formatted data corresponding to renewable energy allocations in this repo, see the [REC Browser](https://filecoin-green-eac-browser.dzunic.net/).

## REC Tracking
Purchased RECs reflected in this repo:

| Order File  | Amount |
| ------------- | ------------- |
| [20210831_EW_PL_transaction_1](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20210831_EW_PL_transaction_1/20210831_EW_PL_transaction_1_step2_orderSupply.csv)  | 712 MWh  |
| [20211115_3D_PL_transaction_1](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20211115_3D_PL_transaction_1/20211115_3D_PL_transaction_1_step2_orderSupply.csv)  | 83,734 MWh  |
| [20220207_SP_PL_transaction_1](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20220207_SP_PL_transaction_1/20220207_SP_PL_transaction_1_step2_orderSupply.csv) | 153,196 MWh |
| [20220420_SP_FF_transaction_1](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20220420_SP_FF_transaction_1/20220420_SP_FF_transaction_1_step2_orderSupply.csv) | 300,000 MWh |
| [20220501_ACT_PL_transaction_1](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20220501_ACT_PL_transaction_1/20220501_ACT_PL_transaction_1_step2_orderSupply.csv) | 857,411 MWh |
| [20220607_SP_FF_transaction_2](https://github.com/redransil/filecoin-renewables-purchases/blob/main/20220607_SP_FF_transaction_2/20220607_SP_FF_transaction_2_step2_orderSupply.csv) | 449,806 MWh |
| **Total:** | 1,844,859 MWh |
