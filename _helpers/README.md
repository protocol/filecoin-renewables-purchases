# Manipulate CSV files | Helper
by [Momcilo Dzunic aka smartbee.eth](https://twitter.com/mdzunic)

Manipulating CSV files in this repo and fixing errors in them

### Use
Fixing invalid dates in CSV files (e.g. 2021-02-31) 

    npm run csv fix-dates file_path "date_column_1, date_column_2, date_column_3, date_column_4"
    
    /* npm run csv fix-dates 20211115_3D_PL_transaction_1/20211115_3D_PL_transaction_1_step2_orderSupply.csv "contractDate, deliveryDate, reportingStart, reportingEnd" */

Fixing invalid non float numbers (e.g. 1,736) 

    npm run csv fix-non-floating-numbers file_path "date_column_1, date_column_2" base

    /* npm run csv fix-non-floating-numbers 20211115_3D_PL_transaction_1/20211115_3D_PL_transaction_1_step2_orderSupply.csv volume_MWh 10 */

### License
Licensed under the MIT license.
http://www.opensource.org/licenses/mit-license.php
