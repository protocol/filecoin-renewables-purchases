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

Creating step 5 CSV 

    npm run csv create-step-5 attestation_folder_path transaction_folder_path

    /* npm run csv create-step-5 20211231_3D_delivery 20211115_3D_PL_transaction_1 */

Creating step 6 CSV (3D)

    npm run csv create-step-6 attestation_folder_path transaction_folder_path

    /* npm run csv create-step-6-3d 20211231_3D_delivery 20211115_3D_PL_transaction_1 */

Creating step 7 CSV and matching certificates against allocations (3D)

    npm run csv create-step-7 attestation_folder_path transaction_folder_path

    /* npm run csv create-step-7-3d 20211231_3D_delivery 20211115_3D_PL_transaction_1 */

### License
Licensed under the MIT license.
http://www.opensource.org/licenses/mit-license.php
