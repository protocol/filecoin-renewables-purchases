let fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');



function test_step_7(delivery_folder, step_7_filename){

  // Load the data we are checking
  const step7 = parse(
      fs.readFileSync(delivery_folder+'/'+step_7_filename,
      {
          encoding:'utf8',
          flag:'r'
      }
  ), {
      columns: true,
      cast: true
  });

  // Load the generation records for this delivery, which we are using to cross-reference
  const genRecords = parse(
      fs.readFileSync(delivery_folder+'/'+delivery_folder+'_step6_generationRecords.csv',
      {
          encoding:'utf8',
          flag:'r'
      }
  ), {
      columns: true,
      cast: true
  });

  for (idj = 0; idj<step7.length; idj++){
    // console.log('')
    // console.log('------')


    // Find this certificate from the order
    thisCert = genRecords.filter(elem => elem.certificate == step7[idj].certificate)[0]
    // console.log(thisCert)


    // Load step2, which has information for this contract
    // console.log(step7[idj].order_folder)
    const order = parse(
        fs.readFileSync(step7[idj].order_folder+'/'+step7[idj].order_folder+'_step2_orderSupply.csv',
        {
            encoding:'utf8',
            flag:'r'
        }
    ), {
        columns: true,
        cast: true
    });
    thisContract = order.filter(elem => elem.contract_id == step7[idj].contract)[0]

    // console.log(thisContract)

    // Compare the info from contract and certificate
    step7[idj].contract_country = thisContract.country
    step7[idj].certificate_country = thisCert.country
    step7[idj].country_match = thisContract.country == thisCert.country

    step7[idj].contract_region = thisContract.region
    step7[idj].certificate_region = thisCert.region
    step7[idj].region_match = thisContract.region == thisCert.region

    step7[idj].contract_start = thisContract.reportingStart
    step7[idj].certificate_start = thisCert.generationStart
    contract_start_date = new Date(thisContract.reportingStart).getTime()
    certificate_start_date = new Date(thisCert.generationStart).getTime()
    step7[idj].start_match = contract_start_date == certificate_start_date

    step7[idj].contract_end = thisContract.reportingEnd
    step7[idj].certificate_end = thisCert.generationEnd
    contract_end_date = new Date(thisContract.reportingEnd).getTime()
    certificate_end_date = new Date(thisCert.generationEnd).getTime()
    step7[idj].end_match = contract_end_date == certificate_end_date

    step7[idj].cert_within_contract = (contract_start_date <= certificate_start_date) && (contract_end_date >= certificate_end_date)
    step7[idj].country_region_and_certWithinContract = step7[idj].country_match && step7[idj].region_match && step7[idj].cert_within_contract

    // console.log(step7[idj])
  }

  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_check_step_7.csv", json2csvparse.parse(step7));

}

module.exports = {test_step_7}

// test_step_7('20210831_delivery', '20210831_delivery_step7_certificate_to_contract.csv')
//test_step_7('20220429_SP_delivery', '20220429_SP_delivery_step7_certificate_to_contract.csv')
test_step_7('20211231_3D_delivery', '20211231_3D_delivery_step7_certificate_to_contract.csv')
