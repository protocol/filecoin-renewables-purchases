let fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');


// Replacing the contract, minerID in step7 with an allocation name
function reafactor_step_7(delivery_folder, step_7_filename){

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

  // console.log(step7)

  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_step7_certificate_to_contract_BACKUP.csv", json2csvparse.parse(step7));


  newStep7 = []

  for (idj = 0; idj<step7.length; idj++){

    console.log('')
    console.log('------')
    console.log(step7[idj])

    // Load the allocation records for this delivery, which we are using to cross-reference
    const allocationRecords = parse(
        fs.readFileSync(step7[idj].order_folder+'/'+step7[idj].order_folder+'_step3_match.csv',
        {
            encoding:'utf8',
            flag:'r'
        }
    ), {
        columns: true,
        cast: true
    });

    // console.log(allocationRecords)


    // Find this allocation from step7 info
    thisAlloc = allocationRecords.filter(elem => (elem.minerID == step7[idj].minerID) && (elem.contract_id == step7[idj].contract))
    if(thisAlloc.length > 1) {error()}
    thisAlloc = thisAlloc[0]
    console.log(thisAlloc)

    allocation_name = thisAlloc.allocation_id
    console.log(allocation_name)

    // Reconstruct record for step 7
    newElem = {
      "certificate":step7[idj].certificate,
      "volume_MWh":step7[idj].volume_MWh,
      "order_folder":step7[idj].order_folder,
      "allocation":allocation_name
    }
    console.log(newElem)
    newStep7.push(newElem)


  fs.writeFileSync(delivery_folder+"/"+delivery_folder+"_step7_certificate_to_contract.csv", json2csvparse.parse(newStep7));
  }

}

module.exports = {reafactor_step_7}

reafactor_step_7('20210831_delivery', '20210831_delivery_step7_certificate_to_contract.csv')
reafactor_step_7('20211231_3D_delivery', '20211231_3D_delivery_step7_certificate_to_contract.csv')
reafactor_step_7('20220429_SP_delivery', '20220429_SP_delivery_step7_certificate_to_contract.csv')
