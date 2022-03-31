// Adds information to map each certificate to a smart contract batch on EW Chain
// Outputs redemptions and new beneficiaries sheets for Evident / I-REC automated redemptions

// To do: check Zero Labs and other unfilled orders for starting batch number

const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const createNote = (
    networkId,
    blockchain,
    batchesContractAddress,
    batchId,
    topic,
    minerID
) => {
    return `The certificates are redeemed (= assigned to the beneficiary) for the purpose of tokenization and bridging to the Blockchain: ${
        blockchain
    } with the Network ID ${networkId}. The smart contract address is ${
        batchesContractAddress
    } and the specific certificate batch ID is ${
        batchId
    }. The certificates will be created as tokens of type ERC1888 - Topic: "${
        topic
    }" This redemption is matched to Filecoin Miner ID ${
      minerID
    }`
}

const redemptions = parse(
    fs.readFileSync("./20220318_order/redemptions.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

const new_beneficiaries = parse(
    fs.readFileSync("./20220318_order/new_beneficiaries.csv",
    {
        encoding:'utf8',
        flag:'r'
    }
), {
    columns: true,
    cast: true
});

// Constants we will use to write redemption notes
const network = 246
const tokenization_protocol = 'ZL 1.0.0'
const chain_name = 'Energy Web Chain'
const contract_address = '0x2248a8E53c8cf533AEEf2369FfF9Dc8C036c8900'
const topic = 1
const starting_batch = 1

// Build up new_beneficiaries_out to replace the old file
new_beneficiaries_out = []

// As we go through redemptions, build both redemptions.csv and new_beneficiaries.csv
for (let i = 0; i < redemptions.length; i++) {

  batch_number = i+starting_batch
  minerID = redemptions[i].name
  new_beneficiary = `Blockchain Network ID: ${network} - Tokenization Protocol: ${tokenization_protocol} - Smart Contract Address: ${contract_address} - Batch ID: ${batch_number} - Filecoin MinerID: ${minerID}`

  // Replace fields in redemptions.csv
  redemptions[i].name = new_beneficiary
  redemptions[i].redemption_purpose = createNote(network, chain_name, contract_address, batch_number, topic, minerID);

  // Replace fields in new_beneficiaries.csv
  correspondingLine = new_beneficiaries.filter(elem => elem.name == minerID)
  new_beneficiary_line = {
    'name':new_beneficiary,
    'country_name':correspondingLine[0].country_name,
    'location':correspondingLine[0].location
  }
  new_beneficiaries_out.push(new_beneficiary_line)

}

fs.writeFileSync('./20220318_order/redemptions_ewc.csv', json2csvparse.parse(redemptions));
fs.writeFileSync('./20220318_order/new_beneficiaries_ewc.csv', json2csvparse.parse(new_beneficiaries_out));
