const fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');

const createNote = (
    networkId,
    blockchain,
    batchesContractAddress,
    batchId,
    topic
) => {
    return `The certificates are redeemed (= assigned to the beneficiary) for the purpose of tokenization and bridging to the Blockchain: ${
        blockchain
    } with the Network ID ${networkId}. The smart contract address is ${
        batchesContractAddress
    } and the specific certificate batch ID is ${
        batchId
    }. The certificates will be created as tokens of type ERC1888 - Topic: "${
        topic
    }" The tokenization is done for the company: Protocol Labs.`
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

for (let i = 0; i < redemptions.length; i++) {
    redemptions[i].notes = createNote(246, 'Energy Web Chain', '0x2248a8E53c8cf533AEEf2369FfF9Dc8C036c8900', i + 1, 1);
}

fs.writeFileSync('./20220318_order/redemptions.csv', json2csvparse.parse(redemptions));
