import fs from 'fs'
import Papa from 'papaparse'
import { globby } from 'globby'
import moment from 'moment'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/csv-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Check for provided parameters
const args = process.argv.slice(2)
const activities = args[0]
let filePath
let jsonContent
let attestationFolder, transactionFolder
let attestationFolderChunks, attestationFolderName
const step2FileNameSuffix = "_step2_orderSupply.csv"
const step3FileNameSuffix = "_step3_match.csv"
const step5FileNameSuffix = "_step5_redemption_information.csv"
const step6FileNameSuffix = "_step6_generationRecords.csv"
const step7FileNameSuffix = "_step7_certificate_to_contract.csv"

switch (activities) {
    case 'fix-dates':
        filePath = args[1]
        const dateColumns = args[2]
        if(dateColumns == null) {
            console.error(`Error! Bad argument provided. Date columns is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }
        jsonContent = await getCsvAndParseToJson(filePath)
        const fixedDatesCsv = await fixDates(jsonContent, dateColumns)   // comma separated list of date columns

        // Bakup existing file
        await fs.promises.rename(filePath, `${filePath}.bak-${(new Date()).toISOString()}`)

        // Create new file
        await fs.promises.writeFile(filePath, fixedDatesCsv)

        break;
    case 'fix-non-floating-numbers':
        filePath = args[1]
        const numbersColumns = args[2]
        let base = args[3]
        if(numbersColumns == null) {
            console.error(`Error! Bad argument provided. Numbers columns is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }
        base = parseInt(base, 10)
        if(isNaN(base)) {
            console.error(`Error! Bad argument provided. Provided base is not a number.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }
        jsonContent = await getCsvAndParseToJson(filePath)
        const fixedNonFloatingNumbersCsv = await fixNonFloatingNumbers(jsonContent, numbersColumns, base)   // comma separated list of numbers columns

        // Bakup existing file
        await fs.promises.rename(filePath, `${filePath}.bak-${(new Date()).toISOString()}`)

        // Create new file
        await fs.promises.writeFile(filePath, fixedNonFloatingNumbersCsv)

        break;
    case 'create-step-5':
        attestationFolder = args[1]
        transactionFolder = args[2]

        const transactionFolderPathChunks = transactionFolder.split("/")
        const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]
    
        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }

        // Create step 5 CSV
        const step5Csv = await createStep5(attestationFolder, transactionFolder)

        // Create new file
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`, step5Csv)

        break;
    case 'create-step-6-3d':
        attestationFolder = args[1]
        transactionFolder = args[2]
        step6FileNameSuffix = "_step6_generationRecords.csv"

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }

        // Create step 6 CSV
        const step6Csv = await createStep63D(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)            
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break;
    case 'create-step-7-3d':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }

        // Create step 7 CSV
        const step7Csv = await createStep73D(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)            
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, step7Csv)

        break;
    default:
        console.error(`Error! Bad argument provided. ${activities} are not supported.`)
}

await new Promise(resolve => setTimeout(resolve, 1000));
process.exit()

// Grabs CSV file and parse it as JSON
async function getCsvAndParseToJson(filePath) {
    try {
        const data = await fs.promises.readFile(filePath, {
            encoding:'utf8',
            flag:'r'
        })

        let rows = []
        return new Promise((resolve) => {
            Papa.parse(data, {
                worker: true,
                header: true,
                dynamicTyping: true,
                comments: "#",
                step: (row) => {
                    rows.push(row.data)
                },
                complete: () => {
                    resolve(rows)
                }
            })
        })
    }
    catch (err) {
        return new Promise((resolve) => {
            console.error(`Error! Can't open ${filePath}. ${err}`)
            resolve(null)
        })
    }
}

// Fix wrong dates in provided CSV files for specified columns
async function fixDates(jsonContent, dateColumns) {
    let columns = dateColumns.trim().split(",")
    if(!columns.length)
        return new Promise((resolve) => {
            console.info(`Nothing to fix here. No date columns provided.`)
            resolve(null)
        })

    // Trim column names for eventual spaces
    columns = columns.map((col) => {return col.trim()})

    // Check dates
    let line = 1
    for (const item of jsonContent) {
        for (const column of columns) {
            if(!isValidDate(item[column])) {
                console.log(`${item[column]} in ${column} at line ${line} is not a valid date`)
                item[column] = guessValidDate(item[column])
                console.log(`Date in ${column} at line ${line} is set to ${item[column]}`)
            }
        }
        line++
    }

    // Set valid Papa unparse config
    let header = getHeaderAndColumnTypes(jsonContent).header
    const columnTypes = getHeaderAndColumnTypes(jsonContent).types
    if(columnTypes == null)
        return new Promise((resolve) => {
            console.info(`Can not determine CSV column types for Papa unparse.`)
            resolve(null)
        })

    let result = header.join(",") + "\r\n" +
        Papa.unparse(jsonContent, {
            quotes: columnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })

    return new Promise((resolve) => {
        resolve(result)
    })
}

function isValidDate(date) {
    const dateChunks = date.split("-")
    const year = dateChunks[0]
    const month = dateChunks[1]
    const day = dateChunks[2]
    const dateObj = new Date(date)
    return (dateObj !== "Invalid Date") && !isNaN(dateObj) &&
        (dateObj.getFullYear() == year && dateObj.getMonth()+1 == month && dateObj.getDate() == day);
}

function guessValidDate(date) {
    const dateChunks = date.split("-")
    const year = dateChunks[0]
    const month = dateChunks[1]
    const day = dateChunks[2]
    if((month == 4 || month == 6 || month == 9 || month == 11) && day > 30)
        return `${year}-${month}-30`
    if(month == 2 && year%4 == 0) {
        return `${year}-${month}-29`
    }
    else if(year%4 !== 0) {
        return `${year}-${month}-28`
    }
    else {
        console.log(`I can't make the best guess for ${date}`)
        return date
    }
}

// Fix wrongly formatted numbers in provided CSV files for specified columns
async function fixNonFloatingNumbers(jsonContent, numbersColumns, base) {
    let columns = numbersColumns.trim().split(",")
    if(!columns.length)
        return new Promise((resolve) => {
            console.info(`Nothing to fix here. No integer columns provided.`)
            resolve(null)
        })

    // Trim column names for eventual spaces
    columns = columns.map((col) => {return col.trim()})

    // Check dates
    let line = 1
    for (const item of jsonContent) {
        for (const column of columns) {
            if(isNaN(item[column])) {
                console.log(`${item[column]} in ${column} at line ${line} is not a valid number`)
                item[column] = item[column].trim().replace(",","")
                item[column] = parseInt(item[column], base)
                console.log(`Date in ${column} at line ${line} is set to ${item[column]}`)
            }
        }
        line++
    }
    
    // Set valid Papa unparse config
    let header = getHeaderAndColumnTypes(jsonContent).header
    const columnTypes = getHeaderAndColumnTypes(jsonContent).types
    if(columnTypes == null)
        return new Promise((resolve) => {
            console.info(`Can not determine CSV column types for Papa unparse.`)
            resolve(null)
        })

    let result = header.join(",") + "\r\n" +
        Papa.unparse(jsonContent, {
            quotes: columnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })

    return new Promise((resolve) => {
        resolve(result)
    })
}

// Extract the header and column types from JSON
function getHeaderAndColumnTypes(jsonArray) {
    // Check if provided object is array
    if(!Array.isArray(jsonArray) || !jsonArray.length)
        return {
            header: null,
            types: null
        }

    // Extract header and column types from first row/object
    const first = jsonArray[0]
    const header = Object.keys(first)
    let types = []
    for (let index = 0; index < header.length; index++) {
        types.push(typeof first[header[index]])
    }

    return {
        header: header.map((h) => {return '"' + h + '"'}),
        types: types
    }
}

// Create step 5
async function createStep5(attestationFolder, transactionFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step5Header = ['"attestation_id"', '"smart_contract_address"', '"batchID"', '"network"',
        '"zl_protocol_version"', '"minerID"', '"beneficiary"', '"redemption_purpose"', '"attestation_folder"']
    const step5ColumnTypes = ["string", "string", "number", "number",
        "string", "string", "string", "string", "string"]
    
    let step5 = []
    
    const pdfs = await globby(`${attestationFolder}/*.pdf`)
    
    for (let index = 1; index <= pdfs.length; index++) {
        step5.push({
            attestation_id: `${transactionFolderName}_attestation_${index}`,
            smart_contract_address: null,
            batchID: null,
            network: 246,
            zl_protocol_version: null,
            minerID: null,
            beneficiary: null,
            redemption_purpose: null,
            attestation_folder: attestationFolderName
        })
    }

    let result = step5Header.join(",") + "\r\n" +
        Papa.unparse(step5, {
            quotes: step5ColumnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })

    return new Promise((resolve) => {
        resolve(result)
    })
}

// Create step 6, 3D
async function createStep63D(attestationFolder, transactionFolder) {
    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step6Header = ['"attestation_id"', '"attestation_file"', '"attestation_cid"', '"certificate"',
        '"certificate_cid"', '"reportingStart"', '"reportingStartTimezoneOffset"', '"reportingEnd"', '"reportingEndTimezoneOffset"',
        '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_Wh"', '"generatorName"', '"productType"',
        '"energySource"', '"generationStart"', '"generationStartTimezoneOffset"', '"generationEnd"', '"generationEndTimezoneOffset"']
    const step6ColumnTypes = ["string", "string", "string", "string",
        "string", "string", "number", "string", "number",
        "string", "string", "string", "string", "number", "string", "string",
        "string", "string", "number", "string", "number"]
    
    let step6 = []
    
    const pdfs = await globby(`${attestationFolder}/*.pdf`)
    
    let attestationIndex = 1
    for (const pdf of pdfs) {
        const pdfPathChunks = pdf.split("/")
        const pdfName = pdfPathChunks[pdfPathChunks.length-1]
        const csvName = pdfName.replace(".pdf", ".csv")

        const certificates = await getCsvAndParseToJson(`${attestationFolder}/${csvName}`)

        let certificateIndex = 1
        for (const certificate of certificates) {
            step6.push({
                attestation_id: `${transactionFolderName}_attestation_${attestationIndex}`,
                attestation_file: pdfName,
                attestation_cid: null,
                certificate: pdfName.replace(".pdf",`_certificate_${certificateIndex}`),
                certificate_cid: null,
                reportingStart: certificate.reportingStart,
                reportingStartTimezoneOffset: certificate.reportingStartTimezoneOffset,
                reportingEnd: certificate.reportingEnd,
                reportingEndTimezoneOffset: certificate.reportingEndTimezoneOffset,
                sellerName: certificate.sellerName,
                sellerAddress: certificate.sellerAddress,
                country: certificate.country,
                region: certificate.region,
                volume_Wh: certificate.volume_Wh * 1000000, // incorrect values in 3D CSVs (MWhs values)
                generatorName: certificate.generatorName,
                productType: certificate.productType,
                energySource: certificate.energySource,
                generationStart: certificate.generationStart,
                generationStartTimezoneOffset: certificate.generationStartTimezoneOffset,
                generationEnd: certificate.generationEnd,
                generationEndTimezoneOffset: certificate.generationEndTimezoneOffset
            })
            certificateIndex++
        }
        attestationIndex++
    }

    let result = step6Header.join(",") + "\r\n" +
        Papa.unparse(step6, {
            quotes: step6ColumnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })

    return new Promise((resolve) => {
        resolve(result)
    })
}

// Create step 7, 3D
async function createStep73D(attestationFolder, transactionFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step7Header = ['"certificate"', '"volume_MWh"', '"order_folder"', '"contract"', '"minerID"']
    const step7ColumnTypes = ["string", "number", "string", "string", "string"]
    
    let step7 = []
    
    // Grab step3, 2 and 6 CSVs
    const step2 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    const step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)
    const step6 = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)

    // Traverse allocations
    for (const allocation of step3) {
        const contractId = allocation.contract_id
        const minerID = allocation.minerID
        let volumeMWh = allocation.volume_MWh

        // Find contract
        let contract = step2.filter((c) => {return c.contract_id == contractId})
        if(!contract.length) {
            console.error(`Can't find ${contractId} in ${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
            continue
        }
        contract = contract[0]
        const reportingStart = moment(contract.reportingStart, "YYYY-MM-DD")
        const reportingEnd = moment(contract.reportingEnd, "YYYY-MM-DD")

        // Try matching generation records
        let matches = step6.filter((cert) => {
            const generationStart = moment(cert.generationStart, "YYYY-MM-DD")
            const generationEnd = moment(cert.generationEnd, "YYYY-MM-DD")
//            const matchingDateRange = generationStart.isSameOrAfter(reportingStart)
//                && generationEnd.isSameOrBefore(reportingEnd)
            const matchingDateRange = generationStart.isSameOrBefore(reportingEnd) &&
                generationEnd.isSameOrAfter(reportingStart)
            return cert.country == contract.country &&
                ((cert.region == "US") ? (cert.region == contract.region) : true) &&
//                cert.productType == contract.productType &&
                matchingDateRange &&
                cert.volume_Wh > 0
        })

        if(!matches.length) {
            // Didn't find any matches for allocation
//            console.error(`Found nothing matching for ${allocation.allocation_id} (${contract.country}, ${contract.region}, ${contract.reportingStart} - ${contract.reportingEnd})`)
        }
        else if(matches.length > 1) {
            // Found several matches for allocation
            for (const certificate of matches) {
                if(volumeMWh > 0 && volumeMWh <= certificate.volume_Wh/1000000) {
                    step7.push({
                        certificate: certificate.certificate,
                        volume_MWh: volumeMWh,
                        order_folder: transactionFolderName,
                        contract: contractId,
                        minerID: minerID
                    })
                    
                    // Deduct from available RECs
                    certificate.volume_Wh -= volumeMWh * 1000000

                    // No need to look further
                    volumeMWh = 0
                    break
                }
                else if(volumeMWh > 0) {
                    // Take what is available
                    step7.push({
                        certificate: certificate.certificate,
                        volume_MWh: certificate.volume_Wh/1000000,
                        order_folder: transactionFolderName,
                        contract: contractId,
                        minerID: minerID
                    })
                    
                    // Deduct from needed RECs
                    volumeMWh -= certificate.volume_Wh/1000000

                    // Deduct from available RECs
                    certificate.volume_Wh = 0
                }
            }
        }
        else {
            // Found exactly 1 match for allocation
            const certificate = matches[0]
            if(volumeMWh> 0 && volumeMWh <= certificate.volume_Wh/1000000) {
                step7.push({
                    certificate: certificate.certificate,
                    volume_MWh: volumeMWh,
                    order_folder: transactionFolderName,
                    contract: contractId,
                    minerID: minerID
                })
                
                // Deduct from available RECs
                certificate.volume_Wh -= volumeMWh * 1000000

                volumeMWh = 0
            }
            else if(volumeMWh > 0) {
                // Take what is available
                step7.push({
                    certificate: certificate.certificate,
                    volume_MWh: certificate.volume_Wh/1000000,
                    order_folder: transactionFolderName,
                    contract: contractId,
                    minerID: minerID
                })

                // This remains as missing
                volumeMWh -= certificate.volume_Wh/1000000

                // Deduct from available RECs
                certificate.volume_Wh = 0
            }
        }

        // Log if we have some RECs missing
        if(volumeMWh > 0) {
            console.error(`Missing ${volumeMWh} needed for ${allocation.allocation_id} (${contract.country}, ${contract.region}, ${contract.reportingStart} - ${contract.reportingEnd})`)
        }
    }

    // Calculate what remained unspent
    const remained = step6.filter((cert) => {return cert.volume_Wh > 0})
    console.info(`\r\nRemained:`)
    for (const r of remained) {
        console.info(`${r.certificate}: ${r.volume_Wh / 1000000} (${r.country}, ${r.region}, ${r.generationStart} - ${r.generationEnd})`)
    }

    let result = step7Header.join(",") + "\r\n" +
        Papa.unparse(step7, {
            quotes: step7ColumnTypes.map((ct) => {return ct != 'number'}),
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ",",
            header: false,
            newline: "\r\n",
            skipEmptyLines: false,
            columns: null
        })

    return new Promise((resolve) => {
        resolve(result)
    })
}
