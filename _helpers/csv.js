import fs from 'fs'
import { create as createClient } from 'ipfs-http-client'
import Papa from 'papaparse'
import { globby } from 'globby'
import moment from 'moment'
import axios from 'axios'
import cat from 'countries-and-timezones'
import all from 'it-all'
import HL from './leaflet-headless.cjs'
import { read as xlsxRead, utils as xlsxUtils, write as xlsxWrite, writeFile as xlsxWriteFile, set_fs as xlsxSetFS } from "xlsx/xlsx.mjs"
import { delimiter } from 'path'
xlsxSetFS(fs)

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/csv-${(new Date()).toISOString()}.log`)
process.stdout.write = process.stderr.write = access.write.bind(access)

// Check for provided parameters
const args = process.argv.slice(2)
const activities = args[0]
let filePath
let jsonContent
let attestationFolder, transactionFolder, purchaseOrderFolder
let attestationFolderChunks, attestationFolderName
let transactionFolderChunks, transactionFolderName
let assetsFolder, assetsFolderChunks, assetsFolderName
let minersLocationsFile
let purchaseOrderFolderChunks, purchaseOrderFolderName
let nercGeoJsonFile
let nercRegionsMappingFile
let externalBatchesFile

const step2FileNameSuffix = "_step2_orderSupply.csv"
const step3FileNameSuffix = "_step3_match.csv"
const step5FileNameSuffix = "_step5_redemption_information.csv"
const step5AutomaticFileNameSuffix = "_step5_redemption_information_automatic.csv"
const step5ManualFileNameSuffix = "_step5_redemption_information_manual.csv"
const step6FileNameSuffix = "_step6_generationRecords.csv"
const step7FileNameSuffix = "_step7_certificate_to_contract.csv"
const gridRegionsFileName = "grid_regions"
let step2Csv, step3Csv, step5Csv, step6Csv, step7Csv, purchaseOrderCsv

switch (activities) {
    case 'fix-dates':
        filePath = args[1]
        const dateColumns = args[2]
        const dateFormat = args[3]
        if(dateColumns == null) {
            console.error(`Error! Bad argument provided. Date columns is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }
        jsonContent = await getCsvAndParseToJson(filePath)
        const fixedDatesCsv = await fixDates(jsonContent, dateColumns, dateFormat)   // comma separated list of date columns

        // Bakup existing file
        await fs.promises.rename(filePath, `${filePath}.bak-${(new Date()).toISOString()}`)

        // Create new file
        await fs.promises.writeFile(filePath, fixedDatesCsv)

        break
    case 'fix-non-floating-numbers':
        filePath = args[1]
        const numbersColumns = args[2]
        let base = args[3]
        if(numbersColumns == null) {
            console.error(`Error! Bad argument provided. Numbers columns is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }
        base = parseInt(base, 10)
        if(isNaN(base)) {
            console.error(`Error! Bad argument provided. Provided base is not a number.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }
        jsonContent = await getCsvAndParseToJson(filePath)
        const fixedNonFloatingNumbersCsv = await fixNonFloatingNumbers(jsonContent, numbersColumns, base)   // comma separated list of numbers columns

        // Bakup existing file
        await fs.promises.rename(filePath, `${filePath}.bak-${(new Date()).toISOString()}`)

        // Create new file
        await fs.promises.writeFile(filePath, fixedNonFloatingNumbersCsv)

        break
    case 'create-step-5-old':
        attestationFolder = args[1]
        transactionFolder = args[2]

        transactionFolderChunks = transactionFolder.split("/")
        transactionFolderName = transactionFolderChunks[transactionFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 5 CSV
        step5Csv = await createStep5Old(attestationFolder, transactionFolder)

        // Create new file
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`, step5Csv)

        break
    case 'unquote-step-6-numeric-fields':
        attestationFolder = args[1]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null) {
            console.error(`Error! Bad arguments provided. Attestation folder path is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Step 6 CSV
        step6Csv = await unquoteStep6NumericFields(attestationFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break
    case 'create-step-6-3d':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 6 CSV
        step6Csv = await createStep63D(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break
    case 'add-timezone-offsets':
        attestationFolder = args[1]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null) {
            console.error(`Error! Bad arguments provided. Attestation folder path is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 6 CSV
        step6Csv = await addTimezoneOffsets(attestationFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break
    case 'add-label':
        attestationFolder = args[1]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null) {
            console.error(`Error! Bad arguments provided. Attestation folder path is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 6 CSV
        step6Csv = await addLabel(attestationFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break
    case 'create-step-7-3d':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 7 CSV
        step7Csv = await createStep73D(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, step7Csv)

        break
    case 'create-step-7-3d-multistep':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 7 CSV
        step7Csv = await createStep73Dmultistep(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, step7Csv)

        break
    case 'rename-attestations-sp':
        attestationFolder = args[1]

        if(attestationFolder == null) {
            console.error(`Error! Bad arguments provided. Attestation folder path is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        await renameAttestationsSP(attestationFolder)

        break
    case 'fix-step-3-sp':
        transactionFolder = args[1]

        transactionFolderChunks = transactionFolder.split("/")
        transactionFolderName = transactionFolderChunks[transactionFolderChunks.length-1]

        if(transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Transaction folder path is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Fix step 3 CSV
        step3Csv = await createStep3SP(transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`, step3Csv)

        break
    case 'create-step-6-sp':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 6 CSV
        step6Csv = await createStep6SP(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`, step6Csv)

        break
    case 'create-step-7-sp':
        attestationFolder = args[1]
        transactionFolder = args[2]

        attestationFolderChunks = attestationFolder.split("/")
        attestationFolderName = attestationFolderChunks[attestationFolderChunks.length-1]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 7 CSV
        step7Csv = await createStep7SP(attestationFolder, transactionFolder)

        try {
            // Bakup existing file
            await fs.promises.rename(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, `${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`, step7Csv)

        break
    case 'list-non-matching-dates':
        attestationFolder = args[1]
        transactionFolder = args[2]

        if(attestationFolder == null || transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Both, attestation folder and transaction folder paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Log non matching records
        await listNonMatchingDates3D(attestationFolder, transactionFolder)

        break
    case 'create-step-3':
        transactionFolder = args[1]
        minersLocationsFile = args[2]
        const priorityMinersFiles = args[3]
        const gridMinersSplitFile = args[4]
        let recsMultFactor = args[5]
        let contractConsumption = args[6]
        let maxRadius = args[7]
        let radiusIncrementStep = args[8]

        if(recsMultFactor == undefined)
            recsMultFactor = 1.5
        else
            recsMultFactor = parseFloat(recsMultFactor)

        if(contractConsumption == undefined)
            contractConsumption = 1
        else
            contractConsumption = parseFloat(contractConsumption)

        if(maxRadius == undefined)
            maxRadius = 250
        else
            maxRadius = parseInt(maxRadius, 10)

        if(radiusIncrementStep == undefined)
            radiusIncrementStep = 250
        else
            radiusIncrementStep = parseInt(radiusIncrementStep, 10)

        transactionFolderChunks = transactionFolder.split("/")
        transactionFolderName = transactionFolderChunks[transactionFolderChunks.length-1]

        if(transactionFolder == null || minersLocationsFile == null || priorityMinersFiles == null || gridMinersSplitFile == null) {
            console.error(`Error! Bad arguments provided. Transaction folder, and miners locations, priority miners list and grid split file paths are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 3 CSV and update step 2 CSV
        const createStep3Response = await createStep3(transactionFolder, minersLocationsFile, priorityMinersFiles, gridMinersSplitFile, recsMultFactor, contractConsumption, maxRadius, radiusIncrementStep)
        if(createStep3Response == null) {
            console.error(`Error! Could not create step 3 CSV.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        step2Csv = createStep3Response.step2
        step3Csv = createStep3Response.step3

        try {
            // Bakup existing files
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}.bak-${(new Date()).toISOString()}`)
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new files
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`, step2Csv)
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`, step3Csv)

        break
    case 'create-step-5':
        transactionFolder = args[1]
        attestationFolder = args[2]
        const network = args[3]
        const networkId = args[4]
        const tokenizationProtocol = args[5]
        const tokenType = args[6]
        const smartContractAddress = args[7]
        const format = args[8]
        const redemptionProcess = args[9]
        minersLocationsFile = args[10]
        externalBatchesFile = args[11]
        const evidentRedemptionFileName = args[12]
        const redemptionsSheetName = args[13]
        const beneficiariesSheetName = args[14]
        const batchId = args[15]

        transactionFolderChunks = transactionFolder.split("/")
        transactionFolderName = transactionFolderChunks[transactionFolderChunks.length-1]

        if(transactionFolder == null || attestationFolder == null || network == null || networkId == null || tokenizationProtocol == null || tokenType == null || smartContractAddress == null || format == null) {
            console.error(`Error! Bad arguments provided. Transaction folder, attestation folder, Network, Network Id, Tokenization protocol, Token type, Smart contract address, and format are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create step 5 CSV
        step5Csv = await createStep5(transactionFolder, attestationFolder, network, networkId, tokenizationProtocol, tokenType,
            smartContractAddress, format, redemptionProcess, minersLocationsFile,
            externalBatchesFile, evidentRedemptionFileName, redemptionsSheetName, beneficiariesSheetName,
            batchId)
        if(step5Csv == null) {
            console.error(`Error! Could not create step 5 CSV.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        try {
            // Bakup existing file
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`, step5Csv)

        break
    case 'split-step-5':
        transactionFolder = args[1]

        transactionFolderChunks = transactionFolder.split("/")
        transactionFolderName = transactionFolderChunks[transactionFolderChunks.length-1]

        if(transactionFolder == null) {
            console.error(`Error! Bad arguments provided. Transaction folder is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Split step 5 CSV
        const step5Csvs = await splitStep5(transactionFolder)
        if(step5Csvs == null) {
            console.error(`Error! Could not split step 5 CSV.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        try {
            // Bakup existing files
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step5AutomaticFileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step5AutomaticFileNameSuffix}.bak-${(new Date()).toISOString()}`)
            await fs.promises.rename(`${transactionFolder}/${transactionFolderName}${step5ManualFileNameSuffix}`, `${transactionFolder}/${transactionFolderName}${step5ManualFileNameSuffix}.bak-${(new Date()).toISOString()}`)
        }
        catch (error) {
            console.log(error)
        }

        // Create new files
        if(step5Csvs['automatic'] != null)
            await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step5AutomaticFileNameSuffix}`, step5Csvs['automatic'])    
        if(step5Csvs['manual'] != null)
            await fs.promises.writeFile(`${transactionFolder}/${transactionFolderName}${step5ManualFileNameSuffix}`, step5Csvs['manual'])

        break
    case 'create-purchase-order':
        purchaseOrderFolder = args[1]
        minersLocationsFile = args[2]
        nercGeoJsonFile = args[3]
        const fromYear = args[4]
        const fromQuarter = args[5]
        const energyFactor = args[6]
        nercRegionsMappingFile = args[7]

        purchaseOrderFolderChunks = purchaseOrderFolder.split("/")
        purchaseOrderFolderName = purchaseOrderFolderChunks[purchaseOrderFolderChunks.length-1]

        if(purchaseOrderFolder == null || minersLocationsFile == null || nercGeoJsonFile == null
            || fromYear == null || fromQuarter == null || energyFactor == null) {
            console.error(`Error! Bad arguments provided. Purchase order folder, Miners location file, NERC GeoJSON file, from year and quarter, and energy factor are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create purchase order CSV
        purchaseOrderCsv = await createPurchaseOrder(purchaseOrderFolder, minersLocationsFile, nercGeoJsonFile,
            fromYear, fromQuarter, energyFactor, nercRegionsMappingFile)
        if(purchaseOrderCsv == null) {
            console.error(`Error! Could not create purchase order CSV.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        try {
            // Bakup existing file
            await fs.promises.rename(`${purchaseOrderFolder}/${purchaseOrderFolderName}.csv`, `${purchaseOrderFolder}/${purchaseOrderFolderName}.bak-${(new Date()).toISOString()}.csv`)
        }
        catch (error) {
//            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${purchaseOrderFolder}/${purchaseOrderFolderName}.csv`, purchaseOrderCsv)

        break
    case 'create-grid-regions':
        assetsFolder = args[1]
        minersLocationsFile = args[2]
        nercGeoJsonFile = args[3]
        nercRegionsMappingFile = args[4]

        assetsFolderChunks = assetsFolder.split("/")
        assetsFolderName = assetsFolderChunks[assetsFolderChunks.length-1]

        if(assetsFolder == null || minersLocationsFile == null || nercGeoJsonFile == null) {
            console.error(`Error! Bad arguments provided. Assets folder, Miners location file and NERC GeoJSON file are required parameters.`)
            await new Promise(resolve => setTimeout(resolve, 100))
            process.exit()
        }

        // Create purchase order CSV
        const gridRegions = await createGridRegions(assetsFolder, minersLocationsFile, nercGeoJsonFile, nercRegionsMappingFile)

        try {
            // Bakup existing file
            await fs.promises.rename(`${assetsFolder}/${gridRegionsFileName}.json`, `${assetsFolder}/${gridRegionsFileName}.bak-${(new Date()).toISOString()}.json`)
        }
        catch (error) {
//            console.log(error)
        }

        // Create new file
        await fs.promises.writeFile(`${assetsFolder}/${gridRegionsFileName}.json`, gridRegions)

        break
    default:
        console.error(`Error! Bad argument provided. ${activities} are not supported.`)
}

await new Promise(resolve => setTimeout(resolve, 1000))
process.exit()

// Generate random HEX
function genRandomHex(size) {
    return [...Array(size)].map(() => Math.floor(Math.random() * 16).toString(16)).join('')
}

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
async function fixDates(jsonContent, dateColumns, dateFormat) {
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
            const mdate = moment(item[column], dateFormat)
            item[column] = mdate.format('YYYY-MM-DD')
            console.log(`Date in ${column} at line ${line} is set to ${item[column]}`)
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
async function createStep5Old(attestationFolder, transactionFolder) {
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

// Add time zone offsets
async function addTimezoneOffsets(attestationFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const step6Header = ['"attestation_id"', '"attestation_file"', '"attestation_cid"', '"certificate"',
        '"certificate_cid"', '"reportingStart"', '"reportingStartTimezoneOffset"', '"reportingEnd"', '"reportingEndTimezoneOffset"',
        '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_Wh"', '"generatorName"', '"productType"',
        '"energySource"', '"generationStart"', '"generationStartTimezoneOffset"', '"generationEnd"', '"generationEndTimezoneOffset"']
    const step6ColumnTypes = ["string", "string", "string", "string",
        "string", "string", "number", "string", "number",
        "string", "string", "string", "string", "number", "string", "string",
        "string", "string", "number", "string", "number"]

    let certificates = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)
    for (let certificate of certificates) {
        const timezones = cat.getTimezonesForCountry(certificate.country)
        if(!timezones.length)
            continue

        let timezone
        if(certificate.country == "US") {
            switch (certificate.region) {
                case "WECC":
                    timezone = timezones.filter((tz) => {return tz.name == 'America/Los_Angeles'})[0]
                    break
                case "NPCC":
                case "RFC":
                case "SERC":
                    timezone = timezones.filter((tz) => {return tz.name == 'America/New_York'})[0]
                    break
                case "MRO":
                case "TRE":
                    timezone = timezones.filter((tz) => {return tz.name == 'America/Denver'})[0]
                    break
                default:
                    break
            }
            timezone = timezones[0]
        }
        else {
            timezone = timezones[0]
        }
        const generationEnd = moment(certificate.generationEnd, "YYYY-MM-DD")
        const dstStart = moment(`${generationEnd.year()}-03-15`, "YYYY-MM-DD")
        const dstEnd = moment(`${generationEnd.year()}-11-06`, "YYYY-MM-DD")
        const offset = (generationEnd.isSameOrAfter(dstStart) && generationEnd.isSameOrBefore(dstEnd))
            ? timezone.dstOffset/60 : timezone.utcOffset/60

        // Set time zone offsets
        certificate.generationStartTimezoneOffset = offset
        certificate.generationEndTimezoneOffset = offset
        certificate.reportingStartTimezoneOffset = offset
        certificate.reportingEndTimezoneOffset = offset
    }

    let result = step6Header.join(",") + "\r\n" +
        Papa.unparse(certificates, {
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

// Add label
async function addLabel(attestationFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const step6Header = ['"attestation_id"', '"attestation_file"', '"attestation_cid"', '"certificate"',
        '"certificate_cid"', '"reportingStart"', '"reportingStartTimezoneOffset"', '"reportingEnd"', '"reportingEndTimezoneOffset"',
        '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_Wh"', '"generatorName"', '"productType"', '"label"',
        '"energySource"', '"generationStart"', '"generationStartTimezoneOffset"', '"generationEnd"', '"generationEndTimezoneOffset"']
    const step6ColumnTypes = ["string", "string", "string", "string",
        "string", "string", "number", "string", "number",
        "string", "string", "string", "string", "number", "string", "string", "string",
        "string", "string", "number", "string", "number"]

    let step6 = []

    let certificates = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)
    for (let certificate of certificates) {
        step6.push({
            attestation_id: certificate.attestation_id,
            attestation_file: certificate.attestation_file,
            attestation_cid: certificate.attestation_cid,
            certificate: certificate.certificate,
            certificate_cid: certificate.certificate_cid,
            reportingStart: certificate.reportingStart,
            reportingStartTimezoneOffset: certificate.reportingStartTimezoneOffset,
            reportingEnd: certificate.reportingEnd,
            reportingEndTimezoneOffset: certificate.reportingEndTimezoneOffset,
            sellerName: certificate.sellerName,
            sellerAddress: certificate.sellerAddress,
            country: certificate.country,
            region: certificate.region,
            volume_Wh: certificate.volume_Wh,
            generatorName: certificate.generatorName,
            productType: (certificate.productType.toLowerCase() == "green-e") ? "REC" : certificate.productType,
            label: (certificate.productType.toLowerCase() == "green-e") ? "GREEN_E_ENERGY" : "",
            energySource: certificate.energySource,
            generationStart: certificate.generationStart,
            generationStartTimezoneOffset: certificate.generationStartTimezoneOffset,
            generationEnd: certificate.generationEnd,
            generationEndTimezoneOffset: certificate.generationEndTimezoneOffset
        })
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

// Rename attestation files, SP
async function renameAttestationsSP(attestationFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const pdfs = await globby(`${attestationFolder}/*.pdf`)

    for (const pdf of pdfs) {
        const attestationFilePathChunks = pdf.split("/")
        const attestationFileName = attestationFilePathChunks[attestationFilePathChunks.length-1]
        const attestationFileNameChunks = attestationFileName.split(".")
        attestationFileNameChunks.pop()
        const attestationFileBaseName = attestationFileNameChunks.join(".")
        try {
            // Rename file
            const attestationFileName = `${attestationFolderName}_${parseInt(attestationFileBaseName, 10) - 2}`
            await fs.promises.rename(pdf, `${attestationFolder}/${attestationFileName}.pdf`)
        }
        catch (error) {
            console.log(error)
        }
    }

    return true
}

// Fix step 3, SP
async function fixStep3SP(transactionFolder) {
    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step3Header = ['"allocation_id"', '"UUID"', '"contract_id"', '"minerID"', '"volume_MWh"', '"defaulted"',
        '"step4_ZL_contract_complete"', '"step5_redemption_data_complete"', '"step6_attestation_info_complete"',
        '"step7_certificates_matched_to_supply"', '"step8_IPLDrecord_complete"', '"step9_transaction_complete"',
        '"step10_volta_complete"', '"step11_finalRecord_complete"']
    const step3ColumnTypes = ["string", "string", "string", "string", "number", "number",
        "number", "number", "number",
        "number", "number", "number",
        "number", "number"]

    const allocations = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)

    let step3 = []
    let allocationIndex = 1

    for (const allocation of allocations) {
        step3.push({
            allocation_id: `${transactionFolderName}_allocation_${allocationIndex}`,
            UUID: allocation.uuid,
            contract_id: allocation.contract_id,
            minerID: allocation.minerID,
            volume_MWh: allocation.volume_MWh,
            defaulted: allocation.defaulted,
            step4_ZL_contract_complete: allocation.step4_ZL_contract_complete,
            step5_redemption_data_complete: allocation.step5_redemption_data_complete,
            step6_attestation_info_complete: allocation.step6_attestation_info_complete,
            step7_certificates_matched_to_supply: allocation.step7_certificates_matched_to_supply,
            step8_IPLDrecord_complete: allocation.step8_IPLDrecord_complete,
            step9_transaction_complete: allocation.step9_transaction_complete,
            step10_volta_complete: allocation.step10_volta_complete,
            step11_finalRecord_complete: allocation.step11_finalRecord_complete
        })
        allocationIndex++
    }

    let result = step3Header.join(",") + "\r\n" +
        Papa.unparse(step3, {
            quotes: step3ColumnTypes.map((ct) => {return ct != 'number'}),
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

// Create step 6, SP
async function createStep6SP(attestationFolder, transactionFolder) {
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
    const step2 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    const step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)

    for (const pdf of pdfs) {
        const pdfPathChunks = pdf.split("/")
        const pdfName = pdfPathChunks[pdfPathChunks.length-1]
        const pdfNameChunks = pdfName.replace(".pdf", "").split("_")
        const attestationIndex = pdfNameChunks[pdfNameChunks.length-1]
        const allocationId = `${transactionFolderName}_allocation_${attestationIndex}`
        const allocation = step3.filter((a) => {return a.allocation_id == allocationId})[0]
        const contract = step2.filter((c) => {return c.contract_id == allocation.contract_id})[0]

        step6.push({
            attestation_id: `${transactionFolderName}_attestation_${attestationIndex}`,
            attestation_file: pdfName,
            attestation_cid: null,
            certificate: pdfName.replace(".pdf",`_certificate_${attestationIndex}`),
            certificate_cid: null,
            reportingStart: contract.reportingStart,
            reportingStartTimezoneOffset: null,
            reportingEnd: contract.reportingEnd,
            reportingEndTimezoneOffset: null,
            sellerName: contract.sellerName,
            sellerAddress: contract.sellerAddress,
            country: contract.country,
            region: contract.region,
            volume_Wh: allocation.volume_MWh * 1000000,
            generatorName: "Longli Jia Tuo Mountain Windfarm",
            productType: contract.productType,
            energySource: contract.energySources,
            generationStart: contract.reportingStart,
            generationStartTimezoneOffset: null,
            generationEnd: contract.reportingEnd,
            generationEndTimezoneOffset: null
        })
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

// Create step 7, SP
async function createStep7SP(attestationFolder, transactionFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step7Header = ['"certificate"', '"volume_MWh"', '"order_folder"', '"contract"', '"minerID"']
    const step7ColumnTypes = ["string", "number", "string", "string", "string"]

    let step7 = []

    // Grab 3 CSV
    const step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)

    for (const allocation of step3) {
        const allocationId = allocation.allocation_id
        const allocationIdChunks = allocationId.split("_")
        const allocationIndex = allocationIdChunks[allocationIdChunks.length-1]
        step7.push({
            certificate: `${attestationFolderName}_${allocationIndex}_certificate_${allocationIndex}`,
            volume_MWh: allocation.volume_MWh,
            order_folder: transactionFolderName,
            contract: allocation.contract_id,
            minerID: allocation.minerID
        })
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

// List non matching (date-to-date / generation vs. reporting start/end), 3D
async function listNonMatchingDates3D(attestationFolder, transactionFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    // Grab step  2, 3, 6 and 7 CSVs
    const step2 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    const step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)
    const step6 = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)
    const step7 = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`)

    // Traverse allocations
    for (const allocation of step3) {
        const contractId = allocation.contract_id
        const orderingMinerID = allocation.minerID
        const volumeMWhOrdered = allocation.volume_MWh

        // Find contract in step 2
        let contract = step2.filter((c) => {return c.contract_id == contractId})
        if(!contract.length) {
            console.error(`Can't find ${contractId} in ${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
            continue
        }
        contract = contract[0]
        const reportingStart = moment(contract.reportingStart, "YYYY-MM-DD")
        const reportingEnd = moment(contract.reportingEnd, "YYYY-MM-DD")

        // Find contract in step 7
        let contractDelivery = step7.filter((c) => {return c.contract == contractId})
        if(!contractDelivery.length) {
            console.error(`Can't find ${contractId} in ${attestationFolder}/${attestationFolderName}${step7FileNameSuffix}`)
            continue
        }
        contractDelivery = contractDelivery[0]
        const receivingMinerID = contractDelivery.minerID
        const volumeMWhDelivered = contractDelivery.volume_MWh
        const certificateId = contractDelivery.certificate

        // Find certificate in step 6
        let certificate = step6.filter((c) => {return c.certificate == certificateId})
        if(!certificate.length) {
            console.error(`Can't find ${certificateId} in ${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)
            continue
        }
        certificate = certificate[0]
        const generationStart = moment(certificate.generationStart, "YYYY-MM-DD")
        const generationEnd = moment(certificate.generationEnd, "YYYY-MM-DD")

        // Check matching
/*         if(!generationStart.isSame(reportingStart) || !generationEnd.isSame(reportingEnd)
            || (orderingMinerID != receivingMinerID) || (volumeMWhOrdered != volumeMWhDelivered)) {
                console.error(`Allocation ${allocation.allocation_id}, ${contract.contract_id}, ${certificate.certificate}:
                Generation start-end: ${generationStart} - ${generationEnd},
                Reporting start-end: ${reportingStart} - ${reportingEnd},
                Miner Id: ${orderingMinerID}  (${receivingMinerID}),
                Volume ordered / delivered: ${volumeMWhOrdered} / ${volumeMWhDelivered}`)
            }
 */
        if(!generationStart.isSame(reportingStart) || !generationEnd.isSame(reportingEnd)) {
                console.error(`${contract.contract_id}, ${certificate.certificate}:\nGeneration start-end: ${generationStart} - ${generationEnd}\nReporting start-end: ${reportingStart} - ${reportingEnd}\nMiner Id: ${orderingMinerID}`)
        }
    }
}

// Create step 7, 3D, multistep approach
async function createStep73Dmultistep(attestationFolder, transactionFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step7Header = ['"certificate"', '"volume_MWh"', '"order_folder"', '"contract"', '"minerID"']
    const step7ColumnTypes = ["string", "number", "string", "string", "string"]

    let step7 = []

    // Grab step 2, 3 and 6 CSVs
    let step2 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    let step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)
    let step6 = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)

    // First itteration
    // Traverse all certificates for exact matches
    console.log(`-------- First itteration (exact matches) --------`)
    _step73DItterateCertificates(step2, step3, step6, step7, transactionFolderName, 1)

    // Second itteration
    // Traverse remaining certificates for partial matches
    // (reportingStart <= generationStart && generationEnd <= reportingEnd)
    console.log(`\r\n-------- Second itteration (partial matches) --------`)
    console.log(`-------- (reportingStart <= generationStart && generationEnd <= reportingEnd) --------`)
    _step73DItterateCertificates(step2, step3, step6, step7, transactionFolderName, 2)

    // Third itteration
    // Traverse remaining certificates for loose overlapping
    // (reportingStart <= generationEnd && generationStart <= reportingEnd)
    console.log(`\r\n-------- Third itteration (overlapping) --------`)
    console.log(`-------- (reportingStart <= generationEnd && generationStart <= reportingEnd) --------`)
    _step73DItterateCertificates(step2, step3, step6, step7, transactionFolderName, 3)

    // Calculate what remained unspent
    const remained = step6.filter((cert) => {return cert.volume_Wh > 0})
    console.info(`\r\nRemained undelivered:`)
    for (const r of remained) {
        console.info(`${r.certificate}: ${r.volume_Wh / 1000000} (${r.country}, ${r.region}, ${r.generationStart} - ${r.generationEnd})`)
    }

    // Calculate what remained unmatched
    const unmatched = step2.filter((contract) => {return contract.volume_MWh > 0})
    console.info(`\r\nRemained unsuplied:`)
    for (const r of unmatched) {
        console.info(`${r.contract_id}: ${r.volume_MWh} (${r.country}, ${r.region}, ${r.reportingStart} - ${r.reportingEnd})`)
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

function _step73DItterateCertificates(step2, step3, step6, step7, transactionFolderName, itteration) {
    let certificatesToRemove = []
    for (const certificate of step6) {
        const certificateId = certificate.certificate
        const generationCountry = certificate.country
        const generationRegion = certificate.region
        const generationStart = moment(certificate.generationStart, "YYYY-MM-DD")
        const generationEnd = moment(certificate.generationEnd, "YYYY-MM-DD")

        let matches = [1]

        while(matches.length) {
            // Reset match flag
            for (const contract of step2) {
                contract.match = null
            }

            let generationVolumeMWh = certificate.volume_Wh / 1000000
            if(generationVolumeMWh <= 0)
                break

            // Try matching contract records
            matches = step2.filter((contract) => {
                const reportingCountry = contract.country
                const reportingRegion = contract.region
                const reportingStart = moment(contract.reportingStart, "YYYY-MM-DD")
                const reportingEnd = moment(contract.reportingEnd, "YYYY-MM-DD")
                let matchingDateRange
                switch (itteration) {
                    case 1:
                        matchingDateRange = generationStart.isSame(reportingStart)
                            && generationEnd.isSame(reportingEnd)
                        break;
                    case 2:
                        matchingDateRange = generationStart.isSameOrAfter(reportingStart)
                            && generationEnd.isSameOrBefore(reportingEnd)
                        break;
                    case 3:
                        matchingDateRange = generationStart.isSameOrBefore(reportingEnd)
                            && generationEnd.isSameOrAfter(reportingStart)
                        break;
                    default:
                        break;
                }
                return generationCountry == reportingCountry &&
                    ((generationCountry == "US") ? (generationRegion == reportingRegion) : true) &&
                    matchingDateRange
            }).sort((a, b) => {return a.volume_MWh - b.volume_MWh})

            console.log(`\n${certificateId} (${generationCountry}, ${generationRegion}, ${generationStart.format("YYYY-MM-DD")}-${generationEnd.format("YYYY-MM-DD")}, ${generationVolumeMWh} MWh) - matches: ${matches.length}`)
            let orderedVolumeMWh = 0
            for (const contract of matches) {
                const contractId = contract.contract_id
                const reportingCountry = contract.country
                const reportingRegion = contract.region
                const reportingStart = contract.reportingStart
                const reportingEnd = contract.reportingEnd
                const reportingVolumeMWh = contract.volume_MWh
                orderedVolumeMWh += reportingVolumeMWh
                console.log(`${contractId} (${reportingCountry}, ${reportingRegion}, ${reportingStart}-${reportingEnd}, ${reportingVolumeMWh} MWh)`)
            }
            console.log(`Reported volume: ${orderedVolumeMWh} - Generated volume: ${generationVolumeMWh}`)

            if(generationVolumeMWh >= orderedVolumeMWh && orderedVolumeMWh > 0) {
                let contractsToRemove = []
                for (const contract of matches) {
                    if(contract.match)
                        continue    // if we already matched this contract against a certificate
                    contract.match = certificateId
                    const allocations = step3.filter((a) => {return a.contract_id == contract.contract_id})
                        .sort((a, b) => {return a.volume_MWh - b.volume_MWh})
                    let allocationsToRemove = []
                    for (const allocation of allocations) {
                        step7.push({
                            certificate: certificateId,
                            volume_MWh: allocation.volume_MWh,
                            order_folder: transactionFolderName,
                            contract: contract.contract_id,
                            minerID: allocation.minerID
                        })

                        // Mark fulfilled allocations to remove from step 3
                        const allocationToRemove = step3
                            .filter((st3) => {return st3.allocation_id == allocation.allocation_id})[0].allocation_id
                        allocationsToRemove.push(allocationToRemove)
                    }
                    // Remove fulfilled allocations from step 3
                    for (const altr of allocationsToRemove) {
                        if(altr == undefined)
                            continue
                        const allocationIndex = step3
                            .map((st3) => {return st3.allocation_id})
                            .indexOf(altr)
                        step3.splice(allocationIndex, 1)
                    }

                    // Mark fulfilled contracts to remove from step 2
                    const contractToRemove = step2
                        .filter((st2) => {return st2.contract_id == contract.contract_id})[0].contract_id
                    contractsToRemove.push(contractToRemove)
                }
                // Remove fulfilled contracts from step 2
                for (const crttr of contractsToRemove) {
                    const contractIndex = step2
                        .map((st2) => {return st2.contract_id})
                        .indexOf(crttr)
                    const contract = step2[contractIndex]
                    if(contract == undefined)
                        continue
                    console.log(`REMOVED ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${contract.volume_MWh} MWh)`)
                    step2.splice(contractIndex, 1)
                }

                // Remove or deduct volume for (partially) fulfilled certificates, step 6
                const certificateIndex = step6.map((st6) => {return st6.certificate})
                    .indexOf(certificate.certificate)
                if(generationVolumeMWh == orderedVolumeMWh) {   // remove
                    // Mark fulfilled certificates to remove from step 6
                    const certificateToRemove = step6
                        .filter((st6) => {return st6.certificate == certificate.certificate})[0].certificate
                    certificatesToRemove.push(certificateToRemove)
                    step6[certificateIndex].volume_Wh = 0
                    console.log(`TO BE REMOVED ${certificateId} (${generationCountry}, ${generationRegion}, ${generationStart.format("YYYY-MM-DD")}-${generationEnd.format("YYYY-MM-DD")}, ${step6[certificateIndex].volume_Wh / 1000000} MWh)`)
                }
                else {  // deduct volume
                    step6[certificateIndex].volume_Wh = (generationVolumeMWh - orderedVolumeMWh) * 1000000
                    console.log(`CHANGED ${certificateId} (${generationCountry}, ${generationRegion}, ${generationStart.format("YYYY-MM-DD")}-${generationEnd.format("YYYY-MM-DD")}, ${step6[certificateIndex].volume_Wh / 1000000} MWh)`)
                }
            }
            else if(generationVolumeMWh < orderedVolumeMWh) {     // missing volume for exact match
                let assignedVolumeMWh = 0
                let matchesVolumes = matches.map((m) => {return m.volume_MWh})
                console.log(`Subset sum is matching: ${_isSubsetSum(matchesVolumes, matchesVolumes.length, generationVolumeMWh)}`)
                if(_isSubsetSum(matchesVolumes, matchesVolumes.length, generationVolumeMWh)) {
                    const subset = _createSubsets(matchesVolumes, generationVolumeMWh)
                    console.log(`${subset}`)
                    let exactMatches = []
                    let emIndexes = []
                    for (const val of subset) {
                        const index = matchesVolumes.indexOf(val)
                        emIndexes.push(index)
                        matchesVolumes[index] = -1
                    }
                    for (const emi of emIndexes) {
                        exactMatches.push(matches[emi])
                    }
                    let contractsToRemove = []
                    for (const contract of exactMatches) {
                        console.log(`EXACT MATCHES ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${contract.volume_MWh} MWh)`)

                        let allocatedVolumeMWh = 0
                        const allocations = step3.filter((a) => {return a.contract_id == contract.contract_id})
                            .sort((a, b) => {return a.volume_MWh - b.volume_MWh})
                        let allocationsToRemove = []
                        for (const allocation of allocations) {
                            const allocationVolumeMWh = (allocation.volume_MWh > (generationVolumeMWh - assignedVolumeMWh))
                                ?  (generationVolumeMWh - assignedVolumeMWh) : allocation.volume_MWh
                            if(allocationVolumeMWh == 0)
                                continue    // skip, no RECs left
                            // assign and duduct volume
                            step7.push({
                                certificate: certificateId,
                                volume_MWh: allocationVolumeMWh,
                                order_folder: transactionFolderName,
                                contract: contract.contract_id,
                                minerID: allocation.minerID
                            })
                            allocatedVolumeMWh += allocationVolumeMWh
                            assignedVolumeMWh += allocationVolumeMWh

                            // Remove fulfilled allocations from step 3
                            const allocationIndex = step3.map((st3) => {return st3.allocation_id})
                                .indexOf(allocation.allocation_id)
                            if(allocation.volume_MWh == allocationVolumeMWh) {  // fulfilled, remove
                                // Mark fulfilled allocations to remove from step 3
                                const allocationToRemove = step3
                                    .filter((st3) => {return st3.allocation_id == allocation.allocation_id})[0].allocation_id
                                allocationsToRemove.push(allocationToRemove)
                            }
                            else {      // deduct volume
                                step3[allocationIndex].volume_MWh = (allocation.volume_MWh - allocationVolumeMWh)
                            }
                        }
                        // Remove fulfilled allocations from step 3
                        for (const altr of allocationsToRemove) {
                            if(altr == undefined)
                                continue
                            const allocationIndex = step3
                                .map((st3) => {return st3.allocation_id})
                                .indexOf(altr)
                            step3.splice(allocationIndex, 1)
                        }

                        // Remove fulfilled contracts or deduct volume from step 2
                        const contractIndex = step2.map((st2) => {return st2.contract_id})
                            .indexOf(contract.contract_id)
                        if(contract.volume_MWh == allocatedVolumeMWh) {  // fulfilled, remove
                            // Mark fulfilled contracts to remove from step 2
                            const contractToRemove = step2
                                .filter((st2) => {return st2.contract_id == contract.contract_id})[0].contract_id
                            contractsToRemove.push(contractToRemove)
                        }
                        else {      // deduct volume
                            step2[contractIndex].volume_MWh = (contract.volume_MWh - allocatedVolumeMWh)
                            console.log(`CHANGED ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${step2[contractIndex].volume_MWh} MWh)`)
                            contract.match = null
                        }
                    }
                    // Remove fulfilled contracts from step 2
                    for (const crttr of contractsToRemove) {
                        const contractIndex = step2
                            .map((st2) => {return st2.contract_id})
                            .indexOf(crttr)
                        const contract = step2[contractIndex]
                        if(contract == undefined)
                            continue
                        console.log(`REMOVED ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${contract.volume_MWh} MWh)`)
                        step2.splice(contractIndex, 1)
                    }
                }
                else {
                    let contractsToRemove = []
                    for (const contract of matches) {
                        let allocatedVolumeMWh = 0
                        const allocations = step3.filter((a) => {return a.contract_id == contract.contract_id})
                            .sort((a, b) => {return a.volume_MWh - b.volume_MWh})
                        let allocationsToRemove = []
                        for (const allocation of allocations) {
                            const allocationVolumeMWh = (allocation.volume_MWh > (generationVolumeMWh - assignedVolumeMWh))
                                ?  (generationVolumeMWh - assignedVolumeMWh) : allocation.volume_MWh
                            if(allocationVolumeMWh == 0)
                                continue    // skip, no RECs left
                            // assign and duduct volume
                            step7.push({
                                certificate: certificateId,
                                volume_MWh: allocationVolumeMWh,
                                order_folder: transactionFolderName,
                                contract: contract.contract_id,
                                minerID: allocation.minerID
                            })
                            allocatedVolumeMWh += allocationVolumeMWh
                            assignedVolumeMWh += allocationVolumeMWh

                            // Remove fulfilled allocations from step 3
                            const allocationIndex = step3.map((st3) => {return st3.allocation_id})
                                .indexOf(allocation.allocation_id)
                            if(allocation.volume_MWh == allocationVolumeMWh) {  // fulfilled, remove
                                // Mark fulfilled allocations to remove from step 3
                                const allocationToRemove = step3
                                    .filter((st3) => {return st3.allocation_id == allocation.allocation_id})[0].allocation_id
                                allocationsToRemove.push(allocationToRemove)
                            }
                            else {      // deduct volume
                                step3[allocationIndex].volume_MWh = (allocation.volume_MWh - allocationVolumeMWh)
                            }
                        }
                        // Remove fulfilled allocations from step 3
                        for (const altr of allocationsToRemove) {
                            if(altr == undefined)
                                continue
                            const allocationIndex = step3
                                .map((st3) => {return st3.allocation_id})
                                .indexOf(altr)
                            step3.splice(allocationIndex, 1)
                        }

                        // Remove fulfilled contracts or deduct volume from step 2
                        const contractIndex = step2.map((st2) => {return st2.contract_id})
                            .indexOf(contract.contract_id)
                        if(contract.volume_MWh == allocatedVolumeMWh) {  // fulfilled, remove
                            // Mark fulfilled contracts to remove from step 2
                            const contractToRemove = step2
                                .filter((st2) => {return st2.contract_id == contract.contract_id})[0].contract_id
                            contractsToRemove.push(contractToRemove)
                        }
                        else {      // deduct volume
                            step2[contractIndex].volume_MWh = (contract.volume_MWh - allocatedVolumeMWh)
                            console.log(`CHANGED ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${step2[contractIndex].volume_MWh} MWh)`)
                            contract.match = null
                        }
                    }
                    // Remove fulfilled contracts from step 2
                    for (const crttr of contractsToRemove) {
                        const contractIndex = step2
                            .map((st2) => {return st2.contract_id})
                            .indexOf(crttr)
                        const contract = step2[contractIndex]
                        if(contract == undefined)
                            continue
                        console.log(`REMOVED ${contract.contract_id} (${contract.country}, ${contract.region}, ${contract.reportingStart}-${contract.reportingEnd}, ${contract.volume_MWh} MWh)`)
                        step2.splice(contractIndex, 1)
                    }
                }
                // Remove or deduct volume for (partially) fulfilled certificates, step 6
                const certificateIndex = step6.map((st6) => {return st6.certificate})
                    .indexOf(certificate.certificate)
                if(generationVolumeMWh == assignedVolumeMWh) {   // remove
                    // Mark fulfilled certificates to remove from step 6
                    const certificateToRemove = step6
                        .filter((st6) => {return st6.certificate == certificate.certificate})[0].certificate
                    certificatesToRemove.push(certificateToRemove)
                    step6[certificateIndex].volume_Wh = 0
                    console.log(`TO BE REMOVED ${certificateId} (${generationCountry}, ${generationRegion}, ${generationStart.format("YYYY-MM-DD")}-${generationEnd.format("YYYY-MM-DD")}, ${step6[certificateIndex].volume_Wh / 1000000} MWh)`)
                }
                else {  // deduct volume
                    step6[certificateIndex].volume_Wh = (generationVolumeMWh - assignedVolumeMWh) * 1000000
                    console.log(`CHANGED ${certificateId} (${generationCountry}, ${generationRegion}, ${generationStart.format("YYYY-MM-DD")}-${generationEnd.format("YYYY-MM-DD")}, ${step6[certificateIndex].volume_Wh / 1000000} MWh)`)
                }
            }
            else {      // order volume is 0 (over delivered)

            }
        }
    }
    // Remove fulfilled certificates from step 6
    for (const crt of certificatesToRemove) {
        const certificateIndex = step6
            .map((st6) => {return st6.certificate})
            .indexOf(crt)
        const certificate = step6[certificateIndex]
        if(certificate == undefined)
            continue
        console.log(`REMOVED ${certificate.certificate} (${certificate.country}, ${certificate.region}, ${certificate.generationStart}-${certificate.generationEnd}, ${certificate.volume_Wh / 1000000} MWh)`)
        step6.splice(certificateIndex, 1)
    }
}

function _isSubsetSum(set, n, sum)
{
    if (sum == 0)
        return true
    if (n == 0)
        return false

    if (set[n - 1] > sum)
        return _isSubsetSum(set, n - 1, sum)

    return _isSubsetSum(set, n - 1, sum)
        || _isSubsetSum(set, n - 1, sum - set[n - 1])
}

function _createSubsets(numbers, target) {
    numbers = numbers.filter(function (value) {
        return value <= target
    })

    numbers.sort(function (a, b) {
        return b - a
    })

    var result = []

    while (numbers.length > 0) {
        var i
        var sum = 0
        var addedIndices = []

        for (i = 0; i < numbers.length; i++) {
            if (sum + numbers[i] <= target) {
                sum += numbers[i]
                addedIndices.push(i)
            }
        }

        var subset = []
        for (i = addedIndices.length - 1; i >= 0; i--) {
            subset.unshift(numbers[addedIndices[i]])
            numbers.splice(addedIndices[i], 1)
        }
        result.push(subset)
    }
    return result[0]
}

// Unquote step 6 numeric fields
async function unquoteStep6NumericFields(attestationFolder) {
    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    const step6Header = ['"attestation_id"', '"attestation_file"', '"attestation_cid"', '"certificate"',
        '"certificate_cid"', '"reportingStart"', '"reportingStartTimezoneOffset"', '"reportingEnd"', '"reportingEndTimezoneOffset"',
        '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_Wh"', '"generatorName"', '"productType"', '"label"',
        '"energySource"', '"generationStart"', '"generationStartTimezoneOffset"', '"generationEnd"', '"generationEndTimezoneOffset"']
    const step6ColumnTypes = ["string", "string", "string", "string",
        "string", "string", "number", "string", "number",
        "string", "string", "string", "string", "number", "string", "string", "string",
        "string", "string", "number", "string", "number"]

    let step6 = []

    let certificates = await getCsvAndParseToJson(`${attestationFolder}/${attestationFolderName}${step6FileNameSuffix}`)
    for (let certificate of certificates) {
        step6.push({
            attestation_id: certificate.attestation_id,
            attestation_file: certificate.attestation_file,
            attestation_cid: certificate.attestation_cid,
            certificate: certificate.certificate,
            certificate_cid: certificate.certificate_cid,
            reportingStart: certificate.reportingStart,
            reportingStartTimezoneOffset: certificate.reportingStartTimezoneOffset,
            reportingEnd: certificate.reportingEnd,
            reportingEndTimezoneOffset: certificate.reportingEndTimezoneOffset,
            sellerName: certificate.sellerName,
            sellerAddress: certificate.sellerAddress,
            country: certificate.country,
            region: certificate.region,
            volume_Wh: certificate.volume_Wh,
            generatorName: certificate.generatorName,
            productType: certificate.productType,
            label: certificate.label,
            energySource: certificate.energySource,
            generationStart: certificate.generationStart,
            generationStartTimezoneOffset: certificate.generationStartTimezoneOffset,
            generationEnd: certificate.generationEnd,
            generationEndTimezoneOffset: certificate.generationEndTimezoneOffset
        })
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

// Create step 3 CSV
async function createStep3(transactionFolder, minersLocationsFile, priorityMinersFiles, gridMinersSplitFile, recsMultFactor = 1.5, contractConsumption = 1, maxRaduis = 250, radiusIncrementStep = 250) {
    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step2Header = ['"contract_id"', '"productType"', '"label"', '"energySources"', '"contractDate"', '"deliveryDate"',
        '"reportingStart"', '"reportingEnd"', '"sellerName"', '"sellerAddress"', '"country"', '"region"', '"volume_MWh"',
        '"step3_match_complete"', '"step4_ZL_contract_complete"', '"step5_redemption_data_complete"', '"step6_attestation_info_complete"',
        '"step7_certificates_matched_to_supply"', '"step8_IPLDrecord_complete"', '"step9_transaction_complete"',
        '"step10_volta_complete"', '"step11_finalRecord_complete"']
    const step2ColumnTypes = ["string", "string", "string", "string", "string", "string",
        "string", "string", "string", "string", "string", "string", "number",
        "number", "number", "number", "number",
        "number", "number", "number",
        "number", "number"]

    const step3Header = ['"allocation_id"', '"UUID"', '"contract_id"', '"minerID"', '"volume_MWh"', '"defaulted"',
        '"step4_ZL_contract_complete"', '"step5_redemption_data_complete"', '"step6_attestation_info_complete"',
        '"step7_certificates_matched_to_supply"', '"step8_IPLDrecord_complete"', '"step9_transaction_complete"',
        '"step10_volta_complete"', '"step11_finalRecord_complete"']
    const step3ColumnTypes = ["string", "string", "string", "string", "number", "number",
        "number", "number", "number",
        "number", "number", "number",
        "number", "number"]

    let step3 = []
    let minersLocations = []
    let miners = []
    let previousAllocations = []
    let previousContracts = []
    let startingContracts = [], contracts = [], consumedContracts = []
    let gridMinersSplit = null
    try {
        const minersLocationsFilePath = `./${transactionFolder}/_assets/${minersLocationsFile}`
        minersLocations = await fs.promises.readFile(minersLocationsFilePath, {
            encoding:'utf8',
            flag:'r'
        })
        minersLocations = JSON.parse(minersLocations).providerLocations

        const priorityMinersFilesArr = priorityMinersFiles.split(",")
        for await (let priorityMinersFile of priorityMinersFilesArr) {
            priorityMinersFile = priorityMinersFile.trim()
            const priorityMinersFilePath = `./${transactionFolder}/_assets/${priorityMinersFile}`
            const mnrs = await fs.promises.readFile(priorityMinersFilePath, {
                encoding:'utf8',
                flag:'r'
            })
            miners = miners.concat(JSON.parse(mnrs))
        }

        // Load contracts
        contracts = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)

        // Filter future contracts and ones already fully consumed / matched against miner IDs
        contracts = contracts.filter((c) => {
            let recsAvailable = (typeof c.volume_MWh != "number")
                ? Number((c.volume_MWh.replace(",", ""))) : c.volume_MWh
            c.volume_MWh = recsAvailable         // Make sure we have a number here (step 2 has strings in some fields)!

            return moment(c.reportingEnd, "YYYY-MM-DD").isSameOrBefore(moment()) && c.step3_match_complete == 0
        })

        // Load previous allocations (step3 CSV if it exists)
        try {
            step3 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`)
            // Deduct previously allocated volumes
            for (const allocation of step3) {
                const contractIndex = contracts.map((c) => {return c.contract_id}).indexOf(allocation.contract_id)
                if(contractIndex > -1)
                    contracts[contractIndex]["volume_MWh"] -= allocation.volume_MWh
            }
        }
        catch (err) {
            step3 = []
            console.log(`Step 3 CSV (allocations file) for ${transactionFolder} is not created yet`)
        }

        // Set max consumtion from contracts
        for (const contract of contracts) {
            contract.volume_MWh_leftover = Math.ceil(contract.volume_MWh * (1 - contractConsumption))
        }
        
        // Load grid miners split file
        const gridMinersSplitPath = `./${transactionFolder}/_assets/${gridMinersSplitFile}`
        gridMinersSplit = await fs.promises.readFile(gridMinersSplitPath, {
            encoding:'utf8',
            flag:'r'
        })
        gridMinersSplit = JSON.parse(gridMinersSplit)
    }
    catch (error)
    {
        console.error('Input files are missing or corrupted')
        return new Promise((resolve) => {
            resolve(null)
        })
    }

    const transactionFolders = fs.readdirSync("./").filter((file) => {
        return file.indexOf("_transaction_") > -1
//        return file.indexOf("_transaction_") > -1 && file.indexOf(transactionFolder) == -1
    })

    for await (const transactionFol of transactionFolders) {
        const transactionFolPathChunks = transactionFol.split("/")
        const transactionFolName = transactionFolPathChunks[transactionFolPathChunks.length-1]
        const alloc = await getCsvAndParseToJson(`${transactionFol}/${transactionFolName}${step3FileNameSuffix}`)
        if(alloc != null)
            previousAllocations = previousAllocations.concat(alloc)
        let contr = await getCsvAndParseToJson(`${transactionFol}/${transactionFolName}${step2FileNameSuffix}`)
        if(contr != null) {
//            contr = contr.filter((c) => {return c.step3_match_complete == 1})
            previousContracts = previousContracts.concat(contr)
        }
    }

    let minersEnergyData = {}

    console.log(`contracts.length: ${contracts.length}, miners.length: ${miners.length}`)

    let step = await _consumeContracts(transactionFolderName, miners, minersEnergyData,
        previousAllocations, previousContracts, consumedContracts, contracts, step3,
        gridMinersSplit, recsMultFactor, contractConsumption)
    minersEnergyData = step.minersEnergyData
    previousContracts = step.previousContracts
    previousAllocations = step.previousAllocations
    consumedContracts = step.consumedContracts
    contracts = step.contracts
    step3 = step.step3

    // If we have unmatched contracts remained
    // search for miners in region (start with radius 250km)
    if(contracts.length) {
        let L = HL()    // headless leaflet
        L.Circle.include({
            contains: function (latLng) {
                return this.getLatLng().distanceTo(latLng) < this.getRadius();
            }
        })
        let map = L.map(L.document.createElement('div'))

        step = await _consumeContractInRadius(contracts, gridMinersSplit, minersLocations, map,
            transactionFolderName, minersEnergyData, previousAllocations, previousContracts, consumedContracts, step3, recsMultFactor, maxRaduis, radiusIncrementStep)
        minersEnergyData = step.minersEnergyData
        previousContracts = step.previousContracts
        previousAllocations = step.previousAllocations
        consumedContracts = step.consumedContracts
        contracts = step.contracts
        step3 = step.step3
    }

    // Load contracts
    startingContracts = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    for (const c of startingContracts) {
        let recsAvailable = (typeof c.volume_MWh != "number")
            ? Number((c.volume_MWh.replace(",", ""))) : c.volume_MWh
        c.volume_MWh = recsAvailable         // Make sure we have a number here (step 2 has strings in some fields)!

        // Check if contract is matched
        if(consumedContracts.indexOf(c.contract_id) > -1)
            c.step3_match_complete = 1
    }

    let result = {
        "step3": step3Header.join(",") + "\r\n" +
            Papa.unparse(step3, {
                quotes: step3ColumnTypes.map((ct) => {return ct != 'number'}),
                quoteChar: '"',
                escapeChar: '"',
                delimiter: ",",
                header: false,
                newline: "\r\n",
                skipEmptyLines: false,
                columns: null
            }),
        "step2": step2Header.join(",") + "\r\n" +
            Papa.unparse(startingContracts, {
                quotes: step2ColumnTypes.map((ct) => {return ct != 'number'}),
                quoteChar: '"',
                escapeChar: '"',
                delimiter: ",",
                header: false,
                newline: "\r\n",
                skipEmptyLines: false,
                columns: null
            })
    }

    return new Promise((resolve) => {
        resolve(result)
    })
}

async function _consumeContracts(transactionFolderName, miners, minersEnergyData,
    previousAllocations, previousContracts, consumedContracts, contracts,
    step3, gridMinersSplit, recsMultFactor = 1.5) {

    // For each contract, find miners with matching coutry/region
    for await (let contract of contracts) {
        // Skip if contract has no more available credits left
        let recsAvailable = (typeof contract.volume_MWh != "number")
            ? Number((contract.volume_MWh.replace(",", ""))) : contract.volume_MWh
        contract.volume_MWh = recsAvailable         // Make sure we have a number here (step 2 has strings in some fields)!
        if(recsAvailable <= contract.volume_MWh_leftover) {
            console.log(`Contract ${contract.contract_id} has no more available RECs (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover})`)
            continue
        }

        const country = contract.country
        const region = contract.region

        // Check if we have region listed in existing grid regions
        if(gridMinersSplit[(region != null) ? region : country] == undefined) {
            console.log(`Grid region ${(region != null) ? region : country} is not listed in grid regions input file.`)
            continue
        }
        
        for await (const miner of miners) {
            // Check if miner is in the grid region
            const minersInGrid = gridMinersSplit[(region != null) ? region : country]
            const minerInGridPos = minersInGrid.map((gm) => {return gm.provider}).indexOf(miner)
            if(minerInGridPos == -1) {
                console.log(`Miner ${miner} is not in ${(region != null) ? region : country} grid region.`)
                continue
            }

            const minerInGrid = minersInGrid[minerInGridPos]
            
            recsAvailable = (typeof contract.volume_MWh != "number")
                ? Number((contract.volume_MWh.replace(",", ""))) : contract.volume_MWh

            console.log(`Trying to allocate from contract ${contract.contract_id} (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}) to miner ${miner}`)

            if(recsAvailable <= contract.volume_MWh_leftover) {
                console.log(`Contract ${contract.contract_id} has no more available RECs (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}) for miner ${miner}`)
                break
            }

            // Get miner's energy consumption data
            // for contract reporting period
            const medIndex = `${miner}-${contract.reportingStart}-${contract.reportingEnd}`
            console.log(`Get energy data for ${miner} (${contract.reportingStart}, ${contract.reportingEnd})`)
            if(minersEnergyData[medIndex] == null)
//                    minersEnergyData[medIndex] = await _totalEnergy(contract.reportingStart, contract.reportingEnd, miner.minerId)
                minersEnergyData[medIndex] = await _totalEnergyFromModel(moment(contract.reportingStart, "YYYY-MM-DD").format("YYYY-MM-DD"), moment(contract.reportingEnd, "YYYY-MM-DD").format("YYYY-MM-DD"), miner)
            console.log(`Got cumulative energy total (upper) ${minersEnergyData[medIndex].total_energy_upper_MWh} for ${miner} (${contract.reportingStart}, ${contract.reportingEnd})`)

            // Get grid split for miner
            let minerSplit = minerInGrid.weight
            console.log(`Cumulative energy total (upper) per grid split (${minerSplit}) for ${miner} (${contract.reportingStart}, ${contract.reportingEnd}) is ${minersEnergyData[medIndex].total_energy_upper_MWh * minerSplit}`)

            let palloc = previousAllocations
            .filter((a) => {
                return a != null && a.minerID == miner
            })
            .map((a) => {
                const pcontr = previousContracts
                    .filter((c) => {
                        return a.contract_id == c.contract_id
                    })
                    .map((c) => {
                        return {
                            "reportingStart": c.reportingStart,
                            "reportingEnd": c.reportingEnd,
                            "allocationGridGeography": (c.region != null) ? c.region : c.country
                        }
                })
                return {
                    "allocation": a.allocation_id,
                    "contract": a.contract_id,
                    "recs": a.volume_MWh,
                    "defaulted": a.defaulted,
                    "reportingStart": pcontr[0].reportingStart,
                    "reportingEnd": pcontr[0].reportingEnd,
                    "allocationGridGeography": pcontr[0].allocationGridGeography
                }
            }).filter((a) => {
                const overlapingDateRanges = moment(a.reportingStart, "YYYY-MM-DD").isSameOrBefore(moment(contract.reportingEnd, "YYYY-MM-DD"))
                    && moment(a.reportingEnd, "YYYY-MM-DD").isSameOrAfter(moment(contract.reportingStart, "YYYY-MM-DD"))
                return (overlapingDateRanges && !a.defaulted && a.allocationGridGeography == minerInGrid.nercRegion)
            })

            if(minersEnergyData[medIndex].total_energy_upper_MWh > 0) {
                const recsNeeded = Math.ceil(minersEnergyData[medIndex].total_energy_upper_MWh * minerSplit * recsMultFactor)
                const totalRecsAllocated = palloc.reduce((prev, elem) => prev + elem.recs, 0)

                // We should decrease overlapping allocated volumes for periods/energy consuptions
                // which are out of curent contract allocation start/end period
                palloc = palloc
                    .map(async (a) => {
                        // Previous allocation starts before current contract allocation
                        if(moment(a.reportingStart, "YYYY-MM-DD").isBefore(moment(contract.reportingStart, "YYYY-MM-DD"))) {
                            // Deduct allocation amount for energy spent before currenct contract allocation
                            const deductingPeriodEnd = moment(contract.reportingStart, "YYYY-MM-DD").add(-1, 'days').format('YYYY-MM-DD')
                            const energySpentBeforeCurrentAllocation = await _totalEnergyFromModel(moment(a.reportingStart, "YYYY-MM-DD").format("YYYY-MM-DD"), deductingPeriodEnd, miner)
                            const recsAllocatedBeforeCurrentAllocationStarts = Math.ceil(energySpentBeforeCurrentAllocation.total_energy_upper_MWh * recsMultFactor)
                            console.log(`Previous allocation start date ${a.reportingStart} is before current contract allocation start date ${contract.reportingStart}
                                so we will deduct ${recsAllocatedBeforeCurrentAllocationStarts} (${energySpentBeforeCurrentAllocation.total_energy_upper_MWh}) RECs from previous allocation ${a.recs}`)
                            a.recs = (a.recs >= recsAllocatedBeforeCurrentAllocationStarts) ? a.recs - recsAllocatedBeforeCurrentAllocationStarts : 0
                        }
                        // Previous allocation ends after current contract allocation
                        if(moment(a.reportingEnd, "YYYY-MM-DD").isAfter(moment(contract.reportingEnd, "YYYY-MM-DD"))) {
                            // Deduct allocation amount for energy spent after currenct contract allocation
                            const deductingPeriodStart = moment(contract.reportingEnd, "YYYY-MM-DD").add(1, 'days').format('YYYY-MM-DD')
                            const energySpentAfterCurrentAllocation = await _totalEnergyFromModel(deductingPeriodStart, moment(a.reportingEnd, "YYYY-MM-DD").format("YYYY-MM-DD"), miner)
                            const recsAllocatedAfterCurrentAllocationEnds = Math.ceil(energySpentAfterCurrentAllocation.total_energy_upper_MWh * recsMultFactor)
                            console.log(`Previous allocation end date ${a.reportingEnd} is after current contract allocation end date ${contract.reportingEnd}
                                so we will deduct ${recsAllocatedAfterCurrentAllocationEnds} (${energySpentAfterCurrentAllocation.total_energy_upper_MWh}) RECs from previous allocation ${a.recs}`)
                            a.recs = (a.recs >= recsAllocatedAfterCurrentAllocationEnds) ? a.recs - recsAllocatedAfterCurrentAllocationEnds : 0
                        }
                        return a
                    })
                palloc = await all(palloc)

                const recsAllocated = palloc.reduce((prev, elem) => prev + elem.recs, 0)

                console.log(`Contract ${contract.contract_id} (recsAvailable: ${recsAvailable}, contract.volume_MWh: ${contract.volume_MWh}), miner ${miner}
                    recsNeeded ${recsNeeded} recsAllocated ${recsAllocated} (totalRecsAllocated ${totalRecsAllocated})`)

                // If we have already allocated this miner what he needs
                // or we have no more RECs available with this contract, then skip it
                if(recsNeeded <= recsAllocated)
                    continue
                const newlyAllocated = (recsAvailable - contract.volume_MWh_leftover >= (recsNeeded - recsAllocated))
                    ? (recsNeeded - recsAllocated) : recsAvailable - contract.volume_MWh_leftover
                contract.volume_MWh = recsAvailable - newlyAllocated

                if(step3 == null)
                    step3 = []

                const allocation = {
                    allocation_id: `${transactionFolderName}_allocation_${step3.length+1}`,
                    UUID: null,
                    contract_id: contract.contract_id,
                    minerID: miner,
                    volume_MWh: newlyAllocated,
                    defaulted: 0,
                    step4_ZL_contract_complete: 0,
                    step5_redemption_data_complete: 0,
                    step6_attestation_info_complete: 0,
                    step7_certificates_matched_to_supply: 0,
                    step8_IPLDrecord_complete: 0,
                    step9_transaction_complete: 0,
                    step10_volta_complete: 0,
                    step11_finalRecord_complete: 0
                }
                step3.push(allocation)
                previousAllocations.push(allocation)
                console.log(`Contract ${contract.contract_id} (recsAvailable: ${recsAvailable}, contract.volume_MWh: ${contract.volume_MWh}), miner ${miner}
                    recsNeeded ${recsNeeded} recsAllocated ${recsAllocated} newlyAllocated ${newlyAllocated}`)
                const pcIds = previousContracts.map((pc) => {return pc.contract_id})
                if(pcIds.indexOf(contract.contract_id) == -1)
                    previousContracts.push(contract)
            }
        }
    }

    // Filter out all "empty" contracts
    contracts = contracts.filter((c) => {
        c.volume_MWh = (typeof c.volume_MWh != "number")
            ? Number((c.volume_MWh.replace(",", ""))) : c.volume_MWh

        if(c.volume_MWh == 0)
            consumedContracts.push(c.contract_id)

        return c.volume_MWh > c.volume_MWh_leftover
    })
    console.log(`Remaining contracts: ${contracts.length}`)

    return {
        "minersEnergyData": minersEnergyData,
        "previousContracts": previousContracts,
        "previousAllocations": previousAllocations,
        "consumedContracts": consumedContracts,
        "contracts": contracts,
        "step3": step3
    }
}

async function _consumeContractInRadius(contracts, gridMinersSplit, minersLocations, map,
    transactionFolderName, minersEnergyData, previousAllocations, previousContracts, consumedContracts, step3, recsMultFactor, maxRaduis, radiusIncrementStep) {
    console.log(`contracts.length: ${contracts.length}`)
    // Find contract country/region
    let contract = contracts[0]
    const country = contract.country
    const region = contract.region
    console.log(`Contract ${contract.contract_id}, Country ${country}, Region ${region}`)

    // Find a miner (and its location) from that country/region
    let minersInRegion = []
    if(region != null) {
        let regionMiners = gridMinersSplit[region]
            .map((rm) => {return rm.provider})
            minersInRegion = minersLocations
                .filter((slm) => {return regionMiners.indexOf(slm.provider) > -1})
    }
    else {
        minersInRegion = minersLocations
            .filter((m) => {return m.country == country})
    }
    console.log(`${minersInRegion.length} miners found in region ${(region != null) ? region : country}`)

    // Find center point for searching miners in a radius
    const minerPositions = minersInRegion
        .map((m) => {return [m.lat, m.long]})
    const minerBounds = new L.LatLngBounds(minerPositions)
    map.fitBounds(minerBounds)
    const minersCenter = map.getCenter()
    console.log(`Center point for seaching miners in radius: ${minersCenter.lat}/${minersCenter.lng}`)

    let radius = 0
    const centerLatLng = L.latLng(minersCenter.lat, minersCenter.lng)

    while (contract.volume_MWh > contract.volume_MWh_leftover && radius < (maxRaduis * 1000)) {
        console.log(`Contract ${contract.contract_id}, volume left ${contract.volume_MWh}, left over: ${contract.volume_MWh_leftover}`)
        radius += (radiusIncrementStep * 1000)    // increase miners search radius for 250km
        const circle = L.circle(centerLatLng, {radius: radius}).addTo(map)

        // Filter miners in radius
        let minersInRadius = minersLocations
            .filter((m) => {
                const lat = m.lat
                const lng = m.long
                return circle.contains(L.latLng(lat, lng))
            })
            .map((m) => {
                m.weight = _getMinerWeight(m, gridMinersSplit)
                return m
            })
        console.log(`${minersInRadius.length} miners found in radius ${radius/1000}km around ${minersCenter.lat}/${minersCenter.lng}`)
        //  Remove radius and prepare for next lap
        map.removeLayer(circle)

        let step = await _consumeContractRegardlessRegion(transactionFolderName, minersInRadius, minersEnergyData,
            previousAllocations, previousContracts, contract, step3, recsMultFactor)
        contract = step.contract
        minersEnergyData = step.minersEnergyData
        previousContracts = step.previousContracts
        previousAllocations = step.previousAllocations
        step3 = step.step3

await new Promise(resolve => setTimeout(resolve, 20000))
    }

    // Remove contract if we search up to a max radius or matched up to a desired leftover
    if((c.volume_MWh <= c.volume_MWh_leftover) || (radius >= (maxRaduis * 1000)))
        contracts.splice(0, 1)

    // Mark all fully consumed contracts
    if(c.volume_MWh == 0)
        consumedContracts.push(c.contract_id)

    console.log(`Remaining contracts: ${contracts.length}`)

    if(contracts.length) {
        let step = await _consumeContractInRadius(contracts, gridMinersSplit, minersLocations, map,
            transactionFolderName, minersEnergyData, previousAllocations, previousContracts, consumedContracts, step3, recsMultFactor, maxRaduis, radiusIncrementStep)
        minersEnergyData = step.minersEnergyData
        previousContracts = step.previousContracts
        previousAllocations = step.previousAllocations
        consumedContracts = step.consumedContracts
        contracts = step.contracts
        step3 = step.step3
    }
    
    return {
        "minersEnergyData": minersEnergyData,
        "previousContracts": previousContracts,
        "previousAllocations": previousAllocations,
        "consumedContracts": consumedContracts,
        "contracts": contracts,
        "step3": step3
    }
}

async function _consumeContractRegardlessRegion(transactionFolderName, miners, minersEnergyData,
    previousAllocations, previousContracts, contract, step3, recsMultFactor = 1.5) {

    // Skip if contract has no more available credits left
    let recsAvailable = (typeof contract.volume_MWh != "number")
        ? Number((contract.volume_MWh.replace(",", ""))) : contract.volume_MWh
    contract.volume_MWh = recsAvailable         // Make sure we have a number here (step 2 has strings in some fields)!
    if(recsAvailable <= contract.volume_MWh_leftover) {
        console.log(`Contract ${contract.contract_id} has no more available RECs (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover})`)
        return {
            "contract": contract,
            "minersEnergyData": minersEnergyData,
            "previousContracts": previousContracts,
            "previousAllocations": previousAllocations,
            "step3": step3
        }
    }

    const country = contract.country
    const region = contract.region

    for await (const minerObj of miners) {
        const miner = minerObj.provider
        const minerSplit = minerObj.weight

        recsAvailable = (typeof contract.volume_MWh != "number")
            ? Number((contract.volume_MWh.replace(",", ""))) : contract.volume_MWh

        console.log(`Trying to allocate from contract ${contract.contract_id} (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}), to miner ${miner}`)

        if(recsAvailable <= contract.volume_MWh_leftover) {
            console.log(`Contract ${contract.contract_id} has no more available RECs (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}) for miner ${miner}`)
            break
        }

        // Get miner's energy consumption data
        // for contract reporting period
        const medIndex = `${miner}-${contract.reportingStart}-${contract.reportingEnd}`
        console.log(`Get energy data for ${miner} (${contract.reportingStart}, ${contract.reportingEnd})`)
        if(minersEnergyData[medIndex] == null)
//          minersEnergyData[medIndex] = await _totalEnergy(contract.reportingStart, contract.reportingEnd, miner)
        minersEnergyData[medIndex] = await _totalEnergyFromModel(moment(contract.reportingStart, "YYYY-MM-DD").format("YYYY-MM-DD"), moment(contract.reportingEnd, "YYYY-MM-DD").format("YYYY-MM-DD"), miner)
        console.log(`Got cumulative energy total (upper) ${minersEnergyData[medIndex].total_energy_upper_MWh} for ${miner} (${contract.reportingStart}, ${contract.reportingEnd})`)

        let palloc = previousAllocations
        .filter((a) => {
            return a != null && a.minerID == miner
        })
        .map((a) => {
            const pcontr = previousContracts
                .filter((c) => {
                    return a.contract_id == c.contract_id
                })
                .map((c) => {
                    return {
                        "reportingStart": c.reportingStart,
                        "reportingEnd": c.reportingEnd
                    }
            })
            return {
                "allocation": a.allocation_id,
                "contract": a.contract_id,
                "recs": a.volume_MWh,
                "defaulted": a.defaulted,
                "reportingStart": pcontr[0].reportingStart,
                "reportingEnd": pcontr[0].reportingEnd
            }
        }).filter((a) => {
            const overlapingDateRanges = moment(a.reportingStart, "YYYY-MM-DD").isSameOrBefore(moment(contract.reportingEnd, "YYYY-MM-DD"))
                && moment(a.reportingEnd, "YYYY-MM-DD").isSameOrAfter(moment(contract.reportingStart, "YYYY-MM-DD"))
            return (overlapingDateRanges && !a.defaulted)
        })

        if(minersEnergyData[medIndex].total_energy_upper_MWh > 0) {
            const recsNeeded = Math.ceil(minersEnergyData[medIndex].total_energy_upper_MWh * minerSplit * recsMultFactor)
            const totalRecsAllocated = palloc.reduce((prev, elem) => prev + elem.recs, 0)

            // We should decrease overlapping allocated volumes for periods/energy consuptions
            // which are out of curent contract allocation start/end period
            palloc = palloc
                .map(async (a) => {
                    // Previous allocation starts before current contract allocation
                    if(moment(a.reportingStart, "YYYY-MM-DD").isBefore(moment(contract.reportingStart, "YYYY-MM-DD"))) {
                        // Deduct allocation amount for energy spent before currenct contract allocation
                        const deductingPeriodEnd = moment(contract.reportingStart, "YYYY-MM-DD").add(-1, 'days').format('YYYY-MM-DD')
                        const energySpentBeforeCurrentAllocation = await _totalEnergyFromModel(moment(a.reportingStart, "YYYY-MM-DD").format("YYYY-MM-DD"), deductingPeriodEnd, miner)
                        const recsAllocatedBeforeCurrentAllocationStarts = Math.ceil(energySpentBeforeCurrentAllocation.total_energy_upper_MWh * recsMultFactor)
                        console.log(`Previous allocation start date ${a.reportingStart} is before current contract allocation start date ${contract.reportingStart}
                            so we will deduct ${recsAllocatedBeforeCurrentAllocationStarts} (${energySpentBeforeCurrentAllocation.total_energy_upper_MWh}) RECs from previous allocation ${a.recs}`)
                        a.recs = (a.recs >= recsAllocatedBeforeCurrentAllocationStarts) ? a.recs - recsAllocatedBeforeCurrentAllocationStarts : 0
                    }
                    // Previous allocation ends after current contract allocation
                    if(moment(a.reportingEnd, "YYYY-MM-DD").isAfter(moment(contract.reportingEnd, "YYYY-MM-DD"))) {
                        // Deduct allocation amount for energy spent after currenct contract allocation
                        const deductingPeriodStart = moment(contract.reportingEnd, "YYYY-MM-DD").add(1, 'days').format('YYYY-MM-DD')
                        const energySpentAfterCurrentAllocation = await _totalEnergyFromModel(deductingPeriodStart, moment(a.reportingEnd, "YYYY-MM-DD").format("YYYY-MM-DD"), miner)
                        const recsAllocatedAfterCurrentAllocationEnds = Math.ceil(energySpentAfterCurrentAllocation.total_energy_upper_MWh * recsMultFactor)
                        console.log(`Previous allocation end date ${a.reportingEnd} is after current contract allocation end date ${contract.reportingEnd}
                            so we will deduct ${recsAllocatedAfterCurrentAllocationEnds} (${energySpentAfterCurrentAllocation.total_energy_upper_MWh}) RECs from previous allocation ${a.recs}`)
                        a.recs = (a.recs >= recsAllocatedAfterCurrentAllocationEnds) ? a.recs - recsAllocatedAfterCurrentAllocationEnds : 0
                    }
                    return a
                })
            palloc = await all(palloc)

            const recsAllocated = palloc.reduce((prev, elem) => prev + elem.recs, 0)

            console.log(`Contract ${contract.contract_id} (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}), miner ${miner}
                recsNeeded ${recsNeeded} recsAllocated ${recsAllocated} (totalRecsAllocated ${totalRecsAllocated})`)

            // If we have already allocated this miner what he needs
            // or we have no more RECs available with this contract, then skip it
            if(recsNeeded <= recsAllocated)
                continue
            const newlyAllocated = (recsAvailable - contract.volume_MWh_leftover >= (recsNeeded - recsAllocated))
                ? (recsNeeded - recsAllocated) : recsAvailable - contract.volume_MWh_leftover
            contract.volume_MWh = recsAvailable - newlyAllocated

            const allocation = {
                allocation_id: `${transactionFolderName}_allocation_${step3.length+1}`,
                UUID: null,
                contract_id: contract.contract_id,
                minerID: miner,
                volume_MWh: newlyAllocated,
                defaulted: 0,
                step4_ZL_contract_complete: 0,
                step5_redemption_data_complete: 0,
                step6_attestation_info_complete: 0,
                step7_certificates_matched_to_supply: 0,
                step8_IPLDrecord_complete: 0,
                step9_transaction_complete: 0,
                step10_volta_complete: 0,
                step11_finalRecord_complete: 0
            }
            step3.push(allocation)
            previousAllocations.push(allocation)
            console.log(`Contract ${contract.contract_id} (available: ${recsAvailable}, total: ${contract.volume_MWh}, leftover: ${contract.volume_MWh_leftover}), miner ${miner}
                recsNeeded ${recsNeeded} recsAllocated ${recsAllocated} newlyAllocated ${newlyAllocated}`)
            const pcIds = previousContracts.map((pc) => {return pc.contract_id})
            if(pcIds.indexOf(contract.contract_id) == -1)
                previousContracts.push(contract)
        }
    }

    return {
        "contract": contract,
        "minersEnergyData": minersEnergyData,
        "previousContracts": previousContracts,
        "previousAllocations": previousAllocations,
        "step3": step3
    }
}

function _getMinerWeight(miner, gridMinersSplit) {
    let weight = 1

    const gridRegions = Object.keys(gridMinersSplit)
    for (const gridRegion of gridRegions) {
        const gridMiner = gridMinersSplit[gridRegion]
            .filter((gm) => {return gm.provider == miner.provider
                && gm.country == miner.country
                && gm.region == miner.region})[0]
        if(gridMiner != undefined) {
            weight = gridMiner.weight
            break
        }
    }

    return weight
}

/*
function _getFilecoinEnergyExportData(miner, dataType, start, end, limit, offset) {
    const self = this,
        getUri = 'https://api.filecoin.energy:443/models/export?code_name=' +
            ((dataType != undefined) ? dataType : 'TotalEnergyModelv_1_0_1') +	// in case of parameter missing -> total energy
            '&miner=' + miner +
            '&start=' + start +
            '&end=' + end +
            '&limit=' + ((limit != undefined) ? limit : 1000) +
            '&offset=' + ((offset != undefined) ? offset : 0)
    return axios(getUri, {
        method: 'get'
    })
}

async function _totalEnergy(start, end, miner){
    let limit = 1000, offset = 0
    const PUEupper = 1.93

    // Sealing request
    let sealingData, sumSealed = 0
    do {
        let serr = true
        while(serr) {
            try {
                sealingData = (await _getFilecoinEnergyExportData(miner, 'SealedModel', start, end, limit, offset)).data.data
                serr = null
            } catch (error) {
                serr = error
            }
        }
        sumSealed += sealingData.reduce((prev, elem) => prev + Number(elem.sealed_this_epoch_GiB), 0)
        offset += limit
    } while (sealingData.length)

    limit = 1000
    offset = 0
    // Storage request
    let capacityData = [], capacityDataBatch = []
    do {
        let cerr = true
        while(cerr) {
            try {
                capacityDataBatch = (await _getFilecoinEnergyExportData(miner, 'CapacityModel', start, end, limit, offset)).data.data
                cerr = null
            } catch (error) {
                cerr = error
            }
        }
        capacityData = capacityData.concat(capacityDataBatch)
        offset += limit
    } while (capacityDataBatch.length)
    let integratedGiBhours = 0
    let differenceTotalPeriodHours = 0
    if(capacityData.length) {
        // Add in records at the beginning and end, for the storage array
        // If we don't do this, the energy used to store files between the end points (of the request)
        // and the first/last data points returned will be zero
        const requestStartTime = moment(start)
        const firstBlockTime = moment(capacityData[0].timestamp)

        if (!firstBlockTime.isSame(requestStartTime)) {
            const newFirstRecord = {
                epoch: null,
                capacity_GiB: capacityData[0].capacity_GiB,
                timestamp: start+'T00:00:00.000Z'
            }
            capacityData.unshift(newFirstRecord)
        }

        const requestEndTime = moment(end+'T23:59:30.000Z')
        const lastBlockTime = moment(capacityData[capacityData.length - 1].timestamp)

        if (!requestEndTime.isSame(lastBlockTime)) {
            const newLastRecord = {
                epoch: null,
                capacity_GiB: capacityData[capacityData.length - 1].capacity_GiB,
                timestamp: end+'T23:59:30.000Z'
            }
            capacityData.push(newLastRecord)
        }

        // Find actual storage energy from API data
        const capacityDataWithElapsedTime = capacityData.map((elem, index) => {
            const prevTime = (index == 0) ? elem.timestamp : capacityData[index - 1].timestamp
            const hours = moment.duration(moment(elem.timestamp).diff(moment(prevTime))).asHours()
            elem["timeDiff_hours"] = hours
            elem["GiB_hours"] = Number(elem.capacity_GiB) * hours
            return elem
        })
        integratedGiBhours = capacityDataWithElapsedTime.reduce((prev, elem) => {
            return prev + elem.GiB_hours
        }, 0)

        // Calculate the whole time period of request, in hours
        const startingTime = moment(capacityDataWithElapsedTime[0].timestamp)
        const endingTime = moment(capacityDataWithElapsedTime[capacityDataWithElapsedTime.length - 1].timestamp)
        differenceTotalPeriodHours = moment.duration(endingTime.diff(startingTime)).asHours()
    }

    const sealingEnergyUpperMWh = sumSealed * 5.60E-8 * 1024**3 / 1E6
    const storageUpperIntegratedMWh = integratedGiBhours * 8.1E-12 * 1024**3 / 1E6
    const totalEnergyUpperMWhRecalc = (sealingEnergyUpperMWh + storageUpperIntegratedMWh) * PUEupper

    return {
        'minerID': miner,
        'start' : start,
        'end' : end,
        'totalSealed_GiB': sumSealed,
        'total_time_hours' : differenceTotalPeriodHours,
        'initial_capacity_GiB': (capacityData.length) ? capacityData[0].capacity_GiB : 0,
        'final_capacity_GiB': (capacityData.length) ? capacityData[capacityData.length - 1].capacity_GiB : 0,
        'time_average_capacity_GiB' : (differenceTotalPeriodHours) ? integratedGiBhours / differenceTotalPeriodHours : 0,
        'total_energy_upper_MWh' : totalEnergyUpperMWhRecalc
    }
}
*/
async function _getFilecoinEnergyModelData(miner, dataType, start, end, filter) {
    const getUri = 'https://api.filecoin.energy:443/models/model?code_name=' +
            ((dataType != undefined) ? dataType : 'TotalEnergyModelv_1_0_1') +	// in case of parameter missing -> total energy
            '&miner=' + miner +
            '&start=' + start +
            '&end=' + end +
            '&filter=' + ((filter != undefined) ? filter : 'day')
    return await axios(getUri, {
        method: 'get'
    })
}

async function _totalEnergyFromModel(start, end, miner){
    // Model request
    let upperDataPoints
    let merr = true
    while(merr) {
        try {
            const en = await _getFilecoinEnergyModelData(miner, 'CumulativeEnergyModel_v_1_0_1', start, end)
            upperDataPoints = en.data.data[2].data
            merr = null
        } catch (error) {
            merr = error
        }
    }

    return {
        'minerID': miner,
        'start' : start,
        'end' : end,
        'total_energy_upper_MWh' : (upperDataPoints.length) ? upperDataPoints[upperDataPoints.length  - 1].value / 1000 : 0
    }
}

// Create step 5 CSV
async function createStep5(transactionFolder, attestationFolder, network, networkId, tokenizationProtocol, tokenType,
    smartContractAddress, format, redemptionProcess, minersLocationsFile,
    externalBatchesFile, evidentRedemptionFileName, redemptionsSheetName, beneficiariesSheetName,
    batchId) {
    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const attestationFolderPathChunks = attestationFolder.split("/")
    const attestationFolderName = attestationFolderPathChunks[attestationFolderPathChunks.length-1]

    let beneficiary, redemptionPurpose

    const step5Header = ['"attestation_id"', '"redemption_process"', '"contract_id"', '"allocation_id"',
        '"smart_contract_address"', '"batchID"', '"network"', '"zl_protocol_version"', '"minerID"',
        '"beneficiary"', '"beneficiary_country"', '"beneficiary_location"', '"supply_country"',
        '"volume_required"', '"start_date"', '"end_date"', '"redemption_purpose"', '"attestation_folder"']
    const step5ColumnTypes = ["string", "string", "string", "string",
        "string", "number", "number", "string", "string",
        "string", "string", "string", "string",
        "string", "string", "string", "string", "string"]

    let externalBatches = []
    if(externalBatchesFile != undefined) {
        externalBatches = (await getCsvAndParseToJson(`./${externalBatchesFile}`))
            .map((b) => {return b.batch})
    }

    let step2 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step2FileNameSuffix}`)
    // Filter only ones already matched against miner IDs
    if(redemptionProcess.toLowerCase() == "automatic")
        step2 = step2.filter((c) => {
            return c.step3_match_complete == 1
        })

    const step3 = (redemptionProcess.toLowerCase() == "automatic") ? await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step3FileNameSuffix}`) : []
    let step5 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`)
    if(step5 == null)
        step5 = []
    let evidentIndex = 0
    let loadFilePath, saveFilePath, buf, workbook, redemptionWorkSheet, newBeneficiariesWorkSheet
    let syntheticLocations

    if(evidentRedemptionFileName != null && redemptionsSheetName != null && beneficiariesSheetName != null
        && minersLocationsFile != null) {
        // Redemption file path
        loadFilePath = `${transactionFolder}/${evidentRedemptionFileName}`
        saveFilePath = `${transactionFolder}/fulfilled-${evidentRedemptionFileName}`
        // Load redemption xls into a buffer
        buf = fs.readFileSync(loadFilePath)
        // Load buffer into a workbook
        workbook = xlsxRead(buf)
        // Get redemptions worksheet
        redemptionWorkSheet = workbook.Sheets[redemptionsSheetName]
        // Get new beneficiaries worksheet
        newBeneficiariesWorkSheet = workbook.Sheets[beneficiariesSheetName]

        const syntheticLocationsFilePath = `./${transactionFolder}/_assets/${minersLocationsFile}`
        syntheticLocations = await fs.promises.readFile(syntheticLocationsFilePath, {
            encoding:'utf8',
            flag:'r'
        })
        syntheticLocations = JSON.parse(syntheticLocations).regions           // when using synthetic-country-state-province-latest.json

        for (const att of step5) {
                // Add item to evident redemptions work sheet
                redemptionWorkSheet = xlsxUtils.sheet_add_aoa(redemptionWorkSheet, [
                    [att.beneficiary, att.beneficiary_country, att.beneficiary_location, , att.volume_required, att.start_date, att.end_date, att.redemption_purpose]
                ], { origin: `B${evidentIndex+3}` })

                // Add item to evident new beneficiaries work sheet
                newBeneficiariesWorkSheet = xlsxUtils.sheet_add_aoa(newBeneficiariesWorkSheet, [
                    [att.beneficiary, att.beneficiary_country, att.beneficiary_location]
                ], { origin: `A${evidentIndex+4}` })

                evidentIndex++
        }
    }

    for (let contractIndex = 0; contractIndex < step2.length; contractIndex++) {
        let attestations = []
        const contract = step2[contractIndex]
        const contractId = contract.contract_id
        const productType = contract.productType
//        if(redemptionProcess.toLowerCase() == "automatic" && productType.toUpperCase() == "IREC") {
        if(redemptionProcess.toLowerCase() == "automatic") {
            // Automatic (1:1)
            const allocations = step3.filter((a) => {return a.contract_id == contractId})
            attestations = allocations.map((a) => {
                // Find miner location
                let location
                let locationObjects = syntheticLocations.filter((l) => {
                    return l.provider == a.minerID && l.region.indexOf(contract.country) == 0
                })
                if(locationObjects.length == 0) {
                    location = null
                }
                else {
                    location = locationObjects[0].region
                }
                return {
                    "network": network,
                    "networkId": networkId,
                    "contractId": contractId,
                    "allocationId": a.allocation_id,
                    "volumeRequired": a.volume_MWh,
                    "tokenizationProtocol": tokenizationProtocol,
                    "smartContractAddress": smartContractAddress,
                    "tokenType": tokenType,
                    "minerId": a.minerID,
                    "minerLocation": location,
                    "RECs": a.volume_MWh,
                    "redemptionProcess": redemptionProcess.toLowerCase()
                }
            })
        }
        else {
            // Multiple minerIds per attestation
            attestations = [{
                "network": network,
                "networkId": networkId,
                "contractId": contractId,
                "allocationId": null,
                "volumeRequired": contract.volume_MWh,
                "tokenizationProtocol": tokenizationProtocol,
                "smartContractAddress": smartContractAddress,
                "tokenType": tokenType,
                "minerId": null,
                "minerLocation": null,
                "RECs": null,
                "redemptionProcess": redemptionProcess.toLowerCase()
            }]
        }

        let skipBatches = 0
        let batch
        
        switch (tokenizationProtocol) {
            case "ZL 1.0.0":
                batch = 0
                batchId = Number(batchId)
                break
            case "ZLv1.1.0a":
                batch = ""
                break
            default:
                console.error(`Unrecognized tokenization protocol ${tokenizationProtocol}.`)
                return new Promise((resolve) => {
                    resolve(null)
                })
        }

        for (let attestationIndex = 0; attestationIndex < attestations.length; attestationIndex++) {
            const attestation = attestations[attestationIndex]

            if((redemptionProcess.toLowerCase() == "automatic"
                && step5.filter((s5)=>{return s5.allocation_id == attestation.allocationId}).length)
                    || (redemptionProcess.toLowerCase() != "automatic"
                        && step5.filter((s5)=>{return s5.contract_id == attestation.contractId}).length)) {
                continue
            }

            switch (tokenizationProtocol) {
                case "ZL 1.0.0":
                    // make sure batchId is not in external batches list
                    batch = batchId + step5.length + skipBatches
                    while(externalBatches.indexOf(batch) != -1) {
                        skipBatches++
                        batch = batchId + step5.length + skipBatches
                    }
                    break
                case "ZLv1.1.0a":
                    const size = 64
                    batch = genRandomHex(size)
                    batch = `0x${batch}`
                    break
                default:
                    console.error(`Unrecognized tokenization protocol ${tokenizationProtocol}.`)
                    return new Promise((resolve) => {
                        resolve(null)
                    })
            }

            switch (format) {
                case "long":
                    beneficiary = `Blockchain Network ID: ${attestation.networkId} - Tokenization Protocol: ${attestation.tokenizationProtocol} - Smart Contract Address: ${attestation.smartContractAddress} - Batch ID: ${batch} ${(attestation.minerId != null) ? '- Filecoin minerID ' + attestation.minerId : ''}`
                    redemptionPurpose = `The certificates are redeemed (= assigned to the beneficiary) for the purpose of tokenization and bridging to the Blockchain: Energy Web Chain with the Network ID ${attestation.networkId}. The smart contract address is ${attestation.smartContractAddress} and the specific certificate batch ID is ${batch}. The certificates will be created as tokens of type ${attestation.tokenType} ${(attestation.minerId != null) ? 'This redemption is matched to Filecoin minerID ' + attestation.minerId : ''}`
                    break
                case "short":
                    beneficiary = `${attestation.smartContractAddress}-${batch}`
                    redemptionPurpose = `${attestation.tokenizationProtocol}-NWID${attestation.networkId}-${attestation.tokenType}`
                    break
                case "cid":
                    const ipfsNodeAddr = '/dns4/sandbox.co2.storage/tcp/5002/https'
                    const ipfs = await createClient(ipfsNodeAddr)
                    const beneficiaryData = {
                        "Description": "Redeemed for the purpose of tokenization",
                        "Protocol": `${attestation.tokenizationProtocol}`,
                        "Blockchain": `${attestation.network} - ID ${attestation.networkId}`,
                        "Address": attestation.smartContractAddress,
                        "Batch": batch,
                        "Beneficiary": (redemptionProcess.toLowerCase() == "automatic") ? attestation.minerId : null
                    }
                    const beneficiaryDataCid = await ipfs.add(JSON.stringify(beneficiaryData), {
                        'cidVersion': 0,
                        'hashAlg': 'sha2-256'
                    })
                    beneficiary = `CID:${beneficiaryDataCid.cid.toString()}`
                    redemptionPurpose = `Zero Labs Tokenization - go to ipfs.io/ipfs/_CID_`
                    break
                default:
                    console.error(`Unrecognized beneficiary format ${format}. Expected values are 'long', 'short', or 'cid'.`)
                    return new Promise((resolve) => {
                        resolve(null)
                    })
            }

            const attestationId = `${transactionFolderName}_attestation_${step5.length + 1}`
            const attestationRecord = {
                attestation_id: attestationId,
                redemption_process: attestation.redemptionProcess,
                contract_id: attestation.contractId,
                allocation_id: attestation.allocationId,
                smart_contract_address: smartContractAddress,
                batchID: batch,
                network: networkId,
                zl_protocol_version: tokenizationProtocol,
                minerID: attestation.minerId,
                beneficiary: beneficiary,
                beneficiary_country: (attestation.minerLocation != null) ? attestation.minerLocation.split("-")[0] : contract.country,
                beneficiary_location: attestation.minerLocation,
                supply_country: contract.country,
                volume_required: attestation.volumeRequired,
                start_date: contract.reportingStart,
                end_date: contract.reportingEnd,
                redemption_purpose: redemptionPurpose,
                attestation_folder: attestationFolderName
            }
            if(redemptionProcess.toLowerCase() == "automatic" && !step5.filter((s5)=>{return s5.allocation_id == attestation.allocationId}).length) {
                step5.push(attestationRecord)
                console.log(`Attestation ${attestationId} is created (${beneficiary}).`)
            }
            else if(!step5.filter((s5)=>{return s5.contract_id == attestation.contractId}).length){
                step5.push(attestationRecord)
                console.log(`Attestation ${attestationId} is created (${beneficiary}).`)
            }
            else {
                console.log(`Attestation for ${attestation.contractId}${(attestation.allocationId) ? ', ' + attestation.allocationId : ''} already exists.`)
            }

            if(evidentRedemptionFileName == null || redemptionsSheetName == null || beneficiariesSheetName == null
                || minersLocationsFile == null)
                continue

            if(attestation.minerId != null) {
                // Add item to evident redemptions work sheet
                redemptionWorkSheet = xlsxUtils.sheet_add_aoa(redemptionWorkSheet, [
                    [beneficiary, contract.country, attestation.minerLocation, , attestation.RECs, contract.reportingStart, contract.reportingEnd, redemptionPurpose]
                ], { origin: `B${evidentIndex+3}` })

                // Add item to evident new beneficiaries work sheet
                newBeneficiariesWorkSheet = xlsxUtils.sheet_add_aoa(newBeneficiariesWorkSheet, [
                    [beneficiary, contract.country, attestation.minerLocation]
                ], { origin: `A${evidentIndex+4}` })

                evidentIndex++
            }
        }
    }

    if(evidentRedemptionFileName != null && redemptionsSheetName != null && beneficiariesSheetName != null
        && minersLocationsFile != null) {
        workbook.Sheets[redemptionsSheetName] = redemptionWorkSheet
        workbook.Sheets[beneficiariesSheetName] = newBeneficiariesWorkSheet
        //    buf = xlsxWrite(workbook, {type: "buffer", bookType: "xlsb"})
        //    workbook = xlsxWriteFile(evidentRedemptionFileName, buf)
        xlsxWriteFile(workbook, saveFilePath)
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

// Split step 5 CSV
async function splitStep5(transactionFolder) {
    const transactionFolderPathChunks = transactionFolder.split("/")
    const transactionFolderName = transactionFolderPathChunks[transactionFolderPathChunks.length-1]

    const step5Header = ['"attestation_id"', '"redemption_process"', '"contract_id"', '"allocation_id"',
        '"smart_contract_address"', '"batchID"', '"network"', '"zl_protocol_version"', '"minerID"',
        '"beneficiary"', '"beneficiary_country"', '"beneficiary_location"', '"supply_country"',
        '"volume_required"', '"start_date"', '"end_date"', '"redemption_purpose"', '"attestation_folder"']
    const step5ColumnTypes = ["string", "string", "string", "string",
        "string", "number", "number", "string", "string",
        "string", "string", "string", "string",
        "string", "string", "string", "string", "string"]

    const step5 = await getCsvAndParseToJson(`${transactionFolder}/${transactionFolderName}${step5FileNameSuffix}`)
    // Filter manual redemption process attestations
    const step5Manual = step5.filter((a) => {
        return a.redemption_process == "manual"
    })
    // Filter automatic redemption process attestations
    const step5Automatic = step5.filter((a) => {
        return a.redemption_process == "automatic"
    })

    const resultManual = (step5Manual.length > 0) ?
        step5Header.join(",") + "\r\n" +
            Papa.unparse(step5Manual, {
                quotes: step5ColumnTypes.map((ct) => {return ct != 'number'}),
                quoteChar: '"',
                escapeChar: '"',
                delimiter: ",",
                header: false,
                newline: "\r\n",
                skipEmptyLines: false,
                columns: null
            })
            : null

    const resultAutomatic = (step5Automatic.length > 0) ?
        step5Header.join(",") + "\r\n" +
            Papa.unparse(step5Automatic, {
                quotes: step5ColumnTypes.map((ct) => {return ct != 'number'}),
                quoteChar: '"',
                escapeChar: '"',
                delimiter: ",",
                header: false,
                newline: "\r\n",
                skipEmptyLines: false,
                columns: null
            })
            : null
    
    const result = {
        manual: resultManual,
        automatic: resultAutomatic
    }

    return new Promise((resolve) => {
        resolve(result)
    })
}

// Create purchase order CSV
async function createPurchaseOrder(purchaseOrderFolder, minersLocationsFile, nercGeoJsonFile, fromYear, fromQuarter, energyFactor, nercRegionsMappingFile) {
    const purchaseOrderFolderPathChunks = purchaseOrderFolder.split("/")

    const purchaseOrderHeader = ['"Region"', '"Quarter"', '"Storage Provider"',
        '"Energy upper bound [MWh]"', '"Energy upper bound * energy coeficient [MWh]"', '"Energy upper bound * energy coeficient * region weight [MWh]"', '"Energy upper bound * energy coeficient * region weight * progressive factor [MWh]"',
        '"Previously allocated RECs (proportional)"', '"Needed RECs"']
    const purchaseOrderColumnTypes = ["string", "string", "string",
        "number", "number", "number", "number",
        "number", "number"]

    let purchaseOrder = []

    let previousAllocations = []
    let previousContracts = []
    const transactionFolders = fs.readdirSync("./").filter((file) => {
        return file.indexOf("_transaction_") > -1
    })

    for (const transactionFol of transactionFolders) {
        const transactionFolPathChunks = transactionFol.split("/")
        const transactionFolName = transactionFolPathChunks[transactionFolPathChunks.length-1]
        const alloc = await getCsvAndParseToJson(`${transactionFol}/${transactionFolName}${step3FileNameSuffix}`)
        if(alloc != null)
            previousAllocations = previousAllocations.concat(alloc)
        let contr = await getCsvAndParseToJson(`${transactionFol}/${transactionFolName}${step2FileNameSuffix}`)
        if(contr != null) {
            contr = contr.filter((c) => {return c.step3_match_complete == 1})
            previousContracts = previousContracts.concat(contr)
        }
    }

    const syntheticLocationsFilePath = `./${purchaseOrderFolder}/_assets/${minersLocationsFile}`
    let syntheticLocations = await fs.promises.readFile(syntheticLocationsFilePath, {
        encoding:'utf8',
        flag:'r'
    })
    syntheticLocations = JSON.parse(syntheticLocations).providerLocations

    let L = HL()    // headless leaflet
    let map = L.map(L.document.createElement('div'))

    const nercGeoJsonFilePath = `./${purchaseOrderFolder}/_assets/${nercGeoJsonFile}`
    let nercGeoJson = await fs.promises.readFile(nercGeoJsonFilePath, {
        encoding:'utf8',
        flag:'r'
    })
    nercGeoJson = JSON.parse(nercGeoJson)

    const nercGeoJsonLayer = L.geoJSON(nercGeoJson).addTo(map)

    let nercRegionsMapping = {}
    if(nercRegionsMappingFile != undefined) {
        const nercRegionsMappingFilePath = `./${assetsFolder}/_assets/${nercRegionsMappingFile}`
        nercRegionsMapping = await fs.promises.readFile(nercRegionsMappingFilePath, {
            encoding:'utf8',
            flag:'r'
        })
        nercRegionsMapping = JSON.parse(nercRegionsMapping)
    }
    let regionsWithMappings = Object.keys(nercRegionsMapping)

    let minersInRegion = {}

    for (const location of syntheticLocations) {
        const country = location.country
        const region = location.region
        const miner = location.provider
        
        const countries = syntheticLocations
            .filter((l) => {return l.provider == location.provider})
            .map((l) => {return l.country})

        const regions = syntheticLocations
            .filter((l) => {return l.provider == location.provider})
            .map((l) => {return l.region})

        location.countries = countries
        location.regions = regions

        await new Promise(resolve => setTimeout(resolve, 1))

        let nercRegion = (regionsWithMappings.indexOf(country) > -1) ? nercRegionsMapping[country] : country
        if (region != null) {
            const m = L.marker([location.lat, location.long])
            let found = false
            await nercGeoJsonLayer.eachLayer((layer) => {
                const nercRegionName = layer.feature.properties.NAME
                if(!found && layer.contains(m.getLatLng())) {
                    nercRegion = nercRegionName.substring(nercRegionName.indexOf("(")+1, nercRegionName.indexOf(")"))
                    found = true
                    console.log(`${location.region} (${location.lat}, ${location.long}) -> ${nercRegion}`)
/*
                    switch (nercRegionName) {
                        case "WESTERN ELECTRICITY COORDINATING COUNCIL (WECC)":
                            nercRegion = "WECC"
                            break
                        case "TEXAS RELIABILITY ENTITY (TRE)":
                            nercRegion = "TRE"
                            break
                        case "FLORIDA RELIABILITY COORDINATING COUNCIL (FRCC)":
                            nercRegion = "FRCC"
                            break
                        case "NORTHEAST POWER COORDINATING COUNCIL (NPCC)":
                            nercRegion = "NPCC"
                            break
                        case "SOUTHWEST POWER POOL, RE (SPP)":
                            nercRegion = "SPP"
                            break
                        case "SERC RELIABILITY CORPORATION (SERC)":
                            nercRegion = "SERC"
                            break
                        case "MIDWEST RELIABILITY ORGANIZATION (MRO)":
                            nercRegion = "MRO"
                            break
                        default:
                            break
                    }
*/
                }
            })
        }
        location.nercRegion = nercRegion

        if (minersInRegion[nercRegion] == undefined)
            minersInRegion[nercRegion] = []
        minersInRegion[nercRegion].push(JSON.parse(JSON.stringify(location)))
    }

    console.dir(minersInRegion, {depth: null})

    const nercRegions = Object.keys(minersInRegion)
    for (const nr of nercRegions) {
        let regionMiners = minersInRegion[nr]

        for (const rm of regionMiners) {
            const occurences = regionMiners.filter((obj) => obj.provider === rm.provider).length
            rm.weight = occurences / rm.numLocations
        }

        regionMiners = regionMiners.filter((value, index, self) =>
            index === self.findIndex((t) => (
                t.provider === value.provider
            ))
        ).map((rm) => {return {
            "provider": rm.provider,
            "country": rm.country,
            "region": rm.region,
            "nercRegion": rm.nercRegion,
            "weight": rm.weight
        }})

        for await(const rm of regionMiners) {
            for await(const quarterObj of _listQuarters(fromYear, parseInt(fromQuarter, 10))) {
                const allocations = previousAllocations.filter((a) => {return a.minerID == rm.provider && a.defaulted == 0})
                let allocatedVolume = 0
                for (const allocation of allocations) {
                    const contractId = allocation.contract_id
                    const volume = allocation.volume_MWh
                    const contract = previousContracts.filter((c) => {return c.contract_id == contractId})[0]
                    const reportingStart = moment(contract.reportingStart, "YYYY-MM-DD")
                    const reportingEnd = moment(contract.reportingEnd, "YYYY-MM-DD")
                    const quarterStart = moment(quarterObj.quarterStart, "YYYY-MM-DD")
                    const quarterEnd = moment(quarterObj.quarterEnd, "YYYY-MM-DD")
                    if(reportingStart.isSameOrBefore(quarterEnd)
                        && reportingEnd.isSameOrAfter(quarterStart)) {
                            const start = (reportingStart.isSameOrAfter(quarterStart)) ? reportingStart : quarterStart
                            const end = (reportingEnd.isSameOrBefore(quarterEnd)) ? reportingEnd : quarterEnd
                            const overlap = (end.diff(start, 'days')) / (reportingEnd.diff(reportingStart, 'days'))
                            allocatedVolume += Math.round(overlap * volume)
                            console.log(`${volume}, ${reportingStart.format("YYYY-MM-DD")}, ${reportingEnd.format("YYYY-MM-DD")}, ${quarterStart.format("YYYY-MM-DD")}, ${quarterEnd.format("YYYY-MM-DD")}, ${start.format("YYYY-MM-DD")}, ${end.format("YYYY-MM-DD")}, ${overlap}, ${overlap * volume}, ${allocatedVolume}`)
                        }
                }
                const energyUpperBound = (await _totalEnergyFromModel(quarterObj.quarterStart, quarterObj.quarterEnd,
                    rm.provider)).total_energy_upper_MWh
                const progressiveFactor = (quarterObj.daysLeftInQuarter > 1) ? (quarterObj.daysInQuarter/(quarterObj.daysInQuarter - quarterObj.daysLeftInQuarter)) : 1
                console.log(`${nr}, ${quarterObj.year}-${quarterObj.quarter}, ${rm.provider}, ${energyUpperBound}, ${energyUpperBound * parseFloat(energyFactor)}, ${energyUpperBound * parseFloat(energyFactor) * rm.weight}, ${energyUpperBound * parseFloat(energyFactor) * rm.weight * progressiveFactor}, ${allocatedVolume}, ${Math.ceil(energyUpperBound * parseFloat(energyFactor) * rm.weight * progressiveFactor) - allocatedVolume}`)
                purchaseOrder.push([nr, `${quarterObj.year}-${quarterObj.quarter}`, rm.provider,
                    energyUpperBound, energyUpperBound * parseFloat(energyFactor), energyUpperBound * parseFloat(energyFactor) * rm.weight,
                    energyUpperBound * parseFloat(energyFactor) * rm.weight * progressiveFactor, allocatedVolume, Math.ceil(energyUpperBound * parseFloat(energyFactor) * rm.weight * progressiveFactor) - allocatedVolume])
            }
        }
    }

    let result = purchaseOrderHeader.join(",") + "\r\n" +
        Papa.unparse(purchaseOrder, {
            quotes: purchaseOrderColumnTypes.map((ct) => {return ct != 'number'}),
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

async function createGridRegions(assetsFolder, minersLocationsFile, nercGeoJsonFile, nercRegionsMappingFile) {
    let gridMiners = {}

    const syntheticLocationsFilePath = `./${assetsFolder}/_assets/${minersLocationsFile}`
    let syntheticLocations = await fs.promises.readFile(syntheticLocationsFilePath, {
        encoding:'utf8',
        flag:'r'
    })
    syntheticLocations = JSON.parse(syntheticLocations).providerLocations

    let L = HL()    // headless leaflet
    let map = L.map(L.document.createElement('div'))

    const nercGeoJsonFilePath = `./${assetsFolder}/_assets/${nercGeoJsonFile}`
    let nercGeoJson = await fs.promises.readFile(nercGeoJsonFilePath, {
        encoding:'utf8',
        flag:'r'
    })
    nercGeoJson = JSON.parse(nercGeoJson)

    const nercGeoJsonLayer = L.geoJSON(nercGeoJson).addTo(map)

    let nercRegionsMapping = {}
    if(nercRegionsMappingFile != undefined) {
        const nercRegionsMappingFilePath = `./${assetsFolder}/_assets/${nercRegionsMappingFile}`
        nercRegionsMapping = await fs.promises.readFile(nercRegionsMappingFilePath, {
            encoding:'utf8',
            flag:'r'
        })
        nercRegionsMapping = JSON.parse(nercRegionsMapping)
    }
    let regionsWithMappings = Object.keys(nercRegionsMapping)

    let minersInRegion = {}

    for (const location of syntheticLocations) {
        const country = location.country
        const region = location.region
        const miner = location.provider
        
        const countries = syntheticLocations
            .filter((l) => {return l.provider == location.provider})
            .map((l) => {return l.country})

        const regions = syntheticLocations
            .filter((l) => {return l.provider == location.provider})
            .map((l) => {return l.region})

        location.countries = countries
        location.regions = regions

        await new Promise(resolve => setTimeout(resolve, 1))

        let nercRegion = (regionsWithMappings.indexOf(country) > -1) ? nercRegionsMapping[country] : country
        if (region != null) {
            const m = L.marker([location.lat, location.long])
            let found = false
            await nercGeoJsonLayer.eachLayer((layer) => {
                const nercRegionName = layer.feature.properties.NAME
                if(!found && layer.contains(m.getLatLng())) {
                    nercRegion = nercRegionName.substring(nercRegionName.indexOf("(")+1, nercRegionName.indexOf(")"))
                    found = true
                    console.log(`${location.region} (${location.lat}, ${location.long}) -> ${nercRegion}`)
/*
                    switch (nercRegionName) {
                        case "WESTERN ELECTRICITY COORDINATING COUNCIL (WECC)":
                            nercRegion = "WECC"
                            break
                        case "TEXAS RELIABILITY ENTITY (TRE)":
                            nercRegion = "TRE"
                            break
                        case "FLORIDA RELIABILITY COORDINATING COUNCIL (FRCC)":
                            nercRegion = "FRCC"
                            break
                        case "NORTHEAST POWER COORDINATING COUNCIL (NPCC)":
                            nercRegion = "NPCC"
                            break
                        case "SOUTHWEST POWER POOL, RE (SPP)":
                            nercRegion = "SPP"
                            break
                        case "SERC RELIABILITY CORPORATION (SERC)":
                            nercRegion = "SERC"
                            break
                        case "MIDWEST RELIABILITY ORGANIZATION (MRO)":
                            nercRegion = "MRO"
                            break
                        default:
                            break
                    }
*/
                }
            })
        }
        location.nercRegion = nercRegion

        if (minersInRegion[nercRegion] == undefined)
            minersInRegion[nercRegion] = []
        minersInRegion[nercRegion].push(JSON.parse(JSON.stringify(location)))
    }

    const nercRegions = Object.keys(minersInRegion)
    for (const nr of nercRegions) {
        let regionMiners = minersInRegion[nr]

        for (const rm of regionMiners) {
            const occurences = regionMiners.filter((obj) => obj.provider === rm.provider).length
            rm.weight = occurences / rm.numLocations
        }

        regionMiners = regionMiners.filter((value, index, self) =>
            index === self.findIndex((t) => (
                t.provider === value.provider
            ))
        ).map((rm) => {return {
            "provider": rm.provider,
            "country": rm.country,
            "region": rm.region,
            "nercRegion": rm.nercRegion,
            "weight": rm.weight
        }})

        gridMiners[nr] = regionMiners
    }

    console.dir(gridMiners, {depth: null})

    return JSON.stringify(gridMiners)
}

function _getQuarter(date = new Date()) {
    return Math.floor(date.getMonth() / 3 + 1)
}
  
function _daysLeftInQuarter(date = new Date()) {
    const quarter = _getQuarter(date)

    const nextQuarter = new Date()

    if (quarter === 4) {
        nextQuarter.setFullYear(date.getFullYear() + 1, 0, 1)
    } else {
        nextQuarter.setFullYear(date.getFullYear(), quarter * 3, 1)
    }

    const ms1 = date.getTime()
    const ms2 = nextQuarter.getTime()

    return Math.floor((ms2 - ms1) / (24 * 60 * 60 * 1000))
}

function _dateQuarter(date = new Date()) {
    const quarter = _getQuarter(date)
    const daysLeftInQuarter = _daysLeftInQuarter(date)
    const year = date.getFullYear()
    let daysInQuarter, quarterStart, quarterEnd

    switch (quarter) {
        case 1:
            quarterStart = year + "-01-01"
            quarterEnd = year + "-03-31"
            daysInQuarter = _daysLeftInQuarter(new Date(year, 0, 1))
            break
        case 2:
            quarterStart = year + "-04-01"
            quarterEnd = year + "-06-30"
            daysInQuarter = _daysLeftInQuarter(new Date(year, 3, 1))
            break
        case 3:
            quarterStart = year + "-07-01"
            quarterEnd = year + "-09-30"
            daysInQuarter = _daysLeftInQuarter(new Date(year, 6, 1))
            break
        case 4:
            quarterStart = year + "-10-01"
            quarterEnd = year + "-12-31"
            daysInQuarter = _daysLeftInQuarter(new Date(year, 9, 1))
            break               
        default:
            console.log(`Invalid quarter ${quarter}`)
            break
    }

    return {
        "year": year,
        "quarter": quarter,
        "daysInQuarter": daysInQuarter,
        "daysLeftInQuarter": daysLeftInQuarter,
        "quarterStart": quarterStart,
        "quarterEnd": quarterEnd
    }
}

function _listQuarters(year, quarter) {
    const date = new Date()
    const toQuarter = _getQuarter(date)
    const toYear = date.getFullYear()
    let result = []
    
    while(year < toYear || (year == toYear && quarter <= toQuarter)) {
        if(year == toYear && quarter == toQuarter)
            result.push(_dateQuarter(date))
        else
            switch (quarter) {
                case 1:
                    result.push(_dateQuarter(new Date(year, 2, 31)))
                    break
                case 2:
                    result.push(_dateQuarter(new Date(year, 5, 30)))
                    break
                case 3:
                    result.push(_dateQuarter(new Date(year, 8, 30)))
                    break
                case 4:
                    result.push(_dateQuarter(new Date(year, 11, 31)))
                    break               
                default:
                    console.log(`Invalid quarter ${quarter}`)
                    break
            }

        if(quarter == 4) {
            quarter = 1
            year++
        }
        else {
            quarter++
        }
    }

    return result
}