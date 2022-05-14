import fs from 'fs'
import Papa from 'papaparse'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/csv-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Check for provided parameters
const args = process.argv.slice(2)
const activities = args[0]
let filePath
let jsonContent
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
    default:
        console.error(`Error! Bad argument provided. ${activities} are not supported.`)
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


await new Promise(resolve => setTimeout(resolve, 1000));
process.exit()
