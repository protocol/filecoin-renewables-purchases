import fs from 'fs'
import Papa from 'papaparse'

// We'll do logging to fs
let access = fs.createWriteStream(`./logs/csv-${(new Date()).toISOString()}.log`);
process.stdout.write = process.stderr.write = access.write.bind(access);

// Check for provided parameters
const args = process.argv.slice(2)
const activities = args[0]
switch (activities) {
    case 'fix-dates':
        const filePath = args[1]
        const dateColumns = args[2]
        if(dateColumns == null) {
            console.error(`Error! Bad argument provided. date columns is required parameter.`)
            await new Promise(resolve => setTimeout(resolve, 100));
            process.exit()
        }
        const jsonContent = await getCsvAndParseToJson(filePath)
        const fixedDates = await fixDates(jsonContent, dateColumns)   // comma separated list of date columns
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
                const correction = guessValidDate(item[column])
                console.log(`I guess it should be ${correction}`)
            }
        }
        line++
    }
    
    return new Promise((resolve) => {
        resolve(columns)
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

await new Promise(resolve => setTimeout(resolve, 1000));
process.exit()
