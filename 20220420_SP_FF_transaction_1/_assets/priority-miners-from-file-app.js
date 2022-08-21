import fs from 'fs'

const fileAppFilePath = `./file.app.json`
let fileApp = await fs.promises.readFile(fileAppFilePath, {
    encoding:'utf8',
    flag:'r'
})
fileApp = JSON.parse(fileApp)

fileApp = fileApp.map((m) => {return m.address})

await fs.promises.writeFile(`${fileAppFilePath}.miners.json`, JSON.stringify(fileApp))

