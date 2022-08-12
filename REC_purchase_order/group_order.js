let fs = require('fs');
const { parse } = require('csv-parse/sync');
const json2csvparse = require('json2csv');
const country_codes = require('../country_codes_irec_reformatted.json')
const nerc = require('../NERC_regions.json')


const country_codes_invert = {}
for(var key in country_codes){
  country_codes_invert[country_codes[key]] = key
}
// console.log(country_codes_invert)


function groupByCountry(inputs, outputName_prefix){

  // Find all of the countries represented
  country_list = inputs.reduce( (prev, elem) => {
    if (! prev.includes(elem.Region)) {prev.push(elem.Region)}
    return prev
  }, [])

  // For each country, find the amount of renewable energy
  total_by_country = country_list.reduce( (prev, elem) => {
    forThisCountry = inputs.filter(datapoint => datapoint.Region == elem)
    energyTotal = forThisCountry.reduce((prev, elem) => {
      return prev + elem['Energy upper bound * energy coeficient * region weight * progressive factor [MWh]']
    }, 0)


    country_code = elem
    region = ''

    if (! (elem in country_codes_invert)){
      country_code = "US"
      region = elem
    }

    prev.push({
      "country_name":country_codes_invert[country_code],
      "country_code":country_code,
      "region":region,
      "energy_MWh":Math.ceil(energyTotal)
    })
    return prev
  }, [])

  fs.writeFileSync(`${outputName_prefix}_by_country.csv`, json2csvparse.parse(total_by_country))


  // Do another version where we group by quarter as well
  total_by_country_quarter = country_list.reduce( (prev, elem) => {
    forThisCountry = inputs.filter(datapoint => datapoint.Region == elem)

    // Find the quarters where an SP consumed energy in this country
    quarter_list = forThisCountry.reduce( (prev, elem) => {
      if (! prev.includes(elem.Quarter)) {prev.push(elem.Quarter)}
      return prev
    }, [])

    // console.log(quarter_list)

    var quarter_totals = {}
    quarter_list.forEach(quarter => {
      forThisCountryQuarter = forThisCountry.filter(datapoint => datapoint.Quarter == quarter)

      energyTotal = forThisCountryQuarter.reduce((prev, elem) => {
        return prev + elem['Energy upper bound * energy coeficient * region weight * progressive factor [MWh]']
      }, 0)

      quarter_totals[quarter] = Math.ceil(energyTotal)

    })


    country_code = elem
    region = ''

    if (! (elem in country_codes_invert)){
      country_code = "US"
      region = elem
    }

    country_level = {
      "country_name":country_codes_invert[country_code],
      "country_code":country_code,
      "region":region
    }

    prev.push({...country_level,...quarter_totals})
    return prev
  }, [])

  fs.writeFileSync(`${outputName_prefix}_by_country_quarter.csv`, json2csvparse.parse(total_by_country_quarter))

}

// Look at total projected energy purchases, by country
const folder = '20220811_REC_purchase'
const raw_order = 'REC_purchase_order.csv'
const raw_order_data = parse(fs.readFileSync(`${folder}/${raw_order}`, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
groupByCountry(raw_order_data, `${folder}/total`)


// Look at ones with active deals only
const fil_app_filename = '20220809_fil_app_export_minerIDs.csv'
const fil_app = parse(fs.readFileSync(`${folder}/${fil_app_filename}`, {encoding:'utf8', flag:'r'}), {columns: true, cast: true});
fil_app_minerIDs = fil_app.map(elem => elem.ADDRESS.replace('\nView on Estuary', ''))
with_deals_only = raw_order_data.filter( elem => fil_app_minerIDs.includes(elem['Storage Provider']) )
groupByCountry(with_deals_only, `${folder}/active_deal_energy`)

// console.log(raw_order_data)
// console.log(with_deals_only)
