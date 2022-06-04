let getEnergy = require('./getEnergy')


async function main(){
  result = await getEnergy.get_total_energy_data('2020-08-01', '2020-12-03', 'f066596', true)
  console.log(result)
}

main()
