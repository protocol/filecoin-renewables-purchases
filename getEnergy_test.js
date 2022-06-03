let getEnergy = require('./getEnergy')


async function main(){
  result = await getEnergy.get_total_energy_data('2020-08-01', '2022-03-20', 'f066596')
  console.log(result)
}

main()
