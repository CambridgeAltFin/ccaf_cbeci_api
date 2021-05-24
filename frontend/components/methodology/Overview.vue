<template>
    <v-flex xs12 md10 my-3 py-3>
        <h2 class="display-3 text-xs-center">Overview</h2>
        <h3 class="display-2 text-md-left">Summary</h3>
        <v-flex class="main-text" my-3>
            <p>The Cambridge Bitcoin Electricity Consumption Index (CBECI) provides a real-time estimate of the total
                electricity load and consumption of the Bitcoin network. The model is based on a bottom-up approach
                initially developed by <a href="http://blog.zorinaq.com/bitcoin-electricity-consumption/"
                                          target="_blank">Marc Bevand</a> in 2017 that takes different types of
                available mining hardware as the starting point.</p>
            <p>Given that the exact electricity consumption cannot be determined, the CBECI provides a range of
                possibilities consisting of a <b>lower bound</b> (floor) and an <b>upper bound</b> (ceiling) estimate.
                Within the boundaries of this range, a <b>best-guess</b> estimate is calculated to provide a more
                realistic figure that we believe comes closest to Bitcoin’s real annual electricity consumption.</p>
            <p>The lower bound estimate corresponds to the absolute minimum total electricity expenditure based on the
                best case assumption that all miners always use the most energy-efficient equipment available on the
                market. The upper bound estimate specifies the absolute maximum total electricity expenditure based on
                the worst case assumption that all miners always use the least energy-efficient hardware available on
                the market as long as running the equipment is still profitable in electricity terms. The best-guess
                estimate is based on the assumption that miners use a basket of profitable hardware rather than a single
                model.</p>
        </v-flex>
        <h3 class="display-2 text-md-left">Representation</h3>
        <v-flex class="main-text" my-3>
            <p>The CBECI landing page displays two numbers for each type of estimate.</p>
            <p>The first number refers to the total <b>electrical power</b> consumed by the Bitcoin network and is
                expressed in gigawatts (GW). This figure is updated every 24 hours and corresponds to the rate at
                which Bitcoin uses electricity.<sup @click="menu1 = true" style="text-decoration: underline; cursor: pointer">1</sup></p>
            <p>The second number refers to the total <b>yearly electricity consumption</b> of the Bitcoin network and is
                expressed in terawatt-hours (TWh). We annualise Bitcoin’s electricity consumption assuming continuous
                power usage at the aforementioned rate over the period of one year. We apply a <b>7-day moving
                    average</b> to the resulting data point in order to make the output value less dependent of
                short-term hashrate movements, and thus more suitable for comparisons with alternative uses of
                electricity.</p>
        </v-flex>
        <v-dialog v-model="menu1" :max-width="600" offset-x>
            <v-card>
                <v-flex pa-4>
                    An earlier version of the model used a real-time estimate of Bitcoin's hashrate to ensure updates to the power
                    estimate in 30-second intervals. However, hashrate is notoriously difficult to calculate given that time
                    elapsed between blocks is entirely random. As a result, only measurements over longer periods can be
                    considered reliable. We have chosen a daily average as an acceptable trade-off and updated the model accordingly.
                </v-flex>
            </v-card>
        </v-dialog>
        <h3 class="display-2 text-md-left">Model parameters</h3>
        <v-flex class="main-text" my-3>
            <p>The model takes into account the parameters outlined in <b>Table 1</b> below. The following sections will
                specify how each estimate is calculated and what assumptions have been used.</p>
            <v-toolbar flat color="white">
                <v-toolbar-title>Table 1: CBECI model parameters</v-toolbar-title>
            </v-toolbar>
            <v-data-table
                    :headers="headers"
                    :items="items"
                    item-key="name"
                    hide-actions
            >
                <template v-slot:items="props">
                    <tr>
                        <td>{{ props.item.parameter }}</td>
                        <td>{{ props.item.description }}</td>
                        <td>{{ props.item.unit }}</td>
                        <td>{{ props.item.source }} <a v-if="props.item.link" :href="props.item.link" target="_blank">{{props.item.link}}</a></td>
                    </tr>
                </template>
            </v-data-table>
        </v-flex>
    </v-flex>
</template>

<script>
  export default {
    name: 'Overview',
    data() {
      return {
        menu1: false,
        headers: [
          {text: 'Parameter', sortable: false, value: 'parameter'},
          {text: 'Description', sortable: false, value: 'description'},
          {text: 'Measure/Unit', sortable: false, value: 'unit'},
          {text: 'Source', sortable: false, value: 'source'},
        ],
        items: [
          {
            parameter: 'Network hashrate, mean daily',
            description: 'The mean rate at which miners are solving hashes that day',
            unit: 'Exahashes per second (Eh/s)',
            source: 'Dynamic: ',
            link: 'https://coinmetrics.io/'
          },
          {
            parameter: 'Bitcoin issuance value, daily',
            description: 'The sum USD value of all bitcoins issued that day',
            unit: 'USD',
            source: 'Dynamic: ',
            link: 'https://coinmetrics.io/'
          },
          {
            parameter: 'Miners fees, daily',
            description: 'The sum USD value of all fees paid to miners that day',
            unit: 'USD',
            source: 'Dynamic: ',
            link: 'https://coinmetrics.io/'
          },
          {
            parameter: 'Difficulty, mean daily',
            description: 'The mean difficulty of finding a new block that day',
            unit: 'Dimensionless',
            source: 'Dynamic: ',
            link: 'https://coinmetrics.io/'
          },
          {
            parameter: 'Bitcoin market price',
            description: 'The fixed closing price of the asset as of 00:00 UTC that day',
            unit: 'USD',
            source: 'Dynamic: ',
            link: 'https://coinmetrics.io/'
          },
          {
            parameter: 'Mining equipment efficiency',
            description: 'Measures the energy efficiency of a given mining hardware type',
            unit: 'Joules per Gigahash (J/Gh)',
            source: 'Static: hardware specs from 60+ equipment types, taken from various sources',
          },
          {
            parameter: 'Electricity cost',
            description: 'Average electricity cost incurred by miners',
            unit: 'USD per kilowatt-hour ($/kWh)',
            source: 'Static: estimate (assumption)',
          },
          {
            parameter: 'Data centre efficiency',
            description: 'Measures how efficiently energy is used in a data centre: expressed via power usage effectiveness (PUE)',
            unit: '',
            source: 'Static: estimate (assumption)',
          },
        ]
      }
    }
  }
</script>
