<template>
    <v-flex xs12 md10 my-3 py-3>
        <h2 class="display-3 text-xs-center">Discussion</h2>
        <h3 class="display-2 text-md-left">Limitations of the model</h3>
        <v-flex class="main-text" my-3>
            <p>Every model is an incomplete representation of reality that relies on specific assumptions, some of which may be debatable. As a result, every model has limitations that need to be discussed. In particular, the current CBECI model exhibits the following limitations (the list is non-exhaustive):</p>
            <ul>
                <li>
                    <p><strong>Strong dependence on electricity cost estimate:</strong> electricity costs can significantly vary from one country, region, and provider to another. Prices are generally dynamic and adjustable, often according to seasonal circumstances, the quantity of electricity consumed, and other factors. Modifying the default electricity cost assumption can substantially change the model output.</p>
                </li>
                <li>
                    <p><strong>Hardware selection:</strong> we may not be aware of new and more efficient hardware that is not yet available on the market. Some have argued that manufacturers are using proprietary equipment to their own benefits before public release.
                        <sup @click="menu6 = true" style="text-decoration: underline; cursor: pointer">7</sup>
                    </p>
                </li>
                <li>
                    <p><strong>Ignoring other cost factors: </strong>other potential factors that influence the decision of miners to switch off and/or replace existing equipment have not been incorporated into the model (e.g. maintenance and cooling costs).</p>
                </li>
                <li>
                    <p><strong>Simplistic weighting of profitable hardware</strong>: assuming that all profitable equipment is equally distributed among miners is unrealistic given that not all hardware is produced in equal quantities and readily available. The exact market share is unknown, although existing data suggests that a few large manufacturers dominate the market. The lack of reliable longitudinal market share data impacts all bottom-up approaches.</p>
                </li>
                <li>
                    <p><strong>Hardware specifications may not correspond to real performance:</strong> hardware manufacturers often advertise the performance and energy efficiency of their products using best case scenarios. Furthermore, miners may decide to overclock or underclock their machines for various reasons, which the model does not take into account.&nbsp;</p>
                </li>
                <li>
                    <p><strong>Short switching periods: </strong>it is unlikely that miners are able to quickly react to short-term changes in the profitability thresholds: they cannot simply replace all machines of an entire data centre in such a short period of time. While we attempt to smoothen the effect of short-term hashrate variations and price volatility, applying a moving average of 7 days (annualised consumption estimate) and 14 days (profitability threshold), respectively, may not be sufficient.</p>
                </li>
            </ul>
            <p>While most limitations do not have a major impact on the performance of the model, we are aware of its imperfections. The CBECI is an ongoing project that is maintained on a continuous basis. The model will be refined in response to changing circumstances, with all changes being transparently highlighted.</p>
            <p>In case you would like to provide suggestions on how we could improve the index, please feel free to send us a message using <nuxt-link to="/contact/">this form</nuxt-link>.</p>
            <v-dialog v-model="menu6" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        Vorick, D. (2018) The State of Cryptocurrency Mining. Available at: <a target="_blank" href="https://blog.sia.tech/the-state-of-cryptocurrency-mining-538004a37f9b">https://blog.sia.tech/the-state-of-cryptocurrency-mining-538004a37f9b</a>
                    </v-flex>
                </v-card>
            </v-dialog>
        </v-flex>
        <h3 class="display-2 text-md-left">How does the CBECI compare to other estimates?</h3>
        <v-flex class="main-text" my-3>
            <p>There have been multiple attempts in the past to analyse the electricity consumption of the Bitcoin network and assess its environmental footprint. A list of available studies and articles is presented in <b>Table 2</b>. With the exception of Alex De Vries’s <a target="_blank" href="https://digiconomist.net/bitcoin-energy-consumption">“Bitcoin Electricity Consumption Index” (BECI)</a>, there is no live index tracking Bitcoin’s electricity load and consumption in real time.</p>
            <v-toolbar flat color="white">
                <v-toolbar-title>Table 2: Overview of previous studies</v-toolbar-title>
            </v-toolbar>
            <v-data-table
                    :headers="headers"
                    :items="items"
                    item-key="name"
                    hide-actions
            >
                <template v-slot:items="props">
                    <tr>
                        <td>{{ props.item.author }}</td>
                        <td>{{ props.item.date }}</td>
                        <td>{{ props.item.title }}</td>
                        <td>{{ props.item.approach }}</td>
                        <td><a target="_blank" :href="props.item.link">Link</a></td>
                    </tr>
                </template>
            </v-data-table>
        </v-flex>
        <v-flex class="main-text" my-3>
            <p>These studies tend to produce considerably diverging findings along a relatively broad range of possible estimates. This can be explained by the application of different methodologies adopted by the study authors: some use a top-down economic approach, whereas others are based on a bottom-up techno-economic approach (like the CBECI model).</p>
            <p>Each study is based on a set of assumptions that can be put into question. As a result, the design of each study - including our own analysis - has its own pitfalls and limitations. Some papers, however, have been criticised for applying overly simplistic assumptions and containing non-trivial errors such as inappropriate averaging over time periods or simple extrapolations. For a more thorough review of previous studies, see Koomey (2019).
                <sup @click="menu7 = true" style="text-decoration: underline; cursor: pointer">8</sup>
            </p>
            <p>
                The CBECI has been designed with the aforementioned studies in mind. We have carefully reviewed the various methodologies and incorporated best practices. This website attempts to provide comprehensive documentation with transparent version control, highlight the model’s dependence on the electricity cost assumption by allowing visitors to adjust the default value, and openly present the uncertainties and limitations of the model. Feedback and suggestions for further improvements can be given <nuxt-link to="/contact/">here</nuxt-link>.
            </p>
            <v-dialog v-model="menu7" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        Koomey, J. (2019) Estimating Bitcoin Electricity Use: A Beginner’s Guide. Coin Center Report. Available at: <a target="_blank" href="https://coincenter.org/files/estimating-bitcoin-electricity-use.pdf">https://coincenter.org/files/estimating-bitcoin-electricity-use.pdf</a>.
                    </v-flex>
                </v-card>
            </v-dialog>
        </v-flex>
    </v-flex>
</template>

<script>
export default {
    name: 'Discussion',
    data() {
        return {
            menu6: false,
            menu7: false,
            headers: [
                { text: 'Author', sortable: false, value: 'author'},
                { text: 'Date of publication', sortable: false, value: 'date'},
                { text: 'Title', sortable: false, value: 'title'},
                { text: 'Approach', sortable: false, value: 'approach'},
                { text: 'Source', sortable: false, value: 'link'},
            ],
            items: [
                {
                    author: 'Stoll, C., Klaaßen, L., and Gallersdorfer, U.',
                    date: 'June 2019',
                    title: 'The Carbon Footprint of Bitcoin',
                    approach: 'Bottom-up',
                    link: 'https://www.cell.com/action/showPdf?pii=S2542-4351%2819%2930255-7'
                },
				{
                    author: 'Zade, M., Myklebost, J., Tzscheutschler, P., and Wagner, U.',
                    date: 'March 2019',
                    title: 'Is Bitcoin the Only Problem? A Scenario Model for the Power Demand of Blockchains',
                    approach: 'Bottom-up',
                    link: 'https://www.frontiersin.org/articles/10.3389/fenrg.2019.00021/full'
                },
                {
                    author: 'Krause, M. J., and Tolaymat, T.',
                    date: 'November 2018',
                    title: 'Quantification of energy and carbon costs for mining cryptocurrencies',
                    approach: 'Bottom-up',
                    link: 'https://www.nature.com/articles/s41893-018-0152-7.pdf'
                },
                {
                    author: 'Mora, C., Rollins, R.L., Taladay, K., Kantar, M.B., Chock, M.K., Shimada, M., and Franklin, E.C.',
                    date: 'October 2018',
                    title: 'Bitcoin emissions alone could push global warming above 2°C',
                    approach: 'Top-down',
                    link: 'https://www.nature.com/articles/s41558-018-0321-8.pdf'
                },
                {
                    author: 'McCook, H.',
                    date: 'August 2018',
                    title: 'The cost & sustainability of Bitcoin',
                    approach: 'Bottom-up',
                    link: 'https://www.academia.edu/37178295/The_Cost_and_Sustainability_of_Bitcoin_August_2018_'
                },
                {
                    author: 'De Vries, A.',
                    date: 'May 2018',
                    title: 'Bitcoin’s Growing Energy Problem',
                    approach: 'Top-down',
                    link: 'https://www.researchgate.net/publication/325188032_Bitcoin\'s_Growing_Energy_Problem'
                },
                {
                    author: 'Vranken, H.',
                    date: 'October 2017',
                    title: 'Sustainability of bitcoin and blockchains',
                    approach: 'Bottom-up',
                    link: 'https://www.sciencedirect.com/science/article/pii/S1877343517300015'
                },
                {
                    author: 'Bevand, M.',
                    date: 'February 2017',
                    title: 'Electricity consumption of Bitcoin: a market-based and technical analysis',
                    approach: 'Bottom-up',
                    link: 'http://blog.zorinaq.com/bitcoin-electricity-consumption/#fnref:refD:1'
                },
                {
                    author: 'Hayes, A. S.',
                    date: 'March 2015',
                    title: 'A Cost Production Model for Bitcoin',
                    approach: 'Top-down',
                    link: 'http://www.economicpolicyresearch.org/econ/2015/NSSR_WP_052015.pdf'
                },
                {
                    author: 'O’Dwyer, K.L., and Malone, D.',
                    date: 'September 2014',
                    title: 'Bitcoin Mining and its Energy Footprint',
                    approach: 'Bottom-up',
                    link: 'http://karlodwyer.com/publications/pdf/bitcoin_KJOD_2014.pdf'
                },
            ]
        }
    }
}
</script>
