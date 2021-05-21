<template>
    <v-flex xs12 md10 my-3 py-3>
        <h3 class="display-2 text-md-left">21 May 2021 15:57 GMT</h3>
        <v-flex class="main-text" my-3>
            <p>
                CBECI > Index: replaced 30-second network power estimate with 24-hour intervals due to unreliability of input data (real-time hashrate estimate)
                <a href="https://blockchain.info/q/hashrate" target="_blank">https://blockchain.info/q/hashrate</a>
            </p>
        </v-flex>
        <h3 class="display-2 text-md-left">20 May 2021 20:25 GMT</h3>
        <v-flex class="main-text" my-3>
            <p>
                CBECI > Index: power estimates have been temporarily removed due to an issue with a data input source. The issue is under investigation and will be fixed as soon as possible.
                <a href="https://blockchain.info/q/hashrate" target="_blank">https://blockchain.info/q/hashrate</a>
            </p>
        </v-flex>
        <h3 class="display-2 text-md-left">19 Mar 2021 13:12 GMT</h3>
        <v-flex class="main-text" my-3>
            <p>
                Mining map > Visualisation: updated disclaimer
            </p>
        </v-flex>
        <h3 class="display-2 text-md-left">17 Feb 2021 08:30 GMT</h3>
        <v-flex class="main-text" my-3>
            <p>
                CBECI > Methodology: The data source for daily network hashrate, difficulty, miners revenue (issuance+ fees),
                bitcoin market price has been changed from <a href="https://api.blockchain.info/" target="_blank">https://api.blockchain.info/</a>
                to <a href="https://community-api.coinmetrics.io/v4/" target="_blank">https://community-api.coinmetrics.io/v4/</a>.
                The real-time hashrate estimate data source remains
                <a href="https://blockchain.info/q/hashrate" target="_blank">https://blockchain.info/q/hashrate</a>.
            </p>
        </v-flex>
        <h3 class="display-2 text-md-left">11 Feb 2021 21:49 GMT</h3>
        <v-flex class="main-text" my-3>
            <p>
                CBECI > Comparisons > Country Ranking: updated country consumption data (<a href="https://www.cia.gov/the-world-factbook/field/electricity-production/country-comparison" target="_blank">U.S. Energy Information Administration</a>, 2019)
            </p>
        </v-flex>
    </v-flex>
</template>

<script>
export default {
    name: 'ChangeLog',
    data() {
        return {
        }
    }
}
</script>
