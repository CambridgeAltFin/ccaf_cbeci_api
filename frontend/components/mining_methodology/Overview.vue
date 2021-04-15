<template>
    <v-flex xs12 md10 my-3 py-3>
        <h3 class="display-2 text-md-left">Calculating Average Country and Provincial Hashrate Share</h3>
        <v-flex class="main-text" my-3>
            <p>The mining map uses aggregate geo-location data based on the IP addresses of hashers connecting to mining pools.</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 1</u>: geo-location data of hashers collected by mining pools provides an accurate picture of global hashrate location. </span>
            </v-flex>
            <p>Aggregate data from participating mining pools represents approximately 37% of Bitcoin’s total hashrate for the period from September 2019 to April 2020 included. We use this data as a proxy for the geographic distribution of Bitcoin’s total hashrate, assuming that it is representative of the total hashrate distribution (please see the next section for a discussion on the limitations of this approach).</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 2</u>: data provided by participating mining pools constitutes a representative sample of Bitcoin’s total geographic hashrate distribution. </span>
            </v-flex>
            <p>Participating mining pools provide the average monthly geographic distribution of their respective hashrate. This data is then aggregated by CCAF and used to extrapolate the global hashrate distribution. With the exception of China, hashrate data is currently only available at the country level. We hope that we can add further granularity in the future to better represent regions with significant hashing activities (e.g. Siberia in Russia, Washington and New York States in the United States of America, or Québec and Alberta in Canada).</p>
            <p>Two of the three mining pools provide data on Chinese provinces. An average of these distributions is applied to the third pool for which no province-level data is available.</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 3</u>: the available sample of Chinese province data is representative of the total hashrate distribution within China.</span>
            </v-flex>
        </v-flex>
        <h3 class="display-2 text-md-left">Limitations</h3>
        <v-flex class="main-text" my-3>
            <p>Every model has its limitations resulting from the application of specific assumptions. There are two particular limitations arising from the approach described above.</p>
            <ul>
                <li>
                    <p><strong>Sample may not be representative</strong>
                    <p>The Bitcoin mining map is based on an extrapolation of a sample of mining pool data. This sample may not be fully representative for the following two reasons: first, it represents only a little more than a third of the total hashrate; and second, the data is provided by three Bitcoin mining pools that are all headquartered in China. </p>
                    <p>While the current version seems overly biased towards China, there are reasons to believe that the sample nevertheless provides a reasonable approximation of the actual hashrate distribution. For one, all participating pools maintain servers in various geographies across the globe to serve their foreign customer base with minimal latency. Furthermore, Chinese pools have dominated Bitcoin mining in recent years, among others because of their relatively low fee structure which has attracted numerous non-Chinese hashers.</p>
                    <p>The research team is actively looking to partner with additional mining pools and hashers to improve the accuracy and reliability of the mining map. Please do not hesitate to
                        <nuxt-link to="/contact/">
                            <span>get in touch</span>
                        </nuxt-link>
                        if you would like to contribute. </p>
                </li>
                <li>
                    <p><strong>Usage of VPNs or proxy services by miners</strong>
                    <p>It is no secret in the industry that hashers in certain locations use virtual private networks (VPNs) or proxy services to hide their IP address and thus location. Such behaviour may distort the overall geographic distribution and result in an overestimation of hashrate in some provinces or countries.</p>
                    <p>For one of the three pools, this effect was particularly visible in the Chinese province of Zhejiang. To mitigate this effect, we have divided the hashrate of Zhejiang province proportionally among other Chinese provinces listed in the pool’s dataset.</p>
                </li>
            </ul>

        </v-flex>
<!--        <h4 class="display-1 text-md-left">Sample may not be representative</h4>-->
<!--        <v-flex class="main-text" my-3>-->
<!--            <p>The Bitcoin mining map is based on an extrapolation of a sample of mining pool data. This sample may not be fully representative for the following two reasons: first, it represents only a little more than a third of the total hashrate; and second, the data is provided by three Bitcoin mining pools that are all headquartered in China. </p>-->
<!--            <p>While the current version seems overly biased towards China, there are reasons to believe that the sample nevertheless provides a reasonable approximation of the actual hashrate distribution. For one, all participating pools maintain servers in various geographies across the globe to serve their foreign customer base with minimal latency. Furthermore, Chinese pools have dominated Bitcoin mining in recent years, among others because of their relatively low fee structure which has attracted numerous non-Chinese hashers.</p>-->
<!--            <p>The research team is actively looking to partner with additional mining pools and hashers to improve the accuracy and reliability of the mining map. Please do not hesitate to-->
<!--                <nuxt-link to="/contact">-->
<!--                    get in touch-->
<!--                </nuxt-link>-->
<!--                if you would like to contribute. </p>-->
<!--        </v-flex>-->
<!--        <h4 class="display-1 text-md-left">Usage of VPNs or proxy services by miners</h4>-->
<!--        <v-flex class="main-text" my-3>-->
<!--            <p>It is no secret in the industry that hashers in certain locations use virtual private networks (VPNs) or proxy services to hide their IP address and thus location. Such behaviour may distort the overall geographic distribution and result in an overestimation of hashrate in some provinces or countries. However, through our data analysis and discussions with pool operators and hashers, we found that the distortion effect was minimal and did not affect the final output.</p>-->
<!--        </v-flex>-->
    </v-flex>
</template>

<script>
export default {
    name: 'Overview2',
    data() {
        return {
            headers: [
                { text: 'Variable name', sortable: false, value: 'parameter'},
                { text: 'Description', sortable: false, value: 'description'},
                { text: 'Unit', sortable: false, value: 'unit'}
            ],
            items: [
                {
                    parameter: 'Date',
                    description: 'Month and year of the corresponding period',
                    unit: 'NA',
                },
                {
                    parameter: 'Country',
                    description: 'Country name from which hashrate originates',
                    unit: 'NA',
                },
                {
                    parameter: 'Province (China only)',
                    description: 'Province name from which hashrate originates',
                    unit: 'NA',
                },
                {
                    parameter: 'Monthly hashrate per province/country',
                    description: 'The number of hashes per second performed in the corresponding province/country in the last 30 days',
                    unit: 'Ph/s or Th/s',
                }
            ]
        }
    }
}
</script>
