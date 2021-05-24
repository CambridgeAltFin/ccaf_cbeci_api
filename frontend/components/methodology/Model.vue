<template>
    <v-flex xs12 md10 my-3 py-3>
        <h2 class="display-3 text-xs-center">CBECI Model</h2>
        <h3 class="display-2 text-md-left">Selecting mining equipment</h3>
        <v-flex class="main-text" my-3>
            <p>In the first years of Bitcoin, mining was mainly performed using general-purpose <a target="_blank" href="https://en.wikipedia.org/wiki/Graphics_processing_unit">graphics processing units (GPUs)</a> and <a target="_blank" href="https://en.wikipedia.org/wiki/Field-programmable_gate_array">field-programmable gate arrays (FPGAs)</a>. This changed considerably when in 2012 the first <a target="_blank" href="https://en.wikipedia.org/wiki/Application-specific_integrated_circuit">application-specific integrated circuits (ASICs)</a> started to emerge. ASICs are specialised hardware specifically optimised for Bitcoin mining that are orders of magnitude more efficient than previous devices used for mining: it didn’t take long for ASICs to become popular and displace GPU and FPGA mining.</p>
            <p>We have compiled a list of more than 60 different Bitcoin ASIC models designed for SHA-256 operations that have been brought to market since October 2014, which serves as the starting date of the CBECI. The list is based on a combination of public resources that list various types of mining equipment and their specifications.
                <sup @click="menu1 = true" style="text-decoration: underline; cursor: pointer">2</sup>
            </p>
            <p>Mining efficiency of each machine type is expressed in Joules per Gigahash (J/Gh): given that real power usage can vary significantly depending on several parameters (e.g. usage conditions, overclocking), the manufacturer specifications have been refined with the help of experts to more accurately reflect real power usage. The full list is available at <a href="http://sha256.cbeci.org" target="_blank">http://sha256.cbeci.org</a> and is open to comments and suggestions. <b>Figure 1</b> shows the evolution of Bitcoin mining equipment efficiency since late 2014.</p>
            <v-dialog v-model="menu1" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        In particular: <em>ASIC Miner Value</em> (available at: <a href="https://www.asicminervalue.com/" target="_blank">https://www.asicminervalue.com/</a>),
                        <em>CryptoCompare</em> (available at:  <a href="https://www.cryptocompare.com/mining/#/equipment" target="_blank">https://www.cryptocompare.com/mining/#/equipment</a>),
                        and the specifications indicated in the pre-IPO filings from three Chinese hardware manufacturers (summarised by <em>Stoll</em> et al.,
                        2019: available at: <a href="https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7" target="_blank">https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7</a>, see “Supplemental Information”).
                    </v-flex>
                </v-card>
            </v-dialog>
        </v-flex>
        <client-only>
            <chart2/>
        </client-only>
        <v-flex py-3>
            <p><em><b>Note:</b> a 1000W mining device that generates 10,000 Gigahashes per second (Gh/s) has an efficiency of 0.1 Joules per Gh (J/Gh). This chart is based on a list of 60+ SHA-256 mining equipment available at <a href="http://sha256.cbeci.org" target="_blank">http://sha256.cbeci.org</a>.</em></p>
        </v-flex>
        <h3 class="display-2 text-md-left">The profitability threshold</h3>
        <v-flex class="main-text" my-3>
            <p>The key idea underlying the CBECI model is that miners will run the equipment as long as it remains profitable in electricity terms. In order to determine the time periods during which a given hardware type is profitable, we model the economic lifetime of each machine by taking into account total miner revenues, total network hashrate, the energy efficiency of the hardware in question, and the average electricity price per kWh that miners have to pay.</p>
            <p>This results in the following mathematical inequality:</p>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula1t" />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula1" class="text" />
            </v-layout>
            <p>It is worth noting that <b>profitability</b> in this context exclusively considers electricity costs incurred for running the machines: it does not take into account capital expenditures (e.g. acquisition and amortisation costs) nor other operational expenditures (e.g. cooling, maintenance, and labour costs).</p>
            <p>The profitability threshold (θ) is then calculated as follows:</p>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula2t" />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula2" class="text"/>
            </v-layout>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 1</u>: the global average electricity price is constant over time and corresponds to 0.05 USD/kWh. </span>
            </v-flex>
            <p>Electricity prices available to miners vary significantly from one region to another for a variety of reasons. We assume that on average, miners face a constant electricity price of 5 USD cents per kilowatt-hour (0.05 USD/kWh). This default value is based on in-depth conversations with miners worldwide and is consistent with estimates used in previous research.
                <sup @click="menu2 = true" style="text-decoration: underline; cursor: pointer">3</sup>
            </p>
            <v-dialog v-model="menu2" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        For instance, see Stoll et al., 2019 (available at:
                        <a target="_blank" href="https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7">https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7</a>), Digiconomist, 2019 (available at:
                        <a target="_blank" href="https://digiconomist.net/bitcoin-energy-consumption">https://digiconomist.net/bitcoin-energy-consumption</a>), Vorick, 2018 (available at:
                        <a target="_blank" href="https://blog.sia.tech/the-state-of-cryptocurrency-mining-538004a37f9b">https://blog.sia.tech/the-state-of-cryptocurrency-mining-538004a37f9b</a>), de Vries, 2018 (available at:
                        <a target="_blank" href="https://www.cell.com/joule/pdf/S2542-4351(18)30177-6.pdf">https://www.cell.com/joule/pdf/S2542-4351(18)30177-6.pdf</a>), McCook, 2018 (available at:
                        <a target="_blank" href="https://www.academia.edu/37178295/The_Cost_and_Sustainability_of_Bitcoin_August_2018_">https://www.academia.edu/37178295/The_Cost_and_Sustainability_of_Bitcoin_August_2018_</a>), and Bevand, 2017 (available at:
                        <a target="_blank" href="http://blog.zorinaq.com/bitcoin-electricity-consumption">http://blog.zorinaq.com/bitcoin-electricity-consumption</a>).
                    </v-flex>
                </v-card>
            </v-dialog>
        </v-flex>
        <v-flex py-3>
            <p><em><b>Note:</b> The CBECI landing page allows visitors to choose different values for the average electricity cost in order to explore how electricity prices influence hardware selection and total electricity consumption.</em></p>
        </v-flex>
        <v-flex class="main-text" my-3>
            <p>Assuming a fixed electricity price of 0.05 USD/kWh, we can model the evolution of the profitability threshold over time (<b>Figure 2</b>). While mining equipment with an energy efficiency below 2 J/Gh remained profitable in early 2015, the threshold has substantially decreased over time as a result of the introduction of newer ASIC generations and a continuous increase in hashrate. Large price spikes occasionally lead to a sharp increase of the profitability threshold (e.g. bull run in late 2017), which tends to correct relatively soon as effects are cancelled out by growing total hashrate.</p>
        </v-flex>
        <chart3/>
        <v-flex class="main-text" my-3>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula3t"  />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula3"  class="text" />
            </v-layout>
            <p><br/>Sometimes, it is possible that no mining equipment is profitable during a certain period. In this case, we use the following assumption:</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 2</u>: during time periods where no mining equipment is profitable, the model uses the last known profitable equipment.</span>
            </v-flex>
            <p>It is reasonable to assume that miners will not immediately switch off unprofitable equipment as long as the time periods are acceptably short and infrequent. </p>
            <p>The model applies a <b>14-day moving average to the profitability threshold</b> in order to smoothen the switch from one equipment type to another as a result of short-term hashrate variations and price volatility. </p>
        </v-flex>
        <h3 class="display-2 text-md-left">Constructing the lower bound estimate</h3>
        <v-flex class="main-text" my-3>
            <p>In a best case scenario, every single miner would always use the most energy-efficient equipment that maximises expected profits. The lower bound estimate (E<sub>lower</sub>) is thus based on the following best-case assumption:</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 3a (lower bound)</u>: all miners always run the most efficient hardware available.</span>
            </v-flex>
            <p>This assumption also implies that miners will rapidly upgrade mining gear as soon as more energy-efficient hardware becomes available on the market.</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 4a (lower bound)</u>: all mining facilities have a PUE of 1.01.</span>
            </v-flex>
            <p>
                <b>Power usage effectiveness (PUE)</b> is a measure of data centre energy efficiency: data centres generally consume more energy than is required to simply run servers, mostly because of cooling, supporting IT equipment, and other overheads. The higher the ratio, the less efficiently energy is used. Data centres with PUE below 1.2 are generally considered efficient. For reference, <a href="https://www.google.com/about/datacenters/efficiency/internal/" target="_blank">Google’s average PUE</a> is 1.11, whereas the average PUE of most data centres <a href="https://www.datacenterknowledge.com/archives/2011/05/10/uptime-institute-the-average-pue-is-1-8/" target="_blank">corresponds to 1.8</a> or more.
            </p>
            <p>
                In the case of Bitcoin mining, however, electricity costs account for the vast majority of operational expenditures: mining farm operators have a clear incentive to optimise cooling systems in order to reduce overall costs. Conversations with miners support the hypothesis that mining facilities generally have significantly lower PUE than traditional data centres.
            </p>
            <p>
                In a best case scenario, mining facilities have optimised data centre operations to a point where there is nearly zero overhead. This scenario is represented by assuming a PUE of 1.01.
            </p>
            <p>The lower bound estimate can be mathematically expressed as follows:</p>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula4t" />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula4"  class="text"/>
            </v-layout>
            <p>
                <br/>The lower bound estimate corresponds to the absolute minimum electricity consumption of the Bitcoin network. While useful for providing a quantifiable floor, it is unrealistic for a variety of reasons:
            </p>
            <ul>
                <li style="margin-bottom: 20px"><b>Not all miners use the most efficient hardware:</b> old equipment can remain profitable for a considerable time when miners have access to cheap electricity and Bitcoin prices remain high. </li>
                <li style="margin-bottom: 20px"><b>Long delivery and installation times:</b> the delivery and installation of newly released equipment can take up to 3 months.</li>
                <li style="margin-bottom: 20px"><b>Hardware supply shortage:</b> the most efficient hardware may not be available in all regions in sufficient quantities.</li>
                <li style="margin-bottom: 20px"><b>Optimistic PUE:</b> not all mining facilities have an optimal PUE.</li>
            </ul>
        </v-flex>
        <h3 class="display-2 text-md-left">Constructing the upper bound estimate</h3>
        <v-flex class="main-text" my-3>
            <p>Calculating the upper bound estimate (E<sub>upper</sub>) is a more difficult task.</p>
            <p>We could imagine a worst case scenario where every miner uses the least efficient computing device available on the market that is capable of generating cryptographic hashes - a <a href="https://en.wikipedia.org/wiki/Central_processing_unit"target="_blank">central processing unit (CPU)</a> powering for instance a computer, a tablet, or even a smartphone. However, with the exponential increase of Bitcoin’s <a href="https://www.blockchain.com/charts/difficulty?timespan=all"target="_blank">network difficulty</a> since 2016, this assumption would quickly lead to a consumption figure that exceeds the world’s total energy production - let alone that miners would need to operate at massive losses. </p>
            <p>We thus adjust the assumption as follows:</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 3b (upper bound)</u>: all miners always use the least efficient hardware available at each time period as long as the equipment is still profitable in terms of electricity costs.</span>
            </v-flex>
            <p>
                As soon as a given equipment type is not profitable anymore, it will be retired and replaced with the next least efficient hardware model that still remains profitable.
            </p>
            <p>
                It is worth remembering that the profitability threshold for each mining hardware type is calculated strictly in electricity terms and does not take into account capital expenditures nor other operational expenditures.
            </p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 4b (upper bound)</u>: all mining facilities have a PUE of 1.20.</span>
            </v-flex>
            <p>We assume that in this scenario, all mining farms have a PUE of 1.20. While still considered efficient by general-purpose data centre standards, it ranges at the higher end of PUE figures reported by miners.</p>
            <p>The upper bound equation can thus be mathematically expressed as follows:</p>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula5t" />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula5"  class="text"/>
            </v-layout>
            <p><br/>The upper bound estimate corresponds to the absolute maximum electricity consumption of the Bitcoin network. While useful for providing a <b>quantifiable ceiling</b>, it is unrealistic for a variety of reasons:</p>
            <ul>
                <li style="margin-bottom: 20px"><b>Miners demand the most energy-efficient hardware:</b> large miners with industrial-scale data centres compete for gaining early access to the newest ASIC generations that are more energy-efficient.</li>
                <li style="margin-bottom: 20px"><b>Old equipment gets replaced:</b> many miners replace old ASIC generations that have remained unprofitable for a long time with new equipment rather than storing old equipment for years hoping for the profitability threshold to increase.</li>
                <li style="margin-bottom: 20px"><b>Other operational expenditures have an impact, too:</b> ignoring additional expenditures such as cooling and maintenance costs may artificially overstate the economic lifetime of inefficient hardware.</li>
            </ul>
        </v-flex>
        <h3 class="display-2 text-md-left">Constructing the best-guess estimate</h3>
        <v-flex class="main-text" my-3>
            <p>Given that both the lower and upper bound estimates rely on fairly unrealistic assumptions, we attempt to provide an educated guess that more accurately quantifies Bitcoin’s real electricity usage.</p>
            <p>In reality, many miners do not run a single type of mining equipment in their data centres, and they do not all switch to the newest hardware at the same time - if they do at all. In many cases, miners operate a combination of different models as long as the equipment remains profitable in electricity terms (i.e. stay below the profitability threshold). </p>
            <p>The difficulty lies in determining a realistic weighting approach for all profitable equipment types on a continuous basis that takes into account changing market and network conditions over time. Analysing the market share evolution of the major mining manufacturers would be a good proxy; however, reliable market share data over multiple periods is unfortunately not available.</p>
            <p>We thus use the following assumption for our best-guess estimate:</p>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 3c (best-guess)</u>: all miners use an equally-weighted basket of hardware types that are profitable in electricity terms.</span>
            </v-flex>
            <p>
                The assumption that all profitable machines are equally distributed among miners may seem very unrealistic at first: many hardware types have not been produced and sold in equal quantities, some equipment may not have been available to everyone at the same time, and other machines may already have been fully retired despite becoming profitable again for a short period of time.
            </p>
            <p>
                However, when comparing our best-guess estimate to a simulation that uses hardware weighting based on Stoll et al.’s (2019) market share calculations,
                <sup @click="menu3 = true" style="text-decoration: underline; cursor: pointer">4</sup>
                the resulting electricity consumption values do not differ substantially <strong>(Figure 3)</strong>.
                <sup @click="menu4 = true" style="text-decoration: underline; cursor: pointer">5</sup>
                This suggests that using the current assumption of equally-weighted profitable equipment is acceptable until further research and analysis on better weighting approaches becomes available.
            </p>
            <v-dialog v-model="menu3" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        Weighting rationale and calculations are available at: <a target="_blank" href="https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7#secsectitle0150">https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7#secsectitle0150</a> (see Supplemental Information: Data S1. Calculation of Power Consumption and Carbon Emissions - 3.4_IPO filing analysis and 3.2_Best-guess p-consumption).
                    </v-flex>
                </v-card>
            </v-dialog>
            <v-dialog v-model="menu4" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        It should be noted that the market share and energy efficiency parameters from Stoll et. al (2019) are static (calculated for each year) and only the hashrate parameter is dynamic (adjusted daily), which explains in part some of the larger divergences (from 20-30%) between the two estimates. Differences in hardware selection approaches are an additional reason for occasional divergences.
                    </v-flex>
                </v-card>
            </v-dialog>
            <chart4/>
            <v-flex class="assumption" mb-4 pa-3>
                <span><u>Assumption 4c (best-guess)</u>: all mining facilities have a PUE of 1.10.</span>
            </v-flex>
        </v-flex>
        <v-flex class="main-text" my-3>
            <p>We assume that all mining farms have a PUE of 1.10 when calculating our best-guess estimate.
                <sup @click="menu5 = true" style="text-decoration: underline; cursor: pointer">6</sup>
                This figure is slightly more conservative than other estimates but has been confirmed during private conversations with miners and mining experts.</p>
            <p>Our best-guess estimate can be mathematically expressed as follows:</p>
            <v-dialog v-model="menu5" :max-width="600" offset-x>
                <v-card>
                    <v-flex pa-4>
                        For instance, <a href="https://www.cell.com/joule/fulltext/S2542-4351(19)30255-7" target="_blank">Stoll et al. (2019)</a> and <a target="_blank" href="http://blog.zorinaq.com/morgan-stanley-bitcoin-research-reports/">Marc Bevand</a> discuss a PUE of 1.05.
                    </v-flex>
                </v-card>
            </v-dialog>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula6t" />
            </v-layout>
            <v-layout align-center justify-center py-3>
                <katex-element :expression="formula6"  class="text"/>
            </v-layout>
            <p><br/>Limitations of this methodology will be discussed in the next section.</p>
        </v-flex>
    </v-flex>
</template>

<script>
import Chart2 from '~/components/methodology/Chart2'
import Chart3 from '~/components/methodology/Chart3'
import Chart4 from '~/components/methodology/Chart4'

export default {
    name: 'Model',
    components: {
        chart2: Chart2,
        chart3: Chart3,
        chart4: Chart4,
    },
    data() {
        return {
            menu1: false,
            menu2: false,
            menu3: false,
            menu4: false,
            menu5: false,
            formula1: String.raw`
                with\\
                \vartheta \ -energy\ efficiency\ of\ mining\ hardware\ [J/h]\\
                P_{el}\ -electricity\ cost\ per\ joule\ [USD/J]\\
                SRev\ -mining\ revenue\ per\ hash\ [USD/h]\\
            `,
			formula1t: String.raw`
                \vartheta *P_{el}\ \le \ SRev,\\
            `,
			formula2t: String.raw`
			\theta =\frac{SRev}{P_{el}},\\
            `,
            formula2: String.raw`
				with\\
				\theta \ -profitability\ threshold\ [J/h]\\
				P_{el}\ -electricity\ cost\ per\ joule\ [USD/J]\\
				SRev\ -mining\ revenue\ per\ hash\ [USD/h]\\
            `,
			formula3t: String.raw`
                {Eq}_{prof}\left(P_{el}\right)=\{{\vartheta }_1,\ {\vartheta }_2,\dots \},\\
            `,
            formula3: String.raw`
                with\\
                {Eq}_{prof}\left(P_{el}\right)\ -\mathrm{\ }set\ of\ profitable\ hardware\ given\ electricity\ price{\ P}_{el}\\
                {\vartheta }_i\ -energy\ efficiency\ of\ mining\ hardware\ [J/h]\\
            `,
            formula4t: String.raw`
                E_{lower}\left(P_{el}\right)={min \left({Eq}_{prof}\left(P_{el}\right)\right)\ }*H*PUE*3.16*{10}^7,\\
            `,
            formula4: String.raw`
                with\\
                E_{lower}\ -\ lower\ bound\ power\ consumption\ [W]\\
                {min \left({Eq}_{prof}\left(P_{el}\right)\right)\ }\ -\ energy\ efficiency\ of\ the\ most\ efficient\ hardware\ [J/h]\\
                {H \ -\ hashrate\ [h/s]\ \ }\\
                {PUE \ -\ power\ usage\ effectiveness\ }\\
            `,
            formula5t: String.raw`
                E_{upper}\left(P_{el}\right)={max \left({Eq}_{prof}\left(P_{el}\right)\right)\ }*H*PUE*3.16* {10}^7,\\
            `,
            formula5: String.raw`
                with\\
                E_{upper}\ -\ upper\ bound\ power\ consumption\ [W]\\
                {max \left({Eq}_{prof}\left(P_{el}\right)\right)\ -\ energy\ efficiency\ of\ the\ least\ efficient\ }\\
                {but\ still\ profitable\ hardware\ [J/h]\ \ }\\
                {H \ -\ hashrate\ [h/s]\ \ }\\
                {PUE \ -\ power\ usage\ effectiveness\ }\\
            `,
            formula6t: String.raw`
                E_{estimated}\left(P_{el}\right)=\frac{\sum^N_{i=1}{{\vartheta }_i}}{N}*H*PUE*3.16*{10}^7,\\
            `,
            formula6: String.raw`
                with\\
                E_{estimated}\ -\ best\ guess\ power\ consumption\ [W]\\
                \frac{\sum^N_{i\mathrm{=1}}{{\vartheta }_i}}{N}\mathrm{\ }-\mathrm{\ }average\mathrm{\ }energy\mathrm{\ }efficiency\mathrm{\ }of\mathrm{\ }profitable\mathrm{\ }hardware\mathrm{\ [}J\mathrm{/}h\mathrm{]}\\
                {H \ -\ hashrate\ [h/s]\ \ }\\
                {PUE \ -\ power\ usage\ effectiveness\ }\\
            `
        }
    },
    computed: {
    }
}
</script>
