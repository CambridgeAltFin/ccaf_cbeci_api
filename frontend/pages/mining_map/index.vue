<template>
  <v-layout column justify-center align-center id="wrap-container">
    <v-flex xs12 md10 my-4>
      <h1 class="display-4 text-xs-center">Bitcoin Mining Map</h1>
    </v-flex>
    <v-layout row align-center justify-center>
      <v-flex xs12 md10 my-4>
        <v-layout column>
          <div style="width: 100%">
            <p>The Bitcoin Mining Map visualises the approximate geographic distribution of global Bitcoin hashrate. The average hashrate share by country is available for display in monthly intervals starting from September 2019. In addition, a second map with an exclusive focus on China’s hashrate distribution by province is provided.
            </p>
            <p>
              <b>Disclaimer</b><br>
              Past periods may be adjusted when additional data becomes available. Please note that the map has been last updated in April 2020 and may not provide an accurate representation of the current state of global hashrate distribution. In some countries, and China in particular, mining operations tend to move between locations according to seasonal variance in renewables production. These migration patterns can only be observed when selecting a longer timeframe for the analysis.
            </p>
            <p>
              All changes are listed in the <nuxt-link to="change-log/">Change log</nuxt-link>
            </p>
          </div>
          <iframe :width="containerWidth" :height="containerWidth / 32 * 22" src="https://app.powerbi.com/view?r=eyJrIjoiYTg4NTNlZDAtNmIzNS00OWQyLWExNmQtNDY5MTAzNTM3YjIyIiwidCI6IjAwYzliM2IxLTAzMTItNGMzMy1hZTdmLTgwZjNhNzU5ZGVjMSIsImMiOjh9" frameborder="0" allowFullScreen="true" />
          <v-flex my-4>
            <div style="width: 100%">
              <v-flex mt-5>
                <p>The chart below shows the average monthly hashrate breakdown by country (and Chinese provinces, if selected) in descending order.</p>
              </v-flex>
            </div>
          </v-flex>
          <iframe :width="containerWidth" :height="containerWidth / 32 * 22" src="https://app.powerbi.com/view?r=eyJrIjoiZGNhNjMyYjgtNDk4Zi00OTRjLThhMWItNjUwMDRlZWY1NmFiIiwidCI6IjAwYzliM2IxLTAzMTItNGMzMy1hZTdmLTgwZjNhNzU5ZGVjMSIsImMiOjh9" frameborder="0" allowFullScreen="true" />
          <v-flex xs12 md10 my-4>
            <div style="width: 100%">
              The map is based on geo-location data (i.e. IP addresses) of hashers connecting to the Bitcoin mining pools BTC.com, Poolin, and ViaBTC, who have kindly agreed to share aggregate-level data for research purposes.
              <sup @click="menu1 = true" style="text-decoration: underline; cursor: pointer">1</sup>
              <v-dialog v-model="menu1" :max-width="600" offset-x>
                <v-card>
                  <v-flex pa-4>
                    CCAF has at no point access to the underlying IP addresses, which are aggregated pool-side before being shared.
                  </v-flex>
                </v-card>
              </v-dialog>
              These pools collectively represent approximately 37% of Bitcoin total hashrate over the examined period.
              <sup @click="menu2 = true" style="text-decoration: underline; cursor: pointer">2</sup>
              <v-dialog v-model="menu2" :max-width="600" offset-x>
                <v-card>
                  <v-flex pa-4>
                    BTC.com, Pool Stats, available at:
                    <a target="_blank" href="https://btc.com/stats/pool?pool_mode=year">https://btc.com/stats/pool?pool_mode=year</a>
                  </v-flex>
                </v-card>
              </v-dialog>
              Figures are presented in aggregate to obfuscate pool-specific information. Please visit the
              <nuxt-link to="/mining_map/methodology">
                <span>Methodology</span>
              </nuxt-link>
              tab for further information.
            </div>
          </v-flex>
        </v-layout>
      </v-flex>
    </v-layout>
    <v-layout row align-center justify-center>
      <v-flex>
        <div>
          Underlying data provided by:
        </div>
      </v-flex>
    </v-layout>
    <v-layout row align-center justify-center>
      <v-flex mx-3>
        <a href="https://pool.btc.com" target="_blank" style="display: inline-block; width: 100%; height: 100%;" >
          <img src="~static/images/logo/BTCcom.png" :width="containerWidth/5">
        </a>
      </v-flex>
      <v-flex mx-3>
        <a href="https://www.poolin.com" target="_blank" style="display: inline-block; width: 100%; height: 100%;">
          <img src="~static/images/logo/Poolin.png" :width="containerWidth/5">
        </a>
      </v-flex>
      <v-flex mx-3>
        <a href="https://www.viabtc.com" target="_blank" style="display: inline-block; width: 100%; height: 100%;">
          <img src="~static/images/logo/ViaBTC.png" :width="containerWidth/5">
        </a>
      </v-flex>
    </v-layout>
    <v-layout row align-center justify-center style="width: 100%;">
      <v-flex xs12 md10 my-4>
        <div style="width: 100%; font-style: italic; text-align: center">
          <span>If you are a mining pool operator and would like to contribute, please
          <nuxt-link to="/contact/">
            <span>get in touch</span>
          </nuxt-link>
            !</span>
        </div>
      </v-flex>
    </v-layout>
  </v-layout>
</template>

<script>
import myMiddleware from '@/middleware/myMiddleware'

export default {
  name: 'index',
  layout: myMiddleware || 'demo',
  components: {
  },
  async fetch ({ store }) {
  },
  data() {
    return {
      menu1: false,
      menu2: false,
      swiperOption: {
        slidesPerView: 3,
        spaceBetween: 30
      },
      containerWidth: null
    }
  },
  mounted() {
    this.containerWidth = this.$vuetify.breakpoint.smAndDown
      ? document.getElementById("wrap-container").getBoundingClientRect().width
      : document.getElementById("wrap-container").getBoundingClientRect().width / 12 * 10
  }
}
</script>
