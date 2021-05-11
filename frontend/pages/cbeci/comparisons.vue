<template>
    <v-layout id="wrap-container3" justify-center align-center wrap>
        <v-layout my-4 justify-center align-center>
            <h1 class="display-4">Comparisons</h1>
        </v-layout>
        <v-flex mb-4>
            <v-layout align-center justify-center wrap>
                <v-flex xs12 md10 class="main-text">
                    <p>While terawatt-hours (TWh) are a standard unit used to measure electricity consumption, it can be difficult for some to put these figures into perspective without additional context.</p>
                    <p>This section attempts to provide an unbiased and objective ground for helping visitors to independently assess the magnitude of Bitcoin’s electricity consumption and compare it to other uses of electricity.</p>
                    <p><em><strong>Note:</strong> All comparisons are based on our <a href="https://cbeci.org/" target="_blank">best-guess estimate</a> of Bitcoin's total electricity consumption. The listed comparisons are for illustrative purposes only and do not suggest that one type of electricity consumption can or should be substituted for another.</em></p>
                </v-flex>
            </v-layout>
        </v-flex>
        <client-only>
            <comparisonsPC />
            <comparisonsRP />
            <comparisonsCards />
            <v-layout mb-4 align-center justify-center wrap>
                <histogram />
            </v-layout>
        </client-only>
        <comparisonsFF />
    </v-layout>
</template>

<script>
import countriesCards from '~/components/comparisons/Countries'
import PC from '~/components/comparisons/PC'
import RP from '~/components/comparisons/RP'
import FF from '~/components/comparisons/FF'
import Histogram from '~/components/comparisons/Histogram'
import myMiddleware from '@/middleware/myMiddleware'

export default {
    name: 'comparisons',
  layout: myMiddleware || 'default',
  components: {
        comparisonsCards: countriesCards,
        comparisonsPC: PC,
        comparisonsRP: RP,
        comparisonsFF: FF,
        histogram: Histogram
        // maskMars: MaskMars
    },
    data() {
        return {
          interval: {}
        }
    },
    beforeMount() {
      const isClientSide = typeof window !== 'undefined'
      if (isClientSide) {
        const self = this
        this.interval = setInterval(function () {
          self.getNewData()
        }, 30000)
      }
    },
    beforeDestroy() {
        clearInterval(this.interval)
    },
    async fetch ({ store }) {
        try {
            await store.dispatch('UPDATE_DATA_AFTER_PRICE_CHANGE', 0.05)
            await Promise.all([
                store.dispatch('INITIALIZATION'),
                store.dispatch('LOAD_COUNTRIES')
            ])
        } catch (e) {
            alert(e)
        }
    },
    methods: {
        async getNewData() {
          await this.$store.dispatch('LOAD_NUMBERS')
        }
    }
}
</script>
