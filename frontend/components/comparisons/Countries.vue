<template>
    <v-flex mb-4 xs10>
        <v-layout my-4 align-center justify-center>
            <h2 class="display-3 text-xs-center" @click="showFlags">
                Country Ranking
            </h2>
        </v-layout>
        <v-layout v-bind="binding" align-center justify-center my-3>
            <v-flex v-for="(country, i) in countries" :key="i" ma-3 :xs2="i!==2" :xs3="i===2">
                <v-card v-if="i === 2" elevation="5">
                    <v-flex px-3 py-4 class="text-xs-center">
                        <img
                            src="data:image/png;base64,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"
                            class="country-img-large"
                        >
                        <br/>
                        <v-flex mb-4>
                            <h4 class="display-1" style="min-height: 36px; font-size: 16px!important;">{{ country.country }}</h4>
                        </v-flex>
                        <span style="font-size: 40px">
                            <b>{{ country.y | decimals }}</b>
                        </span>
                        <br>
                        <span>TWh<br> per year</span>
                    </v-flex>
                </v-card>
                <v-card v-else elevation="5">
                    <v-flex px-3 py-4 class="text-xs-center">
                        <img :src="getFlag(country.country)" class="country-img">
                        <br/>
                        <v-flex mb-4>
                            <h4 class="display-1" style="min-height: 54px; font-size: 14px!important;">{{ country.country }}</h4>
                        </v-flex>
                        <span style="font-size: 24px">
                            <b>{{ country.y | decimals }}</b>
                        </span>
                        <br>
                        <span>TWh<br> per year</span>
                    </v-flex>
                </v-card>
            </v-flex>
        </v-layout>
        <v-layout my-4 wrap align-center justify-center>
            <v-flex xs12>
                <span>
              <b>Source:</b><br/>
              <a target="_blank" href="https://www.eia.gov/international/data/world/electricity/electricity-consumption?pd=2&p=0000002&u=0&f=A&v=mapbubble&a=-&i=none&vo=value&t=C&g=00000000000000000000000000000000000000000000000001&l=249-ruvvvvvfvtvnvv1vrvvvvfvvvvvvfvvvou20evvvvvvvvvvnvvvs0008&s=315532800000&e=1546300800000&">U.S. Energy Information Administration
                </a> country data, 2019 est. (or most recent available year)
          </span>
            </v-flex>
        </v-layout>
    </v-flex>
</template>

<script>
    import flags from '~/assets/flags.json'
    import {percentage, decimals} from '~/assets/js/filters.js'

    export default {
        name: 'Countries',
        filters: {
            percentage,
            decimals
        },
        data() {
            return {
            }
        },
        computed: {
            binding() {
                const binding = {}
                if (!process.server) {
                    if (this.$vuetify.breakpoint.mdAndDown) binding.column = true
                }
                return binding
            },
            progress() {
                return this.$store.state.progress
            },
            countries() {
                const arr = this.$store.getters.GET_COUNTRIES
                const i = arr.findIndex(el => el.country === 'Bitcoin')
                return [arr[i+2] || {}, arr[i+1] || {}, arr[i] || {}, arr[i-1] || {}, arr[i-2] || {}]
            }
        },
        methods: {
          showFlags() {
          },
          getFlag(country) {
            const flag = flags.find(item => item.name === country)
            return flag ? flag.flag : ''
          }
        }
    }
</script>
