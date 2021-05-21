<template>
    <v-layout v-bind="binding" my-4 align-center align-content-space-around>
        <v-flex xs12 md4 ma-3>
            <v-card elevation="5">
                <v-flex pa-4 class="card text-xs-center">
                    <v-flex mb-4>
                        <h4 class="display-1">Theoretical <br/>lower bound</h4>
                    </v-flex>
                    <v-flex class="card__number-sm font-weight-bold">
                        <v-progress-circular v-if="progress" indeterminate :size="50" :width="5"/>
                        <span v-else>{{ numbers[1] | decimals }}</span>
                    </v-flex>
                    <v-flex mb-3 class="card__number-sm">
                        <span>GW</span>
                    </v-flex>
                    <v-divider />
                    <v-flex mt-3 class="card__text-orange font-weight-bold">
                        <span>{{ numbers2[1] | decimals }}</span>
                    </v-flex>
                    <v-flex>
                        <span>TWh</span>
                    </v-flex>
                </v-flex>
            </v-card>
        </v-flex>
        <v-flex xs12 md4 ma-3>
            <v-card elevation="5">
                <v-flex pa-4 class="card text-xs-center">
                    <v-flex mb-4>
                        <h4 class="display-1">Estimated</h4>
                    </v-flex>
                    <v-flex class="card__number-lg font-weight-bold">
                        <v-progress-circular v-if="progress" indeterminate :size="50" :width="5"/>
                        <span v-else>{{ numbers[0] | decimals }}</span>
                    </v-flex>
                    <v-flex mb-3 class="card__number-sm">
                        <span>GW</span>
                    </v-flex>
                    <v-divider />
                    <v-flex mt-3 class="card__description">
                        <span>Annualised consumption </span>
                        <v-tooltip max-width="400" bottom>
                            <template v-slot:activator="{ on }">
                                <v-icon style="cursor: pointer" v-on="on">help</v-icon>
                            </template>
                            <span>
                                Bitcoin’s total electricity consumption is annualised assuming constant power usage over the period of one year. A 7-day moving average is applied to the annualised estimate in order to smoothen the impact of short-term hashrate volatility and enable better comparisons with other uses of electricity.
                                <br><br>
                                More information can be found in the Methodology section under “Representation”.
                            </span>
                        </v-tooltip>
                    </v-flex>
                    <v-flex class="card__text-orange font-weight-bold">
                        <span>{{ numbers2[0] | decimals }}</span>
                    </v-flex>
                    <v-flex>
                        <span>TWh</span>
                    </v-flex>
                </v-flex>
            </v-card>
        </v-flex>
        <v-flex xs12 md4 ma-3>
            <v-card elevation="5">
                <v-flex pa-4 class="card text-xs-center">
                    <v-flex mb-4>
                        <h4 class="display-1">Theoretical <br/>upper bound</h4>
                    </v-flex>
                    <v-flex class="card__number-sm font-weight-bold">
                        <v-progress-circular v-if="progress" indeterminate :size="50" :width="5"/>
                        <span v-else>{{ numbers[2] | decimals }}</span>
                    </v-flex>
                    <v-flex mb-3 class="card__number-sm">
                        <span>GW</span>
                    </v-flex>
                    <v-divider />
                    <v-flex mt-3 class="card__text-orange font-weight-bold">
                        <span>{{ numbers2[2] | decimals }}</span>
                    </v-flex>
                    <v-flex>
                        <span>TWh</span>
                    </v-flex>
                </v-flex>
            </v-card>
        </v-flex>
    </v-layout>
</template>

<script>
    import axios from 'axios'
    import {percentage, decimals} from '~/assets/js/filters.js'

    export default {
        name: 'Cards',
        filters: {
            percentage,
            decimals
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
      computed: {
            binding() {
                const binding = {}
                if (!process.server) {
                    if (this.$vuetify.breakpoint.xsOnly) binding.column = true
                }
                return binding
            },
            numbers() {
                return this.$store.state.numbers
            },
            numbers2() {
                const data  = [...this.$store.getters.GET_DATA].pop() || {}
                return [data.guess_consumption || 0, data.min_consumption || 0, data.max_consumption || 0]
            },
            progress() {
                return this.$store.state.progress
            }
        },
        methods: {
            async getNewData() {
              await this.$store.dispatch('LOAD_NUMBERS')
            }
        }
    }
</script>
