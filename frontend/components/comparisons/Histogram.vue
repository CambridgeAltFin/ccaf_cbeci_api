<template>
    <v-flex xs12 md10>
        <v-switch
                v-model="log"
                label="Logarithmic scale"
                color="#ffb81c"
        ></v-switch>
        <highcharts :options="{
        chart: {
          marginBottom: (containerWidth > 1000) ? 100 : 60,
          reflow: false,
          marginLeft: (containerWidth > 1000) ? 100 : 55,
          marginRight: (containerWidth > 1000) ? 100 : 5,
          height: (containerWidth > 1000) ? '56%' : 400,
          width: (containerWidth > 1000) ? containerWidth * 0.9 : containerWidth,
          type: 'column'
        },
        title: {
            text: 'Country ranking, annual electricity consumption',
            align: 'left'
        },
        tooltip: {
            formatter: function () {
              const point = this.points[0]
              return '<b>' + point.point.country + '</b><br/>' +
              'Total consumption: ' + point.y + ' TWh<br/>' +
              'Country rank: ' + point.x + '<br/>' +
              'Bitcoin percentage: ' + point.point.bitcoin_percentage + '%';
            },
            shared: true
        },
        xAxis: {
            type: 'categories',
            tickAmount: 60,
            min: 0,
            max: 59,
            labels: {
                formatter: function () {
                    return (isBitcoin(this.value) ? '<b>Bitcoin</b>' : '');
                },
                rotation: -45,
                y: 40,
                style: {
                    fontSize: '14px',
                    fontFamily: 'Verdana, sans-serif',
                    color: '#ffb81c'
                }
            }
        },
        yAxis: {
            title: { text: 'TWh per year' },
            max: 7000,
            opposite: false,
            type: log ? 'logarithmic' : ''
        },
        legend: {
            enabled: false
        },
        navigator: {
            enabled: false
        },
        scrollbar: {
            enabled: false
        },
        rangeSelector: {
            enabled: false,
        },
        series: [
          {
            name: 'Country Ranking',
            color: '#a2bdff',
            data: data,
            type: 'column',
          }
        ],
        exporting: {
            enabled: false
        }
      }" />
    </v-flex>
</template>

<script>
import {Chart} from 'highcharts-vue'
import Highcharts from 'highcharts'
import exportingInit from 'highcharts/modules/exporting'
if (typeof Highcharts === 'object') {
    exportingInit(Highcharts)
}

export default {
    name: 'Histogram',
    components: {
        highcharts: Chart,
    },
    data() {
        return {
            containerWidth: 1000,
            log: false
        }
    },
    async mounted() {
        this.containerWidth = document.getElementById("wrap-container3").getBoundingClientRect().width
    },
    computed: {
        data() {
            return this.$store.state.countries
        }
    },
    methods: {
        isBitcoin(n) {
            const country = this.data.find(el => el.x === n)
            if (!country) return ''
            return country.country === 'Bitcoin'
        }
    }
}
</script>
