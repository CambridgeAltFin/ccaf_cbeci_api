<template>
    <v-flex>
        <v-toolbar flat color="white">
            <v-toolbar-title>Figure 1: Evolution of Bitcoin mining equipment efficiency</v-toolbar-title>
        </v-toolbar>
        <highcharts :options="{
        chart: {
          marginBottom: (containerWidth > 400) ? 120 : 30,
          marginLeft: (containerWidth > 400) ? 100 : 65,
          marginRight: (containerWidth > 400) ? 100 : 5,
          height: (containerWidth > 400) ? '56%' : 300,
          width: (containerWidth > 400) ? containerWidth * 0.9 : containerWidth,
          type: 'scatter',
          zoomType: 'xy'
        },
        title: {
            text: null,
            align: 'left'
        },
        xAxis: {
            type: 'datetime',
            plotLines: [{
                color: 'rgb(170,170,170)',
                dashStyle: 'solid',
                value: (new Date()).getTime(), // Value of where the line will appear
                width: 1,
                label: {
                    text: 'Today',
                    verticalAlign: 'middle'
                }
             }]
        },
        yAxis: {
            title: {
                text: 'J/Gh'
            },
            max: 0.6,
            opposite: false,
            type: 'logarithmic'
        },
        legend: {
            enabled: false
        },
        plotOptions: {
            scatter: {
                marker: {
                    radius: 5,
                    states: {
                        hover: {
                            enabled: true,
                            lineColor: 'rgb(100,100,100)'
                        }
                    }
                },
                states: {
                    hover: {
                        marker: {
                            enabled: false
                        }
                    }
                },
                // tooltip: {
                //     pointFormat:  '{point.x} {point.y}',
                //     headerFormat: ''
                // }
            }
        },
        tooltip: {
            formatter: function () {
              const point = this.point
              return '<b>' + 'Miner name' + '</b>' + ': ' + point.pointName + '<br/>' +
              '<b>' + 'Date of release' + '</b>' + ': ' + Highcharts.dateFormat('%B %Y', this.x) + '<br/>' +
              '<b>' + point.series.name + '</b>' + ': ' + Highcharts.numberFormat(point.y, 2) + ' J/Gh';
            },
            shared: true
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
            name: 'Efficiency',
            color: '#ffb81c',
            data: data,
          },
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
import newData from '~/assets/newData.json'

if (typeof Highcharts === 'object') {
    exportingInit(Highcharts)
}

export default {
    name: 'Chart2',
    components: {
        highcharts: Chart,
    },
    data() {
        return {
            containerWidth: 1000,
            data: this.parseDate(newData)
        }
    },
    async mounted() {
        this.containerWidth = document.getElementById("wrap-container2").getBoundingClientRect().width
    },
    computed: {
        Highcharts() {
            return Highcharts
        },
    },
    methods: {
        parseDate(date) {
            return date.map((el)=> {
                return {
                    x: el[1] * 1000,
                    y: el[2],
                    pointName: el[0]
                }
            })
        },
    }
}
</script>

