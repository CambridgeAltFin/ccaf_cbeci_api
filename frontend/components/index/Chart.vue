<template>
    <v-flex my-4>
        <!--<v-progress-circular v-if="progress" indeterminate :size="50" :width="5"/>-->
        <highcharts :constructor-type="'stockChart'" :options="{
        chart: {
          marginBottom: (containerWidth > 1000) ? 100 : 30,
          reflow: false,
          marginLeft: (containerWidth > 1000) ? 100 : 60,
          marginRight: (containerWidth > 1000) ? 100 : 30,
          height: (containerWidth > 1000) ? '56%' : 400,
          width: (containerWidth > 1000) ? containerWidth * 0.9 : containerWidth,
          zoomType: 'x'
        },
        credits: {
            enabled: false
        },
        title: {
            text: 'Bitcoin electricity consumption, TWh (annualised)',
            align: 'left'
        },
        subtitle: {
            text: (containerWidth > 400) ? 'Select an area by dragging across the lower chart' : '',
            align: 'left'
        },
        xAxis: {
            type: 'datetime'
        },
        yAxis: {
            title: {
                text: 'TWh (annualised)',
                style: { fontWeight: 'bold' },
            },
            // max: 100,
            maxZoom: 0.1,
            opposite: false,
            tickInterval: 20
        },
        tooltip: {
            formatter: function () {
              return charts.dateFormat('%A %B %e %Y', this.x) + '<br/>' +
              '<b>' + this.points[1].series.name + '</b>' + ': ' + charts.numberFormat(this.points[1].y, 2) + '<br/>' +
              '<b>' + this.points[2].series.name + '</b>' + ': ' + charts.numberFormat(this.points[2].y, 2) + '<br/>' +
              '<b>' + this.points[0].series.name + '</b>' + ': ' + charts.numberFormat(this.points[0].y, 2);
            },
            shared: true
        },
        legend: {
            enabled: false
        },
        plotOptions: {
            series: {
                showInNavigator: (containerWidth > 400),
                marker: {
                    enabled: false,
                    states: {
                        hover: {
                            enabled: true,
                            radius: 3
                        }
                    }
                }
            }
        },
        navigator: {
            series: {
                dataGrouping: {
                }
            },
            enabled: (containerWidth > 400)
        },
        scrollbar: {
            enabled: (containerWidth > 400)
        },
        rangeSelector: {
            enabled: (containerWidth > 400),
            buttonTheme: { // styles for the buttons
                fill: 'none',
                stroke: 'none',
                'stroke-width': 0,
                r: 8,
                style: {
                    color: '#ffb81c',
                    fontWeight: 'bold'
                },
                states: {
                    hover: {
                    },
                    select: {
                        fill: '#ffb81c',
                        style: {
                            color: 'white'
                        }
                    }
                }
            },
            inputBoxBorderColor: 'gray',
            inputBoxWidth: 90,
            inputBoxHeight: 18,
            inputPosition: {
                x: (containerWidth > 1200) ? -90 : 0
            },
            inputStyle: {
                color: '#ffb81c',
                fontWeight: 'bold'
            },
            labelStyle: {
                color: 'silver',
                fontWeight: 'bold'
            },
            selected: 5
        },
        series: [
          {
            name: 'Lower bound consumption',
            color: '#f2d596',
            data: dataSerieaMIN,
            lineWidth: 1.5
          },
          {
            name: 'Upper bound consumption',
            color: '#bcbcbc',
            data: dataSerieaMAX,
            lineWidth: 1
          },
          {
            name: 'Estimated consumption',
            color: '#ffb81c',
            data: dataSerieaESTIMATED,
            lineWidth: 2.5
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
import charts from 'highcharts'
import stockInit from 'highcharts/modules/stock'


if (typeof charts === 'object') {
    stockInit(charts)
}

export default {
    name: 'Chart',
    components: {
        highcharts: Chart,
    },
    data() {
        return {
            containerWidth: 1000
        }
    },
    mounted() {
        this.containerWidth = document.getElementById("wrap-container").getBoundingClientRect().width
    },
    computed: {
        dataSerieaMIN() {
            const data  = this.$store.getters.GET_DATA || []
            const res = []
            data.forEach((el) => {
                res.push([el.timestamp * 1000, el.min_consumption])
            })
            return res
        },
        dataSerieaMAX() {
            const data  = this.$store.getters.GET_DATA || []
            const res = []
            data.forEach((el) => {
                res.push([el.timestamp * 1000, el.max_consumption])
            })
            return res
        },
        dataSerieaESTIMATED() {
            const data  = this.$store.getters.GET_DATA || []
            const res = []
            data.forEach((el) => {
                res.push([el.timestamp * 1000, el.guess_consumption])
            })
            return res
        },
        charts() {
            return charts
        },
        progress() {
            return this.$store.state.progress2
        }
    }
}
</script>
