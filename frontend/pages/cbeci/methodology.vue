<template>
    <v-layout id="wrap-container2" justify-center align-center wrap>
        <v-flex xs12 md10 my-4>
            <h1 class="display-4 text-xs-center">Methodology</h1>
        </v-flex>
        <overview />
        <model />
        <discussion />
    </v-layout>
</template>

<script>
import Overview from '~/components/methodology/Overview'
import Model from '~/components/methodology/Model'
import Discussion from '~/components/methodology/Discussion'
import ChangeLog from '~/components/methodology/ChangeLog'
import myMiddleware from '@/middleware/myMiddleware'

export default {
    name: 'methodology',
    layout: myMiddleware || 'default',
    components: {
        overview: Overview,
        model: Model,
        discussion: Discussion
    },
    data() {
        return {
        }
    },
  mounted() {
    if (typeof window === 'object') {
      window.addEventListener('resize', this.resize());
    }
  },
  methods: {
    resize () {
      const body = document.body
      const html = document.documentElement
      document.iframeHeight = Math.max( body.scrollHeight, body.clientHeight, body.offsetHeight,
        html.clientHeight, html.scrollHeight, html.offsetHeight )
    }
  }
}
</script>
