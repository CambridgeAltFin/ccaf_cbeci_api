<template>
    <v-layout id="wrap-container2" justify-center align-center wrap>
        <v-flex xs12 md10 my-4>
            <h1 class="display-4 text-xs-center">Methodology</h1>
        </v-flex>
        <overview/>
    </v-layout>
</template>

<script>
  import Overview from '~/components/mining_methodology/Overview'
  import myMiddleware from '@/middleware/myMiddleware'

  export default {
    name: 'methodology',
    layout: myMiddleware || 'demo',
    mounted() {
    },
    components: {
      overview: Overview,
    },
    data() {
      return {}
    }
  }
</script>
