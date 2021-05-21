<template>
    <v-layout id="wrap-container2" justify-center align-center wrap>
        <v-flex xs12 md10 my-4>
            <h1 class="display-4 text-xs-center">Change Log</h1>
        </v-flex>
<!--        <change-log />-->
        <v-flex xs12 md10 my-3 py-3>
            <div class="contact_us__content" v-html="page.content" />
        </v-flex>
    </v-layout>
</template>

<script>
import ChangeLog from '~/components/methodology/ChangeLog'
import { mapState } from 'vuex'


export default {
    name: 'changes',
    components: {
        changeLog: ChangeLog
    },
  async fetch({store}) {
    await store.dispatch('getPage', {pageName: 'cbeci:change_log', variableName: 'change_log'})
  },
    data() {
        return {
        }
    },
  computed: {
    ...mapState({
      page: 'change_log',
    })
  },
}
</script>
