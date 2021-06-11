<template>
    <v-layout justify-center align-center wrap>
        <v-flex xs12 md10 my-4>
            <h1 class="display-4 text-xs-center">Contact</h1>
        </v-flex>
        <feedback ref="feedback" />
<!--        <team />-->
        <v-flex xs12 md10 my-3 py-3>
            <div class="contact_us__content" v-html="page.content" />
        </v-flex>
        <reports ref="carousel" />
    </v-layout>
</template>


<script>
import feedback from '~/components/contact/Feedback'
import reports from '@/components/Reports.vue'
import team from '~/components/contact/Team_contributors'
import { mapState } from 'vuex'

export default {
    name: 'contact',
    components: {
        feedback: feedback,
		team: team,
      reports: reports
    },
    data() {
        return {

        }
    },
  async fetch({store}) {
    await store.dispatch('getPage', {pageName: 'cbeci:contact', variableName: 'contact'})
    await store.dispatch('getReports')
  },
  computed: {
    ...mapState({
      page: 'contact',
    })
  },
  mounted () {
    if (typeof document !== 'undefined') {
      const form = document.getElementsByClassName('contact-form')
      if (form) {
        form[0].appendChild(this.$refs.feedback.$el)
      }
    }
    if (typeof document !== 'undefined') {
      const carousel = document.getElementsByClassName('carousel')
      console.log(carousel)
      console.log(this.$refs.carousel.$el)
      if (carousel) {
        carousel[0].appendChild(this.$refs.carousel.$el)
      }
    }
  }
}
</script>

