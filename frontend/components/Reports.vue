<template>
  <v-layout justify-center align-center class="mt-10" no-gutters>
    <v-flex cols="12">
      <swiper ref="mySwiper" :options="swiperOptions">
        <swiper-slide v-for="report in sortImages" :key="report.order_position">
          <v-flex cols3>
            <a
              target="_blank"
              :href="report.src"
            >
              <img :src="report.image" :alt="report.name" object-fit="contain" width="107px">
            </a>
          </v-flex>
        </swiper-slide>
      </swiper>
    </v-flex>
  </v-layout>
</template>

<script>
import { storage } from '~/plugins/fireinit'

export default {
  name: 'reports',
  data () {
    return {
      swiperOptions: {
        slidesPerView: 6
      },
      images: []
    }
  },
  computed: {
    reports () {
      return this.$store.state.reports
    },
    sortImages () {
      return this.images.sort((a,b) => a.order_position - b.order_position)
    }
  },
  async mounted () {
    this.reports.forEach(async report => {
      const image = await storage.ref(`reports/${report.filename}`).getDownloadURL()
      const obj = report
      obj.image = image
      this.images.push(obj)
    })
  }
}
</script>
