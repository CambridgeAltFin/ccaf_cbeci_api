<template>
  <v-app>
    <v-content>
      <v-container>
        <nuxt />
      </v-container>
    </v-content>
  </v-app>
</template>

<script>
  import Cookie from '@/components/Cookie.vue'
  export default {
    components: {
      Cookie
    },
    data() {
      return {
      }
    },
    computed: {
    },
    methods: {
    }
  }
</script>
