<template>
  <v-app>
    <v-layout column class="menu hidden-sm-and-down">
      <v-list class="demo" expand>
        <v-list-tile>
          <nuxt-link :class="{'active': $route.path === '/mining_map'}" to="/mining_map?page=tmp2">
            Mining Map
          </nuxt-link>
        </v-list-tile>
        <v-list-tile>
          <nuxt-link :class="{'active': $route.path === '/mining_map/methodology'}" to="/mining_map/methodology?page=tmp2">
            Methodology
          </nuxt-link>
        </v-list-tile>
      </v-list>
    </v-layout>
    <v-content style="padding: 0 !important;">
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
