<template>
  <v-app>
    <v-toolbar fixed app color="#ffb81c" class="header">
      <v-layout align-center row justify-start>
        <v-flex mr-4 shrink class="pointer logo1">
          <a href="https://www.jbs.cam.ac.uk/faculty-research/centres/alternative-finance/" target="_blank">
            <img src="~static/logo.png" class="logo">
          </a>
        </v-flex>
        <v-divider vertical/>
        <v-flex mx-3 shrink>
          <img src="~static/logo2.png" class="logo2">
        </v-flex>
        <v-spacer />
        <span class="hidden-sm-and-down" style="font-size: 26px">
          Cambridge Bitcoin Electricity Consumption Index
        </span>
        <v-dialog v-model="dialog">
          <template v-slot:activator="{ on }">
            <v-toolbar-side-icon style="position: absolute; right: 16px" class="hidden-md-and-up" v-on="on" />
          </template>
          <v-card>
            <v-layout class="mobile-menu">
              <v-flex>
                <v-layout align-center justify-center style="height: 60px; font-size: 20px; font-weight: bold;">
                  CBECI
                </v-layout>
                <v-btn nuxt link flat exact to="/" style="width: 100%" @click="dialog=false">
                  Index
                </v-btn>
                <v-btn nuxt link flat exact to="/cbeci/methodology" style="width: 100%" @click="dialog=false">
                  Methodology
                </v-btn>
                <v-btn nuxt link flat exact to="/cbeci/comparisons" style="width: 100%" @click="dialog=false">
                  Comparisons
                </v-btn>
                <v-divider />
                <v-layout align-center justify-center style="height: 60px; font-size: 20px; font-weight: bold;">
                  MINING MAP
                </v-layout>
                <v-btn nuxt link flat exact to="/mining_map" style="width: 100%" @click="dialog=false">
                  Visualisation
                </v-btn>
                <v-btn nuxt link flat exact to="/mining_map/methodology" style="width: 100%" @click="dialog=false">
                  Methodology
                </v-btn>
                <v-divider />
                <v-btn nuxt link flat exact to="/faq" style="width: 100%" @click="dialog=false">
                  FAQ
                </v-btn>
                <v-btn nuxt link flat exact to="/change-log" style="width: 100%" @click="dialog=false">
                  Change Log
                </v-btn>
                <v-btn nuxt link flat exact to="/contact" style="width: 100%" @click="dialog=false">
                  Contact
                </v-btn>
              </v-flex>
            </v-layout>
          </v-card>
        </v-dialog>
      </v-layout>
    </v-toolbar>
    <v-layout column class="menu hidden-sm-and-down">
      <v-list class="demo" expand>
        <v-list-group :value="true">
          <template v-slot:activator>
            <v-list-tile>
              <v-list-tile-content>
                <v-list-tile-title>
                  CBECI
                </v-list-tile-title>
              </v-list-tile-content>
            </v-list-tile>
          </template>
          <v-list-tile>
            <nuxt-link :class="{'active': $route.path === '/'}" to="/">
              Index
            </nuxt-link>
          </v-list-tile>
          <v-list-tile>
            <nuxt-link :class="{'active': $route.path === '/cbeci/methodology'}" to="/cbeci/methodology">
              Methodology
            </nuxt-link>
          </v-list-tile>
          <v-list-tile>
            <nuxt-link :class="{'active': $route.path === '/cbeci/comparisons'}" to="/cbeci/comparisons">
              Comparisons
            </nuxt-link>
          </v-list-tile>
        </v-list-group>
        <v-list-group :value="true">
          <template v-slot:activator>
            <v-list-tile>
              <v-list-tile-content>
                <v-list-tile-title>
                  MINING MAP
                </v-list-tile-title>
              </v-list-tile-content>
            </v-list-tile>
          </template>
          <v-list-tile>
            <nuxt-link :class="{'active': $route.path === '/mining_map'}" to="/mining_map">
              Visualisation
            </nuxt-link>
          </v-list-tile>
          <v-list-tile>
            <nuxt-link :class="{'active': $route.path === '/mining_map/methodology'}" to="/mining_map/methodology">
              Methodology
            </nuxt-link>
          </v-list-tile>
        </v-list-group>
        <v-list-tile>
          <nuxt-link :class="{'active': $route.path === '/faq/'}" to="/faq/">
            FAQ
          </nuxt-link>
        </v-list-tile>
        <v-list-tile>
          <nuxt-link :class="{'active': $route.path === '/change-log/'}" to="/change-log/">
            Change Log
          </nuxt-link>
        </v-list-tile>
        <v-list-tile>
          <nuxt-link :class="{'active': $route.path === '/contact/'}" to="/contact/">
            Contact
          </nuxt-link>
        </v-list-tile>
      </v-list>
    </v-layout>
    <v-content>
      <v-container>
        <nuxt />
      </v-container>
    </v-content>
    <v-footer height="auto">
      <v-card
              class="flex"
              flat
              tile
      >
        <v-card-actions class="footer justify-center">
          Cambridge Centre for Alternative Finance © {{ new Date().getFullYear() }}
        </v-card-actions>
      </v-card>
    </v-footer>
    <client-only>
      <Cookie />
    </client-only>
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
        dialog: false,
        methodologyAnchors: [
          {text: '1', link: ''},
          {text: '2', link: '/#1'},
          {text: '3', link: '/#2'},
          {text: '4', link: '/#3'},
        ]
      }
    },
    computed: {
      cookies() {
        if (!process.server) {
          if (this.$cookies.get('CookieControl')) {
            if (this.$cookies.get('CookieControl').analytics !== 'true')
              this.$store.commit('SET_COOK', false)
          } else {
            this.$store.commit('SET_COOK', false)
          }
        }
        return !this.$store.state.cooks
      }
    },
    methods: {
      gotoMain() {
        this.$router.push('/')
      },
      setCookies() {
        this.$store.commit('SET_COOK', true)
      },
      binding() {
        const binding = {}
        if (!process.server) {
          if (this.$vuetify.breakpoint.xsOnly) binding.column = true
        }
        return binding
      }
    }
  }
</script>

<style lang="scss" scoped>
  .mobile-menu{
    a {
      margin: 0;
      border-radius: 0 !important;
      background-color: #FFFFFF !important;
      height: 40px !important;
      text-transform: none !important;
      font-family: 'MyriadProSemiBold', sans-serif;
      &.v-btn--active {
        background-color: black !important;
        color: white !important;
      }
    }
  }
</style>
