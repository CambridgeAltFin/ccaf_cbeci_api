require('dotenv').config()
import VuetifyLoaderPlugin from 'vuetify-loader/lib/plugin'
import pkg from './package'
import shrinkRay from 'shrink-ray-current'

export default {
  mode: 'spa',
  head: {
    title: 'Cambridge Bitcoin Electricity Consumption Index (CBECI)',
    meta: [
      { charset: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { hid: 'description', name: 'description', content: pkg.description },
      { hid: 'keywords', name: 'keywords', content: 'Bitcoin, Cryptocurrency, Electricity, Energy, Mining, Efficiency, PoW, ASIC, Proof-of-work, Environmental, Blockchain, Index' }
    ],
    link: [
      { rel: 'icon', type: 'image/x-icon', href: '/favicon.ico' },
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css?family=Exo+2:300,400,500,700|Material+Icons'
      },
      {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css?family=Open+Sans:300,400,500,700|Material+Icons'
      },
      {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css?family=Sorts+Mill+Goudy&display=swap'
      }
    ]
  },
  css: [
    '~/assets/style/app.styl',
    '~/assets/style/base.scss',
    'katex/dist/katex.min.css'
  ],
  plugins: [
    '@/plugins/vuetify',
    { src: '~plugins/katex2js.js', ssr: false },
  ],

  server: {
      port: process.env.PORT || 7776
  },
  buildModules: [
    '@nuxtjs/dotenv'
  ],
  modules: [
    '@nuxtjs/axios',
    '@nuxtjs/robots',
    'cookie-universal-nuxt',
    '@nuxtjs/dotenv',
    ['@nuxtjs/google-gtag', {
      id: process.env.GA_ID || 'UA-143050724-1'
    }]
  ],
  robots: {
      UserAgent: '*',
      Allow: '/'
  },
  render: {
      compressor: shrinkRay()
  },
  axios: {
    proxyHeaders: false,
    credentials: false,
    baseURL: process.env.BASE_URL || 'https://cbeci.org/api'
  },
  build: {
    transpile: ['vuetify/lib'],
    plugins: [new VuetifyLoaderPlugin()],
    loaders: {
      stylus: {
        import: ['~assets/style/variables.styl']
      }
    },
    extend(config, ctx) {
    }
  }
}
