import Vue from 'vue'
import axios from 'axios'
import _ from 'lodash'
import Cookies from 'js-cookie'
import * as contentAPI from '~/api/content.js'

export const state = () => ({
  data: [],
  numbers: [0,0,0],
  price: 0.05,
  countries: [],
  progress: true,
  progress2: true,
  cooks: true,
  authenticated: false,
  change_log: {},
  contact: {},
  reports: [],
  error: {
    status: false,
    message: '',
    blocks: []
  }
})

export const getters = {
  GET_DATA: state => state.data,
  GET_COUNTRIES: state => state.countries,
  authenticated: state => {
    return state.authenticated || (Cookies.get('authenticated') ? JSON.parse(Cookies.get('authenticated')).value === 'true' : false)
  }
}

export const mutations = {
  SET_ERROR(state, payload) {
    if (state.error.blocks.length > 10) {
      state.error.block = []
    }
    state.error.status = payload.status
    state.error.message = payload.message
    state.error.blocks.push(payload.blocks)
  },
  SET_DATA(state, payload) { state.data = payload },
  SET_NUMBERS(state, [estimated, min, max]) { state.numbers = [estimated, min, max]},
  SET_PRICE(state, payload) { state.price = payload },
  SET_COUNTRIES(state, payload) { state.countries = payload},
  SET_PROGRESS(state, payload) { state.progress = payload},
  SET_PROGRESS2(state, payload) { state.progress2 = payload},
  SET_COOK(state, payload) {
      state.cooks = payload
      if(payload) {
          Cookies.set('CookieControl', {analytics:'true'}, {
              expires: 365
          });
      }
  },
  SET_AUTH(state, payload) {
    state.authenticated = payload
    if(payload) {
      Cookies.set('authenticated', {value:'true'}, {
        expires: 365
      });
    }
  },
  SET_PAGE (state, {name, payload}) { state[name] = payload},
  SET_REPORTS (state, payload) { state.reports = payload }
}

export const actions = {
  async getPage ({ commit }, {pageName, variableName}) {
    const pageContent = await contentAPI.getPage(pageName)
    commit('SET_PAGE', {name: variableName, payload: pageContent})
  },
  async getReports ({ commit }) {
    const reports = await contentAPI.getReports()
    commit('SET_REPORTS', reports)
  },
    signInWithEmail({ commit }, pass) {
      if (pass === 'qr3Vd2ehS3ei1') {
        commit('SET_AUTH', true)
      }
    },
    async LOAD_COUNTRIES({ commit }) {
        try {
            const res = await this.$axios.$get('/countries')
            await commit('SET_COUNTRIES', res)
        } catch (e) { console.log(e) }
    },

    async LOAD_DATA ({ commit }, price) {
        try {
            const res = await this.$axios.$get(`/data/${price}`)
          await commit('SET_DATA', res.data)
        } catch (e) {
          commit('SET_ERROR', {status: true, message: 'Data is not available now', blocks: 'chart'})
        }
    },

    async LOAD_NUMBERS ({ commit, state }, price) {
        try {
            if (!price) price = state.price
            await commit('SET_PROGRESS', true)
            const [estimated, min, max] = await Promise.all([
                this.$axios.$get(`/guess/${price}`),
                this.$axios.$get(`/min/${price}`),
                this.$axios.$get(`/max/${price}`)
            ])
          await commit('SET_NUMBERS', [estimated, min, max])
            await commit('SET_PROGRESS', false)
        } catch (e) {
          commit('SET_ERROR', {status:true, message: 'Too many requests from your IP address, please try again later.'})
        }
    },

    INITIALIZATION: async ({ commit, dispatch, state }) => {
        if (state.numbers[0] !== 0) return
        try {
            await commit('SET_PROGRESS2', true)
            await Promise.all([
                dispatch('LOAD_DATA', state.price || 0.05),
                dispatch('LOAD_NUMBERS', state.price || 0.05)
            ])
            await commit('SET_PROGRESS2', false)
        } catch (e) { console.log(e) }
    },

    UPDATE_DATA_AFTER_PRICE_CHANGE: ({ commit, dispatch }, price) => {
        try {
            commit('SET_PRICE', price)
            dispatch('LOAD_DATA', price)
            dispatch('LOAD_NUMBERS', price)
        } catch (e) { console.log(e) }
    }
}
