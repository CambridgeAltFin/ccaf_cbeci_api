import firebase from 'firebase'
import 'firebase/auth'
import 'firebase/database'
import 'firebase/storage'

const config = {
  apiKey: 'AIzaSyD_iVNWYedOALjoiDn9YRZj9B267RJEtSM',
  authDomain: 'ccaf-afea-test.firebaseapp.com',
  databaseURL: 'https://ccaf-afea-test.firebaseio.com',
  projectId: 'ccaf-afea-test',
  storageBucket: 'ccaf-afea-test.appspot.com',
  messagingSenderId: '936638856157',
  appId: '1:936638856157:web:36c3c80994aae098525434',
  measurementId: 'G-8YY657KG1C'
}

if (!firebase.apps.length) { firebase.initializeApp(config) }

export const storage = firebase.storage()
export const db = firebase.firestore()
export default firebase
