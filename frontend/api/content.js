import { db } from '~/plugins/fireinit'

async function getPage (name) {
  const doc = await db.collection('TextPages').doc(name).get()
  if (doc.exists) {
    return doc.data()
  } else {
    return {}
  }
}

async function getReports (name) {
  const snapShot = await db.collection('Reports')
    .where('is_active', '==', true)
    .where('project', '==', 'gafb')
    .get()
  if (snapShot.empty) {
    return {}
  } else {
    return snapShot.docs.map(el => el.data())
  }
}

export {
  getPage,
  getReports
}
