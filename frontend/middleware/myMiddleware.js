export default context => {
  const { route: { query } } = context
  console.log('Привет всем!')
  if (query.page === 'tmp1') {
    return 'tmpLayout'
  } else {
    return false
  }
}
