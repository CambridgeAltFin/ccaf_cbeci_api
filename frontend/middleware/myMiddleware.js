export default context => {
  const { route: { query } } = context
  if (query.page === 'tmp1') {
    return 'tmpLayout'
  } else if (query.page === 'tmp2') {
    return 'tmpLayoutMap'
  } else {
    return false
  }
}
