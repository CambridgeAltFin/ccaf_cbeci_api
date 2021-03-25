export default context => {
  const { route: { query } } = context
  if (query.page === 'tmp1') {
    return 'tmpLayout'
  } else {
    return false
  }
}
