export default context => {
  const { route: { query } } = context
  switch (query.page) {
    case 'tmp1':
      return 'tmpLayout'
    default:
      return false
  }
}
