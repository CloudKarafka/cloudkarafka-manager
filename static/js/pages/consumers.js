;(function (g) {
  g.kafkaAuth.setup()
  g.kafkaHelper.renderListTmpl(
    '#consumer-rows',
    '#tmpl-consumers',
    '/api/consumers',
    function () {
      table = new DataTable('#consumers', {
        perPageSelect: false,
        searchable: false,
        sortable: false,
        perPage: 100
      })
      document
        .getElementById('filterTable')
        .addEventListener('keyup', function () {
          table.search(this.value)
        })
    }
  )
})(window)
