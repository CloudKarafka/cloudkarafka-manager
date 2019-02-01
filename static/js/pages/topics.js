;(function (g) {
  g.kafkaAuth.setup()
  g.kafkaHelper.renderListTmpl(
    '#topic-rows',
    '#tmpl-topics',
    '/api/topics',
    function () {
      table = new DataTable('#topics', {
        perPageSelect: false,
        searchable: false,
        sortable: false,
        perPage: 100,
        columnDefs: [{ type: 'file-size', targets: 1 }]
      })
      document
        .querySelector('#filterTable')
        .addEventListener('keyup', function () {
          table.search(this.value)
        })
    }
  )
})(window)
