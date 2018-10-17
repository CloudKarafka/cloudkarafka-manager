;(function (g) {
  function parseConfig (input) {
    return input.split('\n').reduce((res, row) => {
      var [k, v] = row.split('=').map(v => v.trim())
      res[k] = v
      return res
    }, {})
  }

  g.kafkaAuth.setup()
  document.querySelector('#topicForm').addEventListener('submit', function (ev) {
    ev.preventDefault()
    var fields = [
      { field: 'name', format: v => v },
      { field: 'partition_count', format: parseInt },
      { field: 'replication_factor', format: parseInt },
      { field: 'config', format: parseConfig }
    ]
    var data = fields.reduce((res, f) => {
      res[f.field] = f.format(this[f.field].value)
      return res
    }, {})
    g.kafkaHelper.post(ev.currentTarget.action, data).then(() => {
      g.kafkaHelper.redirect('/topic/details.html?name=' + data.name)
    })
  })

  var name = g.kafkaHelper.getParameterByName('name')
  if (name) {
    g.kafkaHelper.get('/api/topics/' + name).then(t => {
      var form = document.getElementById('topicForm')
      form.method = 'put'
      form.name.value = name
      form.replication_factor.value = t.replication_factor
      form.partition_count.value = t.partition_count
    })
  }
})(window)
