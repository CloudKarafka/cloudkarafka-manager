;(function (g) {
  function parseConfig (input) {
    return input.split('\n').reduce((res, row) => {
      var [k, v] = row.split('=').map(v => v.trim())
      res[k] = v
      return res
    }, {})
  }

  g.kafkaAuth.setup()
  var name = g.kafkaHelper.getParameterByName('name')
  var form = document.querySelector('#topicForm')
  var reqMethod = 'post'
  form.addEventListener('submit', function (ev) {
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
    data.name = name
    var req = g.kafkaHelper[reqMethod]
    req(ev.currentTarget.action, data).then(() => {
      g.kafkaHelper.redirect('/topic/details.html?name=' + data.name)
    })
  })

  if (name) {
    document.getElementById('pageHeader').textContent = `Topics / Edit ${name}`
    g.kafkaHelper.get('/api/topics/' + name).then(t => {
      var form = document.getElementById('topicForm')
      reqMethod = 'put'
      form.action = '/api/topics/' + name
      form.name.value = name
      form.name.disabled = true
      form.replication_factor.value = t.replication_factor
      form.partition_count.value = t.partition_count
      form.config.value = Object.keys(t.config).map(k => `${k}=${t.config[k]}`).join('\n')
    })
  } else {
    document.getElementById('pageHeader').textContent = 'Topics / Add'
    form.querySelector('button[type="submit"]').textContent = 'Create new topic'
  }
})(window)
