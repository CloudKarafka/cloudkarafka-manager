;(function (g) {
  g.kafkaAuth.setup()
  var name = g.kafkaHelper.getParameterByName('group')
  document.getElementById('pageHeader').textContent = name
  var path = '/api/consumer/' + name
  g.kafkaHelper.periodicGet(path, (_err, data) => {
    g.kafkaHelper.renderTmpl('#consumed-partitions', '#tmpl-consumed-partitions', data)
  })
}(window))
