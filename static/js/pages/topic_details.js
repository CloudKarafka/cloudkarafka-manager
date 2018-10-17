;(function (g) {
  g.kafkaAuth.setup()
  var name = g.kafkaHelper.getParameterByName('name')
  document.querySelector('#editTopicBtn').href = '/topic/edit.html?name=' + name
  document.querySelector('#consumeTopicBtn').href =
    '/topic/browse.html?name=' + name
  document.querySelector('.main-heading h1').innerText = 'topics/' + name

  var path = '/api/topics/' + name
  document
    .querySelector('#delete-topic')
    .addEventListener('submit', function (evt) {
      evt.preventDefault()
      g.kafkaHelper.del('/api/topics/' + name).then(() => g.kafkaHelper.redirect('/topics'))
    })
  g.kafkaHelper.periodicGet(path, function (err, topic) {
    if (err) throw err
    g.kafkaHelper.renderTmpl('#topic-overview', '#tmpl-topic-overview', topic)
    g.kafkaHelper.renderTmpl('#topic-metrics', '#tmpl-topic-metrics', topic)
    g.kafkaHelper.renderTmpl('#topic-config', '#tmpl-topic-config', topic)
    g.kafkaHelper.renderTmpl('#partition-rows', '#tmpl-partition-rows', topic)
  })
  g.kafkaHelper.get(path + '/throughput').then(data => {
    document.querySelector('#throughput .loading-metric').hidden = true
    g.kafkaChart.drawThroughputChart('#throughput', '#chart', '#axis0', data)
  }).catch(e => {
    g.kafkaHelper.renderTmpl('#throughput', '#tmpl-no-throughput')
  })
})(window)
