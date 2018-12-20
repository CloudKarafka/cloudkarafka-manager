;(function (g) {
  g.kafkaAuth.setup()

  var id = g.kafkaHelper.getParameterByName('id')
  var path = '/api/brokers/' + id

  document.querySelector('.main-heading h1').innerText = '/broker/' + id

  g.kafkaHelper.get(path, function (err, broker) {
    if (err) {
      console.error(err)
      return
    }
    g.kafkaHelper.renderTmpl(
      '#broker-overview',
      '#tmpl-broker-overview',
      broker
    )
    g.kafkaHelper.renderTmpl('#broker-metrics', '#tmpl-broker-metrics', broker)
    g.kafkaHelper.renderTmpl(
      '#broker-connections',
      '#tmpl-broker-connections',
      broker
    )
  })
  g.kafkaHelper.get(path + '/throughput', function (err, data) {
    document.querySelector('#throughput .loading-metric').hidden = true
    g.kafkaChart.drawThroughputChart('#throughput', '#chart', '#axis0', data)
  })
})(window)
