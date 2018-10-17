;(function (g) {
  g.kafkaAuth.setup()
  g.kafkaHelper.renderListTmpl(
    '#broker-rows',
    '#tmpl-brokers',
    '/api/brokers',
    function () {}
  )
})(window)
