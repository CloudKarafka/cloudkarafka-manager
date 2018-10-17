;(function (g) {
  g.kafkaAuth.setup()
  g.kafkaHelper.get('/api/notifications', function (err, notifications) {
    g.kafkaHelper.renderTmpl(
      '#notifications',
      '#tmpl-notifications',
      notifications
    )
  })
  var boxData = [
    { title: 'Consumers', dataKey: 'consumer_count', icon: 'pacman' },
    { title: 'Topics', dataKey: 'topic_count', icon: 'three-bars' },
    { title: 'Brokers', dataKey: 'broker_count', icon: 'pulse' },
    {
      title: 'Total Topic Size',
      dataKey: 'topic_size',
      icon: 'inbox',
      formatter: function (v) {
        var r = g.kafkaHelper.humanFileSize(v, true)
        return r.value + r.unit
      }
    },
    {
      title: 'Total Topic Message Count',
      dataKey: 'topic_msg_count',
      icon: 'mail-read',
      formatter: function (v) {
        return v.toLocaleString()
      }
    },
    { title: 'Users', dataKey: 'user_count', icon: 'organization' }
  ]
  g.kafkaHelper.renderTmpl('#info-boxes', '#tmpl-boxes', boxData)
  g.kafkaHelper.periodicGet('/api/overview', function (err, data) {
    var b = boxData.map(function (row) {
      row.value = data[row.dataKey]
      if (row.formatter) {
        row.value = row.formatter(row.value)
      }
      if (row.value == null) {
        row.value = '-'
      }
      return row
    })
    g.kafkaHelper.renderTmpl('#info-boxes', '#tmpl-boxes', b)
  })
  g.kafkaHelper.get('/api/brokers/throughput').then(data => {
    document.querySelector('#throughput .loading-metric').hidden = true
    g.kafkaChart.drawThroughputChart('#throughput', '#chart', '#axis0', data)
  }).catch(e => {
		console.error(e)
    g.kafkaHelper.renderTmpl('#throughput', '#tmpl-no-throughput')
  })
})(window)
