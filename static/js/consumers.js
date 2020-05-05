(function (ckm) {
  const tableOptions = {
    url: '/api/consumers',
    keyColumns: ['name'],
    interval: 5000,
    pagination: true,
    columnSelector: false,
    search: false
  }
  ckm.table.renderTable('cgs', tableOptions, function (tr, item, all) {
    const href = '/consumer?name=' + encodeURIComponent(item.name)
    const queueLink = ckm.dom.createLink(href, item.name)
    ckm.table.renderCell(tr, 0, queueLink)

    if (item.online) {
      ckm.table.renderCell(
        tr,
        1,
        ckm.helpers.formatNumber(item.unique_clients.length),
        'right'
      )
    } else {
      const badge = ckm.dom.createBadge(
        'Offline',
        'This consumer group has no clients',
        'danger'
      )
      ckm.table.renderCell(tr, 1, badge, 'right')
    }
    const div = document.createElement('div')
    const topics = {}
    item.clients.forEach(c => {
      topics[c.topic] = topics[c.topic] || 0
      topics[c.topic] += Math.max(0, (c.log_end - c.offset))
    })
    Object.keys(topics).forEach(name => {
      const lag = topics[name]
      var badge = ckm.dom.createBadge(name + ': ' + lag, '', 'primary')
      div.appendChild(badge)
    })
    if (item.topics.length > 5) {
      var badge = ckm.dom.createBadge(
        '...',
        'Consuming from more topics',
        'primary'
      )
      div.appendChild(badge)
    }
    ckm.table.renderCell(tr, 2, div)
  })
})(window.ckm)
