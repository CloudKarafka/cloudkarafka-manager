(function() {
  const tableOptions = {
    url: "/api/brokers",
    keyColumns: ['id'],
    interval: 5000,
    pagination: false,
  }
  const queuesTable = ckm.table.renderTable('brokers', tableOptions, function (tr, item, all) {
    if (all) {
      const queueLink = document.createElement('a')
      queueLink.href = '/broker?id=' + encodeURIComponent(item.id)
      queueLink.textContent = item.id
      ckm.table.renderCell(tr, 0, queueLink)
    }
    if (item.controller) {
      const badge = ckm.dom.createBadge('C', 'Controller', 'primary')
      ckm.table.renderCell(tr, 1, badge, 'center')
    } else {
      ckm.table.renderCell(tr, 1, '', 'center')
    }
    ckm.table.renderCell(tr, 2, item.kafka_version, 'center')
    ckm.table.renderCell(tr, 3, item.host, 'center')
    ckm.table.renderCell(tr, 4, item.uptime, 'center')
  })
})()
