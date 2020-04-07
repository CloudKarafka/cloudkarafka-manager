(function() {
  const tableOptions = {
    url: "/api/consumers",
    keyColumns: ['name'],
    interval: 5000,
    pagination: true,
    columnSelector: false,
    search: false
  }
  const queuesTable = ckm.table.renderTable('cgs', tableOptions, function (tr, item, all) {
    const href = '/consumer?name=' + encodeURIComponent(item.name)
    const queueLink = ckm.dom.createLink(href, item.name)
    ckm.table.renderCell(tr, 0, queueLink)

    if (item.online) {
      ckm.table.renderCell(tr, 1, ckm.helpers.formatNumber(item.clients.length), 'right')
    } else {
      const badge = ckm.dom.createBadge('Offline',
        'This consumer group has no clients', 'danger')
      ckm.table.renderCell(tr, 1, badge, 'right')
    }
    div = document.createElement('div')
    item.topics.slice(0,5).forEach(t => {
      var text = t.name
      if (item.online) {
          text += ": " + t.lag;
      }
      var badge = ckm.dom.createBadge(text, '', 'primary')
      div.appendChild(badge)
    })
    if (item.topics.length > 5) {
      var badge = ckm.dom.createBadge('...', 'Consuming from more topics', 'primary')
      div.appendChild(badge)
    }
    ckm.table.renderCell(tr, 2, div)
  })
})()
