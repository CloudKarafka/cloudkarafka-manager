(function() {
  const tableOptions = {
    url: "/api/consumers",
    keyColumns: ['name'],
    interval: 5000,
    pagination: true,
    columnSelector: true,
    search: true
  }
  const queuesTable = ckm.table.renderTable('cgs', tableOptions, function (tr, item, all) {
    var div = document.createElement('div')
    const href = '/consumer?name=' + encodeURIComponent(item.name)
    const queueLink = ckm.dom.createLink(href, item.name)
    div.appendChild(queueLink)
    if (item.online == false) {
      const badge = ckm.dom.createBadge('O', 'This consumer group has no clients', 'danger')
      div.appendChild(badge)
    }
    ckm.table.renderCell(tr, 0, div)

    ckm.table.renderCell(tr, 1, ckm.helpers.formatNumber(item.clients.length), 'right')
    div = document.createElement('div')
    item.topics.slice(0,5).forEach(t => {
      var text = t.name + ": " + t.lag;
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
