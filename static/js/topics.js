(function () {
  const tableOptions = {
    url: "/api/topics",
    interval: 5000,
    pagination: true,
    search: false,
    keyColumns: ["name"]
  }
  const topicsTable = ckm.table.renderTable('topics', tableOptions, function (tr, item, all) {
    if (all) {
      const queueLink = document.createElement('a')
      queueLink.href = '/topic?name=' + encodeURIComponent(item.name)
      queueLink.textContent = item.name
      ckm.table.renderCell(tr, 0, queueLink)
    }

    ckm.table.renderCell(tr, 1, ckm.helpers.formatNumber(item.partitions), 'right')

    var tags = document.createElement('div')
    if (item.message_count !== undefined) {
      ckm.table.renderCell(tr, 2, ckm.helpers.formatNumber(item.message_count || 0), 'right')
    } else {
      ckm.table.renderCell(tr, 2, '-', 'right')
      tags.appendChild(ckm.dom.createBadge('msg', 'Unable to fetch metrics for message count .', 'primary'))
    }
    if (item.size !== undefined) {
      ckm.table.renderCell(tr, 3, ckm.helpers.formatNumber(item.size || 0), 'right')
    } else {
      ckm.table.renderCell(tr, 3, '-', 'right')
      tags.appendChild(ckm.dom.createBadge('size', 'Unable to fetch metrics for topic size .', 'primary'))
    }
    if (item.config !== undefined) {
      tags.appendChild(ckm.dom.createBadge('C', 'This topic has custom configuration.', 'primary'))
    }
    ckm.table.renderCell(tr, 4, tags)
  })
  const form = ckm.topic.form('POST', {}, topicsTable.fetchAndUpdate)
  document.getElementsByTagName('main')[0].insertAdjacentElement('beforeend', form)
})()
