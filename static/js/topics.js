(function (ckm) {
  const tableOptions = {
    url: '/api/topics',
    interval: 5000,
    pagination: true,
    search: false,
    keyColumns: ['name']
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
    if (item.config !== undefined) {
      tags.appendChild(ckm.dom.createBadge('C', 'This topic has custom configuration.', 'primary'))
    }
    ckm.table.renderCell(tr, 2, tags)
  })
  const form = ckm.topic.form('POST', {}, topicsTable.fetchAndUpdate)
  document.getElementsByTagName('main')[0].insertAdjacentElement('beforeend', form)
  ckm.topic = ckm.topic || {}
  ckm.topic.table = topicsTable
})(window.ckm)
