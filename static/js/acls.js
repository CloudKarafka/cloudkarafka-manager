(function() {
  const tableOptions = {
    url: `/api/acls`,
    keyColumns: ['name'],
    interval: 5000,
    pagination: true,
  }
  const aclTable = ckm.table.renderTable('acls', tableOptions, function (tr, item, all) {
    if (all) {
      const queueLink = document.createElement('a')
      queueLink.href = `/acl?name=${encodeURIComponent(item.name)}&type=${encodeURIComponent(item.resource_type)}`
      queueLink.textContent = item.name
      ckm.table.renderCell(tr, 0, queueLink)
    }
    ckm.table.renderCell(tr, 1, item.resource_type, 'center')
    ckm.table.renderCell(tr, 2, item.pattern_type, 'center')
    ckm.table.renderCell(tr, 3, item.users, 'center')
  })
  const addBtn = document.querySelector('#createACL .btn-primary')
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("input", "Name", { placeholder: 'Pattern' })
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("input", "Principal", { placeholder: 'User:USERNAME' })
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("select", "Resource type", ["Cluster", "Group", "Topic"])
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("select", "Permission", ["create", "describe", "alter", "read", "write", "alter_configs", "cluster_action", "delete", "describe", "describe_configs", "idempotent_write", "all"])
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("select", "Permission type", ["Allow", "Deny"])
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("select", "Pattern type", ["Litteral", "Prefixed"])
  )
  addBtn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("input", "Host", {value: "*"})
  )
  document.querySelector('#createACL').addEventListener('submit', function(evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const name = encodeURIComponent(data.get('name'))
    const url = '/api/acls'
    const body = {
      name: name,
      principal: data.get("principal"),
      resource_type: data.get("resource_type"),
      permission: data.get("permission"),
      permission_type: data.get("permission_type"),
      pattern_type: data.get("pattern_type"),
      host: data.get("host")
    }
    ckm.http.request('POST', url, { body }).then(() => {
      evt.target.reset()
      aclTable.fetchAndUpdate()
      ckm.dom.toast(`ACL ${name} created`)
    }).catch(ckm.http.standardErrorHandler)
  })
})()
