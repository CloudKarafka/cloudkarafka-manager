(function() {
  const tableOptions = {
    url: "/api/users",
    interval: 5000,
    pagination: true,
    keyColumns: ["name"]
  }
  const usersTable = ckm.table.renderTable('users', tableOptions, function (tr, item, all) {

    ckm.table.renderCell(tr, 0, item)
    const btn = document.createElement('button')
    btn.classList.add('btn-danger')
    btn.innerHTML = 'Delete'
    btn.addEventListener('click', function(evt) {
      const u = encodeURIComponent(item)
      const url = `/api/users/${u}`
      if (window.confirm('Are you sure? The user is going to be deleted and all using this user will not be able to access the cluster any more.')) {
        ckm.http.request('DELETE', url).then(() => {
          ckm.dom.removeNodes(tr)
        }).catch(ckm.http.standardErrorHandler)
      }
    })

    ckm.table.renderCell(tr, 1, btn, 'right')


  })
  const btn = document.querySelector('#createUser .btn-primary')
  btn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("input", "Name", { placeholder: 'Username' })
  )
  btn.insertAdjacentElement('beforebegin',
    ckm.dom.formInput("input", "Password",
      { type: 'password', placeholder: '******' })
  )
  document.querySelector('#createUser').addEventListener('submit', function(evt) {
    evt.preventDefault()
    const data = new window.FormData(this)
    const name = encodeURIComponent(data.get('name'))
    const url = '/api/users'
    const body = {
      name: name,
      password: data.get("password")
    }
    ckm.http.request('POST', url, { body }).then(() => {
      evt.target.reset()
      usersTable.fetchAndUpdate()
      ckm.dom.toast(`User ${name} created`)
    }).catch(ckm.http.standardErrorHandler)
  })
})()
