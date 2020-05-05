(function () {
  window.ckm = window.ckm || {}
  const resource_type = new URLSearchParams(window.location.search).get('type')
  const name = new URLSearchParams(window.location.search).get('name')
  const url = `/api/acls/${resource_type}/${name}`
  const raw = window.sessionStorage.getItem(cacheKey())
  let pattern_type = null
  let data = null
  let updateTimer = null

  if (raw) {
    try {
      data = JSON.parse(raw)
      if (data) {
        render(data)
      }
    } catch (e) {
      window.sessionStorage.removeItem(cacheKey())
      console.log('Error parsing data from sessionStorage')
      console.error(e)
    }
  }

  if (data === null) {
    update(render)
  }

  function cacheKey () {
    const user = window.sessionStorage.getItem('username')
    return url + '#' + user
  }

  function update (cb) {
    const headers = new window.Headers()
    ckm.http.request('GET', url, { headers }).then(function (response) {
      data = response
      try {
        window.sessionStorage.setItem(cacheKey(), JSON.stringify(response))
      } catch (e) {
        console.error('Saving sessionStorage', e)
      }
      render(response)
      if (cb) {
        cb(response)
      }
    }).catch(ckm.http.standardErrorHandler).catch(stop)
  }

  function render (data) {
    const table = document.querySelector('#acl')
    pattern_type = data.pattern_type
    if (table) {
      table.querySelector('#a-name').innerText = data.name
      table.querySelector('#a-resource_type').innerText = data.resource_type
      table.querySelector('#a-pattern_type').innerText = data.pattern_type
    }
  }

  function start (cb) {
    update(cb)
    updateTimer = setInterval(() => update(cb), 5000)
  }

  // Show that we're offline in the UI
  function stop () {
    if (updateTimer) {
      clearInterval(updateTimer)
    }
  }

  function get (key) {
    return new Promise(function (resolve, reject) {
      try {
        if (data) {
          resolve(data[key])
        } else {
          update(data => {
            resolve(data[key])
          })
        }
      } catch (e) {
        reject(e.message)
      }
    })
  }

  Object.assign(window.ckm, {
    acl: {
      update, start, stop, render, get, url, name, pattern_type, resource_type
    }
  })
  const tableOptions = {
    url: `${ckm.acl.url}/users`,
    interval: 5000,
    pagination: true,
    keyColumns: ['principal'],
    baseQuery: `name=${ckm.acl.name}&type=${ckm.acl.resource_type}`
  }
  const usersTable = ckm.table.renderTable('users', tableOptions, function (tr, p, all) {
    if (all) {
      ckm.table.renderCell(tr, 0, p.principal)
    }
    ckm.table.renderCell(tr, 1, p.permission_type, 'center')
    ckm.table.renderCell(tr, 2, p.operation, 'center')
    ckm.table.renderCell(tr, 3, p.host, 'center')
    const btn = document.createElement('button')
    btn.classList.add('btn-danger')
    btn.innerHTML = 'Delete'
    btn.addEventListener('click', function (evt) {
      const url = '/api/acls'
      const body = {
        resource_type: ckm.acl.resource_type,
        pattern_type: ckm.acl.pattern_type,
        name: ckm.acl.name,
        principal: p.principal,
        permission: p.operation,
        permission_type: p.permission_type
      }
      if (window.confirm('Are you sure? The ACL rule will be deleted and all principals authorized with this rule will be unable to access the resource.')) {
        ckm.http.request('DELETE', url, { body }).then(() => {
          ckm.dom.removeNodes(tr)
        }).catch(ckm.http.standardErrorHandler)
      }
    })
    ckm.table.renderCell(tr, 4, btn, 'right')
  })
  ckm.acl.start()
})()
