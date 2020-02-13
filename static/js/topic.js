(function () {
  window.ckm = window.ckm || {}
  const name = new URLSearchParams(window.location.search).get('name')
  const url = '/api/topics/' + name
  const raw = window.sessionStorage.getItem(cacheKey())
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
    return url + '/' + name + '#' + user
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
    const table = document.querySelector('#topic')
    if (table) {
      table.querySelector('#t-partitions').innerText = data.partitions
      if (data.size === undefined) {
        table.querySelector('#t-size').innerText = '-'
      } else {
        table.querySelector('#t-size').innerText = ckm.helpers.formatNumber(data.size)
      }
      if (data.size === undefined) {
        table.querySelector('#t-messages').innerText = '-'
      } else {
        table.querySelector('#t-messages').innerText = ckm.helpers.formatNumber(data.message_count)
      }
    }
  }

  function form(method = 'POST', data = {}, cb = null) {
    const form = document.createElement('form')
    const h3 = document.createElement('h3')
    form.classList.add('form', 'card')
    if (method === 'POST') {
      h3.innerText = 'Create topic'
    } else {
      h3.innerText = 'Edit topic'
    }
    form.appendChild(h3)

    function input(name, element, options) {
      const label = document.createElement('label')
      const span = document.createElement('span')
      span.innerText = name
      label.appendChild(span)
      const elem = document.createElement(element)
      elem.name = name.toLowerCase().replace(' ', '_')
      Object.keys(options).forEach((key) => {
        var val = options[key]
        if (val !== undefined) { elem[key] = val }
      })
      label.appendChild(elem)
      return label
    }
    form.appendChild(input('Name', 'input', {
      type: 'text',
      value: data.name,
      readOnly: data.name !== undefined,
      placeholder: 'Topic name'
    }))
    form.appendChild(input('Partitions', 'input', {
      type: 'number',
      value: data.partitions || 10,
      min: data.partitions || 1
    }))
    form.appendChild(input('Replication factor', 'input', {
      type: 'number',
      value: data.replication_factor || 1,
      min: data.replication_factor || 1
    }))
    form.appendChild(input('Config', 'textarea', { value: data.config,
      placeholder: '\'{"key": "value"}\''
    }))
    const btn = document.createElement('button')
    btn.classList.add('btn-primary')
    btn.type = 'submit'
    btn.innerText = h3.innerText
    form.appendChild(btn)
    form.addEventListener('submit', function(evt) {
      evt.preventDefault()
      const data = new window.FormData(this)
      const name = encodeURIComponent(data.get('name'))
      const url = `/api/topics/${name}`
      const body = {
        name: name,
        partitions: parseInt(data.get("partitions")),
        replication_factor: parseInt(data.get("replication_factor")),
      }
      if (data.get('config') !== '') {
        var config = ckm.dom.parseJSON(data.get('config'))
        if (config == false) { return }
        body.config = config
      }
      ckm.http.request(method, url, { body }).then(() => {
        if (cb !== null) {
          evt.target.reset()
          cb()
        }
        ckm.dom.toast(`Topic ${name} created`)
      }).catch(ckm.http.standardErrorHandler)
    })
    return form
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
    topic: {
      update, start, stop, render, get, url, name, form
    }
  })
})()

