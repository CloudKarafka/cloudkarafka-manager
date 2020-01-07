(function () {
  window.ckm = window.ckm || {}
  const topic = new URLSearchParams(window.location.search).get('name')
  const url = '/api/topics/' + topic
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
    return url + '/' + topic + '#' + user
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
      table.querySelector('#t-partitions').innerText = data.partitions.length
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
      update, start, stop, render, get
    }
  })
})()

