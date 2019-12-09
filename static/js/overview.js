/* global CloudKarafka Manager */
(function () {
  window.ckm = window.ckm || {}

  const url = '/api/overview'
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
    document.querySelector('#version').innerText = data.version
    const table = document.querySelector('#overview')
    if (table) {
      table.querySelector('.brokers').innerText = data.brokers
      table.querySelector('.topics').innerText = data.topics
      table.querySelector('.partitions').innerText = data.partitions
      table.querySelector('.topic_size').innerText = ckm.helpers.formatNumber(data.topic_size)
      table.querySelector('.messages').innerText = ckm.helpers.formatNumber(data.messages)
      table.querySelector('.consumers').innerText = data.consumers
      table.querySelector('.uptime').innerText = data.uptime
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
    overview: {
      update, start, stop, render, get
    }
  })
})()
