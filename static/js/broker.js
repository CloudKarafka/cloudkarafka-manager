(function () {
  window.ckm = window.ckm || {}
  const broker_id = new URLSearchParams(window.location.search).get('id')
  const url = `/api/brokers/${broker_id}`
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
    return url + '/' + broker_id + '#' + user
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
    const table = document.querySelector('#broker')
    if (table) {
      table.querySelector('#leader_count').innerText = data.leader
      table.querySelector('#partition_count').innerText = data.partitions
      table.querySelector('#size').innerText = data.topic_size
      table.querySelector('#uptime').innerText = data.uptime
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

  Object.assign(window.ckm, {
    broker: {
      update, start, stop, render, get, url, broker_id
    }
  })

  const dataChart = ckm.chart.render('dataChart', 'bytes/s', { aspectRatio: 2 })
  const isrChart = ckm.chart.render('isrChart', 'changes/s', { aspectRatio: 2 })
  function updateView (response) {
    const dataStats = {
      send_details: response.bytes_out,
      receive_details: response.bytes_in
    }
    ckm.chart.update(dataChart, dataStats)
    const isrStats = {
      isr_shrink: response.isr_shrink,
      isr_expand: response.isr_expand
    }
    ckm.chart.update(isrChart, isrStats)
  }

  ckm.broker.start(updateView)
})()
