(function () {
  window.ckm = window.ckm || {}
  const name = new URLSearchParams(window.location.search).get('name')
  const url = '/api/topics/' + name
  const raw = window.sessionStorage.getItem(cacheKey())
  let data = null
  let updateTimer = null

  const nameEl = document.getElementById('name')
  if (nameEl) {
    nameEl.textContent = name
  }

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

  Object.assign(window.ckm.topic, {
    update, start, stop, render, get, url, name
  })

  const dataChart = ckm.chart.render('dataChart', 'bytes/s', { aspectRatio: 2 })
  const tableOptions = {
    url: `${ckm.topic.url}/partitions`,
    interval: 5000,
    pagination: true,
    keyColumns: ["number"],
    baseQuery: `name=${ckm.topic.name}`
  }
  const partitionsTable = ckm.table.renderTable('partitions', tableOptions, function (tr, p, all) {
    if (all) {
      ckm.table.renderCell(tr, 0, p.number)
    }
    ckm.table.renderCell(tr, 1, p.leader, 'center')
    ckm.table.renderCell(tr, 2, p.isr, 'center')
    ckm.table.renderCell(tr, 3, p.replicas, 'center')
    ckm.table.renderCell(tr, 4, p.metrics["LogStartOffset"], 'center')
    ckm.table.renderCell(tr, 5, p.metrics["LogEndOffset"], 'center')
    ckm.table.renderCell(tr, 6, p.metrics["Size"], 'center')
  })

  function updateView (response) {
    let dataStats = {
      send_details: response.bytes_out,
      receive_details: response.bytes_in
    }
    ckm.chart.update(dataChart, dataStats)

    var tBody = document.querySelector('#config tbody')
    ckm.dom.removeChildren(tBody);
    if (response.config !== undefined) {
      const keys = Object.keys(response.config)
      if (keys.length == 0) {
        var tr = document.createElement('tr')
        var td = document.createElement('td')
        td.attributes
        ckm.table.renderCell(tr, 0, td)
        tBody.appendChild(tr)
      } else {
        keys.forEach(k => {
          var tr = document.createElement('tr')
          ckm.table.renderCell(tr, 0, k)
          ckm.table.renderCell(tr, 1, response.config[k])
          tBody.appendChild(tr)
        })
      }
    }
  }

  document.querySelector("#deleteTopic").addEventListener('submit', function(evt) {
    evt.preventDefault();
    if (window.confirm('Are you sure? The topic is going to be deleted. Messages cannot be recovered after deletion.')) {
      showLoader()
      ckm.http.request('DELETE', ckm.topic.url)
        .then(() => { window.location = '/topics' })
        .catch(ckm.http.standardErrorHandler)
        .finally(rmLoader)
    }
  })

  ckm.topic.update((data) => {
    const form = ckm.topic.form('PATCH', data)
    document.querySelector('#deleteTopic').parentElement.insertAdjacentElement('beforebegin', form)
  })
  ckm.topic.start(updateView)
})()
