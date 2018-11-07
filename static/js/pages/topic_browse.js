;(function (g) {
  g.kafkaAuth.setup()

  var tbody = document.querySelector('#msgs-rows')
  var conf = {
    keyFormatter: 'byte-array',
    valueFormatter: 'byte-array'
  }
  var formatters = {
    string: x => x,
    'byte-array': x => x,
    json: x => JSON.stringify(JSON.parse(x), null, 2)
  }
  var name = g.kafkaHelper.getParameterByName('name')
  document.getElementById('pageHeader').textContent = `Topic / ${name} / browse `
  document.querySelectorAll('select[data-set-format]').forEach(e => {
    e.onchange = e => {
      conf[e.currentTarget.name] = e.currentTarget.value
    }
  })
  var browserStartBtn = document.querySelector('#browser-start')
  browserStartBtn.onclick = start
  document.querySelector('#browser-stop').onclick = close

  function formatValue (v) {
    return formatters[conf.valueFormatter](v)
  }
  function formatKey (v) {
    return formatters[conf.keyFormatter](v)
  }
  function renderMessage2 (msg) {
    var renderSmall = function (td, v, t) {
      if (v) {
        var s1 = document.createElement('small')
        s1.textContent = (t ? t + ': ' : '') + v
        td.appendChild(s1)
        td.appendChild(document.createElement('br'))
      }
    }
    var tr = document.createElement('tr')
    var td = document.createElement('td')
    td.style.verticalAlign = 'top'
    renderSmall(td, formatKey(msg['key']), 'Key')
    renderSmall(td, msg['partition'], 'Parttion')
    renderSmall(td, msg['offset'], 'Offset')
    renderSmall(td, msg['timestamp'], 'Timestamp')
    if (msg['headers'] && msg['headers'].length > 0) {
      td.appendChild(document.createElement('hr'))
      msg['headers'].forEach(function (e) {
        renderSmall(td, e)
      })
    }
    tr.appendChild(td)
    var msgTd = document.createElement('td')
    msgTd.style.verticalAlign = 'top'
    var pre = document.createElement('pre')
    pre.textContent = formatValue(msg['message'])
    msgTd.appendChild(pre)
    tr.appendChild(msgTd)
    tbody.insertBefore(tr, tbody.firstChild)
    // tbody.appendChild(tr)
  }

  var evSource
  function start () {
    browserStartBtn.textContent = 'Connecting...'
    browserStartBtn.disabled = true
    close()
    var url = `/api/topics/${name}/browse?_a=${g.kafkaAuth.authHeader()}&kf=${conf.keyFormatter}&vf=${conf.valueFormatter}`
    evSource = new g.EventSource(url)
    window.addEventListener('beforeunload', function (e) {
      close()
    })
    evSource.onerror = function (e) {
      console.error(arguments)
    }
    evSource.onmessage = function (e) {
      browserStartBtn.textContent = 'Consuming...'
      var data = JSON.parse(e.data)
      renderMessage2(data)
    }
  }
  function close () {
    if (evSource) {
      console.log('close')
      browserStartBtn.disabled = false
      browserStartBtn.textContent = 'Start consumer'
      evSource.close()
    }
  }
})(window)
