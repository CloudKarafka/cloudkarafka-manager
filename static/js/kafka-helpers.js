;(function (g) {
  function redirect (path) {
    if (window.location.pathname !== path) {
      window.location = path
    }
  }

  function getParameterByName (name, url) {
    if (!url) url = window.location.href
    name = name.replace(/[\[\]]/g, '\\$&')
    var regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)')
    var results = regex.exec(url)
    if (!results) return null
    if (!results[2]) return ''
    return decodeURIComponent(results[2].replace(/\+/g, ' '))
  }

  function handleResponseErrors (response) {
    if (response.ok) {
      return response
    }
    response.text().then(r => {
      switch (response.status) {
        case 400:
          notify(r, { level: 'warn' })
          break
        case 401:
          redirect('/login')
          break
        case 500:
          notify(r, { level: 'error' })
          break
      }
      throw new Error(response.responseText)
    })
  }

  function periodicGet (path, callback, timeout) {
    timeout = timeout || 10000
    get(path)
      .then(r => {
        callback(null, r)
        setTimeout(() => {
          periodicGet(path, callback, timeout)
        }, timeout)
      })
      .catch(e => callback(e))
    return null
  }

  function get (path, callback) {
    return g
      .fetch(g.location.origin + path, {
        headers: {
          Accept: 'application/json',
          'X-Request-ID': requestId(),
          Authorization: 'Basic ' + g.kafkaAuth.authHeader()
        }
      })
      .then(handleResponseErrors)
      .then(r => r.json())
      .then(r => {
        // Legacy callback support
        if (callback) {
          callback(null, r)
        }
        return r
      })
  }

  function del (path, callback) {
    return g.fetch(g.location.origin + path, {
      method: 'DELETE',
      headers: {
        Authorization: 'Basic ' + g.kafkaAuth.authHeader(),
        'X-Request-ID': requestId()
      }
    })
  }

  function post (path, data, callback) {
    return g.fetch(g.location.origin + path, {
      method: 'POST',
      headers: {
        Authorization: 'Basic ' + g.kafkaAuth.authHeader(),
        'X-Request-ID': requestId(),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    })
  }

  function put (path, data, callback) {
    return g.fetch(g.location.origin + path, {
      method: 'PUT',
      headers: {
        Authorization: 'Basic ' + g.kafkaAuth.authHeader(),
        'X-Request-ID': requestId(),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    })
  }

  function requestId () {
    var r = g.localStorage.getItem('RequestID')
    if (!r) {
      r = Math.floor((1 + Math.random()) * 0x10000).toString(16)
      g.localStorage.setItem('RequestID', r)
    }
    return r
  }

  function notify (msg, config) {
    var color = 'cobalt'
    if (config.level === 'warn') {
      color = 'honey'
    } else if (config.level === 'error') {
      color = 'ruby'
    }
    g.notific8(msg, { theme: 'chicchat', color: color })
  }

  function element (id) {
    return document.querySelector(id)
  }

  function elementHtml (id) {
    return element(id).innerHTML
  }

  function renderTmpl (attachToId, tmplId, elements) {
    var $attachTo = element(attachToId)
    var tmpl = g.Handlebars.compile(elementHtml(tmplId))
    $attachTo.innerHTML = tmpl(elements)
  }

  function renderListTmpl (attachToId, tmplId, path, clb) {
    periodicGet(path, (err, elements) => {
      if (err) {
        console.error(err)
        renderTmpl(attachToId, tmplId, { elements: [] })
        return
      }
      elements = elements || []
      renderTmpl(attachToId, tmplId, { elements: elements })
      clb(elements)
    })
  }

  function humanFileSize (bytes) {
    var thresh = 1024
    if (Math.abs(bytes) < thresh) {
      return { value: bytes, unit: 'B' }
    }
    var units = ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    var u = -1
    do {
      bytes /= thresh
      ++u
    } while (Math.abs(bytes) >= thresh && u < units.length - 1)
    return { value: bytes.toFixed(1), unit: units[u] }
  }
  var formatters = {
    filesize: function filesize (value) {
      var res = humanFileSize(value)
      return new g.Handlebars.SafeString(
        res.value + '<small>' + res.unit + '</small>'
      )
    },
    bandwidth: function bandwidth (value) {
      var res = humanFileSize(value)
      return new g.Handlebars.SafeString(
        res.value + '<small>' + res.unit + '/s</small>'
      )
    },
    toLocalString: function toLocalString (value) {
      return value.toLocalString()
    }
  }
  g.Handlebars.registerHelper('humanFileSize', function (bytes, def) {
    if (bytes === null || bytes === undefined) {
      return def
    }
    var res = humanFileSize(bytes)
    return new g.Handlebars.SafeString(
      res.value + '<small>' + res.unit + '</small>'
    )
  })
  var onlyDigitsRe = /^\d+$/
  g.Handlebars.registerHelper('humanize', function (value) {
    if (onlyDigitsRe.test(value)) {
      value = new Number(value)
      value = value.toLocaleString()
    } else if (typeof value === 'number') {
      value = value.toLocaleString()
    }
    return value
  })

  g.Handlebars.registerHelper('toLocaleString', function (elem, def) {
    if (elem === null || elem === undefined) {
      return def
    }
    return elem.toLocaleString()
  })
  g.Handlebars.registerHelper('consumerLag', function (current, logEnd) {
    if (!current || !logEnd) {
      return '-'
    }
    return (logEnd - current).toLocaleString()
  })
  g.Handlebars.registerHelper('list', function (list) {
    if (Array.isArray(list)) {
      return list.join(',')
    }
    return list
  })
  g.Handlebars.registerHelper('metric', function (value, f) {
    if (!value) {
      return ''
    }

    var v = value.reduce((acc, v) => acc + v.value, 0)
    if (f && typeof f === 'string') {
      return formatters[f](v)
    }
    return v.toLocaleString()
  })
  g.Handlebars.registerHelper('connectionCount', function (conn, key) {
    if (!conn || !conn[key]) {
      return 0
    }
    var v = conn[key].reduce((acc, v) => acc + v.value, 0)
    return v.toLocaleString()
  })

  document.addEventListener('DOMContentLoaded', function (event) {
    var c = document.querySelector('.app-container')
    var t = document.querySelector('.kafka-toggle-menu')
    var b = document.querySelector('.sidebar-backdrop')
    if (t) {
      t.onclick = function (e) {
        e.preventDefault()
        c.classList.toggle('sidebar-open')
      }
    }
    if (b) {
      b.onclick = function (e) {
        e.preventDefault()
        c.classList.remove('sidebar-open')
      }
    }
  })
  g.kafkaHelper = {
    get,
    del,
    post,
    put,
    requestId,
    redirect,
    periodicGet,
    renderTmpl,
    renderListTmpl,
    humanFileSize,
    getParameterByName
  }
})(window)
