;(function (g) {
  function setup () {
    return testLoggedIn()
  }

  function testLoggedIn () {
    var hash = g.location.hash
    if (hash.startsWith('#/login')) {
      var arr = hash.split('/')
      setAuth(arr[2], arr[3])
      g.location.hash = ''
      g.kafkaHelper.redirect('/')
    }
    return g.kafkaHelper.get('/api/whoami').then(d => {
      setUsername()
      setClusterName()
      return d
    })
  }

  function setUsername () {
    var el = document.querySelector('#username')
    if (el) {
      el.innerText = getCookieValue('username')
    }
  }

  function setClusterName () {
    var hostname = g.location.host.split('.')[0]
    var el = document.querySelector('#cluster-name')
    if (el) {
      el.innerText = hostname
    }
  }

  function authHeader () {
    if (getCookieValue('auth')) {
      return 'Basic ' + decodeURIComponent(getCookieValue('auth'))
    } else {
      return null
    }
  }

  function signout () {
    clearCookieValue('auth')
    clearCookieValue('username')
    g.kafkaHelper.redirect('/login')
  }

  function setAuth (username, password) {
    clearCookieValue('auth')
    clearCookieValue('username')
    var userinfo = username + ':' + password
    var b64 = window.btoa(userinfo)
    storeCookie({
      auth: encodeURIComponent(b64),
      username: username
    })
  }

  function storeCookie (dict) {
    var date = new Date()
    date.setHours(date.getHours() + 8)
    dict = Object.assign({}, dict, parseCookie())
    storeCookieWithExpiration(dict, date)
  }

  function storeCookieWithExpiration (dict, expirationDate) {
    var enc = []
    for (var k in dict) {
      enc.push(k + ':' + escape(dict[k]))
    }
    document.cookie =
      'm=' + enc.join('|') + ';path=/; expires=' + expirationDate.toUTCString()
  }

  function clearCookieValue (k) {
    var d = parseCookie()
    delete d[k]
    var date = new Date()
    date.setHours(date.getHours() + 8)
    storeCookieWithExpiration(d, date)
  }

  function getCookieValue (k) {
    return parseCookie()[k]
  }

  function parseCookie () {
    var c = getCookie('m')
    var items = c.length === 0 ? [] : c.split('|')

    var dict = {}
    for (var i in items) {
      var kv = items[i].split(':')
      dict[kv[0]] = unescape(kv[1])
    }
    return dict
  }

  function getCookie (key) {
    var cookies = document.cookie.split(';')
    for (var i in cookies) {
      var kv = cookies[i].trim().split('=')
      if (kv[0] === key) return kv[1]
    }
    return ''
  }
  g.signout = signout
  g.kafkaAuth = {
    setup,
    authHeader,
    setAuth
  }
})(window)
