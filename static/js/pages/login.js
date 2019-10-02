;(function (g) {
  var hostname = g.location.host.split('.')[0]
  document.querySelector('.host').textContent = hostname
  document.getElementById('login').addEventListener('submit', function (evt) {
    evt.preventDefault()
    var user = document.getElementById('kafka-username').value
    var pass = document.getElementById('kafka-password').value
    return g.fetch(g.location.origin + '/api/whoami', {
      headers: {
        Accept: 'application/json',
        'X-Request-ID': g.kafkaHelper.requestId(),
        'Authorization': 'Basic ' + g.btoa(user + ':' + pass)
      }
    }).then(r => {
      if (r.status === 200) {
        g.kafkaAuth.setAuth(user, pass)
        g.kafkaHelper.redirect('/')
      } else if (r.status === 401) {
        g.notific8('Invalid credentials', { theme: 'chicchat', color: 'honey' })
      } else {
        r.text().then(t => g.notific8(t, { theme: 'chicchat', color: 'ruby' }))
      }
    })
  })
})(window)
