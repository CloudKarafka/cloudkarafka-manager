;(function (g) {
  // g.kafkaAuth.setup()

  document.getElementById('login').addEventListener('submit', function (evt) {
    evt.preventDefault()
    var request = new XMLHttpRequest()
    var user = document.getElementById('kafka-username').value
    var pass = document.getElementById('kafka-password').value
    request.open('GET', '/web/whoami', true, user, pass)
    request.onload = function () {
      if (request.status >= 200 && request.status < 400) {
        var data = JSON.parse(request.responseText)
        g.kafkaAuth.store_cookie({ username: user, password: pass })
        g.kafkaAuth.set_auth(user + ':' + pass)
        g.kafkaHelper.redirect('/')
      }
    }
    request.send()
  })
})(window)
