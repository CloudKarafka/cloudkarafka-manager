(function () {
  document.querySelector('#login').addEventListener('submit', function (evt) {
    evt.preventDefault()
    const user = document.querySelector('#kafka-username').value
    const pass = document.querySelector('#kafka-password').value
    ckm.auth.setAuth(user + ':' + pass)
    ckm.http.request('GET', '/api/whoami').then(function () {
      ckm.auth.storeCookie({ username: user, password: pass })
      window.location.assign('/')
    }).catch(function () {
      window.alert('Login failed')
    })
  })
})()
