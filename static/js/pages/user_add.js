;(function (g) {
  g.kafkaAuth.setup()
  var form = document.querySelector('#userForm')
  form.addEventListener('submit', function (ev) {
    ev.preventDefault()
    var fields = [
      { field: 'name', format: v => v },
      { field: 'password', format: v => v }
    ]
    var data = fields.reduce((res, f) => {
      res[f.field] = f.format(this[f.field].value)
      return res
    }, {})
    g.kafkaHelper.post(ev.currentTarget.action, data).then(() => {
      g.kafkaHelper.redirect('/admin')
    })
  })
}(window))
