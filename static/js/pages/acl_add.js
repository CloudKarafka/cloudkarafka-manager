;(function (g) {
  g.kafkaAuth.setup()
  let stringFormat = (v) => {
    if (!v) {
      return ''
    }
    return v.trim()
  }
  var form = document.querySelector('#aclForm')
  form.addEventListener('submit', function (ev) {
    ev.preventDefault()
    var fields = [
      { field: 'resource', format: stringFormat },
      { field: 'principal', format: stringFormat },
      { field: 'name', format: stringFormat },
      { field: 'host', format: stringFormat },
      { field: 'type', format: stringFormat },
      { field: 'permission', format: stringFormat }
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
