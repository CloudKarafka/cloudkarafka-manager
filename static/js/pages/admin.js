;(function (g) {
  g.kafkaAuth.setup()
  g.kafkaHelper.renderListTmpl(
    '#user-rows',
    '#tmpl-users',
    '/api/users',
    function () {
      document.querySelectorAll('[data-delete-user]').forEach(function (elem) {
        elem.addEventListener('click', function (evt) {
          evt.preventDefault()
          g.kafkaHelper.del(`/api/users/${evt.currentTarget.dataset.deleteUser}`, function () {
            g.location.reload()
          })
        })
      })
    }
  )

  function list (resource, elements) {
    var lst = []
    for (var key in elements) {
      elements[key].forEach(function (elem) {
        lst.push({
          name: key,
          resource: resource,
          principal: elem.principal,
          type: elem.permissionType,
          permission: elem.operation,
          host: elem.host
        })
      })
    }
    return lst
  }
  g.kafkaHelper.get('/api/acls').then(acls => {
    g.kafkaHelper.renderTmpl(
      '#cluster-acl-rows',
      '#tmpl-acl-row',
      list('cluster', acls.cluster)
    )
    g.kafkaHelper.renderTmpl(
      '#topic-acl-rows',
      '#tmpl-acl-row',
      list('topic', acls.topics)
    )
    g.kafkaHelper.renderTmpl(
      '#group-acl-rows',
      '#tmpl-acl-row',
      list('group', acls.groups)
    )
    document.querySelectorAll('.acl-form').forEach(function (elem) {
      elem.addEventListener('submit', function (evt) {
        evt.preventDefault()
        var inputs = this.elements
        var resource = inputs['resource'].value
        var name = inputs['name'].value
        var principal = inputs['principal'].value
        g.kafkaHelper.del(
          '/api/acls/' + resource + '/' + name + '/' + principal,
          function () {
            g.location.reload()
          }
        )
      })
    })
  })
})(window)
