(function() {
  window.ckm = window.ckm || {}
  function form(method, data = {}, cb = null) {
    const form = document.createElement('form')
    form.classList.add('form', 'card')

    const h3 = document.createElement('h3')
    if (method === 'POST') {
      h3.innerText = 'Create topic'
    } else {
      h3.innerText = 'Edit topic'
    }
    form.appendChild(h3)
    form.appendChild(ckm.dom.formInput('input', 'Name', {
      type: 'text',
      value: data.name,
      readOnly: data.name !== undefined,
      placeholder: 'Topic name'
    }))
    form.appendChild(ckm.dom.formInput('input', 'Partitions', {
      type: 'number',
      value: data.partitions || 10,
      min: data.partitions || 1
    }))
    form.appendChild(ckm.dom.formInput('input', 'Replication factor', {
      type: 'number',
      value: data.replication_factor || 1,
      min: data.replication_factor || 1
    }))
    form.appendChild(ckm.dom.formInput('textarea', 'Config', {
      value: ckm.dom.jsonToText(data.config), placeholder: '{"key": "value"}'
    }))
    const btn = document.createElement('button')
    btn.classList.add('btn-primary')
    btn.type = 'submit'
    btn.innerText = h3.innerText
    form.appendChild(btn)
    form.addEventListener('submit', function(evt) {
      evt.preventDefault()
      const data = new window.FormData(this)
      const name = encodeURIComponent(data.get('name'))
      const url = `/api/topics/${name}`
      const body = {
        name: name,
        partitions: parseInt(data.get("partitions")),
        replication_factor: parseInt(data.get("replication_factor")),
      }
      if (data.get('config') !== '') {
        var config = ckm.dom.parseJSON(data.get('config'))
        if (config == false) { return }
        body.config = config
      }
      ckm.http.request(method, url, { body }).then(() => {
        if (cb !== null) {
          evt.target.reset()
          cb()
        }
        ckm.dom.toast(`Topic ${name} created`)
      }).catch(ckm.http.standardErrorHandler)
    })
    return form
  }

  Object.assign(window.ckm, {
    topic: { form }
  })
})()