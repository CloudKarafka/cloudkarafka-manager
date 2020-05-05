(function (ckm) {
  function form (method, data = {}, cb = null) {
    const form = document.createElement('form')
    let url = ''
    form.classList.add('form', 'card')

    const h3 = document.createElement('h3')
    if (method === 'POST') {
      h3.innerText = 'Create topic'
      url = '/api/topics'
    } else {
      h3.innerText = 'Edit topic'
      url = `/api/topics/${data.name}`
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
    form.addEventListener('submit', function (evt) {
      evt.preventDefault()
      const data = new window.FormData(this)
      const name = encodeURIComponent(data.get('name'))
      const body = {
        name: name,
        partitions: parseInt(data.get('partitions')),
        replication_factor: parseInt(data.get('replication_factor'))
      }
      if (data.get('config') !== '') {
        var config = ckm.dom.parseJSON(data.get('config'))
        if (config == false) { return }
        body.config = config
      }
      showLoader()
      ckm.http.request(method, url, { body }).then(() => {
        if (cb !== null) {
          evt.target.reset()
          cb()
        }
        ckm.dom.toast(`Topic ${name} created`)
        if (ckm.topic.table) {
          ckm.topic.table.fetchAndUpdate()
        }
      }).catch(ckm.http.standardErrorHandler).finally(rmLoader)
    })
    return form
  }

  Object.assign(ckm, {
    topic: { form }
  })
})(window.ckm)
