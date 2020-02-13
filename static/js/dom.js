(function () {
  window.ckm = window.ckm || {}

  function setChild (selector, element) {
    const els = elements(selector)
    els.forEach(el => {
      while (el.lastChild) {
        el.removeChild(el.lastChild)
      }
      el.appendChild(element)
    })
  }

  function removeNodes (selector) {
    const els = elements(selector)
    if (!els[0]) return
    const parent = els[0].parentNode
    els.forEach(node => {
      parent.removeChild(node)
    })
  }

  function removeChildren (selector) {
    const els = elements(selector)
    els.forEach(el => {
      while (el.lastChild) {
        el.removeChild(el.lastChild)
      }
    })
  }

  function parseJSON (data) {
    try {
      if (data.length) {
        return JSON.parse(data)
      }
      return {}
    } catch (e) {
      if (e instanceof SyntaxError) {
        window.alert('Input must be JSON')
        return false
      } else {
        throw e
      }
    }
  }

  function elements (selector) {
    let els = null
    if (typeof selector === 'string') {
      els = document.querySelectorAll(selector)
    } else if (selector instanceof window.NodeList) {
      els = selector
    } else if (selector instanceof window.Node) {
      els = [selector]
    } else {
      els = []
    }
    return els
  }

  function jsonToText (obj) {
    if (obj == null) return ''
    return JSON.stringify(obj, undefined, 2).replace(/["{},]/g, '')
  }

  function toast (text) {
    removeNodes('.toast')
    let d = document.createElement('div')
    d.classList.add('toast')
    d.textContent = text
    document.body.appendChild(d)
    setTimeout(() => {
      try {
        document.body.removeChild(d)
      } catch (e) {
        // noop
      }
    }, 7000)
  }

  function formInput(element, name, options) {
    const label = document.createElement('label')
    const span = document.createElement('span')
    span.innerText = name
    label.appendChild(span)
    const elem = document.createElement(element)
    elem.name = name.toLowerCase().replace(' ', '_')
    Object.keys(options).forEach((key) => {
      var val = options[key]
      if (val !== undefined) { elem[key] = val }
    })
    label.appendChild(elem)
    return label
  }

  function createLink(href, linkText) {
    const queueLink = document.createElement('a')
    queueLink.href = href
    queueLink.textContent = linkText
    return queueLink
  }

  function createBadge(text, badgeType) {
    const span = document.createElement('span')
    span.innerText = text
    span.classList.add('badge');
    span.classList.add('badge-' + badgeType);
    return span
  }

  Object.assign(window.ckm, {
    dom: {
      formInput,
      setChild,
      removeNodes,
      jsonToText,
      removeChildren,
      parseJSON,
      toast,
      createLink,
      createBadge
    }
  })
})()
