(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
    <h1>
      <a href="/"><img id="ckm-logo" class="logo" src="/assets/mgmt-logo.svg"></a>
      CloudKarafka Mgmt
      <small id="version"></small>
    </h1>
    <ul>
      <li><a href="/">Overview</a></li>
      <li><a href="/consumers">Consumers</a></li>
      <li>
        <a href="/topics">Topics</a>
        <ul class="hide">
          <li><a href="#create">Create topic</a></li>
        </ul>
      </li>
      <li>
        <a href="/brokers">Brokers</a>
      </li>
      <li>
        <a href="/users">Users</a>
        <ul class="hide">
          <li><a href="#createUser">Add User</a></li>
        </ul>
      </li>
      <li>
        <a href="/acls?type=topic">ACL</a>
        <ul class="hide">
          <li><a href="/acls?type=topic">Topic ACLs</a></li>
          <li><a href="/acls?type=group">Consumer Group ACLs</a></li>
          <li><a href="/acls?type=cluster">Cluster ACLs</a></li>
        </ul>
      </li>
    </ul>
  `

  function toggleSubMenu (el, toggle) {
    const subMenu = el.querySelector('ul')
    if (subMenu) {
      subMenu.classList.toggle('hide', toggle)
    }
  }
  const path = window.location.pathname
  document.querySelectorAll('aside li').forEach(li => {
    li.classList.remove('active')
    toggleSubMenu(li, true)
  })
  const active = document.querySelector('aside a[href="' + path + '"]')
  if (active) {
    const activeLi = active.parentElement
    activeLi.classList.add('active')
    toggleSubMenu(activeLi, false)
  }

  document.getElementsByTagName('header')[0].insertAdjacentHTML('beforeend', `
     <ul id="user-menu">
      <li><span id="username"></span></li>
      <li>
        <a href="#" onclick="ckm.auth.signOut()">
          <span class="head">🙈</span>&nbsp; Sign out</span>
        </a>
      </li>
    </div>
  `)
  function showLoader (e) {
    if (e && (e.ctrlKey || e.shiftKey || e.metaKey || e.which === 2)) {
      // ctrl/cmd click
      return
    }
    setTimeout(function () {
      const div = document.createElement('div')
      div.classList.add('loader', 'loader-full')

      document.querySelector('main').insertAdjacentElement('afterbegin', div)
    }, 15)
  }
  window.showLoader = showLoader
  function rmLoader() {
    document.querySelector('.loader').remove()
  }
  window.rmLoader = rmLoader
})()
