(function () {
  document.getElementsByTagName('aside')[0].innerHTML = `
    <h1>
      <a href="/"><img id="ckm-logo" src="/static/assets/mgmt-logo.svg"></a>
      <small id="version"></small>
    </h1>
    <ul>
      <li><a href="/">Overview</a></li>
      <li><a href="/topics">Topics</a></li>
      <li><a href="/consumers">Consumers</a></li>
      <li>
        <a href="/topics">Topics</a>
        <ul class="hide">
          <li><a href="#create">Add topic</a></li>
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
        <a href="/acls">ACL</a>
        <ul class="hide">
          <li><a href="#createTopicACL">Add Topic ACL</a></li>
          <li><a href="#createGroupACL">Add Consumer Group ACL</a></li>
          <li><a href="#createClusterACL">Add Cluster ACL</a></li>
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

  document.getElementsByTagName('footer')[0].innerHTML = `
    CloudKarafka Manager is open source and developed by
    <a href="http://www.84codes.com" target="_blank"><img class="logo" src="/img/logo-84codes.svg"></a>
  `
})()
