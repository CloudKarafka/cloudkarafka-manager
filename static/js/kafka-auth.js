function testLoggedIn() {
  get('/api/whoami.json', function() {
    setUsername();
    if (location.pathname == "/login") {
      redirect('/')
    }
  })
}

function setUsername() {
  element("#username").innerText = get_cookie_value("username");
}

function redirectToLogin() {
  redirect('/login')
}

function redirect(path) {
  if (window.location.pathname != path) {
    window.location = path;
  }
}

function auth_header() {
  if(get_cookie_value('auth')) {
    return "Basic " + decodeURIComponent(get_cookie_value('auth'));
  } else {
    return null;
  }
}

function signout() {
  clear_cookie_value('auth');
  redirectToLogin();
}

function set_auth(userinfo) {
  clear_cookie_value('auth');
  clear_cookie_value('username');

  var b64 = window.btoa(userinfo);
  var date  = new Date();
  // 8 hours from now
  date.setHours(date.getHours() + 8);
  store_cookie_with_expiration({'auth': encodeURIComponent(b64)}, date);
  console.log(userinfo.split(':')[0]);
  store_cookie_with_expiration({'username': userinfo.split(':')[0]}, date);
}

function store_cookie(dict) {
  var date = new Date();
  date.setFullYear(date.getFullYear() + 1);
  store_cookie_with_expiration(dict, date);
}

function store_cookie_with_expiration(dict, expiration_date) {
  var dict = Object.assign({}, parse_cookie(), dict);
  var enc = [];
  for (var k in dict) {
    enc.push(k + ':' + escape(dict[k]));
  }
  document.cookie = 'm=' + enc.join('|') + '; expires=' + expiration_date.toUTCString();
}

function clear_cookie_value(k) {
  var d = parse_cookie();
  delete d[k];
  store_cookie(d);
}

function get_cookie_value(k) {
  return parse_cookie()[k];
}

function parse_cookie() {
  var c = get_cookie('m');
  var items = c.length == 0 ? [] : c.split('|');

  var dict = {};
  for (var i in items) {
    var kv = items[i].split(':');
    dict[kv[0]] = unescape(kv[1]);
  }
  return dict;
}

function get_cookie(key) {
  var cookies = document.cookie.split(';');
  for (var i in cookies) {
    var kv = cookies[i].trim().split('=');
    if (kv[0] == key) return kv[1];
  }
  return '';
}
