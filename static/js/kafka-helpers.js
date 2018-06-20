function redirectToLogin() {
  redirect('/login')
}

function redirect(path) {
  if (window.location.pathname != path) {
    window.location = path;
  }
}

function getParameterByName(name, url) {
  if (!url) url = window.location.href;
  name = name.replace(/[\[\]]/g, "\\$&");
  var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
    results = regex.exec(url);
  if (!results) return null;
  if (!results[2]) return '';
  return decodeURIComponent(results[2].replace(/\+/g, " "));
}

function periodicGet(path, callback) {
  get(path, callback);
  setTimeout(function() { periodicGet(path, callback) }, 5000);
}

function get(path, callback) {
  req('GET', path, callback);
};

function del(path, callback) {
  req('DELETE', path, callback);
};

function putForm(path, formId, callback) {
  var formElement = element(formId)
  req('PUT', path, callback, new FormData(formElement));
};

function postForm(path, formId, callback) {
  var formElement = element(formId)
  req('POST', path, callback, new FormData(formElement));
};

function req(method, path, callback, data) {
  var request = new XMLHttpRequest();
  request.open(method, path, true)
  var auth = auth_header();
  if (!auth) {
    redirectToLogin();
  }
  request.setRequestHeader('authorization', auth);
  request.onload = onLoad(request, callback);
  if (data !== undefined) {
    request.send(data);
  } else {
    request.send();
  }
};

function onLoad(request, callback) {
  return function() {
    if (request.status == 401) {
      redirectToLogin();
    } else if (request.status >= 200 && request.status < 400) {
      // Success!
      var data;
      if (request.responseText.length > 0) {
        data = JSON.parse(request.responseText);
      }
      callback(data)
    } else {
      console.log(request.responseText);
    }
  }
}

function element(id) {
  return document.querySelector(id);
};

function elementHtml(id) {
  return element(id).innerHTML;
};

function renderTmpl(attachToId, tmplId, elements) {
  var $attachTo = element(attachToId);
  var tmpl = Handlebars.compile(elementHtml(tmplId));
  $attachTo.innerHTML = tmpl(elements);
};

function renderListTmpl(attachToId, tmplId, path, clb) {
  //var noTopicsTmpl = Handlebars.compile(elementHtml('#tmpl-no-topics'));
  get(path, function(elements) {
    if (elements !== null) {
      var l = elements.length;
      var result = [];
      elements.forEach(function(e) {
        get(path.replace('.json', '/') + e + '.json', function(elem) {
          result.push(elem)
          if (result.length == l) {
            result.sort(function(a, b) {
              return a.name > b.name
            })
            renderTmpl(attachToId, tmplId, {elements: result})
            if (clb !== undefined) {
              clb();
            }
          }
        })
      })
    } else{
      renderTmpl(attachToId, tmplId, {elements: []})
    }
  })
}

function humanFileSize(bytes) {
  var thresh = 1024;
  if(Math.abs(bytes) < thresh) {
    return { value: bytes, unit: 'B' };
  }
  var units = ['kB','MB','GB','TB','PB','EB','ZB','YB'];
  var u = -1;
  do {
    bytes /= thresh;
    ++u;
  } while(Math.abs(bytes) >= thresh && u < units.length - 1);
  return { value: bytes.toFixed(1), unit: units[u] };
}

Handlebars.registerHelper('humanFileSize', function(bytes) {
  var res = humanFileSize(bytes);
  return new Handlebars.SafeString(
    res.value + "<small>" + res.unit + "</small>"
  );
})

//Handlebars.registerHelper('humanFileSize', humanFileSize)
Handlebars.registerHelper('toLocaleString', function(elem) {
  return elem.toLocaleString();
})
