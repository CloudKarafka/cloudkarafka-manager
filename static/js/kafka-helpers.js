function getParameterByName(name, url) {
  if (!url) url = window.location.href;
  name = name.replace(/[\[\]]/g, "\\$&");
  var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
    results = regex.exec(url);
  if (!results) return null;
  if (!results[2]) return '';
  return decodeURIComponent(results[2].replace(/\+/g, " "));
}

function get(path, callback) {
  var request = new XMLHttpRequest();
  request.open('GET', path, true, get_cookie_value("username"), get_cookie_value("password"));
  request.setRequestHeader('authorization', auth_header());
  request.onload = function() {
    if (request.status >= 200 && request.status < 400) {
      // Success!
      var data = JSON.parse(request.responseText);
      callback(data)
    } else {
      redirectToLogin();
    }
  }
  request.send();
};

function postForm(path, formId, callback) {
  var request = new XMLHttpRequest();
  var formElement = element(formId)
  request.open('POST', path, true)
  var auth = auth_header();
  if (!auth) {
    redirectToLogin();
  }
  req.setRequestHeader('authorization', auth);
  request.onload = function() {
    if (request.status >= 200 && request.status < 400) {
      // Success!
      var data = JSON.parse(request.responseText);
      callback(data)
    } else {
      redirectToLogin();
    }
  }
  request.send(new FormData(formElement));
};

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

function renderListTmpl(attachToId, tmplId, path) {
  //var noTopicsTmpl = Handlebars.compile(elementHtml('#tmpl-no-topics'));
  get(path, function(elements) {
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
        }
      })
    })
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
