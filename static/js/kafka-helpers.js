function get(path, callback) {
  var request = new XMLHttpRequest();
  request.open('GET', path, true);
  request.onload = function() {
    if (request.status >= 200 && request.status < 400) {
      // Success!
      var data = JSON.parse(request.responseText);
      callback(data)
    }
  }
  request.send();
};

function element(id) {
  return document.querySelector(id);
}

function elementHtml(id) {
  return element(id).innerHTML;
}

function renderListTmpl(attachToId, tmplId, path) {
  get(path, function(elements) {
    var $attachTo = element(attachToId);
    var tmpl = Handlebars.compile(elementHtml(tmplId));
    //var noTopicsTmpl = Handlebars.compile(elementHtml('#tmpl-no-topics'));
    var l = elements.length;
    var result = [];
    elements.forEach(function(e) {
      get(path.replace('.json', '/') + e + '.json', function(elem) {
        result.push(elem)
        if (result.length == l) {
          result.sort(function(a, b) {
            return a.name > b.name
          })
          console.log(result);
          $attachTo.innerHTML = tmpl({ elements: result });
        }
      })
    })
  })
}
