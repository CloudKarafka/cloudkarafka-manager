function drawChart(containerId, id, yaxis, xaxis, data) {
  if (data.in.length === 0 || data.out.length === 0) {
    renderTmpl('#throughput', '#tmpl-no-throughput');
    return;
  }

  var graph = new Rickshaw.Graph({
    element: element(id),
    renderer: 'line',
    series: [
      {
        color: 'steelblue',
        data: data.in,
        name: 'Input',
      }, {
        color: 'lightblue',
        data: data.out,
        name: 'Output',
      }
    ]
  })

  var yAxis = new Rickshaw.Graph.Axis.Y({
    graph: graph,
    width: 80,
    orientation: 'left',
    tickFormat: function(x){
      var fs = humanFileSize(x)
      return fs.value + " " + fs.unit + "/s";
    },
    element: yaxis
  });

  var xAxis = new Rickshaw.Graph.Axis.X({
      graph: graph,
      pixelsPerTick: 100,
      tickFormat: function(x){
        return new Date(x * 1000).toLocaleTimeString();
      }
  })
  new Rickshaw.Graph.HoverDetail({
    graph: graph,
    yFormatter: function(x) {
      var fs = humanFileSize(x)
      return fs.value + " " + fs.unit + "/s";
    }
  });

  var resize = function() {
    var chart = element(containerId);
    graph.configure({
      width: chart.clientWidth - 96 ,
      height: chart.clientHeight - 32
    });
    graph.render();
    yAxis.render();
    xAxis.render();
  }
  window.addEventListener('resize', resize);
  resize();
};
