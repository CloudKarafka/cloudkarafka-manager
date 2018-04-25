function drawChart(containerId, id, xId, yId, data) {
  var scales = [];

  var setScale = function(series, type) {
    min = Number.MAX_VALUE;
    max = Number.MIN_VALUE;
    for (_l = 0, _len2 = series.length; _l < _len2; _l++) {
      point = series[_l];
      min = Math.min(min, point.y);
      max = Math.max(max, point.y);
    }
    if (type === 'linear') {
      scales.push(d3.scale.linear().domain([min, max]).nice());
    } else {
      scales.push(d3.scale.pow().domain([min, max]).nice());
    }
  };
  setScale(data.in)
  setScale(data.out, 'linear')

  console.log(scales);
  var graph = new Rickshaw.Graph({
    element: element(id),
    renderer: 'line',
    series: [
      {
        color: 'steelblue',
        data: data.in,
        name: 'Input',
        scale: scales[0]
      }, {
        color: 'lightblue',
        data: data.out,
        name: 'Output',
        scale: scales[1]
      }
    ]
  })

 new Rickshaw.Graph.Axis.Y.Scaled({
    element: document.getElementById(xId),
    graph: graph,
    orientation: 'left',
    scale: scales[0],
    tickFormat: Rickshaw.Fixtures.Number.formatKMBT
  });

  new Rickshaw.Graph.Axis.Y.Scaled({
    element: document.getElementById(yId),
    graph: graph,
    grid: false,
    orientation: 'right',
    scale: scales[1],
    tickFormat: Rickshaw.Fixtures.Number.formatKMBT
  });

  new Rickshaw.Graph.Axis.Time({
    graph: graph
  });

  new Rickshaw.Graph.HoverDetail({
    graph: graph
  });

  var resize = function() {
    var chart = element(containerId);
    graph.configure({
      width: chart.clientWidth - 58 ,
      height: chart.clientHeight - 32
    });
    graph.render();
  }
  window.addEventListener('resize', resize);
  resize();
};
