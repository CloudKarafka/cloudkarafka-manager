function drawChart(containerId, id, xId, yId, data) {
  var setScale = function(series, type) {
    min = Number.MAX_VALUE;
    max = Number.MIN_VALUE;
    for (_l = 0, _len2 = series.length; _l < _len2; _l++) {
      point = series[_l];
      min = Math.min(min, point.y);
      max = Math.max(max, point.y);
    }
    console.log(min, max);
    if (type === 'linear') {
      return d3.scale.linear().domain([min, max]).nice();
    } else {
      return d3.scale.pow().domain([min, max]).nice();
    }
  };
  var scales = {
    in: setScale(data.in),
    out: setScale(data.out, 'linear')
  };
  var graph = new Rickshaw.Graph({
    element: element(id),
    renderer: 'line',
    series: [
      {
        color: 'steelblue',
        data: data.in,
        name: 'Input',
        scale: scales.in
      }, {
        color: 'lightblue',
        data: data.out,
        name: 'Output',
        scale: scales.out
      }
    ]
  })

 new Rickshaw.Graph.Axis.Y.Scaled({
    element: document.getElementById(xId),
    graph: graph,
    orientation: 'left',
    scale: scales.in,
    tickFormat: Rickshaw.Fixtures.Number.formatKMBT
  });

  new Rickshaw.Graph.Axis.Y.Scaled({
    element: document.getElementById(yId),
    graph: graph,
    grid: false,
    orientation: 'right',
    scale: scales.out,
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
