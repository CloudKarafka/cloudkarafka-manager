;(function (g) {
  var colors = [
    '#46b477',
    'steelblue',
    'lightblue',
    'red'
  ]
  function drawThroughputChart (containerId, id, yaxis, data) {
    //var palette = new g.Rickshaw.Color.Palette({ scheme: 'cool' })
    var mm = {
      bytes: { min: Number.MAX_VALUE, max: Number.MIN_VALUE },
      messages: { min: Number.MAX_VALUE, max: Number.MIN_VALUE }
    }
    data.forEach(function (serie) {
      serie.data = serie.data || []
      serie.data.forEach(function (d) {
        mm[serie.type].max = Math.max(mm[serie.type].max, d.y)
        mm[serie.type].min = Math.min(mm[serie.type].min, d.y)
      })
    })
    var scales = {
      bytes: g.d3.scale
        .linear()
        .domain([mm.bytes.min, mm.bytes.max])
        .nice(),
      messages: g.d3.scale
        .linear()
        .domain([mm.messages.min, mm.messages.max])
        .nice()
    }

    var series = data.map(function (serie, i) {
      serie.color = colors[i % colors.length]
      serie.scale = scales[serie.type]
      return serie
    })
    drawChart(id, series, scales)
  }

  function drawChart (id, series, scale) {
    var graph = new g.Rickshaw.Graph({
      element: document.querySelector(id),
      renderer: 'line',
      //interpolation: 'step-after',
      series: series
    })
    var yAxis = new g.Rickshaw.Graph.Axis.Y.Scaled({
      graph: graph,
      element: '#axis0',
      orientation: 'left',
      scale: scale.bytes,
      tickFormat: x => {
        const fs = g.kafkaHelper.humanFileSize(x)
        return fs.value + ' ' + fs.unit + '/s'
      }
    })
    var yAxis2 = new g.Rickshaw.Graph.Axis.Y.Scaled({
      graph: graph,
      grid: false,
      element: '#axis1',
      scale: scale.messages,
      orientation: 'right',
      tickFormat: g.Rickshaw.Fixtures.Number.formatKMBT
    })

    var xAxis = new g.Rickshaw.Graph.Axis.X({
      graph: graph,
      pixelsPerTick: 100,
      tickFormat: function (x) {
        return new Date(x * 1000).toLocaleTimeString()
      }
    })

    var hd = new g.Rickshaw.Graph.HoverDetail({
      graph: graph,
      formatter: (serie, x, y) => {
        switch (serie.type) {
        case 'bytes':
          const fs = g.kafkaHelper.humanFileSize(y)
          return serie.name + ': ' + fs.value + ' ' + fs.unit + '/s'
        case 'messages':
          return serie.name + ':' + y.toLocaleString() + ' msgs'
        default:
          return y.toLocaleString()
        }
      }
    })

    var resize = function () {
      var chart = document.querySelector(id).parentElement
      var axisWidth = Array.from(
        chart.querySelectorAll('.chart-y-axis')
      ).reduce((sum, e) => sum + e.clientWidth, 0)
      graph.configure({
        width: chart.clientWidth - axisWidth - 20,
        height: chart.clientHeight - 32
      })
      graph.render()
    }
    window.addEventListener('resize', resize)
    resize()
  }
  g.kafkaChart = {
    drawThroughputChart
  }
})(window)
