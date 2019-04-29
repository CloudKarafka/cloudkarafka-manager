(function(g) {

    function formatSize(y, r) {
        r = r || 2
        var abs_y = Math.abs(y);
        if (abs_y >= 1125899906842624)  { return Math.round(y / 1125899906842624, r) + "Pb/s" }
        else if (abs_y >= 1099511627776){ return Math.round(y / 1099511627776, r) + "Tb/s" }
        else if (abs_y >= 1073741824)   { return Math.round(y / 1073741824, r) + "Gb/s" }
        else if (abs_y >= 1048576)      { return Math.round(y / 1048576, r) + "Mb/s" }
        else if (abs_y >= 1024)         { return Math.round(y / 1024, r) + "Kb/s" }
        else if (abs_y < 1 && abs_y > 0)    { return Math.round(y.toFixed(2), r) + "b/s" }
        else if (abs_y === 0)           { return '' }
        else                        { return y }
    }
    function renderThroughputChart(id, data) {

        // var palette = new g.Rickshaw.Color.Palette({ scheme: 'cool' })
        var graph = new Rickshaw.Graph( {
            element: document.getElementById(id),
            renderer: 'line',
            interpolation: 'step-after',
            series: new Rickshaw.Series.FixedDuration(data, { scheme: 'cool'}, {
                timeInterval: 5000,
                maxDataPoints: 200,
                timeBase: new Date().getTime() / 1000
            })
        });
        var yAxis = new g.Rickshaw.Graph.Axis.Y({
            graph: graph,
            element: '#axis0',
            orientation: 'left',
            grid: false,
            tickFormat: formatSize
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
                var parts = serie.name.split("/")
                return parts[0] + " " + parts[2].toLowerCase() + ": " + formatSize(y)

            }
        })
        var resize = function () {
            var chart = document.getElementById(id).parentElement
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
        return graph
    }

    document.querySelectorAll('[data-throughput]').forEach((el) => {
        let graph;
        let url = el.dataset.throughput;
        let followUrl = el.dataset.throughputFollow;
        g.fetch(url, {headers: {Accept: 'application/json'}})
            .then(r => r.json())
            .then(r => {
                return renderThroughputChart("chart", r)
            }).then(graph => {
                if (followUrl) {
                    var evtSource = new EventSource(followUrl, { withCredentials: true } )
                    evtSource.onmessage = function(e) {
                        var data = JSON.parse(e.data);
                        graph.series.addData(data.data, data.x);
                        graph.render();
                    }
                    evtSource.onerror = function(e) {
                        console.error("EventSource failed.", e);
                    };
                }

            })
    })
}(window))
