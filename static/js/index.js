(function() {
    const dataChart = ckm.chart.render('dataChart', 'bytes/s', { aspectRatio: 2 })

    function updateCharts (response) {
      let dataStats = {
        send_details: response.bytes_out,
        receive_details: response.bytes_in
      }
      ckm.chart.update(dataChart, dataStats)
    }

    ckm.overview.start(updateCharts)
})()
