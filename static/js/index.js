(function() {
    const dataChart = ckm.chart.render('dataChart', 'bytes/s', { aspectRatio: 2 })
    const isrChart = ckm.chart.render('isrChart', 'changes/s', { aspectRatio: 2 })

    function updateCharts (response) {
      let dataStats = {
        send_details: response.bytes_out,
        receive_details: response.bytes_in
      }
      ckm.chart.update(dataChart, dataStats)
      let isrStats = {
        isr_shrink: response.isr_shrink,
        isr_expand: response.isr_expand,
      }
      ckm.chart.update(isrChart, isrStats)
    }

    ckm.overview.start(updateCharts)
})()
