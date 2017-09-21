var whoami = function() {
  $.get("/api/whoami", function(res) {
    if (res.cluster.includes("Write")) {
      $("[role='cluster-write']").removeClass("hidden");
    }
  })
}
