$().ready(function(){
  var c = $(".app-container");
  $(".kafka-toggle-menu").click(function(e){
    e.preventDefault();
    c.toggleClass("sidebar-open");
  })
  $(".sidebar-backdrop").click(function(e) {
      e.preventDefault();
      c.removeClass("sidebar-open");
  })
});
