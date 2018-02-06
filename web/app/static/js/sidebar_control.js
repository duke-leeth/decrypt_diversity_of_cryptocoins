var sidebar_list = ["CORR_HEATMAP", "PRICE_CHART", "VOLUME_CHART", "COINS_INFO"]

function mark_active(element_id) {
  for(var i=0; i<sidebar_list.length; i++) {
    if (element_id === sidebar_list[i]) {
      document.getElementById(sidebar_list[i]).className = "active";
    } else {
      document.getElementById(sidebar_list[i]).className = "";
    }
  }
}


function goto_corrheatmap() {
  mark_active("CORR_HEATMAP");
}


function goto_pricechart() {
  mark_active("PRICE_CHART");
}


function goto_volumechart() {

  mark_active("VOLUME_CHART");
}


function goto_coinsinfo() {
  mark_active("COINS_INFO");
}
