
function goto_corrheatmap() {
  document.getElementById("CORR_HEATMAP").className = "active";
  document.getElementById("PRICE_CHART").className = "";
  document.getElementById("VOLUMN_CHART").className = "";
  document.getElementById("COINS_INFO").className = "";
}


function goto_pricechart() {
  document.getElementById("CORR_HEATMAP").className = "";
  document.getElementById("PRICE_CHART").className = "active";
  document.getElementById("VOLUMN_CHART").className = "";
  document.getElementById("COINS_INFO").className = "";
}


function goto_volumnchart() {
  document.getElementById("CORR_HEATMAP").className = "";
  document.getElementById("PRICE_CHART").className = "";
  document.getElementById("VOLUMN_CHART").className = "active";
  document.getElementById("COINS_INFO").className = "";
}


function goto_coinsinfo() {
  document.getElementById("CORR_HEATMAP").className = "";
  document.getElementById("PRICE_CHART").className = "";
  document.getElementById("VOLUMN_CHART").className = "";
  document.getElementById("COINS_INFO").className = "active";
}
