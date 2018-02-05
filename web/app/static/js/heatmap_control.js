function update_heatmap() {
  console.log("requesting heatmap info");
  $.ajax({
    url: "/api/correlation/lastest_matrix",
    dataType: "json",
    timeout: 5000,
    type: "GET",
    error: function(data) {
      console.log("Cannot get heatmap data");
    },
    success: function(data) {
      console.log("Successfully got heatmap data");
      draw_heatmap(data);
    }
  });
}


function heatmap_layout(title_name) {
  var layout = {
    title: title_name,
    annotations: [],
    xaxis: {
      ticks: '',
      side: 'top',
      width: 700,
      height: 700,
      autosize: false
    },
    yaxis: {
      ticks: '',
      ticksuffix: ' ',
      width: 700,
      height: 700,
      autosize: false
    }
  };
  return layout;
}


function draw_heatmap(data) {
  console.log("plotting price time series");

  var colorscaleValue = [
    ['0.0', 'rgb(255,255,255)'],
    ['0.2', 'rgb(224,243,248)'],
    ['0.4', 'rgb(171,217,233)'],
    ['0.6', 'rgb(116,173,209)'],
    ['0.8', 'rgb(69,117,180)'],
    ['1.0', 'rgb(49,54,149)']
  ];


  var x_axis = data['tag_list'].slice(0);
  var y_axis = data['tag_list'].slice(0);
  var heatmap_data = [ {
    z: data['corr_matrix'].reverse(),
    x: x_axis,
    y: y_axis.reverse(),
    type: 'heatmap',
    colorscale: colorscaleValue
  }];

  Plotly.newPlot('corr_heatmap', heatmap_data, heatmap_layout('Correlation Heatmap'));
}



function get_corr(){
  var coidid_1 = document.getElementById("CoinID_1").value;
  var coidid_2 = document.getElementById("CoinID_2").value;

  $.ajax({
    url: "/api/correlation/"+coidid_1+"/"+coidid_2+"/",
    dataType: "json",
    timeout: 5000,
    type: "GET",
    error: function(data) {
      console.log("Cannot get corr");
    },
    success: function(data) {
      console.log("Successfully got corr"+data[0]['corr']);
      document.getElementById("output_corr").value = data[0]['corr'].toFixed(4);
    }
  });
}
