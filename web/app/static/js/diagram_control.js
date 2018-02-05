function update_diagram(coinid) {
  console.log("requesting diagram info");
  $.ajax({
    url: "/api/priceinfo/"+coinid+"/",
    dataType: "json",
    timeout: 5000,
    type: "GET",
    error: function(data) {
      console.log("Cannot get price data");
    },
    success: function(data) {
      console.log("Successfully got price data");
      draw_diagram(coinid, data);
    }
  });
}


function draw_diagram(coinid, data) {
  console.log("plotting price time series");
  var price_array = [];
  var time_array = [];
  var volumn_array = [];
  for (var i = data.length-1; i >= 0; i--) {
    price_array.push(data[i]['price_usd']);
    time_array.push(data[i]['time']);
    volumn_array.push(data[i]['volume_usd_24h']);
  }

  var trace1 = {
    type: "scatter",
    mode: "lines",
    name: 'price',
    x: time_array,
    y: price_array,
    line: {color: '#17BECF'}
  }

  var trace2 = {
    type: "scatter",
    mode: "lines",
    name: '24hr volumn',
    x: time_array,
    y: volumn_array,
    line: {color: '#7F7F7F'}
  }

  var price_diagram_data = [trace1];
  var volumn_diagram_data = [trace2];

  Plotly.newPlot('price_time_series', price_diagram_data,
                  diagram_layout(coinid,'time','Price (USD)'));
  Plotly.newPlot('volumn_time_series', volumn_diagram_data,
                  diagram_layout(coinid,'time','24h volumn (USD)'));
}


function diagram_layout(title_name, x_title, y_title) {
  var curr_time = new Date();
  var time_now = (curr_time).toISOString();
  var last_6hr = (new Date(curr_time.getTime() - (1000*60*60))).toISOString();
  var last_24hr = (new Date(curr_time.getTime() - (1000*60*60*24))).toISOString();

  var layout = {
    title: title_name,
    xaxis: {
      title: x_title,
      titlefont: {
        family: 'Helvetica',
        size: 18,
        color: '#7f7f7f'
      },
      autorange: true,
      range: [last_24hr, time_now],
      rangeselector: {buttons: [
        {
          count: 1,
          label: '1hr',
          step: 'hour',
          stepmode: 'backward'
        },
        {
          count: 6,
          label: '6hr',
          step: 'hour',
          stepmode: 'backward'
        },
        {
          count: 1,
          label: '1d',
          step: 'day',
          stepmode: 'backward'
        },
        {
          label: 'all',
          step: 'all'
        }
      ]},
        rangeslider: {range: [last_24hr, time_now]},
        type: 'date'
    },
    yaxis: {
      title: y_title,
      titlefont: {
        family: 'Helvetica',
        size: 18,
        color: '#7f7f7f'
      },
      autorange: true,
      type: 'linear'
    }
  };
  return layout;
}
