function get_coinsinfo() {
  console.log("requesting coins info");
  $.ajax({
    url: "/api/coinlist/",
    dataType: "json",
    timeout: 5000,
    type: "GET",
    error: function(data) {
      console.log("Cannot get coins info data");
    },
    success: function(data) {
      console.log("Successfully got coins info data");
      update_coinsinfotable(data);
    }
  });
}

function update_coinsinfotable(data) {
  console.log("display coins info");

  var strbuffer = new StringBuffer();

  for (var i = 0; i < data.length; i++) {
    strbuffer.append('<tr>');

    strbuffer.append('<td>');
    strbuffer.append(data[i]['rank']);
    strbuffer.append('</td>');

    strbuffer.append('<td>');
    strbuffer.append(data[i]['name']);
    strbuffer.append('</td>');

    strbuffer.append('<td>');
    strbuffer.append(data[i]['symbol']);
    strbuffer.append('</td>');

    strbuffer.append('<td>');
    strbuffer.append(data[i]['id']);
    strbuffer.append('</td>');

    strbuffer.append('</tr>');
  }

  document.getElementById("coindsinfo_tablebody").innerHTML = strbuffer.toString()
}



function StringBuffer(){
  this.content = new Array;
};

StringBuffer.prototype.append = function(str) {
  this.content.push(str);
};
StringBuffer.prototype.toString = function() {
  return this.content.join('');
};
