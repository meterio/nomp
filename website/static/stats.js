var poolHashrateData;

var poolHashrateChart;

var statData;
var poolKeys;

function buildChartData() {
  var pools = {};

  poolKeys = [];
  for (var i = 0; i < statData.length; i++) {
    for (var pool in statData[i].pools) {
      if (poolKeys.indexOf(pool) === -1) poolKeys.push(pool);
    }
  }

  for (var i = 0; i < statData.length; i++) {
    var time = statData[i].time * 1000;

    for (var f = 0; f < poolKeys.length; f++) {
      var pName = poolKeys[f];
      var a = (pools[pName] = pools[pName] || {
        hashrate: []
      });
      if (pName in statData[i].pools) {
        a.hashrate.push([time, statData[i].pools[pName].hashrate]);
      } else {
        a.hashrate.push([time, 0]);
      }
    }
  }

  poolHashrateData = [];

  for (var pool in pools) {
    poolHashrateData.push({
      key: pool,
      values: pools[pool].hashrate
    });

    $('#statsHashrateAvg' + pool).text(getReadableHashRateString(calculateAverageHashrate(pool)))
  }
}

function calculateAverageHashrate(pool) {
  var count = 0;
  var total = 1;
  var avg = 0;
  for (var i = 0; i < poolHashrateData.length; i++) {
    count = 0;
    for (var ii = 0; ii < poolHashrateData[i].values.length; ii++) {
      if (pool == null || poolHashrateData[i].key === pool) {
        count++;
        avg += parseFloat(poolHashrateData[i].values[ii][1]);
      }
    }
    if (count > total)
      total = count;
  }
  avg = avg / total;
  return avg;
}

function getReadableHashRateString(hashrate) {
  var i = -1;
  var byteUnits = [" KH", " MH", " GH", " TH", " PH"];
  do {
    hashrate = hashrate / 1024;
    i++;
  } while (hashrate > 1024);
  return Math.round(hashrate) + byteUnits[i];
}

function timeOfDayFormat(timestamp) {
  var dStr = d3.time.format("%I:%M %p")(new Date(timestamp));
  if (dStr.indexOf("0") === 0) dStr = dStr.slice(1);
  return dStr;
}

if (!String.format) {
  String.format = function(format) {
    var args = Array.prototype.slice.call(arguments, 1);
    return format.replace(/{(\d+)}/g, function(match, number) {
      return typeof args[number] != "undefined" ? args[number] : match;
    });
  };
}
function pad(n, l) {
  return String("00" + n).slice(-1 * l);
}
function pad_space(n, l) {
  return String("    " + n).slice(-1 * l);
}

function formatDate(date) {
  var day = date.getDate();
  var monthIndex = date.getMonth();
  var year = date.getFullYear();
  var hour = date.getHours();
  var min = date.getMinutes();
  var sec = date.getSeconds();

  return String.format(
    "{0}-{1}-{2} {3}:{4}:{5}",
    pad(monthIndex + 1, 2),
    pad(day, 2),
    pad(year, 4),
    pad(hour, 2),
    pad(min, 2),
    pad(sec, 2)
  );
}

// function displayBlocks(blocks) {
//   var elems = blocks.map(function(b) {
//     var date = new Date(0);
//     date.setUTCMilliseconds(b.time);
//     var line = String.format(
//       "<p class='line'><span class='unit'><b>height</b>: {0}</span> <span class='unit'><b>miner</b>: {1}</span> at <span class='unit'>{2}</span></p>",
//       pad_space(b.height, 5),
//       b.addr,
//       formatDate(date)
//     );
//     return $(line);
//   });
//   console.log(elems);
//   root = $("<div/>");
//   for (var i in elems) {
//     root.append(elems[i]);
//   }
//   console.log("ROOT");
//   console.log(root);
//   $("#blocks").html("");
//   $("#blocks").append(root);
// }

function displayCharts() {

  nv.addGraph(function() {
    poolHashrateChart = nv.models
      .lineChart()
      .margin({ left: 80, right: 30 })
      .x(function(d) {
        return d[0];
      })
      .y(function(d) {
        return d[1];
      })
      .useInteractiveGuideline(true);

    poolHashrateChart.xAxis.tickFormat(timeOfDayFormat);

    poolHashrateChart.yAxis.tickFormat(function(d) {
      return getReadableHashRateString(d);
    });

    d3.select("#poolHashrate")
      .datum(poolHashrateData)
      .call(poolHashrateChart);

    return poolHashrateChart;
  });

}

function TriggerChartUpdates() {
  poolHashrateChart.update();
}

nv.utils.windowResize(TriggerChartUpdates);

$.getJSON("/api/pool_stats", function(data) {
  statData = data;
  buildChartData();
  displayCharts();
});

// $.getJSON("/api/blocks", function(data) {
//   displayBlocks(data);
// });

statsSource.addEventListener("message", function(e) {
  var stats = JSON.parse(e.data);
  statData.push(stats);

  var newPoolAdded = (function() {
    for (var p in stats.pools) {
      if (poolKeys.indexOf(p) === -1) return true;
    }
    return false;
  })();

  if (newPoolAdded || Object.keys(stats.pools).length > poolKeys.length) {
    buildChartData();
    displayCharts();
  } else {
    var time = stats.time * 1000;
    for (var f = 0; f < poolKeys.length; f++) {
      var pool = poolKeys[f];
      for (var i = 0; i < poolHashrateData.length; i++) {
        if (poolHashrateData[i].key === pool) {
          poolHashrateData[i].values.shift();
          poolHashrateData[i].values.push([
            time,
            pool in stats.pools ? stats.pools[pool].hashrate : 0
          ]);
          break;
        }
      }
    }
    TriggerChartUpdates();
  }
});
