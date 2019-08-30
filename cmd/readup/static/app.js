// set the dimensions and margins of the graph
var margin = {top: 30, right: 30, bottom: 30, left: 30},
width = 1024 - margin.left - margin.right,
height = 800 - margin.top - margin.bottom;


var svg = d3.select("#alloc")
.append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)

var colorScale = d3.scaleLinear()
  .range(["white", "green"])
  .domain([0,1]);

var heightPadding = 1;
var widthPadding = 1;
var columnSize = 128;
var boxHeight = 8;
var boxWidth = 8;

d3.json("/getalloc", function (data) {
svg.selectAll()
      .data(data)
      .enter()
      .append("rect")
      .attr("x", function(d, index){
          return Math.floor(index % columnSize) * boxWidth;
       })
      .attr("y", function(d, index){
        return Math.floor(index / columnSize) * boxHeight;
      })

      .attr("width", boxWidth - widthPadding)
      .attr("height", boxHeight - heightPadding)
      .style("fill", function(d) { return colorScale(d)} )

})
