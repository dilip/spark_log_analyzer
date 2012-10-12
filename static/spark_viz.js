// Color for the job background
var JOB_COLORS = ["#FFDFF8", "#FFDFDF", "#E6DBFF", "#C9EAF3", "#CAFFD8", "#CEFFFD", "#F7F9D0"];

var barWidth = 20;
var width = 500;
var height = 1000;

var data; // a global

function loadData(url) {
    d3.json("/test/data.json", function(json) {
        data = json;
        visualizeIt();
    });
}

function getTaskIdTextX(datum, index) {
    return x(barWidth*2 + index * (barWidth+5) + barWidth/2 + barWidth/3);
}

function getTaskIdTextY(datum, index) {
    return y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2 + 10);
}


function visualizeIt() {

    var x = d3.scale.linear().domain([0, 500]).range([0, width]);// TODO: fix both x and y ranges.
    //var y = d3.scale.linear().domain([d3.min(data, function(datum) { return datum.startEpochSeconds; }), d3.max(data, function(datum) { return datum.endEpochSeconds; })]).rangeRound([0, height]);
    var y = d3.scale.linear().domain([0, 400]).rangeRound([0, height]);

    // add the canvas to the DOM
    var svg = d3.select("#viz")
                .append("svg:svg")
                .attr("width", width)
                .attr("height", height);

    // Bind each job to an svg group
    var jobGroup = svg.selectAll("g")
                    .data(data)
                    .enter()
                    .append("svg:g");

    // Bind a rect covering the whole width to represent the job
    jobGroup.append("rect")
        .attr("class", "jobMain")
        .attr("x", function(datum, index) { return x(0); })
        .attr("y", function(datum) { return y(datum.startEpochSeconds); })
        .attr("height", function(datum) { return y(datum.endEpochSeconds) - y(datum.startEpochSeconds); })
        .attr("width", width)
        .attr("fill", function(datum, index) { return JOB_COLORS[index % JOB_COLORS.length]; });

    // Text for Job id
    jobGroup.append("svg:text")
        .attr("x", function(datum, index) { return x(barWidth); })
        .attr("y", function(datum) { return  y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2 + 10); })
        .attr("dx", 20)
        .attr("text-anchor", "middle")
        .attr('transform', function(datum, index) { return 'rotate(-90 ' + x(barWidth) + ',' + y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2 + 10)+ ')';})
        .text(function(datum) { return "Job ID " + datum.id;})
        .attr("fill", "white");


    // Bind each task within a job to an svg group
    var taskGroup = jobGroup.selectAll("g")
                        .data(function(d) {return d.taskRuns;})
                        .enter()
                        .append("svg:g")
                        .attr("class", "taskRun");

    taskGroup.append("svg:rect")
         .attr("x", function(datum, index) { return x(barWidth*2 + index * (barWidth+5)); })
         .attr("y", function(datum) { return y(datum.startEpochSeconds); })
         .attr("height", function(datum) { return y(datum.endEpochSeconds) - y(datum.startEpochSeconds); })
         .attr("width", x(barWidth))
         .attr("fill", "#2d578b");

    // Text for Task Id
    taskGroup.append("svg:text")
        .attr("x", getTaskIdTextX)
        .attr("y", getTaskIdTextY)
        //.attr("text-anchor", "middle")
        .attr('transform', function(datum, index) { return 'rotate(-90 ' + getTaskIdTextX(datum, index) + ' ' + getTaskIdTextY(datum, index) + ')';})
        .text(function(datum) { return "TID " + datum.tid;})
        .attr("fill", "white");
}

