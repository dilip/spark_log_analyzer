// Color for the job background
var JOB_COLORS = ["#FFDFF8", "#FFDFDF", "#E6DBFF", "#C9EAF3", "#CAFFD8", "#CEFFFD", "#F7F9D0"];

var barWidth = 20;
var width = 500;
var height = 1000;

var data; // a global

// Scale functions
var x;
var y;

function initScales() {
    x = d3.scale.linear().domain([0, 500]).range([0, width]);
    tmp = [
                d3.min(data, function(d) {return d.startEpochSeconds;}),
                d3.max(data, function(d) {return d.endEpochSeconds;})
            ];
    console.log("y range is : " + tmp);
    y = d3.scale.linear()
            .domain(tmp)
            .rangeRound([0, height]);
}

function initScales2() {
    x = d3.scale.linear().domain([0, 500]).range([0, width]);
    y = d3.scale.linear()
            .domain([
                d3.min(data, function(d) {
                        return d3.min(d.tasks, function(d1) { 
                            return d3.min(d1.runs, function(d2) {
                                return d2.startEpochSeconds;
                            });
                        });
                    }), 
                d3.max(data, function(d) {
                        return d3.max(d.tasks, function(d1) { 
                            return d3.max(d1.runs, function(d2) {
                                return d2.endEpochSeconds;
                            });
                        });
                    })])
            .rangeRound([0, height]);
}



function loadData(url) {
    d3.json(url, function(json) {
        data = json;
        initScales();
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
        .attr("height", function(datum) { t= y(datum.endEpochSeconds) - y(datum.startEpochSeconds); alert(datum.id + ":" + t + ":" + datum.endEpochSeconds + ":" + datum.startEpochSeconds + ":" + y(datum.endEpochSeconds) + ":" + y(datum.startEpochSeconds)); return t; })
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
    // taskGroup defines a new coordinate system for its children.
    var taskGroup = jobGroup.selectAll("g")
                        .data(function(d) {return d.tasks;})
                        .enter()
                        .append("svg:g")
                        .attr("class", "task")
                        .attr("transform", function(d, index) { return "translate(" + x(barWidth*2 + index * (barWidth+5)) + ",0)"; });

    // Bind each task run within a task to an svg group
    var taskRunGroup = taskGroup.selectAll("g .run")
        .data(function(d) {return d.runs;})
        .enter()
        .append("svg:g")
        .attr("class", "run");
 
    taskRunGroup.append("svg:rect")
         .attr("x", function(datum, index) { return x(0); })
         .attr("y", function(datum) { return y(datum.startEpochSeconds); })
         .attr("height", function(datum) { return y(datum.endEpochSeconds) - y(datum.startEpochSeconds); })
         .attr("width", x(barWidth))
         .attr("fill", "#2d578b");

    // Text for Task Id
    taskRunGroup.append("svg:text")
        .attr("x", getTaskIdTextX)
        .attr("y", getTaskIdTextY)
        //.attr("text-anchor", "middle")
        .attr('transform', function(datum, index) { return 'rotate(-90 ' + getTaskIdTextX(datum, index) + ' ' + getTaskIdTextY(datum, index) + ')';})
        .text(function(datum) { return "TID " + datum.tid;})
        .attr("fill", "white");
}

