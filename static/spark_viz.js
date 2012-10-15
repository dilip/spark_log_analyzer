// Color for the job background
var JOB_COLORS = ["#FFDFF8", "#FFDFDF", "#E6DBFF", "#C9EAF3", "#CAFFD8", "#CEFFFD", "#F7F9D0"];

/* The total width of the visualization */
var WIDTH = 1000;
/* The total height of the visualization */
var height = 1000;

/* Width of a bar representing a task run */
var BAR_WIDTH=20;
var BAR_SPACING=5;

var DATA; // a global

// Scale functions
var X;
var Y;

function initScales() {

    var maxTasks = d3.max(DATA, 
                            function(sparkJob) {
                                return d3.max(sparkJob.mesosJobs,
                                    function(mesosJob) {
                                        return mesosJob.tasks.length;
                                    });
                            });
    console.log("Max tasks: " + maxTasks);
 
    X = d3.scale.linear().domain([0, BAR_WIDTH*2 + maxTasks * (BAR_WIDTH+BAR_SPACING)]).range([0, WIDTH]);
    tmp = [
                d3.min(DATA, function(d) {return d.startEpochSeconds;}),
                d3.max(DATA, function(d) {return d.endEpochSeconds;})
            ];
    console.log("y range is : " + tmp);
    Y = d3.scale.linear()
            .domain(tmp)
            .rangeRound([0, height]);
}

function initScales2() {
    X = d3.scale.linear().domain([0, 500]).range([0, WIDTH]);
    Y = d3.scale.linear()
            .domain([
                d3.min(DATA, function(d) {
                        return d3.min(d.tasks, function(d1) { 
                            return d3.min(d1.runs, function(d2) {
                                return d2.startEpochSeconds;
                            });
                        });
                    }), 
                d3.max(DATA, function(d) {
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
        DATA = json;
       
        initScales();
        visualizeIt();
    });
}


function visualizeIt() {
    // add the canvas to the DOM
    var svg = d3.select("#viz")
                .append("svg:svg")
                .attr("width", WIDTH)
                .attr("height", height);
    
    // Bind each spark job to an svg group
    var sparkJobGroup = svg.selectAll("g")
                    .data(DATA)
                    .enter()
                    .append("svg:g")
                    .attr("class", "sparkJob");

    // Bind a rect covering the whole width to represent the spark job
    sparkJobGroup.append("rect")
        .attr("class", "jobMain")
        .attr("x", function(datum, index) { return X(0); })
        .attr("y", function(datum) { return Y(datum.startEpochSeconds); })
        .attr("height", function(datum) { t= Y(datum.endEpochSeconds) - Y(datum.startEpochSeconds); return t; })
        .attr("width", WIDTH)
        .attr("fill", function(datum, index) { return JOB_COLORS[index % JOB_COLORS.length]; });

    drawMesosJobs(sparkJobGroup);

}

function drawMesosJobs(sparkJobGroup) {
    // Bind each mesos job to an svg group
    var mesosJobGroup = sparkJobGroup.selectAll("g.mesosJob")
                    .data(function(d) { return d.mesosJobs;})
                    .enter()
                    .append("svg:g")
                    .attr("class", "mesosJob");

    // Text for Job id
    mesosJobGroup.append("svg:text")
        .attr("x", function(datum, index) { return X(BAR_WIDTH); })
        .attr("y", function(datum) { return  Y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2); })
        .attr("dx", 20)
        .attr("text-anchor", "middle")
        .attr('transform', function(datum, index) { return 'rotate(-90 ' + X(BAR_WIDTH) + ',' + Y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2)+ ')';})
        .text(function(datum) { return "Job ID " + datum.id;})
        .attr("fill", "white");

    // Add a line to show start of mesos job
    mesosJobGroup.append("svg:line")
        .attr("x1", function(datum, index) { return 0; })
        .attr("x2", function(datum, index) { return WIDTH; })
        .attr("y1", function(datum) { return  Y(datum.startEpochSeconds); })
        .attr("y2", function(datum) { return  Y(datum.startEpochSeconds); });

    drawTasks(mesosJobGroup);

}

function drawTasks(mesosJobGroup) {
    // Bind each task within a job to an svg group
    // taskGroup defines a new coordinate system for its children.
    // Tasks are laid out horizontally
    var taskGroup = mesosJobGroup.selectAll("g")
                        .data(function(d) {return d.tasks;})
                        .enter()
                        .append("svg:g")
                        .attr("class", "task")
                        .attr("transform", function(d, index) { return "translate(" + X(BAR_WIDTH*2 + index * (BAR_WIDTH+BAR_SPACING)) + ",0)"; });

    // Bind each task run within a task to an svg group
    var taskRunGroup = taskGroup.selectAll("g .taskRun")
        .data(function(d) {return d.runs;})
        .enter()
        .append("svg:g")
        .attr("class", "taskRun");
 
    taskRunGroup.append("svg:rect")
         .attr("x", function(datum, index) { return X(0); })
         .attr("y", function(datum) { return Y(datum.startEpochSeconds); })
         .attr("height", function(datum) { return Y(datum.endEpochSeconds) - Y(datum.startEpochSeconds); })
         .attr("width", X(BAR_WIDTH))
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

function getTaskIdTextX(datum, index) {
    return X(BAR_WIDTH/2 + BAR_WIDTH/3);
}

function getTaskIdTextY(datum, index) {
    return Y(datum.startEpochSeconds + (datum.endEpochSeconds - datum.startEpochSeconds)/2);
}


