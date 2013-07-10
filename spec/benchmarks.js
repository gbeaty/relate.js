var rand = function() { return Math.floor(Math.random()*1000000) }
var row = function(pk) { return [pk,rand(),rand(),rand(),rand(),rand(),rand(),rand()] }
var first = row(1)

var time = function(its, run, setup, teardown) {
	if(setup)
		setup()
	var start = (new Date).getTime()
	for(var i=0; i<its; i++) {
		run()
	}
	var elapsed = ((new Date).getTime() - start) / 1000
	var perSecond = its/elapsed
	console.log(its + " iterations in " + elapsed + " seconds. " + perSecond + " ops per second.")
	if(teardown)
		teardown()

	return perSecond
}

var pkgen = function(r) { return r[0] }
var rand = function() { return Math.floor(Math.random()*1000000) }
var row = function(pk) { return [pk,rand(),rand(),rand(),rand(),rand(),rand(),rand()] }
var sorter = function(a,b) { return a[1] - b[1] }

var table, ids, bySecond
var num = 100000

var rows = []
for(var i=0; i<num; i++) {
	rows.push(row(i))
}

var result = time(
	10,
	function() {
		table.upsert(rows)
	},
	function() {
		table = Relate.Table("bench", pkgen)
		ids = table.map(function(r) { return [r[0],r[1]] })
		//bySecond = table.sort(function(a,b) { return a[1] - b[1] })		
	},
	function() {
		//console.log(table.toArray())
	}
)

document.getElementById("results").innerHTML = result.toString()