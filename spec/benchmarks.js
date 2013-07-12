var rand = function() { return Math.floor(Math.random()*1000000) }
var row = function(pk) { return [pk,rand(),rand(),rand(),rand(),rand(),rand(),rand()] }
var first = row(1)

var time = function(name, its, run, setup, teardown, mult) {
	if(setup)
		setup()
	var start = (new Date).getTime()
	for(var i=0; i<its; i++) {
		run()
	}
	var elapsed = ((new Date).getTime() - start) / 1000
	var perSecond = its/elapsed * (mult ? mult : 1)
	console.log(name + ": " + its + " iterations in " + elapsed + " seconds. " + perSecond + " ops per second.")
	if(teardown)
		teardown()

	return perSecond
}

var db = Relate.db()
var pkgen = function(r) { return r[0] }
var rand = function() { return Math.floor(Math.random()*1000000) }
var row = function(pk) { return [pk,rand(),rand(),rand(),rand(),rand(),rand(),rand()] }
var sorter = function(a,b) { return a[1] - b[1] }

var table, ids, bySecond
var num = 50000

var rows = []
for(var i=0; i<num; i++) {
	rows.push(row(i))
}

var result = time(
	"Relations",
	20,
	function() {
		table.upsert(rows)
	},
	function() {
		table = db.table(pkgen)
		ids = table.map(function(r) { return [r[0],r[1]] })
		bySecond = table.sort(function(a,b) { return a[1] - b[1] })
		bySecond.getData()
	},
	function() {
		//console.log(table.toArray())
	},
	num
)

document.getElementById("results").innerHTML = result.toString()

/*time("Initial sort", 1, function() { rows.sort(sorter) })
rows.push(row(num))
time("Appended", 1, function() { rows.sort(sorter) })
rows.splice(num / 2, 1)
time("Middle cut out", 1, function() { rows.sort(sorter) })
rows.splice(0,1)
time("Popped", 10, function() { rows.sort(sorter) })*/