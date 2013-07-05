var time = function(its, f) {
	var start = (new Date).getTime()
	for(var i=0; i<its; i++) {
		f()
	}
	var elapsed = ((new Date).getTime() - start) / 1000
	console.log(its + " iterations in " + elapsed + " seconds. " + (its/elapsed) + " ops per second.")
}

var keyMaker = function(r) { return r[0] }
var t1 = Relate.Table("t1", keyMaker)
var t2 = Relate.Table("t2", keyMaker)

var rand = function() { return Math.floor(Math.random()*1000000) }
var row = function(pk) { return [pk,rand(),rand(),rand(),rand(),rand(),rand(),rand()] }
var first = row(1)

var person = function(name, city, state, age) { return {name: name, city: city, state: state, age: age} }
var people = Relate.Table("person", function(p) { return p.name } )
var bob = person("Bob","Miami","FL", 40)
var bob2 = person("Bob","Dallas","TX", 45)
var lumbergh = person("Lumbergh","Dallas","TX", 50)
var milton = person("Milton","Dallas","TX", 42)

var city = function(name, state) { return {name: name, state: state} }
var cities = Relate.Table("city", function(c) { return c } )

var state = function(name, abbriv) { return {name: name, abbriv: abbriv} }
var states = Relate.Table("state", function(s) { return s.abbriv} )
var texas = state('Texas','TX')
states.insert([state('Texas','TX'), state('Florida','FL'), state('Georgia','GA'), state('New York','NY')])

describe("Tables", function() {
	it("should create empty tables", function() {
		expect(people.toArray().length).toEqual(0)
	})

	it("should insert", function() {
		expect(people.insert([bob])).toEqual({inserts: [bob]})
		expect(people.toArray()).toEqual([bob])
	})

	it("should not insert duplicate pks", function() {
		expect(people.insert([bob2])).toEqual({})
		expect(people.toArray()).toEqual([bob])
	})

	it("should upsert duplicate pks", function() {
		expect(people.upsert([bob2])).toEqual({updates: [{last: bob, next: bob2}]})
		expect(people.toArray()).toEqual([bob2])
	})

	it("should insert more rows", function () {
		expect(people.insert([lumbergh, milton, lumbergh])).toEqual({inserts: [lumbergh, milton]})
		expect(people.toArray().length).toEqual(3)
	})
})

var formattedPeople = people.map(function(p) { return p.name + " from " + p.city + ", " + p.state })
describe("Mapped Relations", function() {
	it("should work", function() {
		expect(formattedPeople.toArray()).toEqual([
			"Bob from Dallas, TX",
			"Milton from Dallas, TX",
			"Lumbergh from Dallas, TX"
		])
	})
})

var peopleFromTexas = people.count(function(row) { return row.state === "TX" })
describe("Aggregates", function() {
	describe("Counts", function() {
		it("should count", function() {
			expect(peopleFromTexas()).toEqual(3)
		})
	})
})

var stateGroup = people.group(function(p) {
	return p.state
})
describe("Groups", function() {
	it("should work", function() {
		expect(stateGroup.get("TX")).toEqual({ Bob: bob2, Lumbergh: lumbergh, Milton: milton })
	})

	it("should not contained removed rows", function() {
		expect(stateGroup.get("FL")).toEqual({})
	})
})

var byAge = people.sort(function(a,b) { return a.age - b.age })
describe("Sorting", function() {
	it("should work on existing data", function() {
		expect(byAge.getData()).toEqual([milton,bob2,lumbergh])
	})
})

// var peopleStates = stateGroup.join(states)
/*describe("Joins", function() {
	it("should join all rows", function() {
		expect(peopleStates.toArray()).toEqual([])
	})
})*/