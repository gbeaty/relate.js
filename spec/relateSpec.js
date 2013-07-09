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

var person = function(name, city, state, age) { return {name: name, city: city, state: state, age: age} }
var people = Relate.Table("person", function(p) {
	return p.name
})
var bob = person("Bob","Miami","FL", 40)
var bob2 = person("Bob","Dallas","TX", 45)
var lumbergh = person("Lumbergh","Dallas","TX", 50)
var milton = person("Milton","Dallas","TX", 42)

var city = function(name, state) { return {name: name, state: state} }
var cities = Relate.Table("city", function(c) { return c } )

var state = function(name, abbriv) { return {name: name, abbriv: abbriv} }
var states = Relate.Table("state", function(s) { return s.abbriv} )
var texas = state('Texas','TX')
var florida = state('Florida','FL')
var georgia = state('Georgia','GA')
var newYork = state('New York','NY')

var peopleToInsert = [bob,lumbergh,milton]
var peopleToUpdate = [bob2]
var statesToInsert = [texas, florida, georgia, newYork]
var peopleFormatter = function(p) { return { name: p.name, value: p.name + " from " + p.city + ", " + p.state } }
var formattedPeople = people.map(peopleFormatter)
var byAge = people.sort(function(a,b) { return a.age - b.age })
var stateGroup = people.group(function(p) { return p.state })
var peopleFromTexas = people.count(function(row) { return row.state === "TX" })

describe("Inserts", function() {
	it("Should return inserted rows", function() {			
		expect(people.insert(peopleToInsert)).toEqual({inserts: peopleToInsert.reverse()})			
		expect(states.insert(statesToInsert)).toEqual({inserts: statesToInsert.reverse()})
	})
	it("Should not insert duplicate primary keys", function() {
		expect(people.insert([bob])).toEqual({})
	})
	describe("Should work with", function() {
		it("Tables", function() {
			expect(people.toArray()).toEqual(peopleToInsert)
			expect(states.toArray()).toEqual(statesToInsert)
		})
		it("Mapped Relations", function() {
			expect(formattedPeople.toArray()).toEqual([peopleFormatter(bob), peopleFormatter(lumbergh), peopleFormatter(milton)].reverse())
		})
		it("Sort", function() {
			expect(byAge.getData()).toEqual([bob,milton,lumbergh])
		})
		it("Group", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({ Bob: bob })
		})
		it("Counts", function() {
			expect(peopleFromTexas()).toEqual(2)
		})
	})
})

describe("Upserts", function() {
	it("Should return updated rows", function() {
		expect(people.upsert([bob2])).toEqual({updates: [{last: bob, next: bob2}]})
	})
	describe("Should work with", function() {
		it("Tables", function() {
			expect(people.toArray()).toEqual([bob2,lumbergh,milton].reverse())
		})
		it("Mapped Relations", function() {
			expect(formattedPeople.toArray()).toEqual([peopleFormatter(bob2), peopleFormatter(lumbergh), peopleFormatter(milton)].reverse())
		})
		it("Sort", function() {
			expect(byAge.getData()).toEqual([milton,bob2,lumbergh])
		})
		it("Group", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Bob: bob2, Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({})
		})
		it("Counts", function() {
			expect(peopleFromTexas()).toEqual(3)
		})
	})
})

describe("Removes", function() {	
	it("Should work", function() {
		people.remove(["Milton"])
		expect(people.toArray()).toEqual([lumbergh,bob2])
	})
})

/*var peopleAndStates = stateGroup.outerJoin(states)
describe("Joins", function() {
	it("should join all rows", function() {
		var fullOuter = {}
		fullOuter[["Bob", "TX"]] = [bob2,texas]
		fullOuter[["Milton", "TX"]] = [milton,texas]
		fullOuter[["Lumbergh", "TX"]] = [lumbergh,texas]
		fullOuter[[undefined, "FL"]] = [undefined,florida]
		fullOuter[[undefined, "GA"]] = [undefined,georgia]
		fullOuter[[undefined, "NY"]] = [undefined,newYork]
		console.log(peopleAndStates.rows)
		expect(peopleAndStates.rows).toEqual(fullOuter)
	})
})*/