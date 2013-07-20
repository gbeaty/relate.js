var time = function(its, f) {
	var start = (new Date).getTime()
	for(var i=0; i<its; i++) {
		f()
	}
	var elapsed = ((new Date).getTime() - start) / 1000
	console.log(its + " iterations in " + elapsed + " seconds. " + (its/elapsed) + " ops per second.")
}

var keyMaker = function(r) { return r[0] }
var db = Relate.db()
var t1 = db.table(keyMaker)
var t2 = db.table(keyMaker)

var person = function(name, city, state, age) { return {name: name, city: city, state: state, age: age} }
var people = db.table(function(p) {
	return p.name
})
var bob = person("Bob","Miami","FL", 40)
var bob2 = person("Bob","Dallas","TX", 45)
var lumbergh = person("Lumbergh","Dallas","TX", 50)
var milton = person("Milton","Dallas","TX", 42)

var city = function(name, state) { return {name: name, state: state} }
var cities = db.table(function(c) { return c } )

var state = function(name, abbriv) { return {name: name, abbriv: abbriv} }
var states = db.table(function(s) { return s.abbriv} )
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
var peopleFromTexas = people.count(function(row) {
	return row.state === "TX"
})
var peopleAndStates = stateGroup.join([states])

describe("Inserts", function() {
	it("should insert rows", function() {
		people.insert(peopleToInsert)
		states.insert(statesToInsert)
	})
	it("should not insert duplicate primary keys", function() {
		expect(function() { people.insert([bob]) }).toThrow(new Error("Primary key constraint violation for key: Bob"))
	})
	describe("should work with", function() {
		it("tables", function() {
			expect(people.getRows()).toEqual({ Bob: bob, Lumbergh: lumbergh, Milton: milton })
			expect(states.getRows()).toEqual({ TX: texas, FL: florida, GA: georgia, NY: newYork })
		})
		it("affect row counts", function() {
			expect(people.getRowCount()).toEqual(3)
		})
		it("mapped relations", function() {
			expect(formattedPeople.getRows()).toEqual({
				Bob: peopleFormatter(bob),
				Lumbergh: peopleFormatter(lumbergh),
				Milton: peopleFormatter(milton)
			})
		})
		it("sorts", function() {
			expect(byAge.getData()).toEqual([bob,milton,lumbergh])
		})
		it("groups", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({ Bob: bob })
		})
		it("counters", function() {
			expect(peopleFromTexas()).toEqual(2)
		})
		it("joins", function() {
			var fullOuter = {}
			fullOuter[["Bob", "FL"]] = [bob,florida]
			fullOuter[["Milton", "TX"]] = [milton,texas]
			fullOuter[["Lumbergh", "TX"]] = [lumbergh,texas]
			fullOuter[[undefined, "GA"]] = [undefined,georgia]
			fullOuter[[undefined, "NY"]] = [undefined,newYork]
			console.log(peopleAndStates.getRows())
			expect(peopleAndStates.getRows()).toEqual(fullOuter)
		})
	})
})

describe("Updates", function() {
	it("should update rows", function() {
		people.upsert([bob2])
	})
	describe("should work with", function() {
		it("tables", function() {
			expect(people.getRows()).toEqual({ Bob: bob2, Lumbergh: lumbergh, Milton: milton })
		})
		it("not affect row counts", function() {
			expect(people.getRowCount()).toEqual(3)
		})
		it("mapped relations", function() {
			expect(formattedPeople.getRows()).toEqual({
				Bob: peopleFormatter(bob2),
				Lumbergh: peopleFormatter(lumbergh),
				Milton: peopleFormatter(milton)
			})
		})
		it("sorts", function() {
			expect(byAge.getData()).toEqual([milton,bob2,lumbergh])
		})
		it("groups", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Bob: bob2, Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({})
		})
		it("counters", function() {
			expect(peopleFromTexas()).toEqual(3)
		})
		it("joins", function() {
			var fullOuter = {}
			fullOuter[["Bob", "TX"]] = [bob2,texas]
			fullOuter[["Milton", "TX"]] = [milton,texas]
			fullOuter[["Lumbergh", "TX"]] = [lumbergh,texas]
			fullOuter[[undefined, "GA"]] = [undefined,georgia]
			fullOuter[[undefined, "NY"]] = [undefined,newYork]
			fullOuter[[undefined, "FL"]] = [undefined,florida]
			console.log(peopleAndStates.getRows())
			expect(peopleAndStates.getRows()).toEqual(fullOuter)
		})
	})
})

describe("Removes", function() {
	it("should work", function() {
		people.remove(["Milton"])
		expect(people.getRows()).toEqual({ Bob: bob2, Lumbergh: lumbergh })
	})
	it("affect row counts", function() {
		expect(people.getRowCount()).toEqual(2)
	})
	it("joins", function() {
		var fullOuter = {}
		fullOuter[[undefined, "NY"]] = [undefined,newYork]
		fullOuter[[undefined, "GA"]] = [undefined,georgia]
		fullOuter[["Lumbergh", "TX"]] = [lumbergh,texas]		
		fullOuter[[undefined, "FL"]] = [undefined,florida]
		fullOuter[["Bob", "TX"]] = [bob2,texas]
		console.log(peopleAndStates.getRows())
		expect(peopleAndStates.getRows()).toEqual(fullOuter)
	})
})