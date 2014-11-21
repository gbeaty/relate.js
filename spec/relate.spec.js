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
var t1 = db.Table(keyMaker)
var t2 = db.Table(keyMaker)

var person = function(name, city, state, age) { return {name: name, city: city, state: state, age: age} }
var people = db.Table(function(p) {
	return p.name
})
var bob = person("Bob","Miami","FL", 40)
var bob2 = person("Bob","Dallas","TX", 45)
var lumbergh = person("Lumbergh","Dallas","TX", 50)
var milton = person("Milton","Dallas","TX", 42)

var city = function(name, state) { return {name: name, state: state} }
var cities = db.Table(function(c) { return c } )

var state = function(name, abbriv) { return {name: name, abbriv: abbriv} }
var states = db.Table(function(s) { return s.abbriv} )
var texas = state('Texas','TX')
var florida = state('Florida','FL')
var georgia = state('Georgia','GA')
var newYork = state('New York','NY')

var peopleToInsert = [bob,lumbergh,milton]
var peopleToUpdate = [bob2]
var statesToInsert = [texas, florida, georgia, newYork]
var peopleFormatter = function(p) { return { name: p.name, value: p.name + " from " + p.city + ", " + p.state } }
var formattedPeople = people.map(peopleFormatter)
var orderedPeople = people.order()
var byAge = people.sort(function(a,b) {
	return a.age - b.age
})
var stateGroup = people.group(function(p) { return p.state })
var orderedStates = states.order()
var peopleFromTexas = people.count(function(row) {
	return row.state === "TX"
})
var peopleOrStates = stateGroup.join([states])
var peopleWithStates = stateGroup.join([states], [true, false])
var statesWithPeople = states.join([stateGroup], [true, false])
var peopleAndStates = stateGroup.join([states], [true, true])

var peopleOrStatesResults = {}
var statesWithPeopleResults = {}
var peopleWithStatesResults = {}

var peopleKeyTrigger = people.keyTrigger()
var bobState
var setBobState = function(last, next) {
	bobState = next ? next.state : undefined
}
peopleKeyTrigger.listen("Bob", "Bob", setBobState)

describe("Inserts", function() {
	it("should insert rows", function() {
		people.insert(peopleToInsert)
		states.insert(statesToInsert)
	})
	it("should not insert duplicate primary keys", function() {
		expect(function() { people.insert([bob]) }).toThrow(new Error("Primary key constraint violation for key: Bob"))
	})
	describe("should work with", function() {
		it("key triggers", function() {
			expect(bobState).toEqual("FL")
		})
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
		it("orders", function() {
			expect(orderedPeople.toArray()).toEqual(peopleToInsert)
			// expect(formattedPeople.order().toArray()).toEqual("blah")
		})
		it("groups", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({ Bob: bob })
		})
		it("counters", function() {
			expect(peopleFromTexas()).toEqual(2)
		})
		it("joins with required people", function() {
			peopleWithStatesResults[["Bob", "FL"]] = [bob,florida]
			peopleWithStatesResults[["Milton", "TX"]] = [milton,texas]
			peopleWithStatesResults[["Lumbergh", "TX"]] = [lumbergh,texas]
			expect(peopleWithStates.getRows()).toEqual(peopleWithStatesResults)			
		})
		it("joins with required people and states", function() {
			expect(peopleAndStates.getRows()).toEqual(peopleWithStatesResults)
		})
		it("full joins", function() {
			peopleOrStatesResults = _.clone(peopleWithStatesResults)
			peopleOrStatesResults[[undefined, "GA"]] = [undefined,georgia]
			peopleOrStatesResults[[undefined, "NY"]] = [undefined,newYork]
			expect(peopleOrStates.getRows()).toEqual(peopleOrStatesResults)
		})
		it("joins with required states", function() {
			statesWithPeopleResults[["FL", "Bob"]] = [florida, bob]
			statesWithPeopleResults[["TX", "Milton"]] = [texas, milton]
			statesWithPeopleResults[["TX", "Lumbergh"]] = [texas, lumbergh]
			statesWithPeopleResults[["GA", undefined]] = [georgia, undefined]
			statesWithPeopleResults[["NY", undefined]] = [newYork, undefined]
			expect(statesWithPeople.getRows()).toEqual(statesWithPeopleResults)
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
		it("key triggers", function() {
			expect(bobState).toEqual("TX")
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
		it("orders", function() {
			expect(orderedPeople.toArray()).toEqual([bob2, lumbergh, milton])
		})
		it("groups", function() {
			expect(stateGroup.getGroup("TX").rows).toEqual({ Bob: bob2, Lumbergh: lumbergh, Milton: milton })
			expect(stateGroup.getGroup("FL").rows).toEqual({})
		})
		it("counters", function() {
			expect(peopleFromTexas()).toEqual(3)
		})
		it("joins with required people", function() {
			delete peopleWithStatesResults[["Bob", "FL"]]
			peopleWithStatesResults[["Bob", "TX"]] = [bob2,texas]
			expect(peopleWithStates.getRows()).toEqual(peopleWithStatesResults)			
		})
		it("joins with required people and states", function() {
			expect(peopleAndStates.getRows()).toEqual(peopleWithStatesResults)
		})
		it("full joins", function() {
			delete peopleOrStatesResults[["Bob","FL"]]
			peopleOrStatesResults[[undefined, "FL"]] = [undefined,florida]
			peopleOrStatesResults[["Bob","TX"]] = [bob2, texas]
			expect(peopleOrStates.getRows()).toEqual(peopleOrStatesResults)
		})
		it("joins with required states", function() {
			statesWithPeopleResults[["FL", undefined]] = [florida, undefined]
			statesWithPeopleResults[["TX", "Bob"]] = [texas, bob2]
			delete statesWithPeopleResults[["FL", "Bob"]]
			expect(statesWithPeople.getRows()).toEqual(statesWithPeopleResults)
		})
	})
})

describe("Removes", function() {
	it("should remove rows", function() {
		people.remove(["Milton"])
		expect(people.getRows()).toEqual({ Bob: bob2, Lumbergh: lumbergh })
	})
	it("affect row counts", function() {
		expect(people.getRowCount()).toEqual(2)
	})
	describe("should work with", function() {
		it("orders", function() {
			expect(orderedPeople.toArray()).toEqual([bob2, lumbergh])
		})
		var join = {}
		it("joins with required people", function() {		
			join[["Bob", "TX"]] = [bob2,texas]
			join[["Lumbergh", "TX"]] = [lumbergh,texas]
			expect(peopleWithStates.getRows()).toEqual(join)			
		})
		it("joins with required people and states", function() {
			expect(peopleAndStates.getRows()).toEqual(join)
		})
		it("joins with required people and states", function() {
			delete peopleWithStatesResults[["Milton", "TX"]]
			expect(peopleAndStates.getRows()).toEqual(peopleWithStatesResults)
		})
		it("full joins", function() {
			delete peopleOrStatesResults[["Milton","TX"]]
			expect(peopleOrStates.getRows()).toEqual(peopleOrStatesResults)
		})
		it("joins with required states", function() {
			delete statesWithPeopleResults[["TX", "Milton"]]
			expect(statesWithPeople.getRows()).toEqual(statesWithPeopleResults)
		})
	})	
})

var scalars = db.Scalars()
var num
describe("Scalars", function() {	
	it("set", function() {
		scalars.set("num", 5)
		expect(scalars.get("num")).toEqual(5)
	})
	it("register listeners", function() {
		scalars.listen("num", "num", function(n) { num = n })
		expect(num).toEqual(5)
	})
	it("update", function() {
		scalars.set("num", 10)
		expect(scalars.get("num")).toEqual(10)
		expect(num).toEqual(10)
	})
	it("clear", function() {
		scalars.clear("num")
		expect(scalars.clear("num")).toEqual(undefined)
		expect(num).toEqual(undefined)
	})
})

describe("Clears", function() {
	it("should erase all rows", function() {
		people.clear()
		expect(people.getRows()).toEqual({})
	})
	var join = {}
	it("joins with required people", function() {
		expect(peopleWithStates.getRows()).toEqual({})			
	})
	it("joins with required people and states", function() {
		expect(peopleAndStates.getRows()).toEqual({})
	})
	it("full joins", function() {
		join[[undefined,"TX"]] = [undefined, texas]
		join[[undefined,"FL"]] = [undefined, florida]
		join[[undefined,"GA"]] = [undefined, georgia]
		join[[undefined,"NY"]] = [undefined, newYork]
		expect(peopleOrStates.getRows()).toEqual(join)
	})
})

describe("Sorts", function() {
	var nums = db.Table(function(num) { return num })
	var comparer = function(a,b) { return a - b }
	var sortedNums = nums.sort(comparer)
	var ns = [5,8,3,1,2,7,6,4]
	ns.sort(comparer)
	nums.insert(ns)
	it("should sort numbers", function() {
		expect(sortedNums.getData()).toEqual(ns)
	})
	it("should reverse", function() {
		sortedNums.reverse()
		ns.reverse()
		expect(sortedNums.getData()).toEqual(ns)
		sortedNums.reverse()
		ns.reverse()
		expect(sortedNums.getData()).toEqual(ns)
	})
})

describe("Orders", function() {
	it("should default to as-inserted order", function() {
		expect(orderedStates.toArray()).toEqual([texas, florida, georgia, newYork])
	})
	it("should swap", function() {
		orderedStates.swap(texas, florida)
		expect(orderedStates.toArray()).toEqual([florida, texas, georgia, newYork])
		orderedStates.swap(georgia, newYork)
		expect(orderedStates.toArray()).toEqual([florida, texas, newYork, georgia])
		orderedStates.swap(texas, newYork)
		expect(orderedStates.toArray()).toEqual([florida, newYork, texas, georgia])
		orderedStates.swap(georgia, florida)
		expect(orderedStates.toArray()).toEqual([georgia, newYork, texas, florida])
	})
	it("should move to tail", function() {
		orderedStates.move(florida, undefined)
		expect(orderedStates.toArray()).toEqual([georgia, newYork, texas, florida])
		orderedStates.move(newYork, undefined)
		expect(orderedStates.toArray()).toEqual([georgia, texas, florida, newYork])
	})
	it("should move head to tail", function() {
		orderedStates.move(georgia, undefined)
		expect(orderedStates.toArray()).toEqual([texas, florida, newYork, georgia])
	})
	it("should move to itself", function() {
		orderedStates.move(texas, texas)
		expect(orderedStates.toArray()).toEqual([texas, florida, newYork, georgia])
	})
	it("should move to head", function() {
		orderedStates.move(florida, texas)
		expect(orderedStates.toArray()).toEqual([florida, texas, newYork, georgia])
	})
})