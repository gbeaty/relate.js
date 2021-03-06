var factory = function() {
	var relate = {}
	var lastId = 0
	var identity = function(a) { return a }
	var noop = function() {}
	var idGen = function(row) { return row.id }
	var scalarKeyCompare = function(k1, k2) { return k1 === k2 }
	var arrayKeyCompare = function(k1, k2) {
		var i = k1.length
		if(i !== k2.length)
			return false
		while(--i >= 0) {
			if(k1[i] !== k2[i])
				return false
		}
		return true
	}
	var scalarKeyGen = function(scalar) { return scalar.key }
	var zero = function() { return 0 }
	var one = function() { return 1 }
	var transactions = []
	// Faster than Array.forEach(), but executes in reverse order.
	var forEach = function(array, op) {
		var i = array.length
		while(--i >= 0) {
			op(array[i])
		}
	}

	relate.transact = function(name, t) {
		if(!t) {
			t = name
			name = "\n" + t.toString() + "\n"
		}
		t.relations = []
		transactions.push(t)
		try {
			t()
		} catch(err) {
			var i = t.relations.length
			while(--i >= 0) {
				t.relations[i].rollback()
			}
			console.log("Transaction '" + name + "' rolled back due to exception '" + err + "'.")
		}
		transactions.pop()
	}

	var cartProd = function(paramArray) {
		function addTo(curr, args) {
	    	var i, copy, rest = args.slice(1), last = !rest.length, result = [];
	    	for (i = 0; i < args[0].length; i++) {
	    		copy = curr.slice();
	    		copy.push(args[0][i]);
	      
				if (last) {
					result.push(copy);
				} else {
					result = result.concat(addTo(copy, rest));
				}
			}
			return result;
		}
		return addTo([], paramArray);
	}

	var broadcasts = function() {
		var self = {}
		self.listeners = []

		self.broadcastTo = function(rel) {
			if(self.listeners.indexOf(rel) === -1)
				self.listeners.push(rel)
		}
		self.stopBroadcastingTo = function(rel) {
			self.listeners = self.listeners.filter(function(r) { return r !== rel })
		}

		return self
	}

	relate.listener = function(insert, update, remove, self) {
		self = self ? self : {}
		self.sourceInsert = insert ? insert : noop
		self.sourceUpdate = update ? update : noop
		self.sourceRemove = remove ? remove : noop
		return self
	}

	var simpleRelation = function(keyGen, keyCompare) {
		var self = {}
		self.pub = broadcasts()
		self.keyGen = keyGen
		self.pub.keyGen = keyGen
		var rows = self.rows = {}
		var rowCount = 0
		var listeners = self.pub.listeners

		if(!keyCompare)
			keyCompare = scalarKeyCompare
		self.keyCompare = keyCompare

		self.get = function(key) { return self.rows[key] }

		self.pub.getRows = function() { return rows }
		self.pub.get = function(key) { return rows[key] }
		self.pub.toArray = function() {
			var result = []
			self.pub.forEach(function(r) { result.push(r) })
			return result
		}
		self.pub.forEach = function(f) {
			var keys = Object.keys(rows)
			var len = keys.length
			for(var i=0; i<len; i++) {
				f(rows[keys[i]])
			}
		}

		self.pub.drop = function() {
			var l = self.listeners.length				
			while(--l >= 0) {
				self.listeners[l].drop()
			}
			delete self.listeners
			delete self.broadcastTo
		}

		var checkKeyGen = function(row) {
			var key = keyGen(row)
			if(key === undefined || key === null)
				throw "Cannot generate key for row: " + row;
			return key
		}

		self.signalInsert = function(row) {
			forEach(self.pub.listeners, function(l) { l.sourceInsert(self, row) })
		}
		self.signalUpdate = function(last, next) {
			forEach(self.pub.listeners, function(l) { l.sourceUpdate(self, last, next) })
		}
		self.signalRemove = function(row) {
			forEach(self.pub.listeners, function(l) { l.sourceRemove(self, row) })
		}

		self.insert = function(row) {
			if(!self.insertIfNotExists(row))
				throw "Primary key constraint violation for key: " + keyGen(row)
		}
		self.insertIfNotExists = function(row) {
			var key = checkKeyGen(row)
			if(rows[key] === undefined) {
				rows[key] = row
				rowCount++
				self.signalInsert(row)
				return true
			}
			return false
		}
		self.updateForKeys = function(last, next, lastKey, nextKey) {
			if(!keyCompare(lastKey, nextKey)) {
				self.remove(last)
				self.insert(next)
			} else {					
				rows[nextKey] = next
				self.signalUpdate(last, next)
			}
		}
		self.upsert = function(row) {
			var key = checkKeyGen(row)
			if(key !== undefined) {
				var existing = rows[key]
				if(existing === undefined) {
					self.insert(row)
				} else {
					self.updateForKeys(existing, row, keyGen(existing), key)
				}
			}
		}
		self.update = function(last, next) {
			self.updateForKeys(last, next, checkKeyGen(last), checkKeyGen(next))
		}
		self.remove = function(row) {
			if(!self.removeIfExists(row)) {
				throw "Tried to remove a non-existent row with key: " + keyGen(row);
			}
		}
		self.removeIfExists = function(row) {
			return self.removeKeyIfExists(keyGen(row))
		}
		self.removeKey = function(key) {
			self.remove(rows[key])
		}
		self.removeKeyIfExists = function(key) {
			var row = rows[key]
			if(key !== undefined && row !== undefined) {
				delete rows[key]
				rowCount--
				self.signalRemove(row)
				return true
			}
			return false
		}
		self.pub.getRowCount = function() { return rowCount }

		self.pub.rebroadcastsTo = function(listener) {
			self.pub.forEach(function(row) { listener.sourceInsert(self, row) })
			self.pub.broadcastTo(listener)
		}

		self.rollback = function() {
		}

		self.pub.exists = function(key) {
			return rows[key] !== undefined
		}

		return self
	}

	var relationKeyGen = function(rel) { return rel.pub.id }
	var relations = simpleRelation(relationKeyGen)
	var tables = simpleRelation(relationKeyGen)
	var forTables = function(op) {
		var ts = tables.getRows()
		for(k in ts) if(ts.hasOwnProperty(k)) {
			op(ts[k])
		}
	}

	var relation = function(keyGen, keyCompare) {
		var self = simpleRelation(keyGen, keyCompare)
		self.pub.id = lastId++

		// Public functions:
		self.pub.group = function(groupGen) {
			return group(self, groupGen)
		}
		self.pub.map = function(rowMapper) {
			return Relate.Map([self], rowMapper)
		}
		self.pub.join = function(sources, keyGen, requires) {
			return Relate.Join([self].concat(sources), keyGen, requires)
		}
		self.pub.sum = function(selector) {
			return Relate.Sum([self], selector)
		}
		self.pub.mean = function(selector) {
			return Relate.Mean([self], selector)	
		}
		self.pub.sort = function(comparer) {
			return sort(self, comparer)
		}
		self.pub.order = function() {
			return order(self)
		}
		self.pub.keyTrigger = function() {
			return Relate.KeyTrigger(self)
		}
		self.pub.filter = function(f) {
			return Relate.Filter(f)
		}

		relations.insert(self)
		return self
	}

	relate.Table = function(keyGen) {
		var self = relation(keyGen ? keyGen : idGen)

		self.pub.insert 					 = function(rows) { rows.forEach(self.insert)    }
		self.pub.insertIfNotExists = function(rows) { rows.forEach(self.insertIfNotExists) }
		self.pub.remove						 = function(keys) { keys.forEach(self.removeKey) }
		self.pub.update						 = function(rows) { rows.forEach(self.update)    }
		self.pub.upsert						 = function(rows) { rows.forEach(self.upsert)    }
		self.pub.clear 						 = function() {
			for(k in self.rows) if(self.rows.hasOwnProperty(k)) {
				self.removeKey(k)
			}
		}

		tables.insert(self)
		return self.pub
	}

	var derived = function(self, sources, sourceInsert, sourceUpdate, sourceRemove) {
		var i = sources.length
		while(--i >= 0) {
			sources[i] = sources[i].pub ? sources[i] : relations.get(sources[i].id)
		}
		self.sources = sources

		relate.listener(sourceInsert, sourceUpdate, sourceRemove, self)

		var superDrop
		if(self.pub.drop) {
			superDrop = self.pub.drop
		}
		self.pub.drop = function() {
			var p = sources.length
			while(--p >= 0) {
				sources[p].stopBroadcastingTo(self)
			}
			if(superDrop)
				superDrop()
		}

		var p = sources.length
		while(--p >= 0) {
			sources[p].pub.rebroadcastsTo(self)
		}

		return self
	}

	var derivedRelation = function(sources, keyGen, sourceInsert, sourceUpdate, sourceRemove, keyCompare) {
		return derived(relation(keyGen, keyCompare), sources, sourceInsert, sourceUpdate, sourceRemove)
	}

	var triggerGrouper = function(row) {
		return row.key
	}
	var triggerKeyGen = function(row) {
		return [row.key, row.name]
	}

	relate.KeyTrigger = function(base) {
		var handlers = relation(triggerKeyGen, arrayKeyCompare)
		var pub = {}
		var keyGroups = group(handlers, triggerGrouper)

		pub.listen = function(key, name, handler) {
			handlers.insert({ key: key, name: name, handler: handler })
			var row = base.get(key)
			if(row !== undefined) {
				handler(undefined, row, base)
			}
		}
		pub.unlisten = function(key, name) {
			handlers.removeKey([key, name])
		}

		var sourceChange = function(table, key, last, next) {
			var rows = keyGroups.getGroup(key).pub.getRows()
			for(k in rows) if(rows.hasOwnProperty(k)) {
				rows[k].handler(last, next, table)
			}
		}
		var sourceInsert = function(table, row) {
			sourceChange(table, table.keyGen(row), undefined, row)
		}
		var sourceUpdate = function(table, last, next) {
			var lastKey = table.keyGen(last)
			var nextKey = table.keyGen(next)
			if(table.keyCompare(lastKey, nextKey)) {
				sourceChange(table, lastKey, last, next)
			} else {
				sourceChange(table, lastKey, last, undefined)
				sourceChange(table, nextKey, undefined, next)
			}
		}
		var sourceRemove = function(table, row) {
			sourceChange(table, table.keyGen(row), row, undefined)
		}

		return derived({ pub: pub }, [base], sourceInsert, sourceUpdate, sourceRemove).pub
	}

	relate.Scalars = function() {
		var scalars = relation(scalarKeyGen)
		var pub = relate.KeyTrigger(scalars)
		pub.set = function(key, value) {
			scalars.upsert({key: key, value: value})
		}
		pub.clear = function(key) {
			scalars.removeKeyIfExists(key)
		}
		pub.get = function(key) {
			return scalars.get(key).value
		}
		var trigListen = pub.listen
		pub.listen = function(key, name, handler) {
			trigHandler = function(last, next, table) {
				return handler(next ? next.value : undefined, last ? last.value : undefined)
			}
			trigListen(key, name, trigHandler)
		}

		return pub
	}

	relate.Map = function(bases, mapper, keyGen) {		
		keyGen = keyGen ? keyGen : bases[0].keyGen

		var sourceInsert = function(table, ins) {
			var mapped = mapper(ins, table)
			if(mapped !== undefined)
				self.insert(mapper(ins, table))
		}
		var sourceUpdate = function(table, last, next) {
			var lastMapped = mapper(last, table)
			var nextMapped = mapper(next, table)
			if(lastMapped === undefined) {
				if(nextMapped !== undefined) {
					self.insert(nextMapped)
				}
			} else {
				if(nextMapped === undefined) {
					self.remove(nextMapped)
				} else {
					self.update(lastMapped, nextMapped)
				}
			}
		}
		var sourceRemove = function(table, rem) {
			var mapped = mapper(rem, table)
			if(mapped !== undefined)
				self.remove(mapped)
		}

		var self = derivedRelation(bases, keyGen, sourceInsert, sourceUpdate, sourceRemove)
		return self.pub
	}

	relate.Filter = function(base, filterer) {
		return relate.Map([base], function(row, table) { return filterer(row, table) ? row : undefined }, base.keyGen)
	}

	var group = function(base, grouper) {
		var groups = {}
		var self
		
		var getGroup = function(gkey) {
			var grp = groups[gkey]
			if(grp === undefined) {
				grp = derivedRelation([self], base.keyGen)
				groups[gkey] = grp
			}
			return grp
		}
		var getGroupFor = function(row) {
			return getGroup(grouper(row))
		}		

		var sourceInsert = function(table, row) {
			var gkey = grouper(row, table)
			getGroup(gkey).insert(row)
			self.signalInsert(row)
		}
		var sourceUpdate = function(table, last, next) {
			lastKey = grouper(last, table)
			nextKey = grouper(next, table)
			var lastGroup = getGroup(lastKey)
			if(lastKey === nextKey) {
				lastGroup.updateForKeys(last, next, lastKey, nextKey)
			} else {
				lastGroup.remove(last)
				getGroup(nextKey).insert(next)
			}
			self.signalUpdate(last, next)
		}
		var sourceRemove = function(table, row) {
			var gkey = grouper(row, table)
			var grp = getGroup(gkey)
			grp.remove(row)
			self.signalRemove(row)
		}

		self = derivedRelation([base], base.keyGen, sourceInsert, sourceUpdate, sourceRemove)
		self.pub.groupKeyGen = grouper
		self.pub.getGroup = getGroup
		self.pub.getGroupFor = getGroupFor
		return self.pub
	}

	relate.Aggregate = function(bases, initial, apply, unapply) {
		var value = initial
		var self = broadcasts()
		self.pub = {}

		var update = function(newValue) {
			if(value !== newValue) {
				value = newValue
				forEach(self.listeners, function(l) {
					l.signalUpdate(value, newValue)
				})
			}			
		}

		var sourceInsert = function(table, row) {
			update(apply(value, row, table))
		}
		var sourceUpdate = function(table, last, next) {
			update(apply(unapply(value, last, table), next, table))
		}
		var sourceRemove = function(table, row) {
			update(unapply(value, row, table))
		}

		var self = derived(self, bases, sourceInsert, sourceUpdate, sourceRemove)

		return function() { return value }
	}
	relate.Sum = function(bases, selector) {
		var add = function(total, row, table) {
			return total + selector(row, table)
		}
		var sub = function(total, row, table) {
			return total - selector(row, table)
		}
		return relate.Aggregate(bases, 0, add, sub)
	}
	relate.Mean = function(bases, selector) {
		var total = 0
		var size = 0
		forEach(bases, function(b) {
			size = size + b.pub.getRowCount()
		})
		var add = function(lastMean, row, table) {
			total = total + selector(row, table)
			size++
			return total / size
		}
		var sub = function(lastMean, row, table) {
			total = total - selector(row, table)
			size--
			return total / size
		}
		return relate.Aggregate(bases, 0, add, sub)	
	}

	var sort = function(relation, comparer) {
		var data = []
		var indices = {}
		comparer = comparer ? comparer : zero
		var setIndex = function(index) {
			indices[relation.keyGen(data[index])] = index
		}
		var indexSearch = function(row, start, end) {
			var length = end - start + 1
			var at = start + Math.floor(length / 2)
			if(length <= 0) {
				return at
			}
			var result = comparer(row, data[at])
			if(result === 0) {
				return at
			}
			else if(result < 0) {
				return indexSearch(row, start, at - 1)
			} else {
				return indexSearch(row, at + 1, end)
			}
		}
		var insertIndex = function(row) {
			return indexSearch(row, 0, data.length - 1)
		}
		var indexOf = function(row) {
			return indices[relation.keyGen(row)]
		}
		var sourceInsert = function(table, row) {
			var index = insertIndex(row)
			data.splice(index, 0, row)

			var i = data.length
			while(--i >= index) {
				setIndex(i)
			}

			self.signalInsert(data[i])
			i = data.length - 1
			while(--i >= index) {
				self.signalUpdate(data[i+1], data[i])
			}
		}
		var sourceRemove = function(table, row) {
			var index = indexOf(row)
			data.splice(index, 1)
			delete indices[index]
			self.signalRemove(row)

			var i = data.length
			while(--i >= index) {
				self.signalUpdate(data[i], data[i-1])
			}
		}
		var sourceUpdate = function(table, last, next) {
			var lastIndex = indexOf(last)
			if(comparer(last, next) === 0) {
				data[lastIndex] = next
			} else {					
				data.splice(lastIndex, 1)
				var nextIndex = insertIndex(next)
				data.splice(nextIndex, 0, next)
				setIndex(nextIndex)
			}
			self.signalUpdate(last, next)
		}

		var self = derivedRelation([relation], relation.keyGen, sourceInsert, sourceUpdate, sourceRemove)

		self.pub.indexOf = indexOf
		self.pub.getData = function() {
			return data
		}
		self.pub.resort = function(newComparer) {
			var newData = data.slice(0)
			newData.sort(newComparer)
			var i = data.length
			while(--i >= 0) {
				if(data[i] !== newData[i]) {
					self.signalUpdate(data[i], newData[i])
				}
			}
			data = newData
			comparer = newComparer
		}
		self.pub.reverse = function() {
			data.reverse()

			var i = data.length
			while(--i >= 0) {
				self.signalUpdate(data[data.length - i], data[i])
			}

			if(comparer.original) {
				comparer = comparer.original
			} else {
				var original = comparer
				comparer = function(a,b) {
					return original(b,a)
				}
				comparer.original = original
			}
		}

		return self.pub
	}

	var order = function(base) {
		var head = undefined
		var tail = head
		var orders = {}

		var sourceInsert = function(table, sourceRow) {
			row = {row: sourceRow, succ: undefined}
			if(head === undefined) {
				row.pred = undefined
				head = row					
			} else {
				row.pred = tail
				tail.succ = row
			}
			tail = row
			orders[table.keyGen(sourceRow)] = row

			self.signalInsert(tail)
		}
		var sourceUpdate = function(table, sourceLast, sourceNext) {
			var last = orders[table.keyGen(sourceLast)]
			var next = { pred: last.pred, row: sourceNext, succ: last.succ }
			self.signalUpdate(last, next)
			last.row = sourceNext
		}
		var remove = function(pos) {
			if(pos === head) {
				head = pos.succ
			} else {
				pos.pred.succ = pos.succ
			}
			if(pos === tail) {
				tail = pos.pred
			} else {
				pos.succ.pred = pos.pred
			}
		}
		var sourceRemove = function(table, row) {
			var key = table.keyGen(row)
			row = orders[key]
			remove(row)
			delete orders[key]
			self.signalRemove(row)
		}

		var self = derivedRelation([base], base.keyGen, sourceInsert, sourceUpdate, sourceRemove, base.keyCompare)

		var bound = function(i) {
			return (i < 0) ? 0 : ((i >= orders.length) ? orders.length - 1 : i)
		}
		var order = function(row) {
			return (row === undefined) ? undefined : orders[base.keyGen(row)]
		}
		self.pub.swap = function(a, b) {
			var oa = order(a)
			var ob = order(b)
			if(oa !== undefined && ob !== undefined) {
				oa.row = b
				ob.row = a
				orders[base.keyGen(a)] = ob
				orders[base.keyGen(b)] = oa
				self.signalUpdate(oa)
				self.signalUpdate(ob)
			}
		}			
		var setPos = function(pos, at) {
			var pred = (at === undefined) ? tail : at.pred
			var succ = at

			pos.pred = pred
			pos.succ = succ

			if(pred !== undefined) {
				pred.succ = pos
			} else {
				head = pos
			}
			if(succ !== undefined) {
				succ.pred = pos
			} else {
				tail = pos
			}
		}
		self.pub.move = function(from, to) {
			if(from === to) return;
			
			var fromKey = base.keyGen(from)
			var fromOrder = order(from)
			var toOrder = order(to)

			if(fromOrder === undefined) return;
			
			remove(fromOrder)
			setPos(fromOrder, toOrder)
		}
		self.pub.toArray = function() {
			var i = head
			var res = []
			while(i !== undefined) {
				res[res.length] = i.row
				i = i.succ
			}
			return res
		}

		self.pub.head = function() { return head }
		self.pub.tail = function() { return tail }

		return self.pub
	}

	relate.Join = function(sources, required) {
		var self = {}

		if(!required)
			required = {}

		var keyGen = function(join) {
			var key = []
			var i = sources.length
			while(--i >= 0) {
				key[i] = (join[i] !== undefined) ? sources[i].keyGen(join[i]) : undefined
			}
			return key
		}

		var rowsFor = function(table, joinOn) {
			if(table.pub.getGroupFor)
				return table.pub.getGroup(joinOn).pub.toArray()
				else {
					var row = table.rows[joinOn]
					return (row !== undefined) ? [row] : []
				}
		}
		var hasRowsFor = function(table, joinOn) {
			return table.pub.getGroupFor ? (table.pub.getGroup(joinOn).pub.getRowCount() > 0) : (table.rows[joinOn] !== undefined)
		}
		var keyFor = function(table, row) {
			return table.pub.groupKeyGen ? table.pub.groupKeyGen(row) : table.keyGen(row)
		}
		var joinsFor = function(joinOn, table, row) {
			var rows = []
			var i = sources.length
			var empty = true
			while(--i >= 0) {					
				var rel = sources[i]
				var res = (rel === table) ? [row] : rowsFor(rel, joinOn)
				if(res.length === 0) {
					res = required[i] ? [] : [undefined]
				}
				if(res.legnth > 0 || res[0] !== undefined) {
					empty = false
				}
				rows[i] = res
			}
			return empty ? [] : cartProd(rows)
		}
		var sourceInsert = function(table, row) {
			var joinOn = keyFor(table, row)
			var sourceIndex = sources.indexOf(table)

			rows = joinsFor(joinOn, table, row)
			i = rows.length
			while(--i >= 0) {					
				var row = rows[i]

				var key = keyGen(row)					
				key[sourceIndex] = undefined
				self.removeKeyIfExists(key)

				self.insert(row)
			}
		}
		var sourceUpdate = function(table, last, next) {
			var lastJoinOn = keyFor(table, last)
			var nextJoinOn = keyFor(table, next)

			if(lastJoinOn !== nextJoinOn) {
				sourceRemove(table, last)
				sourceInsert(table, next)
			} else {
				var nexts = joinsFor(nextJoinOn, table, next)
				var lasts = joinsFor(lastJoinOn, table, next)
				i = nexts.length
				while(--i >= 0) {
					self.update(lasts[i], nexts[i])
				}
			}
		}
		var sourceRemove = function(table, row) {
			var sourceIndex = sources.indexOf(table)
			var joinOn = keyFor(table, row)
			var joins = joinsFor(joinOn, table, row)
			var i = joins.length
			while(--i >= 0) {
				var join = joins[i]
				self.remove(join)
				join[sourceIndex] = undefined
				var j = sources.length
				if(!hasRowsFor(sources[sourceIndex], joinOn) && !required[sourceIndex]) {
					self.insert(join)
				}
			}
		}

		self = derivedRelation(sources, keyGen, sourceInsert, sourceUpdate, sourceRemove, arrayKeyCompare)
		return self.pub
	}

	return relate
}

if(typeof define === 'function' && define.amd) {
	define([], factory)
} else {
	Relate = factory()
}