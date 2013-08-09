var factory = function() {
	var relate = {}

	var identity = function(a) { return a }
	var noop = function() {}
	var noobj = {}
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
		var listeners = []

		self.addToListeners = function(rel) {
			if(listeners.indexOf(rel) === -1)
				listeners.push(rel)
		}
		self.removeFromListeners = function(rel) {
			listeners = listeners.filter(function(r) { return r !== rel })
		}
		self.forListeners = function(op) {
			var i = listeners.length
			while(--i >= 0) {				
				op(listeners[i])
			}
		}

		return self
	}

	relate.db = function() {
		var db = {}
		var lastId = 0
		var inTransaction = false

		db.transaction = function(trans) {
			var result = null
			inTransaction = true
			try {
				trans()
			} catch(err) {
				result = err
				forTables(function(t) {

				})
			}
			inTransaction = false
			return result
		}

		var simpleRelation = function(keyGen, keyCompare) {
			var self = {}
			self.pub = broadcasts()
			self.keyGen = keyGen
			var rows = self.rows = {}
			var rowCount = 0

			if(!keyCompare)
				keyCompare = scalarKeyCompare
			self.keyCompare = keyCompare

			self.get = function(key) { return self.rows[key] }

			self.pub.getRows = function() { return rows }
			self.pub.get = function(key) { return rows[key] }
			self.toArray = function() {
				var result = []
				for(k in rows) if(rows.hasOwnProperty(k)) {
					result.push(rows[k])
				}
				return result
			}

			self.pub.drop = function() {
				var l = self.listeners.length				
				while(--l >= 0) {
					self.listeners[l].drop()
				}
				delete self.listeners
				delete self.addToListeners
			}

			var checkKeyGen = function(row) {
				var key = keyGen(row)
				if(key === undefined || key === null)
					throw "Cannot generate key for row: " + row;
				return key
			}

			self.signalInsert = function(row) {
				self.pub.forListeners(function(l) { l.sourceInsert(self, row) })
			}
			self.signalUpdate = function(last, next) {
				self.pub.forListeners(function(l) { l.sourceUpdate(self, last, next) })
			}
			self.signalRemove = function(row) {
				self.pub.forListeners(function(l) { l.sourceRemove(self, row) })
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
				return db.Map([self], rowMapper)
			}
			self.pub.join = function(sources, keyGen, requires) {
				return db.Join([self].concat(sources), keyGen, requires)
			}
			self.pub.count = function(counter) {
				return db.Count([self], counter)
			}
			self.pub.sort = function(comparer) {
				return sort(self, comparer)
			}
			self.pub.keyTrigger = function() {
				return db.KeyTrigger(self)
			}
			self.pub.filter = function(f) {
				return db.Filter(f)
			}

			relations.insert(self)
			return self
		}

		db.Table = function(keyGen) {
			var self = relation(keyGen ? keyGen : idGen)

			var change = function(rows, op) {
				var i = rows.length
				while(--i >= 0) {
					op(rows[i])
				}
			}
			self.pub.insert = function(rows) { change(rows, self.insert)    }
			self.pub.remove = function(keys) { change(keys, self.removeKey) }
			self.pub.update = function(rows) { change(rows, self.update)    }
			self.pub.upsert = function(rows) { change(rows, self.upsert)    }

			tables.insert(self)
			return self.pub
		}

		var derived = function(self, sources, sourceInsert, sourceUpdate, sourceRemove) {
			var i = sources.length
			while(--i >= 0) {
				sources[i] = sources[i].pub ? sources[i] : relations.get(sources[i].id)
			}
			self.sources = sources

			self.sourceInsert = sourceInsert ? sourceInsert : noop
			self.sourceUpdate = sourceUpdate ? sourceUpdate : noop
			self.sourceRemove = sourceRemove ? sourceRemove : noop

			var superDrop
			if(self.pub.drop) {
				superDrop = self.pub.drop
			}
			self.pub.drop = function() {
				var p = sources.length
				while(--p >= 0) {
					sources[p].removeFromListeners(self)
				}
				if(superDrop)
					superDrop()
			}

			var p = sources.length
			while(--p >= 0) {
				var parent = sources[p]
				var parentRows = parent.rows
				for(r in parentRows) if(parentRows.hasOwnProperty(r)) {
					sourceInsert(parent, parentRows[r])
				}
				parent.pub.addToListeners(self)
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
		db.KeyTrigger = function(base) {
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
			pub.htmlListen = function(key, elId, formatter) {
				var element = document.getElementById(elId)
				
				if(!formatter)
					formatter = function(value) { return value.toString() }

				var handler = function(last, next, table) {
					if(!element)
						element = document.getElementById(elId)

					if(element)
						element.innerHTML = formatter(next)
				}
				pub.listen(key, elId, handler)
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
		db.Scalars = function() {
			var scalars = relation(scalarKeyGen)
			var pub = db.KeyTrigger(scalars)
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

		db.Map = function(bases, mapper, keyGen) {		
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

		db.Filter = function(base, filterer) {
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

		db.Aggregate = function(bases, initial, apply, unapply) {		
			var total = initial
			var self = noobj

			var update = function(newTotal) {
				total = newTotal
				self.pub.forListeners(function(l) {
					l.signalUpdate(total, newTotal)
				})
			}

			var sourceInsert = function(table, row) {
				update(apply(total, row, table))
			}
			var sourceUpdate = function(table, last, next) {
				update(apply(unapply(total, last, table), next, table))
			}
			var sourceRemove = function(table, row) {
				update(unapply(total, row, table))
			}

			var self = derivedRelation(bases, undefined, sourceInsert, sourceUpdate, sourceRemove)
			return function() { return total }
		}

		db.Sum = function(bases, apply) {
			var add = function(total, row, table) {
				return total + apply(row, table)
			}
			var sub = function(total, row, table) {
				return total - apply(row, table)
			}
			return db.Aggregate(bases, 0, add, sub)
		}

		db.Count = function(bases, apply) {
			var count = function(row, table) {
				return apply(row, table) ? 1 : 0
			}
			return db.Sum(bases, count)
		}

		var sort = function(relation, comparer) {		
			var needsResort = false
			var data = []
			var indices = {}
			var removedIndices = []
			var keyGen = relation.keyGen

			var indexOf = function(row) {
				var key = keyGen(row)
				var index = indices[key]
				var i = removedIndices.length
				while(--i >= 0) {
					var ri = removedIndices[i]
					if(index >= ri.index) {
						index -= ri.removed
					}
				}
				return index
			}

			var resort = function() {
				if(needsResort) {
					data.sort(comparer)
					
					indices = {}
					removedIndices = []
					var i = data.length
					while(--i >= 0) {
						var row = data[i]
						indices[keyGen(row)] = i
					}

					needsResort = false
				}
			}

			var flagResort = function(index) {
				var prev = data[index-1]
				var row = data[index]
				var next = data[index+1]
				var result = (prev !== undefined && comparer(prev, row) >= 0) || (next !== undefined && comparer(row, next) >= 0)
				if(result) {
					needsResort = true
				}
				return result
			}

			var sourceInsert = function(table, row) {
				var index = data.length
				data.push(row)
				indices[keyGen(row)] = index
				flagResort(index)
			}
			var sourceUpdate = function(table, last, next) {
				var index = indexOf(last)
				data[index] = next
				flagResort(index)
			}
			var sourceRemove = function(table, row) {
				var i = indexOf(row)
				data.splice(i,1)
				removedIndices.push({ index: i, removed: 1 })
			}

			var self = derivedRelation([relation], relation.keyGen, sourceInsert, sourceUpdate, sourceRemove)
			self.pub.getData = function() {
				resort()
				return data
			}
			return self.pub
		}

		db.Join = function(sources, required) {
			var self = noobj

			if(!required)
				required = noobj

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
					return table.pub.getGroup(joinOn).toArray()
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

		return db
	}
	return relate
}

if(typeof define === 'function' && define.amd) {
	define([], factory)
} else {
	Relate = factory()
}