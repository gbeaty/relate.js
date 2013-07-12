Relate = function() {
	var relate = {}

	var identity = function(a) { return a }
	var noop = function() {}
	var INSERT = 1
	var REMOVE = 2
	var UPDATE = 3

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

		self.broadcast

		return self
	}

	relate.db = function() {
		var db = {}
		var tables = []
		var lastId = 0

		db.commit = function() {
			var i = tables.length
			while(--i >= 0) {
				tables[i].commit()
			}
		}

		var simpleRelation = function(keyGen) {
			var self = {}
			self.pub = broadcasts()
			self.keyGen = keyGen		
			var rows = self.rows = {}	

			var dirty = false
			self.dirty = function() { dirty = true }

			self.get = function(key) { return self.rows[key] }

			self.pub.getRows = function() { return rows }

			self.pub.drop = function() {
				var l = self.listeners.length
				while(--l >= 0) {
					self.listeners[l].drop()
				}
				self.listeners = null
				self.addToListeners = null
			}

			var checkKeyGen = function(row) {
				var key = keyGen(row)
				if(key === undefined || key === null)
					throw "Cannot generate key for row: " + row;
				return key
			}

			var changes = {}
			var changesArray = []
			var change = function(key) {
				var c = changes[key]
				if(c === undefined) {
					c = { row: rows[key] }
					changes[key] = c
					changesArray.push(c)
				}
				return c
			}

			self.insert = function(row) {
				var key = checkKeyGen(row)
				if(key !== undefined) {
					var c = change(key)
					if(c.row === undefined) {
						c.row = row
						c.type = INSERT
					} else throw "Primary key constraint violation for key: " + key
				}
			}
			var update = function(c, last, next) {
				if(c.type === undefined) {
					c.type = UPDATE
					c.last = last
					c.row = next
				} else if(c.type === UPDATE) {
					c.row = next
				} else if(c.type === INSERT) {
					c.row = next
				} else if(c.type === REMOVE) {
					throw "Cannot update removed row: " + last
				}
			}
			self.upsert = function(row) {
				var key = checkKeyGen(row)
				if(key !== undefined) {
					var c = change(key)
					if(c.type === REMOVE) {
						c.type = UPDATE
						c.last = c.row
						c.row = row
					} else if(c.row !== undefined) {
						update(c, c.row, row)
					} else {
						c.row = row
						c.type = INSERT
					}
				}
			}
			self.update = function(last, next) {
				var lastKey = checkKeyGen(last)
				var nextKey = checkKeyGen(next)
				if(nextKey !== undefined) {
					if(lastKey === nextKey) {
						update(change(nextKey), last, next)
					} else {
						self.remove(last)
						self.insert(next)
					}
				} else throw "Row '" + last + "' not found. Cannot update."
			}
			self.remove = function(row) {
				var key = checkKeyGen(row)
				var c = change(key)
				if(c.row === undefined)
					throw "Tried to remove a non-existent row on key: " + key;
				c.row = row
				c.type = REMOVE
			}
			self.removeKey = function(key) {
				self.remove(rows[key])
			}
			self.propagateChanges = function(from, sourceChanges) {
				var i = sourceChanges.length
				while(--i >= 0) {
					var c = sourceChanges[i]
					switch(c.type) {
						case UPDATE: self.sourceUpdate(from, c.last, c.row); break
						case INSERT: self.sourceInsert(from, c.row); break
						case REMOVE: self.sourceRemove(from, c.row); break
					}
				}
				if(changesArray.length > 0 || dirty) {
					self.pub.forListeners(function(l) { l.propagateChanges(self, changesArray) })
				}
			}
			var applyChanges = function(toApply) {
				var i = toApply.length
				while(--i >= 0) {				
					var c = toApply[i]
					var k = keyGen(c.row)
					if(c.type === REMOVE) {
						delete rows[k]
					} else {
						rows[k] = c.row
					}
				}
			}
			self.commit = function() {
				if(changesArray.length > 0 || dirty) {
					applyChanges(changesArray)
					self.pub.forListeners(function(l) { l.commit() })

					changes = {}
					changesArray = []
					dirty = false
				}
			}
			self.rollback = function() {			
				var i = self.listeners.length
				while(--i >= 0) {
					self.listeners[i].rollback()
				}
				changes = {}
				changesArray = []
				dirty = false
			}

			return self
		}

		var relations = simpleRelation(function(rel) { return rel.pub.id })

		var relation = function(keyGen) {
			var self = simpleRelation(keyGen)
			self.pub.id = lastId++			

			// Public functions:
			self.pub.group = function(groupGen) {
				return group(self, groupGen)
			}
			self.pub.map = function(rowMapper) {
				return db.Map([self], rowMapper)
			}
			self.pub.outerJoin = function(rel, mapper) {
				return db.Join(self, relations.get(rel.id), mapper)
			}
			self.pub.count = function(counter) {
				return db.Count([self], counter)
			}
			self.pub.sort = function(comparer) {
				return sort(self, comparer)
			}

			relations.insert(self)
			relations.commit()
			return self
		}

		db.table = function(keyGen) {
			var self = relation(keyGen)

			self.sourceInsert = function(from, row) { self.insert(row) }
			self.sourceUpdate = function(from, row) { self.update(row) }
			self.sourceRemove = function(from, row) { self.remove(row) }
			var change = function(rows, op) {
				var i = rows.length
				while(--i >= 0) {
					op(rows[i])
				}
				self.propagateChanges(self, {})
				self.commit()
			}
			self.pub.insert = function(rows) { change(rows, self.insert)    }
			self.pub.remove = function(keys) { change(keys, self.removeKey) }
			self.pub.update = function(rows) { change(rows, self.update)    }
			self.pub.upsert = function(rows) { change(rows, self.upsert)    }

			return self.pub
		}

		var derived = function(sources, keyGen, sourceInsert, sourceUpdate, sourceRemove) {
			var self = relation(keyGen)
			var sup = {}
			self.sources = sources

			self.sourceInsert = sourceInsert ? sourceInsert : noop
			self.sourceUpdate = sourceUpdate ? sourceUpdate : noop
			self.sourceRemove = sourceRemove ? sourceRemove : noop

			sup.drop = self.pub.drop
			self.pub.drop = function() {
				var p = sources.length
				while(--p >= 0) {
					sources[p].removeFromListeners(self)
				}
				sup.drop()
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

			var self = derived(bases, keyGen, sourceInsert, sourceUpdate, sourceRemove)
			return self.pub
		}

		var filter = function(base, filterer) {
			return relate.Map([base], function(row, table) { return filterer(row, table) ? row : undefined }, base.keyGen)
		}

		var group = function(base, grouper) {
			var groups = {}
			var self
			
			var getGroup = function(gkey) {
				var grp = groups[gkey]
				if(grp === undefined) {
					grp = derived([self], base.keyGen)
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
				self.dirty()
			}
			var sourceUpdate = function(table, last, next) {
				lastKey = grouper(last, table)
				nextKey = grouper(next, table)
				var lastGroup = getGroup(lastKey)
				if(lastKey === nextKey) {
					lastGroup.update(last, next)
				} else {
					lastGroup.remove(last)
					getGroup(nextKey).insert(next)
				}
				self.dirty()
			}
			var sourceRemove = function(table, row) {
				var gkey = grouper(row, table)
				var grp = getGroup(gkey)
				grp.remove(row)
				self.dirty()
			}

			self = derived([base], base.keyGen, sourceInsert, sourceUpdate, sourceRemove)
			self.pub.groupKeyGen = grouper
			self.pub.getGroup = getGroup
			self.pub.getGroupFor = getGroupFor			
			return self.pub
		}

		db.Aggregate = function(bases, initial, apply, unapply) {		
			var total = initial

			var sourceInsert = function(table, row) {
				total = apply(total, row, table)
			}
			var sourceUpdate = function(table, last, next) {
				total = apply(unapply(total, last, table), next, table)
			}
			var sourceRemove = function(table, row) {
				total = unapply(total, row, table)
			}

			var self = derived(bases, undefined, sourceInsert, sourceUpdate, sourceRemove)
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

			var self = derived([relation], relation.keyGen, sourceInsert, sourceUpdate, sourceRemove)
			self.pub.getData = function() {
				resort()
				return data
			}
			return self.pub
		}

		db.LeftOuterJoin = function(left, right, joinMapper) {
			return relate.Join(left, right, function(l,r) {
				if(left !== undefined) {
					return joinMapper(l,r)
				}
			})
		}

		db.RightOuterJoin = function(left, right, joinMapper) {
			return relate.Join(left, right, function(l,r) {
				if(right !== undefined) {
					return joinMapper(l,r)
				}
			})
		}

		db.Join = function(left, right, joinMapper) {
			var self

			var keyGen = function(joinedRow) {
				var key = []
				var toKey = function(rel, index) {
					return (joinedRow[index] !== undefined) ? rel.keyGen(joinedRow[index]) : undefined
				}
				key[0] = toKey(left, 0)
				key[1] = toKey(right, 1)
				return key
			}

			var joinRow = function(table, row) {
				var otherTable = (table === left) ? right : left
				var joiner = (table === left) ? function(right) { return [row, right] } : function(left) { return [left, row] }
				var thisKeyGen = (table.groupKeyGen) ? table.groupKeyGen : table.keyGen
				var changes = []

				var changeTuple = function(thisRow, otherRow) {
					self.remove(joiner(undefined, otherRow))
					self.remove(joiner(thisRow, undefined))
					self.insert(joiner(thisRow, otherRow))			
				}
				
				var v
				var hasJoin = false
				if(otherTable.getGroup) {
					var group = otherTable.getGroupFor(row)
					var rows = group.rows				
					for(k in rows) if(rows.hasOwnProperty(k)) {
						changes.push(joiner(rows[k]))
						hasJoin = true
					}
				} else {
					v = otherTable.get(thisKeyGen(row))
					if(v !== undefined) {
						changes.push(joiner(v))
						hasJoin = true
					}
				}
				if(!hasJoin) {
					changes.push(joiner(undefined))
				}

				return changes
			}
			var sourceInsert = function(table, row) {
				console.log(row)
				var changes = joinRow(table, row)
				var i = changes.length
				while(--i >= 0) {
					var change = changes[i]
					if(joinMapper) {
						change = joinMapper(change)
					}
					self.insert(change)
				}
			}
			var sourceUpdate = function(table, last, next) {
				//console.log(joinRow(table, last))
				//console.log(joinRow(table, next))
			}
			var sourceRemove = function(table, row) {
				//var key = table.keyGen(row)
			}

			self = derived([left,right], keyGen, sourceInsert, sourceUpdate, sourceRemove)
			return self.pub
		}

		return db
	}

	return relate
}()