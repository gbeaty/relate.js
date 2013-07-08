Relate = function() {
	var relate = {}

	var identity = function(a) { return a }
	var zero = function() { return 0 }

	var relation = function(keyGen) {
		var self = {}
		self.keyGen = keyGen
		self.listeners = []
		self.unique = true
		var rows = self.rows = {}

		self.get = function(key) { return self.rows[key] }

		self.toArray = function() {
			var result = []
			var rows = self.rows
			for(key in rows) if(rows.hasOwnProperty(key)) {
				result.push(rows[key])
			}
			return result
		}

		self.drop = function() {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].drop()
			}
			self.listeners = null
			self.addToListeners = null
		}

		self.update = function(last, next) {
			if(last !== undefined && next !== undefined) {
				var lastKey = self.keyGen(last)
				var nextKey = self.keyGen(next)
				if(nextKey !== undefined) {
					if(lastKey === nextKey) {
						rows[lastKey] = next
						self.signalUpdate(last, next)
						return true
					} else {
						delete rows[lastKey]
						rows[nextKey] = next
						self.signalRemove(last)
						self.signalInsert(next)
						return true
					}
				}
			}
			return false
		}
		self.insert = function(row) {
			if(row !== undefined) {
				var key = self.keyGen(row)
				if(key !== undefined) {
						if(rows[key] === undefined) {
						rows[key] = row
						self.signalInsert(row)
						return true
					} else throw "Primary key constraint violation."
				}
			}
			return false
		}
		self.remove = function(row) {
			if(row !== undefined) {
				var key = self.keyGen(row)
				if(key !== undefined && rows[key] !== undefined) {
					delete rows[key]
					self.signalRemove(row)
					return true
				}
				return false
			}
		}
		self.upsert = function(row) {
			if(row !== undefined) {
				var key = self.keyGen(row)
				if(key !== undefined) {
					var inserted = rows[key] === undefined
					rows[key] = row
					if(inserted)
						self.signalInsert(row)
						else self.signalUpdate(row)
				}
			}
		}

		self.signalUpdate = function(last, next) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].handleUpdate(self, last, next)
			}
		}
		self.signalInsert = function(row) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].handleInsert(self, row)
			}
		}
		self.signalRemove = function(key) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].handleRemove(self, key)
			}
		}

		self.addToListeners = function(rel) {
			if(self.listeners.indexOf(rel) === -1)
				self.listeners.push(rel)
		}
		self.removeFromListeners = function(rel) {
			self.listeners = self.listeners.filter(function(r) { return r !== rel })
		}

		self.joinWith = function(key, otherKey, otherRow) {
			var result = []
			result[0] = self.get(key)
			result[1] = otherRow
			return [result]
		}

		// Public functions:
		self.group = function(groupGen) {
			var rel = group(self, groupGen)
			rel.load()
			return rel
		}
		self.map = function(rowMapper) {
			var rel = relate.Map([self], rowMapper)			
			rel.load()
			return rel
		}
		self.outerJoin = function(rel, mapper) {
			var rel = relate.OuterJoin(self, rel, mapper)
			rel.load()
			return rel
		}
		self.count = function(counter) {
			return relate.Count([self], counter)
		}
		self.sort = function(comparer) {
			var rel = sort(self, comparer)
			rel.load()
			return rel
		}

		return self
	}

	relate.Table = function(name, keyGen) {
		var self = relation(keyGen)
		self.name = name

		var insert = function(toInsert, upsert) {
			var inserts = []
			var updates = []
			var r = toInsert.length
			while(--r >= 0) {
				var row = toInsert[r]
				var pk = keyGen(row)
				if(pk !== undefined) {
					var oldRow = self.rows[pk]
					if(oldRow === undefined) {
						self.rows[pk] = row
						self.signalInsert(row)
						inserts.push(row)
					} else if(upsert) {
						self.rows[pk] = row
						self.signalUpdate(oldRow, row)
						updates.push({last: oldRow, next: row})
					}
				}
			}
			var results = {}
			if(updates.length)
				results.updates = updates
			if(inserts.length)
				results.inserts = inserts

			return results
		}
		self.insert = function(toInsert) { return insert(toInsert, false) }
		self.upsert = function(toUpsert) { return insert(toUpsert, true) }
		self.remove = function(pks) {
			var r = pks.length
			while(--r >= 0) {
				var pk = pks[r]
				self.signalRemove(self.rows[pk])
				delete self.rows[pk]
			}
		}

		return self
	}

	var derived = function(parents, keyGen) {
		var self = relation(keyGen)
		var sup = {}
		self.parents = parents

		self.load = function() {
			var p = parents.length
			while(--p >= 0) {
				var parent = parents[p]
				var rows = parent.toArray()
				var i = rows.length
				while(--i >= 0) {
					self.handleInsert(parent, rows[i])
				}
				parent.addToListeners(self)
			}
		}

		sup.drop = self.drop
		self.drop = function() {
			var p = parents.length
			while(--p >= 0) {
				parents[p].removeFromListeners(self)
			}
			sup.drop()
		}

		return self
	}

	relate.Map = function(bases, mapper, keyGen) {
		var self = derived(bases, keyGen)
		self.keyGen = keyGen = keyGen ? keyGen : bases[0].keyGen

		self.handleInsert = function(table, ins) {
			self.insert(mapper(ins, table))
		}
		self.handleUpdate = function(table, last, next) {
			self.update(mapper(last, table), mapper(next, table))
		}
		self.handleRemove = function(table, rem) {
			self.remove(mapper(rem, table))
		}

		return self
	}

	var filter = function(base, filterer) {
		return relate.Map([base], function(row, table) { return filterer(row, table) ? row : undefined }, base.keyGen)
	}

	var group = function(base, grouper) {
		var self = filter(base, function(row, table) { return grouper(row, table) === undefined }, base.keyGen)
		var groups = {}

		self.getGroup = function(gkey) {
			var grp = groups[gkey]
			if(grp === undefined) {
				grp = relation(base.keyGen)
				groups[gkey] = grp
			}
			return grp
		}
		self.getGroupFor = function(row) {
			return self.getGroup(grouper(row))
		}

		var filterInsert = self.handleInsert
		self.handleInsert = function(table, row) {
			filterInsert(table, row)
			var gkey = grouper(row, table)
			if(gkey !== undefined) {
				self.getGroup(gkey).insert(row)
				self.signalInsert(row)
			}
		}
		var filterUpdate = self.handleUpdate
		self.handleUpdate = function(table, last, next) {
			filterUpdate(table, last, next)
			lastKey = grouper(last, table)
			nextKey = grouper(next, table)
			var lastGroup = self.getGroup(lastKey)
			if(lastKey === nextKey && nextKey !== undefined) {
				lastGroup.update(last, next)
			} else {
				if(lastGroup !== undefined)
					lastGroup.remove(last)
				if(nextKey !== undefined) {
					self.getGroup(nextKey).insert(next)
				}
			}
			self.signalUpdate(row)
		}
		var filterRemove = self.handleRemove
		self.handleRemove = function(table, row) {
			filterRemove(table, row)
			var gkey = grouper(row, table)
			if(gkey !== undefined) {
				var grp = self.getGroup(gkey)
				grp.remove(row)
			}
			self.signalRemove(row)
		}
		return self
	}

	relate.Aggregate = function(bases, initial, apply, unapply) {
		var self = derived(bases)
		var total = initial

		self.handleInsert = function(table, row) {
			total = apply(total, row, table)
		}
		self.handleUpdate = function(table, last, next) {
			total = apply(unapply(total, last, table), next, table)
		}
		self.handleRemove = function(table, row) {
			total = unapply(total, row, table)
		}

		self.load()
		return function() { return total }
	}

	relate.Sum = function(bases, apply) {
		var add = function(total, row, table) {
			return total + apply(row, table)
		}
		var sub = function(total, row, table) {
			return total - apply(row, table)
		}
		return relate.Aggregate(bases, 0, add, sub)
	}

	relate.Count = function(bases, apply) {
		var count = function(row, table) {
			return apply(row, table) ? 1 : 0
		}
		return relate.Sum(bases, count)
	}

	var sort = function(relation, comparer) {
		var self = derived([relation])
		var needsResort = false
		var data = []

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

		self.handleInsert = function(table, row) {
			var i = data.length
			data.push(row)
			flagResort(data.length - 1)
		}
		self.handleUpdate = function(table, last, next) {
			var index = data.indexOf(last)
			data[index] = next
			flagResort(index)
		}
		self.handleRemove = function(table, row) {
			data.splice(data.indexOf(row),1)
		}
		self.getData = function() {
			if(needsResort) {
				data.sort(comparer)
				needsResort = false
			}
			return data
		}
		return self
	}

	relate.LeftOuterJoin = function(left, right, joinMapper) {
		return relate.OuterJoin(left, right, function(l,r) {
			if(left !== undefined) {
				return joinMapper(l,r)
			}
		})
	}

	relate.RightOuterJoin = function(left, right, joinMapper) {
		return relate.OuterJoin(left, right, function(l,r) {
			if(right !== undefined) {
				return joinMapper(l,r)
			}
		})
	}

	/*
		Left => LeftKey
		LeftKey => Rights
		Rights => 
	*/

	relate.OuterJoin = function(left, right, joinMapper) {

		var keyGen = function(joinedRow) {
			var key = []
			var toKey = function(rel, index) {
				return (joinedRow[index] !== undefined) ? rel.keyGen(joinedRow[index]) : undefined
			}
			key[0] = toKey(left, 0)
			key[1] = toKey(right, 1)
			return key
		}

		var self = derived([left,right], keyGen)
		self.keyGen = keyGen

		joinMapper = joinMapper ? joinMapper : function(left, right) {
			var result = []
			result[0] = left
			result[1] = right
			return result
		}

		self.handleInsert = function(table, row) {
			var otherTable = (table === left) ? right : left
			var otherKey = otherTable.keyGen(row)
			var joiner = (table === left) ? joinMapper : function(right, left) { return joinMapper(left, right) }

			var otherRows = []
			console.log(row)
			if(otherTable.getGroup) {
				var group = otherTable.getGroupFor(row)
				var rows = group.rows
				for(k in rows) if(rows.hasOwnProperty(k)) {
					var v = rows[k]
					if(v !== undefined) {
						otherRows.push(joiner(row,v))
					}
				}
			} else {
				otherRows.push(joiner(row, table.get(table.keyGen(row))))
			}
			if(otherRows.length === 0) {
				otherRows.push(joiner(row, undefined))
			}

			var i = otherRows.length
			while(--i >= 0) {
				self.insert(otherRows[i])
			}
		}
		self.handleUpdate = function(table, last, next) {
			var key = table.keyGen(row)
		}
		self.handleRemove = function(table, row) {
			var key = table.keyGen(row)
		}
		return self
	}

	return relate
}()