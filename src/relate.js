Relate = function() {
	var relate = {}

	var identity = function(a) { return a }

	var relation = function() {
		var self = {}
		self.listeners = []
		self.rows = {}

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

		self.signalUpdate = function(last, next) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].update(self, last, next)
			}
		}
		self.signalInsert = function(row) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].insert(self, row)
			}
		}
		self.signalRemove = function(key) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].remove(self, key)
			}
		}

		self.addToListeners = function(rel) {
			if(self.listeners.indexOf(rel) === -1)
				self.listeners.push(rel)
		}
		self.removeFromListeners = function(rel) {
			self.listeners = self.listeners.filter(function(r) { return r !== rel })
		}

		self.group = function(groupGen) { return group(self, groupGen) }
		self.map = function(rowMapper) { return relate.Map([self], rowMapper) }
		self.join = function(rel) { return relate.Join(self, rel) }
		self.count = function(counter) { return relate.Count([self], counter)}
		self.sort = function(comparer) { return sort(self, comparer) }

		return self
	}

	relate.Table = function(name, keyGen) {
		var self = relation()
		self.keyGen = keyGen
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

	var derived = function(parents) {
		var self = relation()
		var sup = {}
		self.parents = parents

		self.load = function() {
			var p = parents.length
			while(--p >= 0) {
				var parent = parents[p]
				var rows = parent.toArray()
				var i = rows.length
				while(--i >= 0) {
					self.insert(parent, rows[i])
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
		var self = derived(bases)
		keyGen = keyGen ? keyGen : bases[0].keyGen

		var upsert = function(table, row) {
			self.rows[table.keyGen(row)] = mapper(row, table)
		}
		self.insert = function(table, ins) {
			upsert(table, ins)				
		}
		self.update = function(table, last, next) {
			upsert(table, next)				
		}
		self.remove = function(table, rem) {
			self.rows[keyGen(rem)] = undefined
		}

		self.load()

		return self
	}

	var group = function(base, grouper) {
		var self = derived([base])
		self.keyGen = grouper

		self.get = function(groupKey) {
			var grp = self.rows[groupKey]
			if(grp === undefined) {
				grp = self.rows[groupKey] = relate.Map([], identity, base.keyGen)
			}
			return grp
		}

		self.insert = function(table, row) {
			var key = grouper(row)
			self.get(key).insert(table, row)
		}
		self.update = function(table, last, next) {
			var lastKey = grouper(last)
			var nextKey = grouper(next)
			var lastGroup = self.get(lastKey)
			if(lastKey === nextKey) {
				lastGroup.update(table, last, next)
			} else {
				lastGroup.remove(table, last)
				self.get(nextKey).insert(table, next)
			}
		}
		self.remove = function(table, row) {
			var key = grouper(row)
			self.get(key).remove(table, row)
		}

		self.load()

		return self
	}

	relate.Aggregate = function(bases, initial, apply, unapply) {
		var self = derived(bases)
		var total = initial

		self.insert = function(table, row) {
			total = apply(total, row, table)
		}
		self.update = function(table, last, next) {
			total = apply(unapply(total, last, table), next, table)
		}
		self.remove = function(table, row) {
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

		self.insert = function(table, row) {
			var i = data.length
			data.push(row)
			flagResort(data.length - 1)
		}
		self.update = function(table, last, next) {
			var index = data.indexOf(last)
			data[index] = next
			flagResort(index)
		}
		self.remove = function(table, row) {
			data.splice(data.indexOf(row),1)
		}
		self.getData = function() {
			if(needsResort) {
				data.sort(comparer)
				needsResort = false
			}
			return data
		}

		self.load()
		return self
	}

	relate.Join = function(left, right) {
		return relate.Map([left, right], function(row, table) {
			var result = {}
			var key = table.keyGen(row)			
			if(table === left) {
				result.left = row
				result.right = right.get(key)
			} else {
				result.left = left.get(key)
				result.right = row
			}
			return result
		})
	}

	return relate
}()