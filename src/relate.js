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

		self.signalUpdated = function(rows) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].updated(self, rows)
			}
		}
		self.signalInserted = function(rows) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].inserted(self, rows)
			}
		}
		self.signalRemoved = function(keys) {
			var l = self.listeners.length
			while(--l >= 0) {
				self.listeners[l].removed(self, keys)
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
						inserts.push(row)
					} else if(upsert) {
						self.rows[pk] = row
						updates.push({last: oldRow, next: row})
					}
				}
			}
			var results = {}
			if(updates.length > 0) {
				self.signalUpdated(updates)
				results.updates = updates
			}
			if(inserts.length > 0) {
				self.signalInserted(inserts)
				results.inserts = inserts
			}
			return results
		}
		self.insert = function(toInsert) { return insert(toInsert, false) }
		self.upsert = function(toUpsert) { return insert(toUpsert, true) }
		self.remove = function(pks) {
			var r = pks.length
			while(--r >= 0) {
				var pk = pks[r]
				self.signalRemoved(self.rows[pk])
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
				self.inserted(parent, parent.toArray())
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

	relate.Map = function(bases, mapper) {
		var self = derived(bases)
		var keyGen = bases[0].keyGen

		var upsert = function(table, row) {
			self.rows[table.keyGen(row)] = mapper(row, table)
		}
		self.inserted = function(table, ins) {
			var r = ins.length
			while(--r >= 0) {
				upsert(table, ins[r])				
			}
		}
		self.updated = function(table, ups) {
			var r = ups.length
			while(--r >= 0) {
				upsert(table, ups[r].next)				
			}
		}
		self.removed = function(table, rems) {
			var r = ups.length
			while(--r >= 0) {
				self.rows[keyGen(rems[r])] = undefined
			}
		}

		self.load()

		return self
	}

	var group = function(base, grouper) {
		var self = derived([base])
		self.keyGen = grouper

		var changed = function(changes, applier, signaler) {
			var c = changes.length
			var results = []
			while(--c >= 0) {
				var row = changes[c]
				var groupKey = grouper(row)
				var key = base.keyGen(row)
				if(groupKey !== undefined && key !== undefined) {
					applier(key, groupKey, self.rows[groupKey], row)
					results.push(row)
				}
				if(signaler)
					signaler(results)
			}
		}

		var inserted = function(table, ins, signaler) {
			changed(ins, function(key, groupKey, group, row) {
				if(group === undefined)
					group = self.rows[groupKey] = {}
				group[key] = row
			}, signaler)
		}
		var removed = function(table, rems, signaler) {
			changed(rems, function(key, groupKey, group, row) {
				if(group !== undefined)
					delete group[key]				
			}, signaler)
		}

		self.inserted = function(table, ins) {
			inserted(table, ins, self.signalInserted)
		}
		self.removed = function(table, rems) {
			removed(table, rems, self.signalRemoved)
		}
		self.updated = function(table, ups) {
			var rems = []
			var ins = []
			var u = ups.length
			while(--u >= 0) {
				var up = ups[u]
				rems.push(up.last)
				ins.push(up.next)
			}
			removed(table, rems)
			inserted(table, ins, self.signalUpdated)
		}

		self.load()

		return self
	}

	relate.Aggregate = function(bases, initial, apply, unapply) {
		var self = derived(bases)
		var total = initial

		var change = function(table, rows, op) {
			var r = rows.length
			while(--r >= 0) {
				var result = op(total, rows[r], table)
				if(result !== undefined)
					total = result
			}
		}
		self.inserted = function(table, rows) {
			change(table, rows, apply)
		}
		self.updated = function(table, rows) {
			change(table, rows, function(tot, row, tab) {
				return apply(unapply(tot, row.last, tab), row.next, tab)
			})
		}
		self.removed = function(table, rows) {
			change(table, rows, unapply)
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

		self.inserted = function(table, rows) {
			var i = data.length
			data = data.concat(rows)
			while(i < data.length) {
				flagResort(i)
				i += 2
			}
		}
		self.updated = function(table, rows) {
			var i = rows.length
			while(--i >= 0) {
				var tuple = rows[i]
				var index = data.indexOf(tuple.last)
				data[index] = tuple.next
				flagResort(index)
			}
		}
		self.removed = function(table, rows) {
			var i = rows.length
			while(--i >= 0) {
				data.splice(data.indexOf(rows[i]),1)
			}
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