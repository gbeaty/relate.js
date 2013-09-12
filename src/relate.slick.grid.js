Relate.SlickGrid = function() {
	var sg = {}

	sg.create = function(el, sorted, cols, opts) {		

		var grid
		var inserted = function(table, row) {
			grid.updateRowCount()
			grid.render()
		}
		var updated = function(table, last, next) {
			var i = sorted.indexOf(next)
			grid.invalidateRow(i)
			grid.render()
		}
		var removed = function(table, row) {
			grid.updateRowCount()
			grid.render()
		}

		var listener = Relate.listener(inserted, updated, removed)
		sorted.addToListeners(listener)
		listener.grid = grid
		listener.getLength = function() { return sorted.getData().length }
		listener.getItem = function(i) {
			return sorted.getData()[i]
		}
		listener.pause = function() {
			sorted.removeFromListeners(listener)
		}
		listener.resume = function() {
			sorted.addToListeners(listener)
		}

		grid = new Slick.Grid(el, listener, cols, opts)
		grid.render()

		return listener
	}

	return sg
}()