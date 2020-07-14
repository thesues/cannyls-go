package metrics

import (
	"testing"

	"go.opencensus.io/stats/view"
)

func TestCreateView(t *testing.T) {
	x := newJournalRegionMetric()

	viewList := make([]*view.View, 0)
	viewList = createAppendViews(x, viewList)
}
