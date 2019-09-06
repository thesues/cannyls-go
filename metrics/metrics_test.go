package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opencensus.io/stats/view"
)

func TestCreateView(t *testing.T) {
	x := newJournalRegionMetric()

	viewList := make([]*view.View, 0)
	viewList = createAppendViews(x, viewList)

	assert.Equal(t, x.Syncs.Description(), viewList[0].Description)
}
