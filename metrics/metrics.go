package metrics

import (
	"fmt"
	"reflect"

	"contrib.go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

/*
	TAG_PUT            byte = 3
	TAG_EMBED          byte = 4
	TAG_DELETE         byte = 5
	TAG_DELETE_RANGE   byte = 6
	//I do not support tag for now
	RecordType, _ = tag.NewKey("type")
*/

var (
	//This is the key indicate each record's tag
	//Metrics  for JournalRegion
	JournalRegionMetric = newJournalRegionMetric()
	//Metrics for Storage
	StorageMetric     = newStorageMetric()
	PrometheusHandler *prometheus.Exporter
)

type journalRegionMetric struct {
	Syncs        *stats.Int64Measure `aggr:"Counter"`
	GcQueueSize  *stats.Int64Measure `aggr:"LastValue"`
	RecordCounts *stats.Int64Measure `aggr:"Sum"`
	Capacity     *stats.Int64Measure `aggr:"LastValue""`
}

type storageMetric struct {
	Reads      *stats.Int64Measure `aggr:"Counter"`
	Writes     *stats.Int64Measure `aggr:"Counter"`
	ReadBytes  *stats.Int64Measure `aggr:"Sum"`
	WriteBytes *stats.Int64Measure `aggr:"Sum"`
}

func newStorageMetric() *storageMetric {
	return &storageMetric{
		Reads:      stats.Int64("Reads", "reads", stats.UnitDimensionless),
		Writes:     stats.Int64("Writes", "writes", stats.UnitDimensionless),
		ReadBytes:  stats.Int64("ReadBytes", "read bytes", stats.UnitBytes),
		WriteBytes: stats.Int64("WriteBytes", "write bytes", stats.UnitBytes),
	}

}
func newJournalRegionMetric() *journalRegionMetric {
	return &journalRegionMetric{
		Syncs:        stats.Int64("JournalSync", "how many time Journal syncs", "1"),
		GcQueueSize:  stats.Int64("GcQueueSize", "how many records have be put in gcqueue", "1"),
		RecordCounts: stats.Int64("Records in journal", "records put in the journal region since start", "1"),
		Capacity:     stats.Int64("JournalUsage", "The usage of JournalRegion, by bytes", "byte"),
	}
}

//use golang tag to create views from measurements
//https://gist.github.com/drewolson/4771479 is a great example.
func createAppendViews(m interface{}, list []*view.View) []*view.View {
	val := reflect.ValueOf(m).Elem()
	for i := 0; i < val.NumField(); i++ {
		typeField := val.Type().Field(i)
		valueField, _ := val.Field(i).Interface().(*stats.Int64Measure)
		golangTag := typeField.Tag
		v := &view.View{
			Name:        valueField.Name(),
			Description: valueField.Description(),
			Measure:     valueField,
		}
		//aggreation
		var aggr *view.Aggregation
		switch golangTag.Get("aggr") {
		case "Counter":
			aggr = view.Count()
		case "LastValue":
			aggr = view.LastValue()
		case "Sum":
			aggr = view.Sum()
		default:
			panic("now we only suppport Counter and Gauge")
		}
		v.Aggregation = aggr

		list = append(list, v)
	}
	return list
}

func init() {
	var err error
	viewList := make([]*view.View, 0)
	viewList = createAppendViews(JournalRegionMetric, viewList)
	viewList = createAppendViews(StorageMetric, viewList)

	if err := view.Register(viewList...); err != nil {
		panic("failed to register view")
	}

	PrometheusHandler, err = prometheus.NewExporter(prometheus.Options{
		Namespace: "cannyls_go",
		OnError:   func(err error) { fmt.Printf("%v\n", err) },
	})
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}

	view.RegisterExporter(PrometheusHandler)
}
