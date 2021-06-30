package storage

import (
	"strconv"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/gofiber/fiber/v2"
)

type server struct {
	app *fiber.App
	ref *TSDB
}

func newServer() *server {
	return &server{app: fiber.New()}
}

func (s *server) Run(addr string) error {
	apiv1 := s.app.Group("/api/v1")

	apiv1.Get("/label/:name/values", s.queryLabelValues)
	apiv1.Post("/series", s.querySeries)
	apiv1.Post("/query_range", s.queryRange)
	return s.app.Listen(addr)
}

func tryInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

type qlResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func (s *server) queryLabelValues(c *fiber.Ctx) error {
	start, err := tryInt64(c.FormValue("start"))
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	end, err := tryInt64(c.FormValue("end"))
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	metrics := s.ref.QueryLabelValues(c.Params("name"), start, end)
	return c.JSON(qlResponse{Status: "success", Data: metrics})
}

type qsResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

func (s *server) querySeries(c *fiber.Ctx) error {
	expr, err := metricsql.Parse(c.FormValue("match[]"))
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	me, ok := expr.(*metricsql.MetricExpr)
	if !ok {
		return c.JSON(qsResponse{Status: "error"})
	}

	start, err := tryInt64(c.FormValue("start"))
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	end, err := tryInt64(c.FormValue("end"))
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	labels := make([]Label, 0)
	for _, label := range me.LabelFilters {
		if label.IsRegexp {
			labels = append(labels, Label{
				Name:  label.Label,
				Value: labelValuesRegxPrefix + label.Value + labelValuesRegxSuffix,
			})
			continue
		}

		labels = append(labels, Label{Name: label.Label, Value: label.Value})
	}

	ret, err := s.ref.QuerySeries(labels, start, end)
	if err != nil {
		return c.JSON(qsResponse{Status: "error"})
	}

	return c.JSON(qsResponse{Status: "success", Data: ret})
}

type qrResponse struct {
	Status string `json:"status"`
	Data   qrData `json:"data"`
}

type qrData struct {
	ResultType string         `json:"resultType"`
	Result     []qrDataResult `json:"result"`
}

type qrDataResult struct {
	Metric map[string]string `json:"metric"`
	Value  [][2]interface{}  `json:"values"`
}

func convert2QueryRangeData(met []MetricRet) qrData {
	data := qrData{ResultType: "matrix"}

	items := make([]qrDataResult, 0)
	for _, m := range met {
		item := qrDataResult{
			Metric: LabelSet(m.Labels).Map(),
		}

		for _, dp := range m.DataPoints {
			item.Value = append(item.Value, dp.ToInterface())
		}

		items = append(items, item)
	}

	data.Result = items
	return data
}

func (s *server) queryRange(c *fiber.Ctx) error {
	expr, err := metricsql.Parse(c.FormValue("query"))
	if err != nil {
		return c.JSON(qrResponse{Status: "error"})
	}

	me, ok := expr.(*metricsql.MetricExpr)
	if !ok {
		return c.JSON(qrResponse{Status: "error"})
	}

	start, err := tryInt64(c.FormValue("start"))
	if err != nil {
		return c.JSON(qrResponse{Status: "error"})
	}

	end, err := tryInt64(c.FormValue("end"))
	if err != nil {
		return c.JSON(qrResponse{Status: "error"})
	}

	labels := make([]Label, 0)
	var metric string

	for _, label := range me.LabelFilters {
		if label.Label == metricName {
			metric = label.Value
			continue
		}

		if label.IsRegexp {
			labels = append(labels, Label{
				Name:  label.Label,
				Value: labelValuesRegxPrefix + label.Value + labelValuesRegxSuffix,
			})
			continue
		}

		labels = append(labels, Label{Name: label.Label, Value: label.Value})
	}

	ret, err := s.ref.QueryRange(metric, labels, start, end)
	if err != nil {
		return c.JSON(qrResponse{Status: "error"})
	}

	return c.JSON(qrResponse{Status: "success", Data: convert2QueryRangeData(ret)})
}
