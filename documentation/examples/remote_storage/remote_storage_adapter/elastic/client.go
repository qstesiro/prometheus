// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elastic

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/prompb"
)

// Client allows sending batches of Prometheus samples to InfluxDB.
type Client struct {
	logger log.Logger
}

// NewClient creates a new Client.
func NewClient(logger log.Logger) *Client {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Client{
		logger: logger,
	}
}

func (c *Client) Close() {
	// Todo ???
}

func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	return &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			&prompb.QueryResult{
				Timeseries: []*prompb.TimeSeries{
					&prompb.TimeSeries{
						Labels: []prompb.Label{
							prompb.Label{Name: "__name__", Value: "go_gc_duration_seconds_count"},
						},
						Samples: []prompb.Sample{
							// 注: 毫秒为单位
							// 查询时间对齐
							// prompb.Sample{Value: 0.1, Timestamp: 1628848080000}, // 2021-08-13T17:48:00.000
							// prompb.Sample{Value: 0.2, Timestamp: 1628848090000}, // 2021-08-13T17:48:10.000
							// prompb.Sample{Value: 0.3, Timestamp: 1628848100000}, // 2021-08-13T17:48:20.000
							// 查询时间非对齐
							// prompb.Sample{Value: 0.1, Timestamp: 1628848081000}, // 2021-08-13T17:48:01.000
							// prompb.Sample{Value: 0.2, Timestamp: 1628848091000}, // 2021-08-13T17:48:11.000
							// prompb.Sample{Value: 0.3, Timestamp: 1628848101000}, // 2021-08-13T17:48:21.000
							// 查询数据离散
							prompb.Sample{Value: 0.1, Timestamp: 1628848081000}, // 2021-08-13T17:48:01.000
							prompb.Sample{Value: 0.2, Timestamp: 1628848091000}, // 2021-08-13T17:48:11.000
							prompb.Sample{Value: 0.3, Timestamp: 1628848451000}, // 2021-08-13T17:54:11.000
						},
					},
				},
			},
		},
	}, nil
}

// Name identifies the client as an Elasticsearch client.
func (c Client) Name() string {
	return "elasticsearch"
}
