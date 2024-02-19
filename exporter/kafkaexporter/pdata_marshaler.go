// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"hash/crc32"
	"math"

	"github.com/Shopify/sarama"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
	"go.opentelemetry.io/collector/model/pdata"
)

type pdataLogsMarshaler struct {
	marshaler pdata.LogsMarshaler
	encoding  string
}

func (p pdataLogsMarshaler) Marshal(ld pdata.Logs, topic string) ([]*sarama.ProducerMessage, error) {
	bts, err := p.marshaler.MarshalLogs(ld)
	if err != nil {
		return nil, err
	}
	return []*sarama.ProducerMessage{
		{
			Topic: topic,
			Value: sarama.ByteEncoder(bts),
		},
	}, nil
}

func (p pdataLogsMarshaler) Encoding() string {
	return p.encoding
}

func newPdataLogsMarshaler(marshaler pdata.LogsMarshaler, encoding string) LogsMarshaler {
	return pdataLogsMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}

type pdataMetricsMarshaler struct {
	marshaler pdata.MetricsMarshaler
	encoding  string
}

func (p pdataMetricsMarshaler) Marshal(ld pdata.Metrics, topic string) ([]*sarama.ProducerMessage, error) {
	bts, err := p.marshaler.MarshalMetrics(ld)
	if err != nil {
		return nil, err
	}
	return []*sarama.ProducerMessage{
		{
			Topic: topic,
			Value: sarama.ByteEncoder(bts),
		},
	}, nil
}

func (p pdataMetricsMarshaler) Encoding() string {
	return p.encoding
}

func newPdataMetricsMarshaler(marshaler pdata.MetricsMarshaler, encoding string) MetricsMarshaler {
	return pdataMetricsMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}

type pdataTracesMarshaler struct {
	marshaler pdata.TracesMarshaler
	encoding  string
}

func hash(id pdata.TraceID) uint8 {
	b := id.Bytes()
	return uint8((crc32.ChecksumIEEE(b[:]) >> 16) % math.MaxInt8)
}

func splitBy(td pdata.Traces) map[string]pdata.Traces {
	const partitions = 255
	groups := make(map[string]pdata.Traces, partitions)
	for i := uint8(0); i < partitions; i++ {
		groups[string(i)] = pdata.NewTraces()
	}
	for _, td := range batchpersignal.SplitTraces(td) {
		id := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans().At(0).TraceID()
		h := hash(id)
		i := h % partitions
		td.ResourceSpans().MoveAndAppendTo(groups[string(i)].ResourceSpans())
	}
	return groups
}

func (p pdataTracesMarshaler) Marshal(td pdata.Traces, topic string) ([]*sarama.ProducerMessage, error) {
	var msgs []*sarama.ProducerMessage
	for key, trace := range splitBy(td) {
		if trace.SpanCount() == 0 {
			continue
		}
		bts, err := p.marshaler.MarshalTraces(trace)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(bts),
		})
	}
	return msgs, nil
}

func (p pdataTracesMarshaler) Encoding() string {
	return p.encoding
}

func newPdataTracesMarshaler(marshaler pdata.TracesMarshaler, encoding string) TracesMarshaler {
	return pdataTracesMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}
