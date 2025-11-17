module github.com/confluentinc/confluent-kafka-go/v2/examples/opentelemetry_example

go 1.19

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.21.0
	go.opentelemetry.io/otel/trace v1.21.0
)

replace github.com/confluentinc/confluent-kafka-go/v2 => ../..
