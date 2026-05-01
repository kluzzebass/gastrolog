package mqtt

import (
	"strconv"
	"time"

	"gastrolog/internal/orchestrator"
)

// buildMessage assembles an IngestMessage from the protocol-agnostic
// fields that both v3 and v5 handlers extract from their respective
// paho message types. Centralising the construction here keeps the
// IngesterID + IngestTS invariant in a single seam — the MQTT
// ingester is broker-only so we can't drive an end-to-end test, but
// this helper is trivially unit-testable from gastrolog-44b9r tests.
func buildMessage(topic string, qos byte, retained bool, msgID uint16, payload []byte, ingesterID string, now time.Time) orchestrator.IngestMessage {
	return orchestrator.IngestMessage{
		Attrs: map[string]string{
			"ingester_type":   "mqtt",
			"mqtt_topic":      topic,
			"mqtt_qos":        strconv.Itoa(int(qos)),
			"mqtt_retained":   strconv.FormatBool(retained),
			"mqtt_message_id": strconv.Itoa(int(msgID)),
		},
		Raw:        payload,
		IngestTS:   now,
		IngesterID: ingesterID,
	}
}
