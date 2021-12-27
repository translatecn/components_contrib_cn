// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	contrib_contenttype "github.com/dapr/components-contrib/contenttype"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
)

const (
	// DefaultCloudEventType 是一个Dapr发布事件的默认事件类型。
	DefaultCloudEventType = "com.dapr.event.sent"
	// CloudEventsSpecVersion 是dapr用于云事件 实现的spec version
	CloudEventsSpecVersion = "1.0"
	// DefaultCloudEventSource is the default event source.
	DefaultCloudEventSource = "Dapr"
	// DefaultCloudEventDataContentType content-type 默认值
	DefaultCloudEventDataContentType = "text/plain"
	TraceIDField                     = "traceid"
	TopicField                       = "topic"
	PubsubField                      = "pubsubname"
	ExpirationField                  = "expiration"
	DataContentTypeField             = "datacontenttype"
	DataField                        = "data"
	DataBase64Field                  = "data_base64"
	SpecVersionField                 = "specversion"
	TypeField                        = "type"
	SourceField                      = "source"
	IDField                          = "id"
	SubjectField                     = "subject"
)

// NewCloudEventsEnvelope 返回一个cloudevents JSON的映射表示。
func NewCloudEventsEnvelope(id, source, eventType, subject string, topic string, pubsubName string, dataContentType string, data []byte, traceID string) map[string]interface{} {
	// defaults
	if id == "" {
		id = uuid.New().String()
	}
	if source == "" {
		source = DefaultCloudEventSource // Dapr
	}
	if eventType == "" {
		eventType = DefaultCloudEventType
	}
	if dataContentType == "" {
		dataContentType = DefaultCloudEventDataContentType
	}

	var ceData interface{} // 用户上传的数据
	ceDataField := DataField // data
	var err error
	// 只支持三种 bin、json、text
	if contrib_contenttype.IsJSONContentType(dataContentType) {
		// 检查是不是json数据
		err = jsoniter.Unmarshal(data, &ceData)
	} else if contrib_contenttype.IsBinaryContentType(dataContentType) {
		ceData = base64.StdEncoding.EncodeToString(data)
		ceDataField = DataBase64Field
	} else {
		ceData = string(data)
	}

	if err != nil {
		ceData = string(data)
	}

	ce := map[string]interface{}{
		IDField:              id,                     // 随机生成的
		SpecVersionField:     CloudEventsSpecVersion, // 版本
		DataContentTypeField: dataContentType,        // text\bin\json
		SourceField:          source,                 // Dapr
		TypeField:            eventType,              // com.dapr.event.sent
		TopicField:           topic,                  // 主题名
		PubsubField:          pubsubName,             // 组件名
		TraceIDField:         traceID,                // 追踪ID
	}

	ce[ceDataField] = ceData

	if subject != "" {
		ce[SubjectField] = subject
	}

	return ce
}

// FromCloudEvent 返回一个现有的cloudevents JSON的映射表示。
func FromCloudEvent(cloudEvent []byte, topic, pubsub, traceID string) (map[string]interface{}, error) {
	var m map[string]interface{}
	err := jsoniter.Unmarshal(cloudEvent, &m)
	if err != nil {
		return m, err
	}

	m[TraceIDField] = traceID
	m[TopicField] = topic
	m[PubsubField] = pubsub // 组件名

	// 如果原始的clouddevent中没有指定默认值，则指定默认值
	if m[SourceField] == nil {
		m[SourceField] = DefaultCloudEventSource
	}

	if m[TypeField] == nil {
		m[TypeField] = DefaultCloudEventType
	}

	if m[SpecVersionField] == nil {
		m[SpecVersionField] = CloudEventsSpecVersion
	}

	return m, nil
}

// FromRawPayload 在订阅者端返回原始负载的clouddevent。
func FromRawPayload(data []byte, topic, pubsub string) map[string]interface{} {
	// Limitations of generating the CloudEvent on the subscriber side based on raw payload:
	// - The CloudEvent ID will be random, so the same message can be redelivered as a different ID.
	// - TraceID is not useful since it is random and not from publisher side.
	// - Data is always returned as `data_base64` since we don't know the actual content type.
	return map[string]interface{}{
		IDField:              uuid.New().String(),
		SpecVersionField:     CloudEventsSpecVersion,
		DataContentTypeField: "application/octet-stream",
		SourceField:          DefaultCloudEventSource,
		TypeField:            DefaultCloudEventType,
		TopicField:           topic,
		PubsubField:          pubsub,
		DataBase64Field:      base64.StdEncoding.EncodeToString(data),
	}
}

// HasExpired determines if the current cloud event has expired.
func HasExpired(cloudEvent map[string]interface{}) bool {
	e, ok := cloudEvent[ExpirationField]
	if ok && e != "" {
		expiration, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", e))
		if err != nil {
			return false
		}

		return expiration.UTC().Before(time.Now().UTC())
	}

	return false
}

// ApplyMetadata 将根据组件的特性集处理元数据来修改云事件。
func ApplyMetadata(cloudEvent map[string]interface{}, componentFeatures []Feature, metadata map[string]string) {
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(metadata)
	if hasTTL && !FeatureMessageTTL.IsPresent(componentFeatures) {
		// 如果组件不处理，Dapr只处理消息TTL。
		now := time.Now().UTC()
		//最大ttl是maxxint64，目前还不足以溢出时间。在编写此代码时(2020年12月28日)，now()添加maxInt64的最大时间是~“2313-04-09T23:30:26Z”。戈朗目前最大时间为292277024627- 12-06t15:30:07. 99999999z。所以，在下面发生溢出之前，我们还有一些时间:)
		//The maximum ttl is maxInt64, which is not enough to overflow time, for now. As of the time this code was written (2020 Dec 28th), the maximum time of now() adding maxInt64 is ~ "2313-04-09T23:30:26Z". Max time in golang is currently 292277024627-12-06T15:30:07.999999999Z. So, we have some time before the overflow below happens :)
		expiration := now.Add(ttl)
		cloudEvent[ExpirationField] = expiration.Format(time.RFC3339)
	}
}
