// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package contenttype

import "strings"

const (
	// CloudEventContentType 云事件的消息类型
	CloudEventContentType = "application/cloudevents+json"
	// JSONContentType
	JSONContentType = "application/json"
)

// IsCloudEventContentType 检查内容类型。
func IsCloudEventContentType(contentType string) bool {
	return isContentType(contentType, CloudEventContentType)
}

// IsJSONContentType checks for content type.
func IsJSONContentType(contentType string) bool {
	return isContentType(contentType, JSONContentType)
}

// IsStringContentType determines if content type is string.
func IsStringContentType(contentType string) bool {
	if strings.HasPrefix(strings.ToLower(contentType), "text/") {
		return true
	}

	return isContentType(contentType, "application/xml")
}

// IsBinaryContentType determines if content type is byte[].
func IsBinaryContentType(contentType string) bool {
	return isContentType(contentType, "application/octet-stream")
}

func isContentType(contentType string, expected string) bool {
	// 用户请求的、期待的
	lowerContentType := strings.ToLower(contentType)
	if lowerContentType == expected {
		return true
	}

	semiColonPos := strings.Index(lowerContentType, ";")
	if semiColonPos >= 0 {
		// 匹配第一个content-type     e.g.   a;b
		return lowerContentType[0:semiColonPos] == expected
	}

	return false
}
