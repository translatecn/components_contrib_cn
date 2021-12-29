// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nameresolution

const (
	// MDNSInstanceName 是被广播的应用名称。
	MDNSInstanceName string = "name"
	// MDNSInstanceAddress 是实例的地址,本机地址
	MDNSInstanceAddress string = "address"
	// MDNSInstancePort .是实例的端口   daprd内部通信的端口
	MDNSInstancePort string = "port"
	// MDNSInstanceID 是一个可选的唯一的实例ID.
	MDNSInstanceID string = "instance"

	HostAddress string = "HOST_ADDRESS"
	DaprHTTPPort string = "DAPR_HTTP_PORT"
	DaprPort string = "DAPR_PORT"
	AppPort string = "APP_PORT"
	AppID string = "APP_ID"
)

// Metadata 包含一个名称解析特定的元数据属性集。
type Metadata struct {
	Properties    map[string]string `json:"properties"`
	Configuration interface{}
}
