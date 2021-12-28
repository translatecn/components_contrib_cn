// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nameresolution

// Resolver is the interface of name resolver.
type Resolver interface {
	// Init 初始化域名解析
	Init(metadata Metadata) error
	// ResolveID 解析域名
	ResolveID(req ResolveRequest) (string, error)
}
