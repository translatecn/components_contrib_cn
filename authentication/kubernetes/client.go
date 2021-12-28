// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"flag"
	"fmt"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// nolint:gochecknoglobals
var kubeconfig *string

// nolint:gochecknoinits
func init() {
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
}

// GetKubeClient 根据SA config 创建k8s客户端
func GetKubeClient() (*kubernetes.Clientset, error) {
	flag.Parse()
	fmt.Println(*kubeconfig)
	// unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined
	//返回使用kubernetes提供给pod的服务帐户的配置对象。它是为那些希望在kubernetes上运行的pod中运行的客户端设计的。
	//如果从一个不在kubernetes环境中运行的进程调用，它将返回errnotinclcluster。
	conf, err := rest.InClusterConfig()
	if err != nil {
		conf, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}
