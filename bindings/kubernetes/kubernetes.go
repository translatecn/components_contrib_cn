// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"encoding/json"
	"errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	raw_k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type kubernetesInput struct {
	kubeClient        kubernetes.Interface
	namespace         string
	resyncPeriodInSec time.Duration // nolint:stylecheck
	logger            logger.Logger
}

type EventResponse struct {
	Event  string   `json:"event"`
	OldVal v1.Event `json:"oldVal"`
	NewVal v1.Event `json:"newVal"`
}

var _ = bindings.InputBinding(&kubernetesInput{})

// NewKubernetes 返回一个新的Kubernetes事件输入绑定。
func NewKubernetes(logger logger.Logger) bindings.InputBinding {
	return &kubernetesInput{logger: logger}
}

// GetK8s 此处改用加载本地配置文件 ~/.kube/config
func getK8s() *raw_k8s.Clientset {
	conf, err := rest.InClusterConfig()
	if err != nil {
		// 路径直接写死
		conf, err = clientcmd.BuildConfigFromFlags("", filepath.Join(homedir.HomeDir(), ".kube", "config"))
		if err != nil {
			panic(err)
		}
	}

	kubeClient, _ := raw_k8s.NewForConfig(conf)
	return kubeClient
}
func (k *kubernetesInput) Init(metadata bindings.Metadata) error {
	//client, err := kubeclient.GetKubeClient()
	//if err != nil {
	//	return err
	//}
	//k.kubeClient = client
	k.kubeClient = getK8s()
	return k.parseMetadata(metadata)
}

func (k *kubernetesInput) parseMetadata(metadata bindings.Metadata) error {
	if val, ok := metadata.Properties["namespace"]; ok && val != "" {
		k.namespace = val
	} else {
		return errors.New("元数据中缺少命名空间")
	}
	if val, ok := metadata.Properties["resyncPeriodInSec"]; ok && val != "" {
		intval, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			k.logger.Warnf("invalid resyncPeriodInSec %s; %v; defaulting to 10s", val, err)
			k.resyncPeriodInSec = time.Second * 10
		} else {
			k.resyncPeriodInSec = time.Second * time.Duration(intval)
		}
	}

	return nil
}

func (k *kubernetesInput) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	watchlist := cache.NewListWatchFromClient(
		k.kubeClient.CoreV1().RESTClient(),
		"events",
		k.namespace,
		fields.Everything())
	var resultChan chan EventResponse = make(chan EventResponse)
	_, controller := cache.NewInformer(
		watchlist,
		&v1.Event{},
		k.resyncPeriodInSec,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "add",
						NewVal: *(obj.(*v1.Event)),
						OldVal: v1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Add handle %v", obj)
				}
			},
			DeleteFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "delete",
						OldVal: *(obj.(*v1.Event)),
						NewVal: v1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Delete handle %v", obj)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if oldObj != nil && newObj != nil {
					resultChan <- EventResponse{
						Event:  "update",
						OldVal: *(oldObj.(*v1.Event)),
						NewVal: *(newObj.(*v1.Event)),
					}
				} else {
					k.logger.Warnf("Nil Objects in Update handle %v %v", oldObj, newObj)
				}
			},
		},
	)
	stopCh := make(chan struct{})
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	go controller.Run(stopCh)
	done := false
	for !done {
		select {
		case obj := <-resultChan:
			data, err := json.Marshal(obj)
			if err != nil {
				k.logger.Errorf("Error marshalling event %w", err)
			} else {
				handler(&bindings.ReadResponse{
					Data: data,
				})
			}
		case <-sigterm:
			done = true
			close(stopCh)
		}
	}

	return nil
}
