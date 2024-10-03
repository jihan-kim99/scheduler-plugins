package blockallscheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const Name = "BlockAllScheduler"

type BlockAllScheduler struct {
    handle framework.Handle
}

var _ framework.PreFilterPlugin = &BlockAllScheduler{}

func (pl *BlockAllScheduler) Name() string {
    return Name
}

// Define a struct to hold network metrics
type NetworkMetrics struct {
    Bandwidth map[string]map[string]float64 // Map of worker -> peer -> bandwidth
    Latency   map[string]map[string]float64 // Map of worker -> peer -> latency
}

func getCRDData() (*NetworkMetrics, error) {
    config, err := rest.InClusterConfig()
    if err != nil {
        klog.Errorf("Failed to create in-cluster config: %v", err)
        return nil, err
    }

    dynamicClient, err := dynamic.NewForConfig(config)
    if err != nil {
        klog.Errorf("Failed to create dynamic client: %v", err)
        return nil, err
    }

    gvr := schema.GroupVersionResource{
        Group:    "network.example.com",
        Version:  "v1",
        Resource: "networkmetrics",
    }

    crd, err := dynamicClient.Resource(gvr).Namespace("kube-system").Get(context.TODO(), "cluster-metrics", metav1.GetOptions{})
    if err != nil {
        klog.Errorf("Failed to retrieve CRD: %v", err)
        return nil, err
    }

	metrics := crd.Object["spec"].(map[string]interface{})["metrics"].(map[string]interface{})
    bandwidth := metrics["bandwidth"].(map[string]interface{})
    latency := metrics["latency"].(map[string]interface{})

    networkMetrics := &NetworkMetrics{
        Bandwidth: make(map[string]map[string]float64),
        Latency:   make(map[string]map[string]float64),
    }

    for worker, workerMetrics := range bandwidth {
        workerData := workerMetrics.(map[string]interface{})
        networkMetrics.Bandwidth[worker] = make(map[string]float64)
        for peer, bw := range workerData {
            networkMetrics.Bandwidth[worker][peer] = bw.(float64)
        }
    }

    for worker, workerMetrics := range latency {
        workerData := workerMetrics.(map[string]interface{})
        networkMetrics.Latency[worker] = make(map[string]float64)
        for peer, lat := range workerData {
            networkMetrics.Latency[worker][peer] = lat.(float64)
        }
    }

    return networkMetrics, nil
}

func (pl *BlockAllScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
    klog.InfoS("BlockAllScheduler: PreFilter called", "pod", klog.KObj(pod))

	metrics, err := getCRDData()

	klog.Infof("BlockAllScheduler: Network metrics - bandwidth: %v, latency: %v", metrics.Bandwidth, metrics.Latency)

	if err != nil {
		klog.Errorf("Failed to get CRD data: %v", err)
		return nil, framework.AsStatus(fmt.Errorf("FAILED TO GET CRD: %v", err))
	}
	
    klog.InfoS("BlockAllScheduler: Pod details", 
        "podName", pod.Name, 
        "podNamespace", pod.Namespace, 
        "podUID", pod.UID,
        "podResourceRequests", pod.Spec.Containers[0].Resources.Requests)

    status := framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %v/%v blocked by BlockAllScheduler", pod.Namespace, pod.Name))
    klog.InfoS("BlockAllScheduler: Blocking pod", "pod", klog.KObj(pod), "status", status)
    
    return nil, status
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *BlockAllScheduler) PreFilterExtensions() framework.PreFilterExtensions {
    return nil
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
    klog.V(1).InfoS("BlockAllScheduler: Initializing plugin")
    
    // You can log configuration details here if you add any in the future
    klog.V(2).InfoS("BlockAllScheduler: Plugin configuration", "config", obj)

    return &BlockAllScheduler{
        handle: h,
    }, nil
}