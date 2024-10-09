package topologyscheduling

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
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
)

const Name = "TopologyScheduling"

type TopologyScheduling struct {
	handle framework.Handle
}

var _ framework.QueueSortPlugin = &TopologyScheduling{}
var _ framework.PreFilterPlugin = &TopologyScheduling{}

func (pl *TopologyScheduling) Name() string {
	return Name
}

func (pl *TopologyScheduling) Less(podInfo1 *framework.QueuedPodInfo, podInfo2 *framework.QueuedPodInfo) bool {
	pod1 := podInfo1.Pod
	pod2 := podInfo2.Pod
	klog.V(1).InfoS("TopologyScheduling: QueueSort called", "pod1", pod1.Name, "pod2", pod2.Name)

	if pod1.Namespace != pod2.Namespace {
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}

	resource1, exists1 := pod1.Labels["resource"]
	resource2, exists2 := pod2.Labels["resource"]
	if !exists1 || !exists2 {
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}
	klog.V(1).InfoS("TopologyScheduling: QueueSort: Pods have resource label", "pod1", pod1.Name, "Resource1", resource1, "pod2", pod2.Name, "Resource2", resource2)

	workloadStatus, err := getDistributedJobStatus(pod1.Namespace)
	if err != nil {
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}

	var pod1Order = -1
	var pod2Order = -1
	for i, _ := range workloadStatus.Resource {
		if resource1 == workloadStatus.Resource[i] {
			pod1Order = workloadStatus.Order[i]
		}
		if resource2 == workloadStatus.Resource[i] {
			pod2Order = workloadStatus.Order[i]
		}
	}
	if pod1Order == -1 || pod2Order == -1 {
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}
	klog.V(1).InfoS("TopologyScheduling: QueueSort: Got order level", "order1", pod1Order, "order2", pod2Order)

	if pod1Order != pod2Order {
		return pod1Order < pod2Order
	}

	s := &queuesort.PrioritySort{}
	return s.Less(podInfo1, podInfo2)
}

func (pl *TopologyScheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	klog.InfoS("TopologyScheduling: PreFilter called", "pod", klog.KObj(pod))

	resource, exists := pod.Labels["resource"]
	if !exists {
		return &framework.PreFilterResult{}, framework.NewStatus(framework.Success, "")
	}

	workloadStatus, err := getDistributedJobStatus(pod.Namespace)
	if err != nil {
		return &framework.PreFilterResult{}, framework.NewStatus(framework.Success, "")
	}
	var podOrder = -1
	for i, _ := range workloadStatus.Resource {
		if resource == workloadStatus.Resource[i] {
			podOrder = workloadStatus.Order[i]
			break
		}
	}
	if podOrder == -1 {
		klog.V(1).InfoS("TopologyScheduling: PreFilter: Unknown resource label", "resource", resource)
		return &framework.PreFilterResult{}, framework.NewStatus(framework.UnschedulableAndUnresolvable, "")
	}

	for i, _ := range workloadStatus.Resource {
		if workloadStatus.Order[i] < podOrder && workloadStatus.Phase[i] != "Running" && workloadStatus.Phase[i] != "Succeeded" {
			klog.V(1).InfoS("TopologyScheduling: PreFilter: Pending until prev step starting", "order", podOrder, "prevOrder", workloadStatus.Order[i])
			return nil, framework.NewStatus(framework.Unschedulable, "")
		}
	}

	klog.V(1).InfoS("TopologyScheduling: PreFilter: All steps started before current pod order. Good to go")
	return &framework.PreFilterResult{}, framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *TopologyScheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	klog.V(1).InfoS("TopologyScheduling: Initializing plugin")

	// You can log configuration details here if you add any in the future
	klog.V(2).InfoS("TopologyScheduling: Plugin configuration", "config", obj)

	return &TopologyScheduling{
		handle: h,
	}, nil
}

type WorkloadStatus struct {
	Resource []string
	Order    []int
	Phase    []string
}

func getDistributedJobStatus(namespace string) (*WorkloadStatus, error) {
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
		Group:    "batch.ddl.com",
		Version:  "v1",
		Resource: "distributedjobs",
	}

	crd, err := dynamicClient.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to retrieve CRD: %v", err)
		return nil, err
	}

	if len(crd.Items) == 0 {
		return nil, fmt.Errorf("no DistributedJob resources found in namespace %s", namespace)
	}

	if len(crd.Items) > 1 {
		return nil, fmt.Errorf("multiple DistributedJob resources found in namespace %s", namespace)
	}

	workloadStatusData, ok := crd.Items[0].Object["status"].(map[string]interface{})["workloadStatuses"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse workloadStatuses")
	}
	workloadStatus := &WorkloadStatus{
		Resource: []string{},
		Order:    []int{},
		Phase:    []string{},
	}

	for _, w := range workloadStatusData {
		ws := w.(map[string]interface{})
		workloadStatus.Resource = append(workloadStatus.Resource, ws["resource"].(string))
		workloadStatus.Order = append(workloadStatus.Order, int(ws["order"].(int64)))
		workloadStatus.Phase = append(workloadStatus.Phase, ws["phase"].(string))
	}

	return workloadStatus, nil
}
