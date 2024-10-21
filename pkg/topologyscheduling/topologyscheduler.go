package topologyscheduling

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
)

const Name = "TopologyScheduling"

type TopologyScheduling struct {
	handle            framework.Handle
	clientset         kubernetes.Interface
	podInformer       cache.SharedIndexInformer
	unschedulablePods sync.Map
}

var _ framework.QueueSortPlugin = &TopologyScheduling{}
var _ framework.PreFilterPlugin = &TopologyScheduling{}
var _ framework.FilterPlugin = &TopologyScheduling{}

func (pl *TopologyScheduling) Name() string {
	return Name
}

func (pl *TopologyScheduling) Less(podInfo1 *framework.QueuedPodInfo, podInfo2 *framework.QueuedPodInfo) bool {
	pod1 := podInfo1.Pod
	pod2 := podInfo2.Pod
	klog.V(1).InfoS("TopologyScheduling: QueueSort called", "pod1", pod1.Name, "pod2", pod2.Name)

	if pod1.Namespace != pod2.Namespace {
		return strings.Compare(pod1.Namespace, pod1.Namespace) > 0
	}

	resource1, exists1 := pod1.Labels["resource"]
	resource2, exists2 := pod2.Labels["resource"]
	if !exists1 || !exists2 {
		if exists1 {
			return true
		}
		if exists2 {
			return false
		}
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}
	klog.V(1).InfoS("TopologyScheduling: QueueSort: Pods have resource label", "pod1", pod1.Name, "Resource1", resource1, "pod2", pod2.Name, "Resource2", resource2)

	_, workloadStatus, err := getDistributedJobWorkloads(pod1.Namespace)
	if err != nil {
		s := &queuesort.PrioritySort{}
		return s.Less(podInfo1, podInfo2)
	}

	var pod1Order = -1
	var pod2Order = -1
	for _, workloadS := range workloadStatus {
		if resource1 == workloadS.Resource {
			pod1Order = workloadS.Order
		}
		if resource2 == workloadS.Resource {
			pod2Order = workloadS.Order
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

// func temp (clientset *kubernetes.Clientset) {
// 	podInformer := corev1informers.N
// 	factory := corev1informers.NewSharedInformerFactory(clientset, 0)
// }

// nodes := pl.handle.SharedInformerFactory().Node()

const stateKey framework.StateKey = "PreFilterState"

type PreFilterState struct {
	Data []string
}

// Clone implements framework.StateData.
func (p *PreFilterState) Clone() framework.StateData {
	copy := *p
	return &copy
}

func (pl *TopologyScheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// Filter based on CPU using ratio (0.7)
	nodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	nodeList := []string{}
	for _, node := range nodes {
		if _, ok := node.Node().Labels["node-role.kubernetes.io/control-plane"]; !ok {
			cpuCapacity := node.Node().Status.Capacity.Cpu().MilliValue()
			cpuAllocatable := node.Node().Status.Allocatable.Cpu().MilliValue()
			klog.V(1).InfoS("TopologyScheduling: PreFilter: get Node used", "node", node.Node().Name, "allocatable", float64(cpuAllocatable/cpuCapacity))
			if float64(cpuAllocatable/cpuCapacity) > 0.7 {
				nodeList = append(nodeList, node.Node().Name)
			}
		}
	}

	// Get network cost
	networkMetrics, err := getNetworkMetrics()
	if err != nil {
		return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("failed to get network metrics: %v", err))
	}
	klog.V(1).InfoS("TopologyScheduling: PreFilter: got network metrics", "data", networkMetrics)

	// Get DistributedJob info
	workloadSpec, workloadStatus, err := getDistributedJobWorkloads(pod.Namespace)
	if err != nil {
		return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("failed to get DistributedJob CRD: %v", err))
	}
	klog.V(1).InfoS("TopologyScheduling: PreFilter: got CRD Info", "spec", workloadSpec, "status", workloadStatus)

	resourceList := []string{}
	for _, workloadS := range workloadStatus {
		resourceList = append(resourceList, workloadS.Resource)
	}

	podRequirements := [][]float64{}
	for _, resource := range resourceList {
		for _, workload := range workloadSpec {
			if resource == workload.Resource {
				for _, dependency := range workload.Dependencies {
					podRequirements = append(podRequirements, []float64{float64(dependency.Bandwidth.Limits), float64(dependency.Bandwidth.Requests), float64(dependency.Latency.Limits), float64(dependency.Latency.Requests)})
				}
			}
		}
	}
	klog.V(1).InfoS("TopologyScheduling: PreFilter: get dependencies", "resourceList", resourceList, "dependencies", podRequirements)

	nodeSet, score := getBestNodeSet(nodeList, networkMetrics, podRequirements, len(resourceList))
	if len(nodeSet) != len(resourceList) {
		klog.V(1).InfoS("TopologyScheduling: PreFilter: cannot find best node set", "nodeSet", nodeSet)
		return &framework.PreFilterResult{}, framework.NewStatus(framework.Unschedulable, "")
	}
	klog.V(1).InfoS("TopologyScheduling: PreFilter: got best node set", "nodeSet", nodeSet, "bestScore", score)

	data := &PreFilterState{
		Data: nodeSet,
	}
	state.Write(stateKey, data)

	return &framework.PreFilterResult{}, framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *TopologyScheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (pl *TopologyScheduling) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.V(1).InfoS("TopologyScheduler: Filter called", "pod", pod.Name, "node", nodeInfo.Node().Name)

	resource, exists := pod.Labels["resource"]
	if !exists {
		return framework.NewStatus(framework.Success, "")
	}

	data, err := getPreFilterState(state)
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("error reading state: %v", err))
	}

	klog.V(1).InfoS("TopologyScheduler: Filter: read data from PreFilter", "data", data.Data)

	_, workloadStatus, err := getDistributedJobWorkloads(pod.Namespace)
	var podOrder int
	for _, workloadS := range workloadStatus {
		if resource == workloadS.Resource {
			podOrder = workloadS.Order
		}
	}

	if nodeInfo.Node().Name == data.Data[podOrder-1] {
		klog.V(1).InfoS("TopologyScheduler: Filter: pod should be scheduled on current node", "pod", pod.Name, "order", podOrder+1, "node", nodeInfo.Node().Name)
		return framework.NewStatus(framework.Success, "")
	} else {
		klog.V(1).InfoS("TopologyScheduler: Filter: filter current node", "pod", pod.Name, "order", podOrder, "node", nodeInfo.Node().Name)
		return framework.NewStatus(framework.Unschedulable, "")
	}
}

func getPreFilterState(state *framework.CycleState) (*PreFilterState, error) {
	if state == nil {
		return nil, fmt.Errorf("cycle state is nil")
	}

	s, err := state.Read(stateKey)
	for err != nil {
		time.Sleep(5 * time.Second)
		s, err = state.Read(stateKey)
		klog.V(1).Infof("error reading %q from cyclestate: %v, Waiting...", stateKey, err)
	}

	preFilterState, ok := s.(*PreFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to PreFilterState error", s)
	}

	return preFilterState, nil
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

// NetworkMetrics CRD: current bandwidth and latency
type NetworkMetrics struct {
	Bandwidth map[string]map[string]float64 // Map of worker -> peer -> bandwidth
	Latency   map[string]map[string]float64 // Map of worker -> peer -> latency
}

func getNetworkMetrics() (*NetworkMetrics, error) {
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
			if val, ok := bw.(float64); ok {
				networkMetrics.Bandwidth[worker][peer] = val
			} else {
				networkMetrics.Bandwidth[worker][peer] = float64(bw.(int64))
			}
		}
	}

	for worker, workerMetrics := range latency {
		workerData := workerMetrics.(map[string]interface{})
		networkMetrics.Latency[worker] = make(map[string]float64)
		for peer, lat := range workerData {
			if val, ok := lat.(float64); ok {
				networkMetrics.Latency[worker][peer] = val
			} else {
				networkMetrics.Latency[worker][peer] = float64(lat.(int64))
			}
		}
	}

	return networkMetrics, nil
}

// DistributedJob CRD: required bandwidth and latency
type Dependency struct {
	Resource  string     `json:"resource"`
	Bandwidth Allocation `json:"bandwidth"`
	Latency   Allocation `json:"latency"`
}

type Allocation struct {
	Requests int `json:"requests,omitempty"`
	Limits   int `json:"limits"`
}

type Workload struct {
	Resource     string       `json:"resource"`
	Dependencies []Dependency `json:"dependencies,omitempty"`
}

type PodScheduling struct {
	PodName  string
	NodeName string
}

type WorkloadStatus struct {
	Resource       string
	Order          int
	Phase          string
	SchedulingInfo []PodScheduling
}

func getDistributedJobWorkloads(namespace string) ([]Workload, []WorkloadStatus, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("Failed to create in-cluster config: %v", err)
		return nil, nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		klog.Errorf("Failed to create dynamic client: %v", err)
		return nil, nil, err
	}
	gvr := schema.GroupVersionResource{
		Group:    "batch.ddl.com",
		Version:  "v1",
		Resource: "distributedjobs",
	}
	distributedJobs, err := dynamicClient.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to retrieve DistributedJobs: %v", err)
		return nil, nil, err
	}
	if len(distributedJobs.Items) == 0 {
		return nil, nil, fmt.Errorf("no DistributedJob resources found in namespace %s", namespace)
	}
	if len(distributedJobs.Items) > 1 {
		return nil, nil, fmt.Errorf("multiple DistributedJob resources found in namespace %s", namespace)
	}

	// Workload (DistributedJob Spec)
	var workloads []Workload
	workloadData, ok := distributedJobs.Items[0].Object["spec"].(map[string]interface{})["workloads"].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse workloads")
	}
	for _, w := range workloadData {
		workloadMap := w.(map[string]interface{})
		resource, ok := workloadMap["resource"].(string)
		if !ok {
			return nil, nil, fmt.Errorf("failed to parse resource in workload")
		}

		var dependencies []Dependency
		if deps, exists := workloadMap["dependencies"].([]interface{}); exists {
			for _, d := range deps {
				depMap := d.(map[string]interface{})
				depResource := depMap["resource"].(string)

				bandwidthMap := depMap["bandwidth"].(map[string]interface{})
				latencyMap := depMap["latency"].(map[string]interface{})

				dependency := Dependency{
					Resource: depResource,
					Bandwidth: Allocation{
						Requests: int(bandwidthMap["requests"].(int64)),
						Limits:   int(bandwidthMap["limits"].(int64)),
					},
					Latency: Allocation{
						Requests: int(latencyMap["requests"].(int64)),
						Limits:   int(latencyMap["limits"].(int64)),
					},
				}
				dependencies = append(dependencies, dependency)
			}
		}
		workload := Workload{
			Resource:     resource,
			Dependencies: dependencies,
		}

		workloads = append(workloads, workload)
	}

	// WorkloadStatus (DistributedJob Status)
	var workloadStatuses []WorkloadStatus
	workloadStatusData, ok := distributedJobs.Items[0].Object["status"].(map[string]interface{})["workloadStatuses"].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse workloadStatuses")
	}

	for _, w := range workloadStatusData {
		ws := w.(map[string]interface{})
		resource, ok := ws["resource"].(string)
		if !ok {
			return nil, nil, fmt.Errorf("failed to parse resource in workload")
		}
		order := int(ws["order"].(int64))
		if !ok {
			return nil, nil, fmt.Errorf("failed to parse order in workload")
		}
		phase, ok := ws["phase"].(string)
		if !ok {
			return nil, nil, fmt.Errorf("failed to parse phase in workload")
		}

		var schedulingInfo []PodScheduling
		if schedulingInfoData, exists := ws["schedulingInfo"].([]interface{}); exists && len(schedulingInfoData) > 0 {
			firstSchedulingInfo := schedulingInfoData[0].(map[string]interface{})
			var podName = ""
			var nodeName = ""
			if podInfo, ok := firstSchedulingInfo["podName"].(string); ok {
				podName = podInfo
			}
			if nodeInfo, ok := firstSchedulingInfo["nodeName"].(string); ok {
				nodeName = nodeInfo
			}

			schedInfo := PodScheduling{
				PodName:  podName,
				NodeName: nodeName,
			}
			schedulingInfo = append(schedulingInfo, schedInfo)
		}

		workloadStatus := WorkloadStatus{
			Resource:       resource,
			Order:          order,
			Phase:          phase,
			SchedulingInfo: schedulingInfo,
		}
		workloadStatuses = append(workloadStatuses, workloadStatus)
	}

	return workloads, workloadStatuses, nil
}

func getBestNodeSet(nodes []string, networkMetrics *NetworkMetrics, podRequirements [][]float64, m int) ([]string, float64) {
	bestScore := math.Inf(-1)
	bestCombination := []string{}

	combinations := getCombinations(nodes, m)

	for _, combination := range combinations {
		score := getScore(combination, networkMetrics, podRequirements)
		if score > bestScore {
			bestScore = score
			bestCombination = combination
		}
	}

	return bestCombination, bestScore
}

func getCombinations(nodes []string, m int) [][]string {
	var result [][]string
	var current []string
	dfs(nodes, m, 0, current, &result)
	return result
}

func dfs(nodes []string, m, start int, current []string, result *[][]string) {
	if len(current) == m {
		combination := make([]string, m)
		copy(combination, current)
		*result = append(*result, combination)
		return
	}

	for i := start; i < len(nodes); i++ {
		current = append(current, nodes[i])
		dfs(nodes, m, i+1, current, result)
		current = current[:len(current)-1]
	}
}

func getScore(combination []string, networkMetrics *NetworkMetrics, podRequirements [][]float64) float64 {
	score := 0.0
	for i := 0; i < len(combination)-1; i++ {
		curBandwidth := networkMetrics.Bandwidth[combination[i]][combination[i+1]]
		curLatency := networkMetrics.Latency[combination[i]][combination[i+1]]
		klog.V(1).InfoS("Data: ", "combination", combination, "curBandwidth", curBandwidth, "curLatency", curLatency, "podRequirements", podRequirements)

		if curBandwidth < podRequirements[i][0] || curLatency > podRequirements[i][2] {
			return 0
		} else if curBandwidth >= podRequirements[i][1] || curLatency <= podRequirements[i][3] {
			score += 200
		} else {
			var bandwidthScore float64
			var latencyScore float64
			if curBandwidth >= podRequirements[i][1] {
				bandwidthScore = 100.0
			} else {
				bandwidthScore = (curBandwidth - podRequirements[i][0]) / (podRequirements[i][1] - podRequirements[i][0]) * 100
			}
			if curLatency < podRequirements[i][3] {
				latencyScore = 100.0
			} else {
				latencyScore = (curBandwidth - podRequirements[i][3]) / (podRequirements[i][2] - podRequirements[i][3]) * 100
			}
			klog.V(1).InfoS("Score", "1", bandwidthScore+latencyScore)
			score += bandwidthScore + latencyScore
		}
	}
	return score
}
