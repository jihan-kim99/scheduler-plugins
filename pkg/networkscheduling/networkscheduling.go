package networkscheduling

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

const Name = "NetworkScheduling"

type NetworkScheduling struct {
	handle framework.Handle
}

var _ framework.FilterPlugin = &NetworkScheduling{}
var _ framework.ScorePlugin = &NetworkScheduling{}

func (pl *NetworkScheduling) Name() string {
	return Name
}

func (pl *NetworkScheduling) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.V(1).InfoS("NetworkScheduling: Filter called", "pod", pod.Name, "node", nodeInfo.Node().Name)
	// Get node where prev pod exists, and network requirements
	job, workloadStatus, err := getDistributedJobWorkloads(pod.Namespace)
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get DistributedJob: %v", err))
	}

	resource, exists := pod.Labels["resource"]
	if !exists {
		return framework.NewStatus(framework.Success, "")
	}

	var podOrder int
	for i, _ := range workloadStatus.Resource {
		if resource == workloadStatus.Resource[i] {
			podOrder = workloadStatus.Order[i]
		}
	}
	if podOrder == 1 {
		return framework.NewStatus(framework.Success, "")
	}

	var prevNode string
	for i, _ := range workloadStatus.Resource {
		if podOrder-1 == workloadStatus.Order[i] {
			prevNode = workloadStatus.SchedulingInfo[i].NodeName
		}
	}
	if prevNode == "" {
		return framework.NewStatus(framework.Unschedulable, "")
	}

	// Get network cost
	networkMetrics, err := getNetworkMetrics()
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get network metrics: %v", err))
	}

	var idx int
	for i, _ := range job {
		if job[i].Resource == resource {
			idx = i
			break
		}
	}

	if networkMetrics.Bandwidth[prevNode][nodeInfo.Node().Name] >= float64(job[idx].Dependencies[0].Bandwidth.Limits) {
		if networkMetrics.Latency[prevNode][nodeInfo.Node().Name] <= float64(job[idx].Dependencies[0].Latency.Limits) {
			klog.V(1).InfoS("NetworkScheduling: Filter: network satisfied", "pod", pod.Name, "node", nodeInfo.Node().Name, "limitBandwidth", job[idx].Dependencies[0].Bandwidth.Limits, "limitLatency", job[idx].Dependencies[0].Latency.Limits, "curBandwidth", networkMetrics.Bandwidth[prevNode][nodeInfo.Node().Name], "curLatency", networkMetrics.Latency[prevNode][nodeInfo.Node().Name])
			return framework.NewStatus(framework.Success, "")
		} else {
			klog.V(1).InfoS("NetworkScheduling: Filter: Latency too high", "pod", pod.Name, "node", nodeInfo.Node().Name, "limitBandwidth", job[idx].Dependencies[0].Bandwidth.Limits, "limitLatency", job[idx].Dependencies[0].Latency.Limits, "curBandwidth", networkMetrics.Bandwidth[prevNode][nodeInfo.Node().Name], "curLatency", networkMetrics.Latency[prevNode][nodeInfo.Node().Name])
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Latency too high for pod %s", pod.Name))
		}
	} else {
		klog.V(1).InfoS("NetworkScheduling: Filter: Insufficient bandwidth", "pod", pod.Name, "node", nodeInfo.Node().Name, "limitBandwidth", job[idx].Dependencies[0].Bandwidth.Limits, "limitLatency", job[idx].Dependencies[0].Latency.Limits, "curBandwidth", networkMetrics.Bandwidth[prevNode][nodeInfo.Node().Name], "curLatency", networkMetrics.Latency[prevNode][nodeInfo.Node().Name])
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient bandwidth for pod %s", pod.Name))
	}
}

func (pl *NetworkScheduling) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.V(1).InfoS("NetworkScheduling: Score called", "pod", pod.Name, "node", nodeName)
	// Get node where prev pod exists, and network requirements
	job, workloadStatus, err := getDistributedJobWorkloads(pod.Namespace)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("failed to get DistributedJob: %v", err))
	}

	resource, exists := pod.Labels["resource"]
	if !exists {
		return 100, framework.NewStatus(framework.Success, "")
	}

	var podOrder int
	for i, _ := range workloadStatus.Resource {
		if resource == workloadStatus.Resource[i] {
			podOrder = workloadStatus.Order[i]
		}
	}
	if podOrder == 1 {
		return 100, framework.NewStatus(framework.Success, "")
	}

	var prevNode string
	for i, _ := range workloadStatus.Resource {
		if podOrder-1 == workloadStatus.Order[i] {
			prevNode = workloadStatus.SchedulingInfo[i].NodeName
		}
	}
	if prevNode == "" {
		return 0, framework.NewStatus(framework.Success, "")
	}

	// Get network cost
	networkMetrics, err := getNetworkMetrics()
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("failed to get network metrics: %v", err))
	}

	var idx int
	for i, _ := range job {
		if job[i].Resource == resource {
			idx = i
			break
		}
	}

	// Get bandwidth and latency limits from the job
	limitBandwidth := float64(job[idx].Dependencies[0].Bandwidth.Limits)
	limitLatency := float64(job[idx].Dependencies[0].Latency.Limits)

	requestBandwidth := float64(job[idx].Dependencies[0].Bandwidth.Requests)
	// requestLatency := float64(job[idx].Dependencies[0].Latency.Requests)

	// Get current node's bandwidth and latency metrics with the previous node
	curBandwidth := networkMetrics.Bandwidth[prevNode][nodeName]
	curLatency := networkMetrics.Latency[prevNode][nodeName]

	// Check if the node meets latency requirements first
	if curLatency > limitLatency {
		klog.V(1).InfoS("NetworkScheduling: Score: Node latency too high", "pod", pod.Name, "node", nodeName, "curLatency", curLatency, "limitLatency", limitLatency)
		return 0, framework.NewStatus(framework.Success, "")
	} else if curBandwidth < limitBandwidth {
		klog.V(1).InfoS("NetworkScheduling: Score: Node bandwidth too low", "pod", pod.Name, "node", nodeName, "curBandwidth", curBandwidth, "limitBandwidth", limitBandwidth)
		return 0, framework.NewStatus(framework.Success, "")
	}

	// Scoring based on bandwidth:
	// Minimum score (30) if it just meets the limit, maximum score (100) if it's meet the requirement
	score := calculateScore(curBandwidth, limitBandwidth, requestBandwidth)
	klog.V(1).InfoS("NetworkScheduling: Score: Node scored", "pod", pod.Name, "node", nodeName, "score", score, "curBandwidth", curBandwidth, "limitBandwidth", limitBandwidth)
	return score, framework.NewStatus(framework.Success, "")
}

// Helper function to calculate the score based on bandwidth
func calculateScore(curBandwidth, limitBandwidth, requestBandwidth float64) int64 {
	// Scale the score: if curBandwidth = limitBandwidth -> score = 30, if curBandwidth > limitBandwidth -> score = 100
	score := 30 + (curBandwidth-limitBandwidth)/(requestBandwidth-limitBandwidth)*70
	return int64(score)
}

func (pl *NetworkScheduling) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	klog.V(1).InfoS("NetworkScheduling: Initializing plugin")

	// You can log configuration details here if you add any in the future
	klog.V(2).InfoS("NetworkScheduling: Plugin configuration", "config", obj)

	return &NetworkScheduling{
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
	Resource       []string
	Order          []int
	Phase          []string
	SchedulingInfo []PodScheduling
}

func getDistributedJobWorkloads(namespace string) ([]Workload, *WorkloadStatus, error) {
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

	workloadStatusData, ok := distributedJobs.Items[0].Object["status"].(map[string]interface{})["workloadStatuses"].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse workloadStatuses")
	}
	workloadStatus := &WorkloadStatus{
		Resource:       []string{},
		Order:          []int{},
		Phase:          []string{},
		SchedulingInfo: []PodScheduling{},
	}

	for _, w := range workloadStatusData {
		ws := w.(map[string]interface{})
		workloadStatus.Resource = append(workloadStatus.Resource, ws["resource"].(string))
		workloadStatus.Order = append(workloadStatus.Order, int(ws["order"].(int64)))
		workloadStatus.Phase = append(workloadStatus.Phase, ws["phase"].(string))

		if schedulingInfoData, ok := ws["schedulingInfo"].([]interface{}); ok && len(schedulingInfoData) > 0 {
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

			workloadStatus.SchedulingInfo = append(workloadStatus.SchedulingInfo, schedInfo)
		} else {
			workloadStatus.SchedulingInfo = append(workloadStatus.SchedulingInfo, PodScheduling{})
		}
	}

	return workloads, workloadStatus, nil
}
