package blockallscheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "BlockAllScheduler"

// BlockAllScheduler is a plugin that blocks all scheduling attempts
type BlockAllScheduler struct {
	handle framework.Handle
}

var _ framework.PreFilterPlugin = &BlockAllScheduler{}

// Name returns name of the plugin
func (pl *BlockAllScheduler) Name() string {
	return Name
}

// PreFilter invoked at the prefilter extension point.
func (pl *BlockAllScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	klog.V(1).InfoS("BlockAllScheduler: PreFilter called", "pod", klog.KObj(pod))
	
	// Log more details about the pod
	klog.V(2).InfoS("BlockAllScheduler: Pod details", 
		"podName", pod.Name, 
		"podNamespace", pod.Namespace, 
		"podUID", pod.UID,
		"podResourceRequests", pod.Spec.Containers[0].Resources.Requests)

	status := framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %v/%v blocked by BlockAllScheduler", pod.Namespace, pod.Name))
	klog.V(1).InfoS("BlockAllScheduler: Blocking pod", "pod", klog.KObj(pod), "status", status)
	
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