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
type BlockAllScheduler struct{}

var _ framework.PreFilterPlugin = &BlockAllScheduler{}

// Name returns name of the plugin
func (pl *BlockAllScheduler) Name() string {
	return Name
}

// PreFilter invoked at the prefilter extension point.
func (pl *BlockAllScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	klog.V(3).Infof("Pod %v/%v blocked by BlockAllScheduler", pod.Namespace, pod.Name)
	return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %v/%v blocked by BlockAllScheduler", pod.Namespace, pod.Name))
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *BlockAllScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, f framework.Handle) (framework.Plugin, error) {
	return &BlockAllScheduler{}, nil
}