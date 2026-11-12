package rollout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/apisix"

	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/dynamic/dynamiclister"
	"k8s.io/utils/pointer"
	"k8s.io/utils/ptr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/rollout/mocks"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/alb"
	apisixMocks "github.com/argoproj/argo-rollouts/rollout/trafficrouting/apisix/mocks"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/appmesh"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/istio"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/nginx"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/smi"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/traefik"
	traefikMocks "github.com/argoproj/argo-rollouts/rollout/trafficrouting/traefik/mocks"
	testutil "github.com/argoproj/argo-rollouts/test/util"
	"github.com/argoproj/argo-rollouts/utils/aws"
	awsmocks "github.com/argoproj/argo-rollouts/utils/aws/mocks"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	ingressutil "github.com/argoproj/argo-rollouts/utils/ingress"
	istioutil "github.com/argoproj/argo-rollouts/utils/istio"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	unstructuredutil "github.com/argoproj/argo-rollouts/utils/unstructured"
)

// newFakeTrafficRoutingReconciler returns a fake TrafficRoutingReconciler with mocked success return values
func newFakeSingleTrafficRoutingReconciler() *mocks.TrafficRoutingReconciler {
	trafficRoutingReconciler := mocks.TrafficRoutingReconciler{}
	trafficRoutingReconciler.On("Type").Return("fake")
	trafficRoutingReconciler.On("SetWeight", mock.Anything, mock.Anything).Return(nil)
	trafficRoutingReconciler.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	trafficRoutingReconciler.On("SetMirrorRoute", mock.Anything, mock.Anything).Return(nil)
	trafficRoutingReconciler.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	trafficRoutingReconciler.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	trafficRoutingReconciler.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	return &trafficRoutingReconciler
}

// newUnmockedFakeTrafficRoutingReconciler returns a fake TrafficRoutingReconciler with unmocked
// methods (except Type() mocked)
func newUnmockedFakeTrafficRoutingReconciler() *mocks.TrafficRoutingReconciler {
	trafficRoutingReconciler := mocks.TrafficRoutingReconciler{}
	trafficRoutingReconciler.On("Type").Return("fake")
	return &trafficRoutingReconciler
}

func newTrafficWeightFixture(t *testing.T) (*fixture, *v1alpha1.Rollout) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)
	return f, r2
}

func TestReconcileTrafficRoutingSetWeightErr(t *testing.T) {
	f, ro := newTrafficWeightFixture(t)
	defer f.Close()
	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(errors.New("Error message"))
	f.runExpectError(getKey(ro, t), true)
}

// verify error is not returned when VerifyWeight returns error (so that we can continue reconciling)
func TestReconcileTrafficRoutingVerifyWeightErr(t *testing.T) {
	f, ro := newTrafficWeightFixture(t)
	defer f.Close()
	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](false), errors.New("Error message"))
	f.expectPatchRolloutAction(ro)
	f.run(getKey(ro, t))
}

// verify we requeue when VerifyWeight returns false
func TestReconcileTrafficRoutingVerifyWeightFalse(t *testing.T) {
	f, ro := newTrafficWeightFixture(t)
	defer f.Close()
	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](false), nil)
	c, i, k8sI := f.newController(noResyncPeriodFunc)
	enqueued := false
	c.enqueueRolloutAfter = func(obj any, duration time.Duration) {
		enqueued = true
	}
	f.expectPatchRolloutAction(ro)
	f.runController(getKey(ro, t), true, false, c, i, k8sI)
	assert.True(t, enqueued)
}

func TestReconcileTrafficRoutingVerifyWeightEndOfRollout(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](2), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 10, 10)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(100), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](false), nil)
	f.runExpectError(getKey(r2, t), true)
}

func TestRolloutUseDesiredWeight(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	progressingCondition, _ := newProgressingCondition(conditions.RolloutPausedReason, r2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)

	pausedCondition, _ := newPausedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, pausedCondition)

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, true)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(10), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r2, t))
}

func TestRolloutUseDesiredWeight100(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](2), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 10, 10)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(100), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r2, t))
}

func TestRolloutWithExperimentStep(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
		{
			Experiment: &v1alpha1.RolloutExperimentStep{
				Templates: []v1alpha1.RolloutExperimentTemplate{{
					Name:     "experiment-template",
					SpecRef:  "canary",
					Replicas: ptr.To[int32](1),
					Weight:   ptr.To[int32](5),
				},
					{
						Name:     "experiment-template-without-weight",
						SpecRef:  "stable",
						Replicas: ptr.To[int32](1),
					}},
			},
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{SMI: &v1alpha1.SMITrafficRouting{}}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)
	ex, _ := GetExperimentFromTemplate(r1, rs1, rs2)
	ex.Status.TemplateStatuses = []v1alpha1.TemplateStatus{{
		Name:            "experiment-template",
		ServiceName:     "experiment-service",
		PodTemplateHash: rs2PodHash,
	},
		{
			Name:            "experiment-template-without-weight",
			ServiceName:     "experiment-service-without-weight",
			PodTemplateHash: rs2PodHash,
		}}
	r2.Status.Canary.CurrentExperiment = ex.Name

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, ex)

	f.expectPatchRolloutAction(r2)

	t.Run("Experiment Running - WeightDestination created", func(t *testing.T) {
		ex.Status.Phase = v1alpha1.AnalysisPhaseRunning
		f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
		f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, weightDestinations ...v1alpha1.WeightDestination) error {
			// make sure SetWeight was called with correct value
			assert.Equal(t, int32(10), desiredWeight)
			assert.Equal(t, int32(5), weightDestinations[0].Weight)
			assert.Len(t, weightDestinations, 1)
			assert.Equal(t, ex.Status.TemplateStatuses[0].ServiceName, weightDestinations[0].ServiceName)
			assert.Equal(t, ex.Status.TemplateStatuses[0].PodTemplateHash, weightDestinations[0].PodTemplateHash)
			return nil
		})
		f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("VerifyWeight", mock.Anything, mock.Anything).Return(ptr.To[bool](true), nil)
		f.run(getKey(r2, t))
	})

	t.Run("Experiment Pending - no WeightDestination created", func(t *testing.T) {
		ex.Status.Phase = v1alpha1.AnalysisPhasePending
		f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
		f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, weightDestinations ...v1alpha1.WeightDestination) error {
			// make sure SetWeight was called with correct value
			assert.Equal(t, int32(10), desiredWeight)
			assert.Len(t, weightDestinations, 0)
			return nil
		})
		f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("VerifyWeight", mock.Anything, mock.Anything).Return(ptr.To[bool](true), nil)
		f.run(getKey(r2, t))
	})
}

func TestRolloutUsePreviousSetWeight(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
		{
			SetWeight: ptr.To[int32](20),
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectUpdateReplicaSetAction(rs2)
	f.expectPatchRolloutAction(r2)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(10), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything, mock.Anything).Return(ptr.To[bool](true), nil)
	f.fakeTrafficRouting.On("error patching alb ingress", mock.Anything, mock.Anything).Return(true, nil)
	f.run(getKey(r2, t))
}

func TestRolloutUseDynamicWeightOnPromoteFull(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](5),
		},
		{
			SetWeight: ptr.To[int32](25),
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r2 := bumpVersion(r1)
	r2.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r2.Spec.Strategy.Canary.CanaryService = "canary"
	r2.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 5, 5)
	rs2 := newReplicaSetWithStatus(r2, 10, 5)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r2)
	stableSvc := newService("stable", 80, stableSelector, r2)

	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          5,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          95,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	r2 = updateCanaryRolloutStatus(r2, rs1PodHash, 15, 0, 10, false)
	r2.Status.PromoteFull = true
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectUpdateReplicaSetAction(rs2)
	f.expectPatchRolloutAction(r2)

	t.Run("DynamicStableScale true", func(t *testing.T) {
		r2.Spec.Strategy.Canary.DynamicStableScale = true
		f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
		f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
			assert.Equal(t, int32(50), desiredWeight)
			return nil
		})
		f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
		f.run(getKey(r2, t))
	})

	t.Run("DynamicStableScale false", func(t *testing.T) {
		r2.Spec.Strategy.Canary.DynamicStableScale = false
		f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
		f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
			assert.Equal(t, int32(5), desiredWeight)
			return nil
		})
		f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
		f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
		f.run(getKey(r2, t))
	})
}

func TestRolloutSetWeightToZeroWhenFullyRolledOut(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"

	rs1 := newReplicaSetWithStatus(r1, 10, 10)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)

	f.kubeobjects = append(f.kubeobjects, rs1, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1)

	r1 = updateCanaryRolloutStatus(r1, rs1PodHash, 10, 0, 10, false)
	f.rolloutLister = append(f.rolloutLister, r1)
	f.objects = append(f.objects, r1)

	f.expectPatchRolloutAction(r1)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(0), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r1, t))
}

func TestNewTrafficRoutingReconciler(t *testing.T) {
	rc := Controller{}
	dynamicInformerFactory := dynamicinformer.NewDynamicSharedInformerFactory(testutil.NewFakeDynamicClient(), 0)
	vsvcGVR := istioutil.GetIstioVirtualServiceGVR()
	druleGVR := istioutil.GetIstioDestinationRuleGVR()
	rc.IstioController = &istio.IstioController{}
	rc.IstioController.VirtualServiceInformer = dynamicInformerFactory.ForResource(vsvcGVR).Informer()
	rc.IstioController.DestinationRuleInformer = dynamicInformerFactory.ForResource(druleGVR).Informer()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](10),
		},
	}

	{
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconciler, err := rc.NewTrafficRoutingReconciler(roCtx)
		assert.Nil(t, err)
		assert.Nil(t, networkReconciler)
	}
	{
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconciler, err := rc.NewTrafficRoutingReconciler(roCtx)
		assert.Nil(t, err)
		assert.Nil(t, networkReconciler)
	}
	{
		// Without istioVirtualServiceLister
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Istio: &v1alpha1.IstioTrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := rc.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, istio.Type, networkReconciler.Type())
		}
	}
	{
		// With istioVirtualServiceLister
		stopCh := make(chan struct{})
		dynamicInformerFactory.Start(stopCh)
		dynamicInformerFactory.WaitForCacheSync(stopCh)
		close(stopCh)
		rc.IstioController.VirtualServiceLister = dynamiclister.New(rc.IstioController.VirtualServiceInformer.GetIndexer(), vsvcGVR)
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Istio: &v1alpha1.IstioTrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := rc.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, istio.Type, networkReconciler.Type())
		}
	}
	{
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Nginx: &v1alpha1.NginxTrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := rc.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, nginx.Type, networkReconciler.Type())
		}
	}
	{
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			ALB: &v1alpha1.ALBTrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := rc.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, alb.Type, networkReconciler.Type())
		}
	}
	{
		tsController := Controller{}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			SMI: &v1alpha1.SMITrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, smi.Type, networkReconciler.Type())
		}
	}
	{
		tsController := Controller{}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			AppMesh: &v1alpha1.AppMeshTrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, appmesh.Type, networkReconciler.Type())
		}
	}
	{
		tsController := Controller{
			reconcilerBase: reconcilerBase{
				dynamicclientset: &traefikMocks.FakeDynamicClient{},
			},
		}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Traefik: &v1alpha1.TraefikTrafficRouting{
				WeightedTraefikServiceName: "traefik-service",
			},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, traefik.Type, networkReconciler.Type())
		}
	}
	{
		tsController := Controller{
			reconcilerBase: reconcilerBase{
				dynamicclientset: &apisixMocks.FakeDynamicClient{},
			},
		}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Apisix: &v1alpha1.ApisixTrafficRouting{
				Route: &v1alpha1.ApisixRoute{
					Name: "apisix-route",
				},
			},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for _, networkReconciler := range networkReconcilerList {
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
			assert.Equal(t, apisix.Type, networkReconciler.Type())
		}
	}
	{
		// (2) Multiple Reconcilers (Nginx + SMI)
		tsController := Controller{}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			Nginx: &v1alpha1.NginxTrafficRouting{},
			SMI:   &v1alpha1.SMITrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for position, networkReconciler := range networkReconcilerList {
			if position == 0 {
				assert.Equal(t, nginx.Type, networkReconciler.Type())
			} else if position == 1 {
				assert.Equal(t, smi.Type, networkReconciler.Type())
			}
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
		}
	}
	{
		// (3) Multiple Reconcilers (ALB + Nginx + SMI)
		tsController := Controller{}
		r := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
		r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			ALB:   &v1alpha1.ALBTrafficRouting{},
			Nginx: &v1alpha1.NginxTrafficRouting{},
			SMI:   &v1alpha1.SMITrafficRouting{},
		}
		roCtx := &rolloutContext{
			rollout:      r,
			log:          logutil.WithRollout(r),
			pauseContext: &pauseContext{rollout: r},
		}
		networkReconcilerList, err := tsController.NewTrafficRoutingReconciler(roCtx)
		for position, networkReconciler := range networkReconcilerList {
			if position == 0 {
				assert.Equal(t, nginx.Type, networkReconciler.Type())
			} else if position == 1 {
				assert.Equal(t, alb.Type, networkReconciler.Type())
			} else if position == 2 {
				assert.Equal(t, smi.Type, networkReconciler.Type())
			}
			assert.Nil(t, err)
			assert.NotNil(t, networkReconciler)
		}
	}
}

// Verifies with a canary using traffic routing, we add a scaledown delay to the old ReplicaSet
// after promoting desired ReplicaSet to stable.
// NOTE: As of v1.1, scale down delays are added to  ReplicaSets on *subsequent* reconciliations
// after the desired RS has been promoted to stable
func TestCanaryWithTrafficRoutingAddScaleDownDelay(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r1 := newCanaryRollout("foo", 1, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r2 := bumpVersion(r1)
	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 2, 1, 2, false)
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.CurrentStepIndex = ptr.To[int32](1)
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	_, r2.Status.Canary.Weights = calculateWeightStatus(r2, rs2PodHash, rs2PodHash, 0)
	// Duration is already completed so reconciliation makes no rollout status patch
	finishedAt := timeutil.MetaNow()
	promoted := v1alpha1.CompletionStatusPromoted
	r2.Status.Duration.FinishedAt = &finishedAt
	r2.Status.Duration.CompletionStatus = &promoted

	selector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	canarySvc := newService("canary", 80, selector, r2)
	stableSvc := newService("stable", 80, selector, r2)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	rs1Patch := f.expectPatchReplicaSetAction(rs1) // set scale-down-deadline annotation
	f.run(getKey(r2, t))

	f.verifyPatchedReplicaSet(rs1Patch, 30)
}

// Verifies with a canary using traffic routing, we scale down old ReplicaSets which exceed our limit
// after promoting desired ReplicaSet to stable
func TestCanaryWithTrafficRoutingScaleDownLimit(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	inTheFuture := timeutil.MetaNow().Add(10 * time.Second).UTC().Format(time.RFC3339)

	r1 := newCanaryRollout("foo", 1, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	rs1.Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey] = inTheFuture
	r1.Spec.Strategy.Canary.ScaleDownDelayRevisionLimit = ptr.To[int32](1)
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}

	r2 := bumpVersion(r1)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)
	rs2.Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey] = inTheFuture

	r3 := bumpVersion(r2)
	rs3 := newReplicaSetWithStatus(r3, 1, 1)
	rs3PodHash := rs3.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	r3 = updateCanaryRolloutStatus(r3, rs3PodHash, 2, 2, 2, false)

	r3.Status.ObservedGeneration = strconv.Itoa(int(r3.Generation))
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs3PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs3PodHash}
	canarySvc := newService("canary", 80, canarySelector, r3)
	stableSvc := newService("stable", 80, stableSelector, r3)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, rs3, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2, rs3)
	f.rolloutLister = append(f.rolloutLister, r3)
	f.objects = append(f.objects, r3)

	rs1ScaleDownIndex := f.expectUpdateReplicaSetAction(rs1) // scale down ReplicaSet
	_ = f.expectPatchRolloutAction(r3)                       // updates the rollout status
	f.run(getKey(r3, t))

	rs1Updated := f.getUpdatedReplicaSet(rs1ScaleDownIndex)
	assert.Equal(t, int32(0), *rs1Updated.Spec.Replicas)
	_, ok := rs1Updated.Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey]
	assert.False(t, ok, "annotation not removed")
}

// TestDynamicScalingDontIncreaseWeightWhenAborted verifies we don't increase the traffic weight if
// we are aborted, using dynamic scaling, and available stable replicas is less than desired
func TestDynamicScalingDontIncreaseWeightWhenAborted(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](50),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 5, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.ReadyReplicas = 4
	r1.Status.AvailableReplicas = 4
	r1.Status.Abort = true
	r1.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 5, 4) // have less available than desired to test calculation
	rs2 := newReplicaSetWithStatus(r2, 0, 0)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          0,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          100,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)
	f.expectPatchServiceAction(canarySvc, rs1PodHash)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(0), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r1, t))
}

// TestDynamicScalingDecreaseWeightAccordingToStepWeightsAvailabilityWhenAborted verifies we decrease the weight
// to the canary depending on the weight in previous Step when aborting
func TestDynamicScalingDecreaseWeightAccordingToStepWeightsAvailabilityWhenAborted(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](50),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 5, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.ReadyReplicas = 5
	r1.Status.AvailableReplicas = 5
	r1.Status.Abort = true
	r1.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 5, 3)
	rs2 := newReplicaSetWithStatus(r2, 4, 4)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          100,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          0,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)
	f.expectUpdateReplicaSetAction(rs1)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(50), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(pointer.BoolPtr(true), nil)
	f.run(getKey(r1, t))
}

// TestDynamicScalingDecreaseWeightAccordingToStableAvailabilityWhenAborted verifies we decrease the weight
// to the canary depending on the availability of the stable ReplicaSet when aborting
func TestDynamicScalingDecreaseWeightAccordingToStableAvailabilityWhenAborted(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 5, nil, steps, pointer.Int32Ptr(1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.ReadyReplicas = 5
	r1.Status.AvailableReplicas = 5
	r1.Status.Abort = true
	r1.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 5, 1)
	rs2 := newReplicaSetWithStatus(r2, 4, 4)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          100,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          0,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(80), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r1, t))
}

// TestDynamicScalingDecreaseWeightAccordingToStableAvailabilityWhenAbortedAndResetService verifies we decrease the weight
// to the canary depending on the availability of the stable ReplicaSet when aborting and also that at the end of the abort
// we reset the canary service selectors back to the stable service
func TestDynamicScalingDecreaseWeightAccordingToStableAvailabilityWhenAbortedAndResetService(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](50),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 5, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.ReadyReplicas = 5
	r1.Status.AvailableReplicas = 5
	r1.Status.Abort = true
	r1.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 5, 5)
	rs2 := newReplicaSetWithStatus(r2, 0, 0)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          20,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          80,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)
	f.expectPatchServiceAction(canarySvc, rs1PodHash)

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was called with correct value
		assert.Equal(t, int32(0), desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r1, t))
}

func TestRolloutReplicaIsAvailableAndGenerationNotBeModifiedShouldModifyVirtualServiceSHeaderRoute(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	steps := []v1alpha1.CanaryStep{
		{
			SetCanaryScale: &v1alpha1.SetCanaryScale{
				Replicas: ptr.To[int32](1),
			},
		},
		{
			SetHeaderRoute: &v1alpha1.SetHeaderRoute{
				Name: "test-header",
				Match: []v1alpha1.HeaderRoutingMatch{
					{
						HeaderName: "test",
						HeaderValue: &v1alpha1.StringMatch{
							Prefix: "test",
						},
					},
				},
			},
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 1, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	vs := istio.VirtualService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: r1.Namespace,
		},
		Spec: istio.VirtualServiceSpec{
			HTTP: []istio.VirtualServiceHTTPRoute{{
				Name: "primary",
				Route: []istio.VirtualServiceRouteDestination{{
					Destination: istio.VirtualServiceDestination{
						Host: "stable",
						//Port: &istio.Port{
						//	Number: 80,
						//},
					},
					Weight: 100,
				}, {
					Destination: istio.VirtualServiceDestination{
						Host: "canary",
						//Port: &istio.Port{
						//	Number: 80,
						//},
					},
					Weight: 0,
				}},
			}},
		},
	}
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		Istio: &v1alpha1.IstioTrafficRouting{
			VirtualService: &v1alpha1.IstioVirtualService{
				Name: "test",
				Routes: []string{
					"primary",
				},
			},
		},
		ManagedRoutes: []v1alpha1.MangedRoutes{
			{
				Name: "test-header",
			},
		},
	}
	r1.Spec.WorkloadRef = &v1alpha1.ObjectRef{
		Name:       "test",
		APIVersion: "apps/v1",
		Kind:       "Deployment",
	}
	r1.Spec.SelectorResolvedFromRef = false
	r1.Spec.TemplateResolvedFromRef = true
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Labels = map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: "testsha"}
	r2 := bumpVersion(r1)

	// if set WorkloadRef it does not change the generation
	r2.ObjectMeta.Generation = r2.ObjectMeta.Generation - 1

	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	rs2 := newReplicaSetWithStatus(r2, 1, 1)
	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          0,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          100,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	mapObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&vs)
	assert.Nil(t, err)

	unstructuredObj := &unstructured.Unstructured{Object: mapObj}
	unstructuredObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   istioutil.GetIstioVirtualServiceGVR().Group,
		Version: istioutil.GetIstioVirtualServiceGVR().Version,
		Kind:    "VirtualService",
	})

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.serviceLister = append(f.serviceLister, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, unstructuredObj)

	// remarshalRollout strips the pod template for TemplateResolvedFromRef rollouts. In production the
	// real workload ref resolver repopulates it from the referenced workload; mimic that here so the
	// controller recognizes rs2 as the new ReplicaSet instead of creating a templateless one.
	resolvedTemplate := *r2.Spec.Template.DeepCopy()
	f.workloadRefResolveFn = func(ro *v1alpha1.Rollout) error {
		ro.Spec.SetResolvedTemplate(*resolvedTemplate.DeepCopy())
		return nil
	}

	f.expectPatchRolloutAction(r2)
	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.fakeTrafficRouting.On("SetHeaderRoute", &v1alpha1.SetHeaderRoute{
		Name: "test-header",
		Match: []v1alpha1.HeaderRoutingMatch{
			{
				HeaderName: "test",
				HeaderValue: &v1alpha1.StringMatch{
					Prefix: "test",
				},
			},
		},
	}).Once().Return(nil)
	f.run(getKey(r1, t))

	// Verify SetHeaderRoute was actually invoked (the whole point of this test). Without this the
	// .Once() expectation is never checked, since the fixture does not assert mock expectations.
	f.fakeTrafficRouting.AssertExpectations(t)
}

// This makes sure we don't set weight to zero if we are rolling back to stable with DynamicStableScale
func TestDontWeightToZeroWhenDynamicallyRollingBackToStable(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](90),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.ReadyReplicas = 10
	r1.Status.AvailableReplicas = 10
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	rs2 := newReplicaSetWithStatus(r2, 9, 9)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)

	// simulate rollback to stable
	r2.Spec = r1.Spec
	r2.Status.StableRS = rs1PodHash
	r2.Status.CurrentPodHash = rs1PodHash // will cause IsFullyPromoted() to be true
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          10,
			ServiceName:     "canary",
			PodTemplateHash: rs2PodHash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          90,
			ServiceName:     "stable",
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectUpdateReplicaSetAction(rs1)                 // Updates the revision annotation from 1 to 3 from func isScalingEvent
	f.expectUpdateRolloutAction(r2)                     // Update the rollout revision from 1 to 3
	scaleUpIndex := f.expectUpdateReplicaSetAction(rs1) // Scale The replicaset from 1 to 10 from func scaleReplicaSet
	f.expectPatchRolloutAction(r2)                      // Updates the rollout status from the scaling to 10 action

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(func(canaryHash, stableHash string, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure UpdateHash was called with previous desired hash (not current pod hash)
		if canaryHash != rs2PodHash {
			return fmt.Errorf("UpdateHash was called with canary hash: %s. Expected: %s", canaryHash, rs2PodHash)
		}
		if stableHash != rs1PodHash {
			return fmt.Errorf("UpdateHash was called with stable hash: %s. Expected: %s", canaryHash, rs1PodHash)
		}
		return nil

	})
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// make sure SetWeight was not changed
		if desiredWeight != 10 {
			return fmt.Errorf("SetWeight was called with unexpected weight: %d. Expected: 10", desiredWeight)
		}
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)
	f.run(getKey(r1, t))

	// Make sure we scale up stable ReplicaSet to 10
	rs1Updated := f.getUpdatedReplicaSet(scaleUpIndex)
	assert.Equal(t, int32(10), *rs1Updated.Spec.Replicas)
}

// TestDontWeightOrHaveManagedRoutesDuringInterruptedUpdate builds off of TestCanaryDontScaleDownOldRsDuringInterruptedUpdate
// in canary_test when we scale down an intermediate V2 ReplicaSet when applying a V3 spec in the middle of updating.
// We want to make sure that traffic routing is cleared in both weight AND managed routes when the V2 rs is
// nil or has 0 available replicas.
func TestDontWeightOrHaveManagedRoutesDuringInterruptedUpdate(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetHeaderRoute: &v1alpha1.SetHeaderRoute{
				Name: "test-header",
				Match: []v1alpha1.HeaderRoutingMatch{
					{
						HeaderName: "test",
						HeaderValue: &v1alpha1.StringMatch{
							Exact: "test",
						},
					},
				},
			},
		},
		{
			SetWeight: ptr.To[int32](90),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 5, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		ALB: &v1alpha1.ALBTrafficRouting{
			Ingress: "test-ingress",
		},
		ManagedRoutes: []v1alpha1.MangedRoutes{
			{Name: "test-header"},
		},
	}

	r1.Spec.Strategy.Canary.StableService = "stable-svc"
	r1.Spec.Strategy.Canary.CanaryService = "canary-svc"
	r2 := bumpVersion(r1)
	r3 := bumpVersion(r2)

	stableSvc := newService("stable-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: r1.Status.CurrentPodHash}, r1)
	canarySvc := newService("canary-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: r3.Status.CurrentPodHash}, r3)
	r3.Status.StableRS = r1.Status.CurrentPodHash

	ingress := newIngress("test-ingress", canarySvc, stableSvc)
	ingress.Spec.Rules[0].HTTP.Paths[0].Backend.ServiceName = stableSvc.Name

	rs1 := newReplicaSetWithStatus(r1, 5, 5)
	rs2 := newReplicaSetWithStatus(r2, 5, 5)
	rs3 := newReplicaSetWithStatus(r3, 5, 0)
	r3.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			PodTemplateHash: replicasetutil.GetPodTemplateHash(rs2),
		},
	}

	f.objects = append(f.objects, r3)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, rs3, canarySvc, stableSvc, ingress)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2, rs3)
	f.serviceLister = append(f.serviceLister, canarySvc, stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(ingress))

	f.expectPatchRolloutAction(r3)
	f.run(getKey(r3, t))

	r3.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			PodTemplateHash: replicasetutil.GetPodTemplateHash(rs3),
		},
	}

	f.expectUpdateReplicaSetAction(rs3)
	f.run(getKey(r3, t))

	// Make sure that our weight is zero
	assert.Equal(t, int32(0), r3.Status.Canary.Weights.Canary.Weight)
	assert.Equal(t, replicasetutil.GetPodTemplateHash(rs3), r3.Status.Canary.Weights.Canary.PodTemplateHash)
	// Make sure that RemoveManagedRoutes was called
	f.fakeTrafficRouting.AssertCalled(t, "RemoveManagedRoutes", mock.Anything, mock.Anything)

}

// This test verifies that if we are shifting traffic to stable replicaset without the stable replicaset being available proportional to the weight, the traffic shouldn't be switched immediately to the stable replicaset.
func TestCheckReplicaSetAvailable(t *testing.T) {
	fix := newFixture(t)
	defer fix.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](60),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}

	rollout1 := newCanaryRollout("test-rollout", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	rollout1.Spec.Strategy.Canary.DynamicStableScale = true
	rollout1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	rollout1.Spec.Strategy.Canary.CanaryService = "canary-service"
	rollout1.Spec.Strategy.Canary.StableService = "stable-service"
	rollout1.Status.ReadyReplicas = 10
	rollout1.Status.AvailableReplicas = 10

	rollout2 := bumpVersion(rollout1)

	replicaSet1 := newReplicaSetWithStatus(rollout1, 1, 1)
	replicaSet2 := newReplicaSetWithStatus(rollout2, 9, 9)

	replicaSet1Hash := replicaSet1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	replicaSet2Hash := replicaSet2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: replicaSet2Hash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: replicaSet1Hash}
	canarySvc := newService("canary-service", 80, canarySelector, rollout1)
	stableSvc := newService("stable-service", 80, stableSelector, rollout1)

	rollout2.Spec = rollout1.Spec
	rollout2.Status.StableRS = replicaSet1Hash
	rollout2.Status.CurrentPodHash = replicaSet1Hash
	rollout2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          10,
			ServiceName:     "canary-service",
			PodTemplateHash: replicaSet2Hash,
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          90,
			ServiceName:     "stable-service",
			PodTemplateHash: replicaSet1Hash,
		},
	}

	fix.kubeobjects = append(fix.kubeobjects, replicaSet1, replicaSet2, canarySvc, stableSvc)
	fix.replicaSetLister = append(fix.replicaSetLister, replicaSet1, replicaSet2)

	fix.rolloutLister = append(fix.rolloutLister, rollout2)
	fix.objects = append(fix.objects, rollout2)

	fix.expectUpdateReplicaSetAction(replicaSet1)
	fix.expectUpdateRolloutAction(rollout2)
	fix.expectUpdateReplicaSetAction(replicaSet1)
	fix.expectPatchRolloutAction(rollout2)
	fix.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	fix.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	fix.run(getKey(rollout1, t))
}

func TestCheckReplicasAvailableWithReplicaProgressThreshold(t *testing.T) {
	tests := []struct {
		name              string
		availableReplicas int
		threshold         *v1alpha1.ReplicaProgressThreshold
		expectedResult    bool
		description       string
	}{
		{
			name:              "threshold met - 80% available with 80% threshold",
			availableReplicas: 4,
			threshold: &v1alpha1.ReplicaProgressThreshold{
				Type:  v1alpha1.ProgressTypePercentage,
				Value: 80,
			},
			expectedResult: true,
			description:    "Should return true when threshold is met (4/5 = 80% >= 80%)",
		},
		{
			name:              "threshold not met - 60% available with 80% threshold",
			availableReplicas: 3,
			threshold: &v1alpha1.ReplicaProgressThreshold{
				Type:  v1alpha1.ProgressTypePercentage,
				Value: 80,
			},
			expectedResult: false,
			description:    "Should return false when threshold is not met (3/5 = 60% < 80%)",
		},
		{
			name:              "nil threshold - requires 100% availability",
			availableReplicas: 4,
			threshold:         nil,
			expectedResult:    false,
			description:       "Should return false with nil threshold, requiring 100% availability (4/5 = 80%)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newCanaryRollout("foo", 10, nil, nil, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(0))
			r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
				SMI: &v1alpha1.SMITrafficRouting{},
			}
			r.Spec.Strategy.Canary.ReplicaProgressThreshold = tt.threshold

			roCtx := &rolloutContext{
				rollout:      r,
				log:          logutil.WithRollout(r),
				pauseContext: &pauseContext{rollout: r},
			}

			// Create a ReplicaSet with specified available replicas out of 5
			// For 50% weight on 10 total replicas = 5 desired replicas
			rs := newReplicaSetWithStatus(r, 5, tt.availableReplicas)

			// Check if replicas are available for 50% weight
			result := roCtx.checkReplicasAvailable(rs, 50)

			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

func TestCheckReplicasAvailableWithCustomMaxTrafficWeight(t *testing.T) {
	tests := []struct {
		name              string
		totalReplicas     int32
		availableReplicas int
		desiredWeight     int32
		maxTrafficWeight  int32
		expectedResult    bool
		description       string
	}{
		{
			name:              "custom maxTrafficWeight - replicas available",
			totalReplicas:     2,
			availableReplicas: 1,
			desiredWeight:     50000000,
			maxTrafficWeight:  100000000,
			expectedResult:    true,
			description:       "With maxTrafficWeight=100000000, 50% weight on 2 replicas needs 1 replica (2*50000000/100000000=1)",
		},
		{
			name:              "custom maxTrafficWeight - replicas not available",
			totalReplicas:     2,
			availableReplicas: 0,
			desiredWeight:     50000000,
			maxTrafficWeight:  100000000,
			expectedResult:    false,
			description:       "With maxTrafficWeight=100000000, 50% weight on 2 replicas needs 1 replica but 0 available",
		},
		{
			name:              "custom maxTrafficWeight - high weight needs all replicas",
			totalReplicas:     2,
			availableReplicas: 2,
			desiredWeight:     100000000,
			maxTrafficWeight:  100000000,
			expectedResult:    true,
			description:       "With maxTrafficWeight=100000000, 100% weight on 2 replicas needs 2 replicas",
		},
		{
			name:              "custom maxTrafficWeight - low weight needs zero replicas",
			totalReplicas:     2,
			availableReplicas: 0,
			desiredWeight:     0,
			maxTrafficWeight:  100000000,
			expectedResult:    true,
			description:       "With maxTrafficWeight=100000000, 0% weight needs 0 replicas",
		},
		{
			name:              "custom maxTrafficWeight - weight times replicas exceeds int32",
			totalReplicas:     43,
			availableReplicas: 0,
			desiredWeight:     50000000,
			maxTrafficWeight:  100000000,
			expectedResult:    false,
			description:       "With maxTrafficWeight=100000000, 50% weight on 43 replicas needs 21 replicas but 0 available",
		},
		{
			name:              "default maxTrafficWeight - same behavior as before",
			totalReplicas:     10,
			availableReplicas: 5,
			desiredWeight:     50,
			maxTrafficWeight:  0, // 0 means use default (100)
			expectedResult:    true,
			description:       "With default maxTrafficWeight (100), 50% weight on 10 replicas needs 5 replicas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newCanaryRollout("foo", int(tt.totalReplicas), nil, nil, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(0))
			r.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
				SMI: &v1alpha1.SMITrafficRouting{},
			}
			if tt.maxTrafficWeight > 0 {
				r.Spec.Strategy.Canary.TrafficRouting.MaxTrafficWeight = ptr.To[int32](tt.maxTrafficWeight)
			}

			roCtx := &rolloutContext{
				rollout: r,
				log:     logutil.WithRollout(r),
			}

			rs := newReplicaSetWithStatus(r, int(tt.totalReplicas), tt.availableReplicas)

			result := roCtx.checkReplicasAvailable(rs, tt.desiredWeight)

			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

// TestDynamicScalingAbortWithZeroReplicas verifies we handle zero replicas gracefully during abort
func TestDynamicScalingAbortWithZeroReplicas(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{
			SetWeight: ptr.To[int32](50),
		},
		{
			Pause: &v1alpha1.RolloutPause{},
		},
	}
	r1 := newCanaryRollout("foo", 0, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(1))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r1.Status.Abort = true
	r1.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 0, 0)
	rs2 := newReplicaSetWithStatus(r2, 0, 0)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService("canary", 80, canarySelector, r1)
	stableSvc := newService("stable", 80, stableSelector, r1)
	r2.Status.StableRS = rs1PodHash

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)
	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)

	f.expectPatchRolloutAction(r2)
	f.expectPatchServiceAction(canarySvc, rs1PodHash) // Expect canary service to switch to stable RS

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		// With zero replicas, we should fall back to immediate rollback (weight = 0)
		assert.Equal(t, int32(0), desiredWeight, "Should set weight to 0 when replicas is zero")
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)

	// This should not panic or cause division by zero
	f.run(getKey(r1, t))
}

// TestNewRolloutResetsWeightBeforeUpdatingHash verifies that when a new rollout is triggered
// while one is already in progress with canary weight > 0, the VirtualService weight is reset
// to 0 BEFORE the DestinationRule subsets are updated to point to the new canary.
//
// This is critical because if the order is reversed (hash updated first, then weight reset),
// there's a window where traffic is routed to a canary subset that points to pods that don't
// exist yet (or have 0 replicas).
//
// Scenario from production logs showing the problematic order:
//
//	10:11:25.678 - Created ReplicaSet myservice-5c88b7f59c
//	10:11:25.912 - DestinationRule myservice subset updated (canary: 5c88b7f59c, stable: cff675f55)
//	10:11:25.921 - VirtualService `myservice` set to desiredWeight '0'
//	10:11:25.939 - Traffic weight updated from 5 to 0
//
// The correct order should be:
//  1. SetWeight to 0 (stop sending traffic to canary subset)
//  2. UpdateHash (update DestinationRule to point to new canary pods)
//  3. Scale up new canary pods
//  4. SetWeight to desired canary weight
func TestNewRolloutResetsWeightBeforeUpdatingHash(t *testing.T) {
	const (
		canaryService = "myservice-canary"
		stableService = "myservice-stable"
	)

	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{SetWeight: ptr.To[int32](5)},
		{Pause: &v1alpha1.RolloutPause{}},
		{SetWeight: ptr.To[int32](50)},
		{Pause: &v1alpha1.RolloutPause{}},
	}

	// r1 is the stable version
	r1 := newCanaryRollout("myservice", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		// Use a generic traffic routing config (SMI) that uses the mock reconciler
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = canaryService
	r1.Spec.Strategy.Canary.StableService = stableService

	// r2 is the "previous canary" - rollout in progress at 5% weight
	r2 := bumpVersion(r1)

	// r3 is the "new canary" - just triggered, should reset to 0%
	r3 := bumpVersion(r2)

	rs1 := newReplicaSetWithStatus(r1, 10, 10) // stable - fully available
	rs2 := newReplicaSetWithStatus(r2, 1, 1)   // previous canary - 1 replica at 5%
	rs3 := newReplicaSetWithStatus(r3, 0, 0)   // new canary - 0 replicas (just created)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs3PodHash := rs3.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs3PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService(canaryService, 80, canarySelector, r3)
	stableSvc := newService(stableService, 80, stableSelector, r3)

	// Status reflects the previous rollout was in progress at 5% weight
	r3.Status.StableRS = rs1PodHash
	r3.Status.CurrentPodHash = rs3PodHash
	r3.Status.CurrentStepIndex = ptr.To[int32](0) // Reset to beginning
	r3.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          5, // Previous canary weight - this is the bug scenario
			ServiceName:     canaryService,
			PodTemplateHash: rs2PodHash, // Still pointing to previous canary!
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          95,
			ServiceName:     stableService,
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, rs3, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2, rs3)
	f.rolloutLister = append(f.rolloutLister, r3)
	f.objects = append(f.objects, r3)

	// Track the order of operations
	var operationOrder []string

	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()

	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(func(canaryHash, stableHash string, additionalDestinations ...v1alpha1.WeightDestination) error {
		operationOrder = append(operationOrder, fmt.Sprintf("UpdateHash(canary=%s)", canaryHash))
		t.Logf("UpdateHash called: canary=%s, stable=%s", canaryHash, stableHash)
		return nil
	})

	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		operationOrder = append(operationOrder, fmt.Sprintf("SetWeight(%d)", desiredWeight))
		t.Logf("SetWeight called: weight=%d", desiredWeight)
		return nil
	})

	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)

	f.expectPatchRolloutAction(r3)
	f.expectUpdateReplicaSetAction(rs3) // Scale up new canary
	f.run(getKey(r3, t))

	t.Logf("Operation order: %v", operationOrder)
	t.Logf("New canary hash: %s, Previous canary hash: %s", rs3PodHash, rs2PodHash)

	// Find the indices of the key operations (first occurrence of each)
	firstSetWeightZeroIndex := -1
	updateHashNewCanaryIndex := -1

	for i, op := range operationOrder {
		if op == "SetWeight(0)" && firstSetWeightZeroIndex == -1 {
			firstSetWeightZeroIndex = i
		}
		if op == fmt.Sprintf("UpdateHash(canary=%s)", rs3PodHash) && updateHashNewCanaryIndex == -1 {
			updateHashNewCanaryIndex = i
		}
	}

	// CRITICAL ASSERTION: SetWeight(0) must happen BEFORE UpdateHash with new canary
	// If UpdateHash happens first, there's a window where 5% of traffic goes to
	// a canary subset pointing to pods that don't exist yet.
	// Note: SetWeight is idempotent, so it may be called multiple times (before and after UpdateHash).
	assert.NotEqual(t, -1, firstSetWeightZeroIndex,
		"SetWeight(0) should have been called to reset traffic before updating hash")
	assert.NotEqual(t, -1, updateHashNewCanaryIndex,
		"UpdateHash with new canary hash should have been called")

	if firstSetWeightZeroIndex != -1 && updateHashNewCanaryIndex != -1 {
		assert.Less(t, firstSetWeightZeroIndex, updateHashNewCanaryIndex,
			"SetWeight(0) must be called BEFORE UpdateHash with new canary hash to avoid "+
				"routing traffic to non-existent pods. Current order: %v", operationOrder)
	}
}

// TestTrafficRoutingErrorsWhenNewCanaryHasNoReplicas verifies that errors from
// SetWeight and RemoveManagedRoutes are properly propagated when the new canary
// has no available replicas.
func TestTrafficRoutingErrorsWhenNewCanaryHasNoReplicas(t *testing.T) {
	const (
		canaryService = "myservice-canary"
		stableService = "myservice-stable"
	)

	tests := []struct {
		name                   string
		previousCanaryWeights  *v1alpha1.TrafficWeights
		setWeightErr           error
		removeManagedRoutesErr error
		expectedCall           string
	}{
		{
			name: "SetWeight error when resetting canary weight",
			previousCanaryWeights: &v1alpha1.TrafficWeights{
				Canary: v1alpha1.WeightDestination{Weight: 5, ServiceName: canaryService, PodTemplateHash: "oldcanary"},
				Stable: v1alpha1.WeightDestination{Weight: 95, ServiceName: stableService},
			},
			setWeightErr:           fmt.Errorf("failed to set weight: connection refused"),
			removeManagedRoutesErr: nil,
			expectedCall:           "SetWeight",
		},
		{
			name:                   "RemoveManagedRoutes error",
			previousCanaryWeights:  nil,
			setWeightErr:           nil,
			removeManagedRoutesErr: fmt.Errorf("failed to remove managed routes: API server unavailable"),
			expectedCall:           "RemoveManagedRoutes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			defer f.Close()

			steps := []v1alpha1.CanaryStep{
				{SetWeight: ptr.To[int32](5)},
				{Pause: &v1alpha1.RolloutPause{}},
			}

			r1 := newCanaryRollout("myservice", 10, nil, steps, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(0))
			r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
				SMI: &v1alpha1.SMITrafficRouting{},
			}
			r1.Spec.Strategy.Canary.CanaryService = canaryService
			r1.Spec.Strategy.Canary.StableService = stableService

			r2 := bumpVersion(r1)

			rs1 := newReplicaSetWithStatus(r1, 10, 10)
			rs2 := newReplicaSetWithStatus(r2, 0, 0) // New canary with 0 replicas

			rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
			rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

			canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}
			stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
			canarySvc := newService(canaryService, 80, canarySelector, r2)
			stableSvc := newService(stableService, 80, stableSelector, r2)

			r2.Status.StableRS = rs1PodHash
			r2.Status.CurrentPodHash = rs2PodHash
			r2.Status.CurrentStepIndex = ptr.To[int32](0)
			r2.Status.Canary.Weights = tc.previousCanaryWeights

			f.kubeobjects = append(f.kubeobjects, rs1, rs2, canarySvc, stableSvc)
			f.replicaSetLister = append(f.replicaSetLister, rs1, rs2)
			f.rolloutLister = append(f.rolloutLister, r2)
			f.objects = append(f.objects, r2)

			f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
			f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(tc.setWeightErr)
			f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
			f.fakeTrafficRouting.On("RemoveManagedRoutes").Return(tc.removeManagedRoutesErr)
			f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)

			f.runExpectError(getKey(r2, t), true)

			f.fakeTrafficRouting.AssertCalled(t, tc.expectedCall, mock.Anything, mock.Anything)
		})
	}
}

// TestRollbackDestinationRuleBeforeSetWeight is a scenario test for PR 4612: on fast rollback
// (within rollback window), the DestinationRule must be updated before SetWeight so traffic
// does not hit the canary subset at final step weight (e.g. 70%) while still pointing at old subsets.
// Uses subset-level Istio traffic routing (VirtualService + DestinationRule with canary/stable subsets).
func TestRollbackDestinationRuleBeforeSetWeight(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	f.fakeTrafficRouting = nil // use real Istio reconciler so we see "delaying destination rule switch" log

	pause5m := &v1alpha1.RolloutPause{Duration: v1alpha1.DurationFromInt(300)}
	steps := []v1alpha1.CanaryStep{
		{SetWeight: ptr.To[int32](5)},
		{Pause: pause5m},
		{SetWeight: ptr.To[int32](10)},
		{Pause: pause5m},
		{SetWeight: ptr.To[int32](25)},
		{Pause: pause5m},
		{SetWeight: ptr.To[int32](50)},
		{Pause: pause5m},
		{SetWeight: ptr.To[int32](70)},
	}

	// Build 6 revisions: 5 previously successful + current canary (r6 = current rollout)
	r1 := newCanaryRollout("foo", 10, nil, steps, ptr.To[int32](0), intstr.FromInt(1), intstr.FromInt(0))
	r1.Spec.RollbackWindow = &v1alpha1.RollbackWindowSpec{Revisions: 3}
	r2 := bumpVersion(r1)
	r3 := bumpVersion(r2)
	r4 := bumpVersion(r3)
	r5 := bumpVersion(r4)
	r6 := bumpVersion(r5)

	r6.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		Istio: &v1alpha1.IstioTrafficRouting{
			VirtualService: &v1alpha1.IstioVirtualService{Name: "foo-vsvc"},
			DestinationRule: &v1alpha1.IstioDestinationRule{
				Name:             "foo-dr",
				CanarySubsetName: "canary",
				StableSubsetName: "stable",
			},
		},
	}
	// Empty canary/stable service names so UpdateHash runs the "subset-only" availability check (delaying destination rule switch when rs not fully available)
	r6.Spec.Strategy.Canary.CanaryService = ""
	r6.Spec.Strategy.Canary.StableService = ""

	// ReplicaSets for all 6 revisions (stable = rs5, canary = rs6; rs3 is rollback target and must be active)
	rs1 := newReplicaSetWithStatus(r1, 0, 0)
	rs2 := newReplicaSetWithStatus(r2, 0, 0)
	rs3 := newReplicaSetWithStatus(r3, 10, 2) // rollback target, scaled to 10 but only 2 ready (not available yet)
	rs4 := newReplicaSetWithStatus(r4, 0, 0)
	rs5 := newReplicaSetWithStatus(r5, 10, 10) // stable, fully available
	rs6 := newReplicaSetWithStatus(r6, 1, 1)   // canary, e.g. at 5% capacity

	// Rollback to rs3: set spec template to r3 so controller treats rs3 as desired revision (after building RSs so rs6 keeps canary hash)
	r6.Spec.Template = r3.Spec.Template

	stableHash := rs5.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	canaryHash := rs6.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	r6.Status.StableRS = stableHash
	r6.Status.CurrentPodHash = canaryHash
	r6.Status.CurrentStepIndex = ptr.To[int32](0) // 5% step (index 0)
	r6 = updateCanaryRolloutStatus(r6, stableHash, 11, 1, 11, false)

	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: canaryHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: stableHash}
	canarySvc := newService("canary", 80, canarySelector, r6)
	stableSvc := newService("stable", 80, stableSelector, r6)

	// VirtualService and DestinationRule for subset-level traffic routing (required when controller runs)
	fooVsvc := unstructuredutil.StrToUnstructuredUnsafe(`
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: foo-vsvc
  namespace: default
spec:
  hosts:
  - stable
  http:
  - route:
    - destination:
        host: stable
        subset: stable
      weight: 95
    - destination:
        host: stable
        subset: canary
      weight: 5
`)
	fooDR := unstructuredutil.StrToUnstructuredUnsafe(`
apiVersion: networking.istio.io/v1alpha3
kind: DestinationRule
metadata:
  name: foo-dr
  namespace: default
spec:
  host: stable
  subsets:
  - name: stable
    labels: {}
  - name: canary
    labels: {}
`)

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, rs3, rs4, rs5, rs6, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2, rs3, rs4, rs5, rs6)
	f.rolloutLister = append(f.rolloutLister, r6)
	f.objects = append(f.objects, r6)
	f.dynamicOnlyObjects = append(f.dynamicOnlyObjects, fooVsvc, fooDR)
	f.virtualServiceLister = append(f.virtualServiceLister, fooVsvc)

	f.expectUpdateReplicaSetAction(rs3)
	f.expectUpdateRolloutAction(r6)
	f.expectPatchRolloutAction(r6)
	f.expectGetRolloutAction(r6) // re-seed between syncs
	// Sync 2 returns error "delaying destination rule switch" and does not complete, so we do NOT expect update rs6 or second patch.

	assert.Nil(t, f.fakeTrafficRouting, "test must use real Istio reconciler (fakeTrafficRouting=nil)")

	// When we roll back within the rollback window, the controller progresses to the last step (70%).
	// Re-seed the rollout at the last step so the second sync will try to SetWeight(70); we assert we must NOT see that when delaying DR switch.
	stepCount := int32(len(r6.Spec.Strategy.Canary.Steps))
	f.reseedRolloutMutator = func(ro *v1alpha1.Rollout) {
		ro.Status.CurrentStepIndex = &stepCount
	}
	f.allowErrorOnLastSync = true // sync 2 returns "delaying destination rule switch" and does not complete

	prevLog := log.StandardLogger().Out
	defer log.SetOutput(prevLog)
	logBuf := bytes.NewBuffer(nil)
	log.SetOutput(logBuf)

	f.runWithSyncs(getKey(r6, t), 2)

	logOut := logBuf.String()
	assert.True(t, strings.Contains(logOut, "Reconciling TrafficRouting with type 'Istio'"),
		"expected Istio reconciler (UpdateHash) to run on second sync; log: %s", logOut)
	assert.True(t, strings.Contains(logOut, "delaying destination rule switch"),
		"expected UpdateHash to log 'delaying destination rule switch' when rollback target rs3 is not fully available; log: %s", logOut)
	assert.True(t, strings.Contains(logOut, rs3.Name),
		"expected log to mention rollback target ReplicaSet %q; log: %s", rs3.Name, logOut)
	// Production bug: when we delay destination rule switch, we must not call SetWeight (no "Updated VirtualService" event, no desiredWeight '70' in log).
	eventsStr := strings.Join(f.events, " ")
	assert.False(t, strings.Contains(eventsStr, "Updated VirtualService"),
		"when delaying DR switch, must not emit Updated VirtualService event (SetWeight not called); events: %v", f.events)
	assert.False(t, strings.Contains(logOut, "desiredWeight '70'"),
		"when delaying destination rule switch, must not set weight to 70; log: %s", logOut)
}

// TestDynamicStableScaleNewCanarySupersedeShouldNotOverloadStable verifies that superseding an
// in-progress rollout with dynamicStableScale enabled does NOT route 100% of traffic to a
// stable RS that only has a fraction of its pods available.
//
// Scenario from https://github.com/argoproj/argo-rollouts/pull/4564#issuecomment-4015022647:
//   - dynamicStableScale: true
//   - Rolling A→B: stable (A) dynamically scaled to 1/10 pods, canary (B) at 9/10 pods, 90% traffic weight
//   - New canary C triggered (superseding B): C has 0 replicas, 0% traffic
//
// The fix moves the early SetWeight(0) (needed to reset traffic before UpdateHash points the
// destination rule to the new empty canary) so that it runs AFTER the checkReplicasAvailable
// guard. This ensures:
//  1. When stable lacks capacity (this test): checkReplicasAvailable returns early, and neither
//     SetWeight(0) nor UpdateHash is called. Traffic stays on the old canary until stable scales up.
//  2. When stable has capacity: SetWeight(0) runs before UpdateHash, preserving the ordering
//     fix from PR #4564 that prevents Istio 503s.
func TestDynamicStableScaleNewCanarySupersedeShouldNotOverloadStable(t *testing.T) {
	const (
		canaryService = "myservice-canary"
		stableService = "myservice-stable"
	)

	f := newFixture(t)
	defer f.Close()

	steps := []v1alpha1.CanaryStep{
		{SetWeight: ptr.To[int32](90)},
		{Pause: &v1alpha1.RolloutPause{}},
	}

	// r1 is the stable version (A)
	r1 := newCanaryRollout("myservice", 10, nil, steps, ptr.To[int32](1), intstr.FromInt(1), intstr.FromInt(0))
	r1.Spec.Strategy.Canary.DynamicStableScale = true
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		SMI: &v1alpha1.SMITrafficRouting{},
	}
	r1.Spec.Strategy.Canary.CanaryService = canaryService
	r1.Spec.Strategy.Canary.StableService = stableService

	// r2 is the "previous canary" (B) — rollout in progress at 90% weight
	r2 := bumpVersion(r1)

	// r3 is the "new canary" (C) — superseding B, just triggered, 0 replicas
	r3 := bumpVersion(r2)

	// Stable (A): 1 of 10 replicas available — dynamically scaled down because
	// 90% of traffic was being served by the canary (B).
	rs1 := newReplicaSetWithStatus(r1, 1, 1)
	// Old canary (B): 9 of 10 replicas — was carrying 90% of traffic.
	rs2 := newReplicaSetWithStatus(r2, 9, 9)
	// New canary (C): 0 replicas — just triggered, not yet scaled up.
	rs3 := newReplicaSetWithStatus(r3, 0, 0)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs3PodHash := rs3.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	canarySelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs3PodHash}
	stableSelector := map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs1PodHash}
	canarySvc := newService(canaryService, 80, canarySelector, r3)
	stableSvc := newService(stableService, 80, stableSelector, r3)

	// Status: C is the new active canary, A is stable.
	// Weights still reflect the previous rollout state: 90% to B, 10% to A.
	r3.Status.StableRS = rs1PodHash
	r3.Status.CurrentPodHash = rs3PodHash
	r3.Status.CurrentStepIndex = ptr.To[int32](0)
	r3.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{
			Weight:          90,
			ServiceName:     canaryService,
			PodTemplateHash: rs2PodHash, // Still pointing to old canary B
		},
		Stable: v1alpha1.WeightDestination{
			Weight:          10,
			ServiceName:     stableService,
			PodTemplateHash: rs1PodHash,
		},
	}

	f.kubeobjects = append(f.kubeobjects, rs1, rs2, rs3, canarySvc, stableSvc)
	f.replicaSetLister = append(f.replicaSetLister, rs1, rs2, rs3)
	f.rolloutLister = append(f.rolloutLister, r3)
	f.objects = append(f.objects, r3)

	var setWeightCalls []int32
	f.fakeTrafficRouting = newUnmockedFakeTrafficRoutingReconciler()
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		setWeightCalls = append(setWeightCalls, desiredWeight)
		t.Logf("SetWeight called with weight=%d", desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)

	f.expectPatchRolloutAction(r3)
	f.expectUpdateReplicaSetAction(rs3) // Scale up new canary C
	f.run(getKey(r3, t))

	// With dynamicStableScale=true, stable (A) only has 1/10 pods available.
	// checkReplicasAvailable should return early, preventing both SetWeight and
	// UpdateHash from being called. If SetWeight(0) were called, it would route
	// 100% of traffic to a stable RS with only 10% of its pods.
	assert.Empty(t, setWeightCalls,
		"SetWeight should not be called when stable lacks capacity with dynamicStableScale; "+
			"checkReplicasAvailable should return early. Calls observed: %v", setWeightCalls)
	f.fakeTrafficRouting.AssertNotCalled(t, "UpdateHash", mock.Anything, mock.Anything, mock.Anything)
}

// promotionHealthGateStableIPs are the pod IPs of the new stable ReplicaSet in promotionHealthGateTest
var promotionHealthGateStableIPs = []string{"1.2.3.4", "5.6.7.8", "2.4.6.8"}

// promotionHealthGateTest builds the reconcile right after a normal end-of-steps promotion, with ALB
// traffic routing and --aws-verify-target-group on:
//   - the rollout is fully promoted (stableRS is rs2);
//   - the selector switch is done: both the canary and the stable service select rs2;
//   - the weights recorded by the previous reconcile send the max weight to the canary service on
//     rs2, and record rs1 for the stable service: the final weight shift has not happened yet;
//   - the stable service's Endpoints list the pods of rs2, and an ip mode TargetGroupBinding binds
//     the stable service to the stable target group (arn::1234).
//
// Tests adjust the fields, stub DescribeTargetHealth on fakeELB, then call run.
type promotionHealthGateTest struct {
	t       *testing.T
	f       *fixture
	fakeELB *awsmocks.ELBv2APIClient

	ro                            *v1alpha1.Rollout
	rs1, rs2                      *appsv1.ReplicaSet
	rootSvc, canarySvc, stableSvc *corev1.Service
	ingress                       *extensionsv1beta1.Ingress
	endpoints                     *corev1.Endpoints
	pods                          []*corev1.Pod
	tgbs                          []*unstructured.Unstructured
	routerType                    string

	// set by run
	setWeightCalls []int32
	requeues       []time.Duration
	events         []string
}

func newPromotionHealthGateTest(t *testing.T) *promotionHealthGateTest {
	t.Helper()
	defaults.SetVerifyTargetGroup(true)
	t.Cleanup(func() { defaults.SetVerifyTargetGroup(false) })
	fakeELB := &awsmocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(fakeELB)
	t.Cleanup(func() { aws.NewClient = aws.DefaultNewClientFunc })

	f := newFixture(t)
	t.Cleanup(f.Close)

	steps := []v1alpha1.CanaryStep{{SetWeight: ptr.To[int32](10)}}
	r1 := newCanaryRollout("foo", 3, nil, steps, ptr.To[int32](0), intstr.FromString("25%"), intstr.FromString("25%"))
	r1.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		ALB: &v1alpha1.ALBTrafficRouting{
			Ingress:     "ingress",
			RootService: "root",
		},
	}
	r1.Spec.Strategy.Canary.CanaryService = "canary"
	r1.Spec.Strategy.Canary.StableService = "stable"
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 3, 3)
	rs2 := newReplicaSetWithStatus(r2, 3, 3)
	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	// Selector switch done: both services already select rs2.
	rootSvc := newService("root", 80, map[string]string{"app": "foo"}, nil)
	canarySvc := newService("canary", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	stableSvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	ing := newIngress("ingress", canarySvc, stableSvc)

	// Promoted at the end of the steps.
	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 6, 3, 6, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.StableRS = rs2PodHash
	r2.Status.CurrentStepIndex = ptr.To[int32](int32(len(steps)))
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	healthyCondition, _ := newHealthyCondition(false)
	conditions.SetRolloutCondition(&r2.Status, healthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	// Weights recorded by the previous reconcile: canary at max weight on rs2, stable at 0 on rs1.
	r2.Status.Canary.Weights = &v1alpha1.TrafficWeights{
		Canary: v1alpha1.WeightDestination{Weight: 100, ServiceName: "canary", PodTemplateHash: rs2PodHash},
		Stable: v1alpha1.WeightDestination{Weight: 0, ServiceName: "stable", PodTemplateHash: rs1PodHash},
	}

	return &promotionHealthGateTest{
		t:          t,
		f:          f,
		fakeELB:    fakeELB,
		ro:         r2,
		rs1:        rs1,
		rs2:        rs2,
		rootSvc:    rootSvc,
		canarySvc:  canarySvc,
		stableSvc:  stableSvc,
		ingress:    ing,
		endpoints:  newEndpoints("stable", promotionHealthGateStableIPs...),
		pods:       newPodsForReplicaSet(rs2, promotionHealthGateStableIPs...),
		tgbs:       []*unstructured.Unstructured{newTargetGroupBinding("stable")},
		routerType: alb.Type,
	}
}

func (g *promotionHealthGateTest) rs1PodHash() string {
	return g.rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
}

func (g *promotionHealthGateTest) rs2PodHash() string {
	return g.rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
}

// scaleDownPreviousStable scales rs1 to zero, so that the old ReplicaSet scale down has nothing to
// verify: any Endpoints read or AWS call left comes from the promotion health gate
func (g *promotionHealthGateTest) scaleDownPreviousStable() {
	g.rs1.Spec.Replicas = ptr.To[int32](0)
	g.rs1.Status.Replicas = 0
	g.rs1.Status.AvailableReplicas = 0
	g.rs1.Status.ReadyReplicas = 0
}

// run reconciles the rollout once and returns the stored rollout. endpointsGets is the number of
// reads of the stable service's Endpoints: one by the promotion health gate when it verifies the
// target groups, one by the old ReplicaSet scale down verification while rs1 is scaled up.
func (g *promotionHealthGateTest) run(endpointsGets int) *v1alpha1.Rollout {
	g.t.Helper()
	return g.reconcile(endpointsGets, false)
}

// reconcile is run, optionally expecting the reconcile to fail. A failed reconcile skips the status
// sync, so no rollout patch is expected then.
func (g *promotionHealthGateTest) reconcile(endpointsGets int, expectError bool) *v1alpha1.Rollout {
	g.t.Helper()
	f := g.f
	f.rolloutLister = append(f.rolloutLister, g.ro)
	f.objects = append(f.objects, g.ro)
	for _, tgb := range g.tgbs {
		f.objects = append(f.objects, tgb)
	}
	if g.rs1 != nil {
		f.kubeobjects = append(f.kubeobjects, g.rs1)
	}
	f.kubeobjects = append(f.kubeobjects, g.rs2, g.ingress, g.rootSvc, g.canarySvc, g.stableSvc, g.endpoints)
	for _, pod := range g.pods {
		f.kubeobjects = append(f.kubeobjects, pod)
	}
	f.serviceLister = append(f.serviceLister, g.rootSvc, g.canarySvc, g.stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(g.ingress))

	f.fakeTrafficRouting = &mocks.TrafficRoutingReconciler{}
	f.fakeTrafficRouting.On("Type").Return(g.routerType)
	f.fakeTrafficRouting.On("UpdateHash", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetWeight", mock.Anything, mock.Anything).Return(func(desiredWeight int32, additionalDestinations ...v1alpha1.WeightDestination) error {
		g.setWeightCalls = append(g.setWeightCalls, desiredWeight)
		return nil
	})
	f.fakeTrafficRouting.On("SetHeaderRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("SetMirrorRoute", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("RemoveManagedRoutes", mock.Anything, mock.Anything).Return(nil)
	f.fakeTrafficRouting.On("VerifyWeight", mock.Anything).Return(ptr.To[bool](true), nil)

	for n := 0; n < endpointsGets; n++ {
		f.expectGetEndpointsAction(g.endpoints)
	}
	if !expectError {
		f.expectPatchRolloutAction(g.ro)
	}

	c, i, k8sI := f.newController(noResyncPeriodFunc)
	c.enqueueRolloutAfter = func(obj any, duration time.Duration) {
		g.requeues = append(g.requeues, duration)
	}
	f.runController(getKey(g.ro, g.t), true, expectError, c, i, k8sI)
	g.events = fakeRecorderEventMessages(c.recorder)

	ro, err := f.client.ArgoprojV1alpha1().Rollouts(metav1.NamespaceDefault).Get(context.Background(), g.ro.Name, metav1.GetOptions{})
	require.NoError(g.t, err)
	return ro
}

// assertHeld asserts the final weight shift was held by the promotion health gate: the router kept
// the recorded max canary weight, the recorded weights still reference rs1, and a requeue was
// scheduled at the verify retry interval. The hold sets no status message or condition.
func (g *promotionHealthGateTest) assertHeld(ro *v1alpha1.Rollout) {
	t := g.t
	t.Helper()
	assert.Equal(t, []int32{100}, g.setWeightCalls, "router did not keep the recorded max canary weight")
	g.f.fakeTrafficRouting.AssertNotCalled(t, "UpdateHash", mock.Anything, mock.Anything, mock.Anything)
	require.NotNil(t, ro.Status.Canary.Weights)
	assert.Equal(t, int32(100), ro.Status.Canary.Weights.Canary.Weight)
	assert.Equal(t, g.rs1PodHash(), ro.Status.Canary.Weights.Stable.PodTemplateHash,
		"recorded stable weight moved off the previous stable ReplicaSet")
	assert.Contains(t, g.requeues, defaults.GetRolloutVerifyRetryInterval())
	assert.Empty(t, ro.Status.Message)
	for _, cond := range ro.Status.Conditions {
		assert.NotContains(t, []string{conditions.TargetGroupUnverifiedReason, conditions.TargetGroupVerifyErrorReason}, cond.Reason)
	}
}

// assertFinalWeightShift asserts the final weight shift was applied, and the recorded weights moved
// to the new stable ReplicaSet
func (g *promotionHealthGateTest) assertFinalWeightShift(ro *v1alpha1.Rollout) {
	t := g.t
	t.Helper()
	assert.Equal(t, []int32{0}, g.setWeightCalls, "final weight shift not applied")
	require.NotNil(t, ro.Status.Canary.Weights)
	assert.Equal(t, int32(0), ro.Status.Canary.Weights.Canary.Weight)
	assert.Equal(t, int32(100), ro.Status.Canary.Weights.Stable.Weight)
	assert.Equal(t, g.rs2PodHash(), ro.Status.Canary.Weights.Stable.PodTemplateHash)
}

// TestCanaryALBPromotionHoldsFinalWeightShiftWhileStableTargetsInitial reproduces the promotion 503s
// (upstream #4433): on the reconcile right after promotion, the selector switch has already moved the
// stable service to the new pods, whose targets are registered in the stable target group but still
// `initial`. The promotion health gate must hold the final weight shift until they are healthy (ADR-0001).
func TestCanaryALBPromotionHoldsFinalWeightShiftWhileStableTargetsInitial(t *testing.T) {
	g := newPromotionHealthGateTest(t)
	// Every stable endpoint is registered in the stable target group, but none is healthy yet.
	g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
		Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, promotionHealthGateStableIPs...), nil)

	ro := g.run(2)

	assert.NotContains(t, g.setWeightCalls, int32(0),
		"final weight shift applied while stable targets are initial; SetWeight calls: %v", g.setWeightCalls)
	g.assertHeld(ro)
	assert.Contains(t, g.events,
		"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable (TargetGroup arn::1234) not verified: 0/3 endpoints healthy")

	// The old ReplicaSet is not scaled down while held, and stays referenced by the recorded
	// stable weight, so the next reconciles keep it too.
	rs1, err := g.f.kubeclient.AppsV1().ReplicaSets(metav1.NamespaceDefault).Get(context.Background(), g.rs1.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(3), *rs1.Spec.Replicas)
	assert.NotContains(t, rs1.Annotations, v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey)
}

// TestCanaryALBPromotionHealthGate covers the promotion health gate (ADR-0001) on the reconcile
// right after a normal end-of-steps promotion
func TestCanaryALBPromotionHealthGate(t *testing.T) {
	t.Run("AppliedOnceStableTargetsHealthy", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
			Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, promotionHealthGateStableIPs...), nil)

		ro := g.run(2)

		g.assertFinalWeightShift(ro)
		assert.Contains(t, g.events,
			"Normal "+conditions.TargetGroupVerifiedReason+" Service stable (TargetGroup arn::1234) verified: 3 endpoints healthy")
	})

	t.Run("NotEvaluatedAgainAfterFinalWeightShift", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.scaleDownPreviousStable()
		// weights recorded by the reconcile that applied the final weight shift
		g.ro.Status.Canary.Weights = &v1alpha1.TrafficWeights{
			Canary: v1alpha1.WeightDestination{Weight: 0, ServiceName: "canary", PodTemplateHash: g.rs2PodHash()},
			Stable: v1alpha1.WeightDestination{Weight: 100, ServiceName: "stable", PodTemplateHash: g.rs2PodHash()},
		}

		ro := g.run(0)

		g.assertFinalWeightShift(ro)
		g.fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
	})

	t.Run("HeldWhileSomeStableTargetsUnregistered", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
			Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, "1.2.3.4", "5.6.7.8"), nil)

		ro := g.run(2)

		g.assertHeld(ro)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable (TargetGroup arn::1234) not verified: 2/3 endpoints healthy")
	})

	t.Run("HeldWhileOneOfTwoTargetGroupBindingsUnhealthy", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.tgbs = []*unstructured.Unstructured{
			newTargetGroupBindingFor("stable-a", "stable", "arn::healthy", "ip"),
			newTargetGroupBindingFor("stable-b", "stable", "arn::initial", "ip"),
		}
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.MatchedBy(func(in *elbv2.DescribeTargetHealthInput) bool {
			return *in.TargetGroupArn == "arn::healthy"
		})).Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, promotionHealthGateStableIPs...), nil)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.MatchedBy(func(in *elbv2.DescribeTargetHealthInput) bool {
			return *in.TargetGroupArn == "arn::initial"
		})).Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, promotionHealthGateStableIPs...), nil)

		ro := g.run(2)

		g.assertHeld(ro)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable (TargetGroup arn::initial) not verified: 0/3 endpoints healthy")
	})

	t.Run("VerifyErrorLeavesWeightsAndFailsReconcile", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))

		ro := g.reconcile(1, true)

		// the routers are not touched, so they keep the max canary weight set by the previous
		// reconcile, and the recorded weights are unchanged; the failed reconcile is retried
		assert.Empty(t, g.setWeightCalls)
		require.NotNil(t, ro.Status.Canary.Weights)
		assert.Equal(t, g.rs1PodHash(), ro.Status.Canary.Weights.Stable.PodTemplateHash)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupVerifyErrorReason+" Failed to verify Service stable (TargetGroup arn::1234): boom")
	})
}

// TestCanaryALBPromotionHealthGateHoldsOnStaleEndpoints verifies the promotion health gate only
// counts the endpoints of the new stable ReplicaSet. Right after the selector switch, the stable
// service's Endpoints still list the pods of the previous stable ReplicaSet until the endpoints
// controller catches up. Those pods are registered and healthy in the stable target group, so
// counting them would open the gate before the new pods can take traffic.
func TestCanaryALBPromotionHealthGateHoldsOnStaleEndpoints(t *testing.T) {
	oldIPs := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

	t.Run("EndpointsStillListPreviousStablePods", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.endpoints = newEndpoints("stable", oldIPs...)
		g.pods = append(g.pods, newPodsForReplicaSet(g.rs1, oldIPs...)...)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
			Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, oldIPs...), nil)

		ro := g.run(2)

		g.assertHeld(ro)
		// the only DescribeTargetHealth call is the old ReplicaSet scale down verification
		g.fakeELB.AssertNumberOfCalls(t, "DescribeTargetHealth", 1)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable not verified: 0/3 endpoints are pods of ReplicaSet "+g.rs2.Name)
	})

	t.Run("EndpointsListPreviousAndNewStablePods", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		ips := append([]string{oldIPs[0]}, promotionHealthGateStableIPs...)
		g.endpoints = newEndpoints("stable", ips...)
		g.pods = append(g.pods, newPodsForReplicaSet(g.rs1, oldIPs[0])...)
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
			Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, ips...), nil)

		ro := g.run(2)

		g.assertHeld(ro)
		g.fakeELB.AssertNumberOfCalls(t, "DescribeTargetHealth", 1)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable not verified: 3/4 endpoints are pods of ReplicaSet "+g.rs2.Name)
	})

	t.Run("NoEndpoints", func(t *testing.T) {
		g := newPromotionHealthGateTest(t)
		g.endpoints = newEndpoints("stable")
		g.fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).
			Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, oldIPs...), nil)

		ro := g.run(2)

		g.assertHeld(ro)
		g.fakeELB.AssertNumberOfCalls(t, "DescribeTargetHealth", 1)
		assert.Contains(t, g.events,
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable not verified: 0/0 endpoints are pods of ReplicaSet "+g.rs2.Name)
	})
}

// TestCanaryALBPromotionHealthGateNotApplicable verifies the promotion health gate does nothing
// (no AWS call, immediate final weight shift) when it does not apply. The previous stable
// ReplicaSet is scaled down, so the old ReplicaSet scale down makes no AWS call either.
func TestCanaryALBPromotionHealthGateNotApplicable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(g *promotionHealthGateTest)
	}{
		{
			name:  "FlagOff",
			setup: func(g *promotionHealthGateTest) { defaults.SetVerifyTargetGroup(false) },
		},
		{
			name:  "NoTargetGroupBinding",
			setup: func(g *promotionHealthGateTest) { g.tgbs = nil },
		},
		{
			name: "InstanceTargetType",
			setup: func(g *promotionHealthGateTest) {
				g.tgbs = []*unstructured.Unstructured{newTargetGroupBindingFor("stable", "stable", "arn::1234", "instance")}
			},
		},
		{
			name: "PingPong",
			setup: func(g *promotionHealthGateTest) {
				g.ro.Spec.Strategy.Canary.PingPong = &v1alpha1.PingPongSpec{PingService: "stable", PongService: "canary"}
				g.ro.Status.Canary.StablePingPong = v1alpha1.PPPing
			},
		},
		{
			name:  "NonALBRouter",
			setup: func(g *promotionHealthGateTest) { g.routerType = "fake" },
		},
		{
			// promote --full at 10%: the canary is below max weight at promotion
			name: "FullPromotionBelowMaxWeight",
			setup: func(g *promotionHealthGateTest) {
				g.ro.Status.Canary.Weights.Canary.Weight = 10
				g.ro.Status.Canary.Weights.Stable.Weight = 90
			},
		},
		{
			// a rollout with no steps never shifts weight to the canary
			name: "NoSteps",
			setup: func(g *promotionHealthGateTest) {
				g.ro.Spec.Strategy.Canary.Steps = nil
				g.ro.Status.CurrentStepHash = conditions.ComputeStepHash(g.ro)
				g.ro.Status.CurrentStepIndex = nil
				g.ro.Status.Canary.Weights.Canary.Weight = 0
				g.ro.Status.Canary.Weights.Stable.Weight = 100
			},
		},
		{
			// the max weight was recorded for another ReplicaSet than the new stable one, so
			// holding it would not keep traffic on the new stable pods
			name: "RecordedCanaryHashNotNewStable",
			setup: func(g *promotionHealthGateTest) {
				g.ro.Status.Canary.Weights.Canary.PodTemplateHash = "otherhash"
			},
		},
		{
			name:  "NilRecordedWeights",
			setup: func(g *promotionHealthGateTest) { g.ro.Status.Canary.Weights = nil },
		},
		{
			name:  "EmptyRecordedStableHash",
			setup: func(g *promotionHealthGateTest) { g.ro.Status.Canary.Weights.Stable.PodTemplateHash = "" },
		},
		{
			// a single ReplicaSet and no recorded weights
			name: "InitialDeploy",
			setup: func(g *promotionHealthGateTest) {
				g.rs1 = nil
				g.ro.Status.Canary.Weights = nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newPromotionHealthGateTest(t)
			g.scaleDownPreviousStable()
			tc.setup(g)

			ro := g.run(0)

			g.assertFinalWeightShift(ro)
			g.fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
		})
	}

	t.Run("Abort", func(t *testing.T) {
		// aborted while the canary is at max weight: stableRS is still rs1, so the rollout is not
		// promoted and the abort returns all traffic to stable as before
		g := newPromotionHealthGateTest(t)
		g.ro.Status.StableRS = g.rs1PodHash()
		g.ro.Status.Abort = true
		g.ro.Status.AbortedAt = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
		g.rs2 = newReplicaSetWithStatus(g.ro, 0, 0)
		g.stableSvc.Spec.Selector = map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: g.rs1PodHash()}
		g.f.expectPatchServiceAction(g.canarySvc, g.rs1PodHash())

		ro := g.run(0)

		assert.Contains(t, g.setWeightCalls, int32(0))
		assert.NotContains(t, g.setWeightCalls, int32(100))
		require.NotNil(t, ro.Status.Canary.Weights)
		assert.Equal(t, int32(0), ro.Status.Canary.Weights.Canary.Weight)
		g.fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
	})
}
