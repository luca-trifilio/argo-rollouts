package rollout

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8srecord "k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/aws"
	"github.com/argoproj/argo-rollouts/utils/aws/mocks"
	"github.com/argoproj/argo-rollouts/utils/conditions"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	ingressutil "github.com/argoproj/argo-rollouts/utils/ingress"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	unstructuredutil "github.com/argoproj/argo-rollouts/utils/unstructured"
)

func newService(name string, port int, selector map[string]string, ro *v1alpha1.Rollout) *corev1.Service {
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Protocol:   "TCP",
				Port:       int32(port),
				TargetPort: intstr.FromInt(port),
			}},
		},
	}
	if ro != nil {
		s.Annotations = map[string]string{
			v1alpha1.ManagedByRolloutsKey: ro.Name,
		}
	}
	return s
}

func TestGetPreviewAndActiveServices(t *testing.T) {
	f := newFixture(t)
	defer f.Close()
	expActive := newService("active", 80, nil, nil)
	expPreview := newService("preview", 80, nil, nil)
	otherRo := &v1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-ro",
		},
	}
	otherRoSvc := newService("other-svc", 80, nil, otherRo)
	f.kubeobjects = append(f.kubeobjects, expActive, expPreview, otherRoSvc)
	f.serviceLister = append(f.serviceLister, expActive, expPreview, otherRoSvc)
	rollout := newRollout("foo", 1, nil, map[string]string{"foo": "bar"})
	rollout.Spec.Strategy = v1alpha1.RolloutStrategy{
		BlueGreen: &v1alpha1.BlueGreenStrategy{
			PreviewService: "preview",
			ActiveService:  "active",
		},
	}
	f.rolloutLister = append(f.rolloutLister, rollout)
	f.objects = append(f.objects, rollout)

	c, _, _ := f.newController(noResyncPeriodFunc)
	t.Run("Get Both", func(t *testing.T) {
		roCtx, err := c.newRolloutContext(rollout)
		assert.NoError(t, err)
		preview, active, err := roCtx.getPreviewAndActiveServices()
		assert.Nil(t, err)
		assert.Equal(t, expPreview, preview)
		assert.Equal(t, expActive, active)
	})
	t.Run("Preview not found", func(t *testing.T) {
		noPreviewSvcRollout := rollout.DeepCopy()
		noPreviewSvcRollout.Spec.Strategy.BlueGreen.PreviewService = "not-preview"
		_, err := c.newRolloutContext(noPreviewSvcRollout)
		assert.Error(t, err)
	})
	t.Run("Active not found", func(t *testing.T) {
		noActiveSvcRollout := rollout.DeepCopy()
		noActiveSvcRollout.Spec.Strategy.BlueGreen.ActiveService = "not-active"
		_, err := c.newRolloutContext(noActiveSvcRollout)
		assert.Error(t, err)
	})

	t.Run("Invalid Spec: No Active Svc", func(t *testing.T) {
		noActiveSvcRollout := rollout.DeepCopy()
		noActiveSvcRollout.Spec.Strategy.BlueGreen.ActiveService = ""
		roCtx, err := c.newRolloutContext(noActiveSvcRollout)
		assert.Error(t, err)
		assert.Nil(t, roCtx)
	})
}

func TestActiveServiceNotFound(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r := newBlueGreenRollout("foo", 1, nil, "active-svc", "preview-svc")
	r.Status.Conditions = []v1alpha1.RolloutCondition{}
	f.rolloutLister = append(f.rolloutLister, r)
	f.objects = append(f.objects, r)
	previewSvc := newService("preview-svc", 80, nil, r)
	notUsedActiveSvc := newService("active-svc", 80, nil, nil)
	f.kubeobjects = append(f.kubeobjects, previewSvc)
	f.serviceLister = append(f.serviceLister, previewSvc)

	patchIndex := f.expectPatchRolloutAction(r)
	f.runExpectError(getKey(r, t), true)

	patch := f.getPatchedRollout(patchIndex)
	errmsg := "The Rollout \"foo\" is invalid: spec.strategy.blueGreen.activeService: Invalid value: \"active-svc\": service \"active-svc\" not found"
	expectedPatch := `{
			"status": {
				"conditions": [%s],
				"phase": "Degraded",
				"message": "%s: %s"
			}
		}`
	_, pausedCondition := newInvalidSpecCondition(conditions.InvalidSpecReason, notUsedActiveSvc, errmsg)
	assert.JSONEq(t, calculatePatch(r, fmt.Sprintf(expectedPatch, pausedCondition, conditions.InvalidSpecReason, strings.ReplaceAll(errmsg, "\"", "\\\""))), patch)
}

func TestPreviewServiceNotFound(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	r := newBlueGreenRollout("foo", 1, nil, "active-svc", "preview-svc")
	r.Status.Conditions = []v1alpha1.RolloutCondition{}
	f.rolloutLister = append(f.rolloutLister, r)
	f.objects = append(f.objects, r)
	activeSvc := newService("active-svc", 80, nil, nil)
	notUsedPreviewSvc := newService("preview-svc", 80, nil, nil)
	f.kubeobjects = append(f.kubeobjects, activeSvc)

	patchIndex := f.expectPatchRolloutAction(r)
	f.runExpectError(getKey(r, t), true)

	patch := f.getPatchedRollout(patchIndex)
	errmsg := "The Rollout \"foo\" is invalid: spec.strategy.blueGreen.previewService: Invalid value: \"preview-svc\": service \"preview-svc\" not found"
	expectedPatch := `{
			"status": {
				"conditions": [%s],
				"phase": "Degraded",
				"message": "%s: %s"
			}
		}`
	_, pausedCondition := newInvalidSpecCondition(conditions.InvalidSpecReason, notUsedPreviewSvc, errmsg)
	assert.JSONEq(t, calculatePatch(r, fmt.Sprintf(expectedPatch, pausedCondition, conditions.InvalidSpecReason, strings.ReplaceAll(errmsg, "\"", "\\\""))), patch)

}

func newEndpoints(name string, ips ...string) *corev1.Endpoints {
	ep := corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{},
			},
		},
	}
	for _, ip := range ips {
		address := corev1.EndpointAddress{
			IP: ip,
		}
		ep.Subsets[0].Addresses = append(ep.Subsets[0].Addresses, address)
	}
	return &ep
}

func newTargetGroupBinding(name string) *unstructured.Unstructured {
	return newTargetGroupBindingFor(name, name, "arn::1234", "ip")
}

func newIngress(name string, canary, stable *corev1.Service) *extensionsv1beta1.Ingress {
	ingress := extensionsv1beta1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: extensionsv1beta1.IngressSpec{
			Rules: []extensionsv1beta1.IngressRule{
				{
					Host: "fakehost.example.com",
					IngressRuleValue: extensionsv1beta1.IngressRuleValue{
						HTTP: &extensionsv1beta1.HTTPIngressRuleValue{
							Paths: []extensionsv1beta1.HTTPIngressPath{
								{
									Path: "/foo",
									Backend: extensionsv1beta1.IngressBackend{
										ServiceName: "root",
										ServicePort: intstr.FromString("use-annotations"),
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return &ingress
}

// TestBlueGreenAWSVerifyTargetGroupsNotYetReady verifies we don't proceed with setting stable with
// the blue-green strategy until target group verification is successful
func TestBlueGreenAWSVerifyTargetGroupsNotYetReady(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("active")
	ep := newEndpoints("active", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("1.2.3.4"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("5.6.7.8"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("2.4.6.8"), // irrelevant
					Port: ptr.To[int32](81),         // wrong port
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("9.8.7.6"), // irrelevant ip
					Port: ptr.To[int32](80),
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)

	r1 := newBlueGreenRollout("foo", 3, nil, "active", "")
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 3, 3)
	rs2 := newReplicaSetWithStatus(r2, 3, 3)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	svc := newService("active", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	r2 = updateBlueGreenRolloutStatus(r2, "", rs2PodHash, rs1PodHash, 3, 3, 6, 3, false, true, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	completedHealthyCondition, _ := newHealthyCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedHealthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(false)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, svc, ep)
	f.serviceLister = append(f.serviceLister, svc)

	f.expectGetEndpointsAction(ep)
	patchIndex := f.expectPatchRolloutAction(r2) // update status message
	f.run(getKey(r2, t))

	patch := f.getPatchedRollout(patchIndex)
	expectedPatch := `{"status":{"message":"waiting for post-promotion verification to complete"}}`
	assert.Equal(t, expectedPatch, patch)
	f.assertEvents([]string{
		conditions.TargetGroupUnverifiedReason,
	})
}

// TestBlueGreenAWSVerifyTargetGroupsReady verifies we proceed with setting stable with
// the blue-green strategy when target group verification is successful
func TestBlueGreenAWSVerifyTargetGroupsReady(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("active")
	ep := newEndpoints("active", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("1.2.3.4"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("5.6.7.8"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("2.4.6.8"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("9.8.7.6"), // irrelevant ip
					Port: ptr.To[int32](80),
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)

	r1 := newBlueGreenRollout("foo", 3, nil, "active", "")
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 3, 3)
	rs2 := newReplicaSetWithStatus(r2, 3, 3)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	svc := newService("active", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	r2 = updateBlueGreenRolloutStatus(r2, "", rs2PodHash, rs1PodHash, 3, 3, 6, 3, false, true, false)
	r2.Status.Message = "waiting for post-promotion verification to complete"
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	completedCondition, _ := newHealthyCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCond := conditions.NewRolloutCondition(v1alpha1.RolloutCompleted, corev1.ConditionTrue, conditions.RolloutCompletedReason, conditions.RolloutCompletedReason)
	conditions.SetRolloutCondition(&r2.Status, *completedCond)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, svc, ep)
	f.serviceLister = append(f.serviceLister, svc)

	f.expectGetEndpointsAction(ep)
	patchIndex := f.expectPatchRolloutAction(r2) // update status message
	f.run(getKey(r2, t))

	patch := f.getPatchedRollout(patchIndex)
	now := timeutil.MetaNow().UTC().Format(time.RFC3339)
	expectedPatch := fmt.Sprintf(`
	{
		"status":{
			"message":null,
			"phase":"Healthy",
			"stableRS":"%s",
			"duration": {
				"completionStatus": "Promoted",
				"finishedAt": "%s"
			}
		}
	}`, rs2PodHash, now)
	assert.Equal(t, cleanPatch(expectedPatch), patch)
	f.assertEvents([]string{
		conditions.TargetGroupVerifiedReason,
		conditions.RolloutCompletedReason,
	})
	f.metricsRecorder.AssertNumberOfCalls(t, "EmitRolloutDuration", 1)
}

// TestCanaryAWSVerifyTargetGroupsNotYetReady verifies we don't proceed with scale down of old
// ReplicaSets in the canary strategy until target group verification is successful
func TestCanaryAWSVerifyTargetGroupsNotYetReady(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("stable")
	ep := newEndpoints("stable", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("1.2.3.4"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("5.6.7.8"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("2.4.6.8"), // irrelevant
					Port: ptr.To[int32](81),         // wrong port
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("9.8.7.6"), // irrelevant ip
					Port: ptr.To[int32](80),
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)

	r1 := newCanaryRollout("foo", 3, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](0), intstr.FromString("25%"), intstr.FromString("25%"))

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

	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	rootSvc := newService("root", 80, map[string]string{"app": "foo"}, nil)
	stableSvc := newService("canary", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	canarySvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	ing := newIngress("ingress", canarySvc, stableSvc)

	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 6, 3, 6, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.StableRS = rs2PodHash
	r2.Status.CurrentStepIndex = ptr.To[int32](1)
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	healthyCondition, _ := newHealthyCondition(false)
	conditions.SetRolloutCondition(&r2.Status, healthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	_, r2.Status.Canary.Weights = calculateWeightStatus(r2, rs2PodHash, rs2PodHash, 0)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, ing, rootSvc, canarySvc, stableSvc, ep)
	f.serviceLister = append(f.serviceLister, rootSvc, canarySvc, stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(ing))

	// Duration is already completed so reconciliation makes no rollout status patch
	finishedAt := timeutil.MetaNow()
	promoted := v1alpha1.CompletionStatusPromoted
	r2.Status.Duration.FinishedAt = &finishedAt
	r2.Status.Duration.CompletionStatus = &promoted

	f.expectGetEndpointsAction(ep)
	f.run(getKey(r2, t))
	f.assertEvents([]string{
		conditions.TargetGroupUnverifiedReason,
	})
}

// TestCanaryAWSVerifyTargetGroupsReady verifies we proceed with scale down of old
// ReplicaSets in the canary strategy after target group verification is successful
func TestCanaryAWSVerifyTargetGroupsReady(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("stable")
	ep := newEndpoints("stable", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("1.2.3.4"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("5.6.7.8"),
					Port: ptr.To[int32](80),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("2.4.6.8"), // irrelevant
					Port: ptr.To[int32](80),         // wrong port
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("9.8.7.6"), // irrelevant ip
					Port: ptr.To[int32](80),
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)

	r1 := newCanaryRollout("foo", 3, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](0), intstr.FromString("25%"), intstr.FromString("25%"))
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

	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	rootSvc := newService("root", 80, map[string]string{"app": "foo"}, nil)
	stableSvc := newService("canary", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	canarySvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	ing := newIngress("ingress", canarySvc, stableSvc)

	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 6, 3, 6, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.StableRS = rs2PodHash
	r2.Status.CurrentStepIndex = ptr.To[int32](1)
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	healthyCondition, _ := newHealthyCondition(false)
	conditions.SetRolloutCondition(&r2.Status, healthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	_, r2.Status.Canary.Weights = calculateWeightStatus(r2, rs2PodHash, rs2PodHash, 0)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, ing, rootSvc, canarySvc, stableSvc, ep)
	f.serviceLister = append(f.serviceLister, rootSvc, canarySvc, stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(ing))

	// Duration is already completed so reconciliation makes no rollout status patch
	finishedAt := timeutil.MetaNow()
	promoted := v1alpha1.CompletionStatusPromoted
	r2.Status.Duration.FinishedAt = &finishedAt
	r2.Status.Duration.CompletionStatus = &promoted

	f.expectGetEndpointsAction(ep)
	scaleDownRSIndex := f.expectPatchReplicaSetAction(rs1)

	f.run(getKey(r2, t))
	f.verifyPatchedReplicaSet(scaleDownRSIndex, 30)
	f.assertEvents([]string{
		conditions.TargetGroupVerifiedReason,
	})
}

// TestCanaryAWSVerifyTargetGroupsSkip verifies we skip unnecessary verification if scaledown
// annotation does not need to happen
func TestCanaryAWSVerifyTargetGroupsSkip(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	f := newFixture(t)
	defer f.Close()

	r1 := newCanaryRollout("foo", 3, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](0), intstr.FromString("25%"), intstr.FromString("25%"))
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
	// set an annotation on old RS to cause verification to be skipped
	rs1.Annotations[v1alpha1.DefaultReplicaSetScaleDownDeadlineAnnotationKey] = timeutil.Now().Add(600 * time.Second).UTC().Format(time.RFC3339)
	rs2 := newReplicaSetWithStatus(r2, 3, 3)

	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	rootSvc := newService("root", 80, map[string]string{"app": "foo"}, nil)
	stableSvc := newService("canary", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	canarySvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	ing := newIngress("ingress", canarySvc, stableSvc)

	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 6, 3, 6, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.StableRS = rs2PodHash
	r2.Status.CurrentStepIndex = ptr.To[int32](1)
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	healthyCondition, _ := newHealthyCondition(false)
	conditions.SetRolloutCondition(&r2.Status, healthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	_, r2.Status.Canary.Weights = calculateWeightStatus(r2, rs2PodHash, rs2PodHash, 0)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, ing, rootSvc, canarySvc, stableSvc)
	f.serviceLister = append(f.serviceLister, rootSvc, canarySvc, stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(ing))

	// Duration is already completed so reconciliation makes no rollout status patch
	finishedAt := timeutil.MetaNow()
	promoted := v1alpha1.CompletionStatusPromoted
	r2.Status.Duration.FinishedAt = &finishedAt
	r2.Status.Duration.CompletionStatus = &promoted

	f.run(getKey(r2, t)) // there should be no api calls
	f.assertEvents(nil)
}

// TestShouldVerifyTargetGroups returns whether or not we should verify the target group
func TestShouldVerifyTargetGroups(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	t.Run("CanaryNotUsingTrafficRouting", func(t *testing.T) {
		ro := newCanaryRollout("foo", 3, nil, nil, nil, intstr.FromString("25%"), intstr.FromString("25%"))

		newRS := newReplicaSetWithStatus(ro, 3, 3)
		stableSvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		assert.False(t, shouldVerifyTargetGroup(ro, newRS, stableSvc))
	})
	t.Run("CanaryNotFullyPromoted", func(t *testing.T) {
		ro := newCanaryRollout("foo", 3, nil, nil, nil, intstr.FromString("25%"), intstr.FromString("25%"))
		ro.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			ALB: &v1alpha1.ALBTrafficRouting{
				Ingress: "ingress",
			},
		}
		newRS := newReplicaSetWithStatus(ro, 3, 3)
		stableSvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		ro.Status.StableRS = "somethingelse"
		assert.False(t, shouldVerifyTargetGroup(ro, newRS, stableSvc))
	})
	t.Run("CanaryFullyPromoted", func(t *testing.T) {
		ro := newCanaryRollout("foo", 3, nil, nil, nil, intstr.FromString("25%"), intstr.FromString("25%"))
		ro.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
			ALB: &v1alpha1.ALBTrafficRouting{
				Ingress: "ingress",
			},
		}
		newRS := newReplicaSetWithStatus(ro, 3, 3)
		stableSvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		ro.Status.StableRS = newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.True(t, shouldVerifyTargetGroup(ro, newRS, stableSvc))
	})
	t.Run("BlueGreenFullyPromoted", func(t *testing.T) {
		ro := newBlueGreenRollout("foo", 3, nil, "active-svc", "")

		newRS := newReplicaSetWithStatus(ro, 3, 3)
		activeSvc := newService("active-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		ro.Status.StableRS = newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, shouldVerifyTargetGroup(ro, newRS, activeSvc))
	})
	t.Run("BlueGreenBeforePromotion", func(t *testing.T) {
		ro := newBlueGreenRollout("foo", 3, nil, "active-svc", "")
		newRS := newReplicaSetWithStatus(ro, 3, 3)
		activeSvc := newService("active-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: "oldrshash"}, ro)
		ro.Status.StableRS = "oldrshash"
		assert.False(t, shouldVerifyTargetGroup(ro, newRS, activeSvc))
	})
	t.Run("BlueGreenAfterPromotionAfterPromotionAnalysisStarted", func(t *testing.T) {
		ro := newBlueGreenRollout("foo", 3, nil, "active-svc", "")
		newRS := newReplicaSetWithStatus(ro, 3, 3)
		activeSvc := newService("active-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		ro.Status.StableRS = "oldrshash"
		ro.Status.BlueGreen.PostPromotionAnalysisRunStatus = &v1alpha1.RolloutAnalysisRunStatus{}
		assert.False(t, shouldVerifyTargetGroup(ro, newRS, activeSvc))
	})
	t.Run("BlueGreenAfterPromotion", func(t *testing.T) {
		ro := newBlueGreenRollout("foo", 3, nil, "active-svc", "")
		newRS := newReplicaSetWithStatus(ro, 3, 3)
		activeSvc := newService("active-svc", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]}, ro)
		ro.Status.StableRS = "oldrshash"
		assert.True(t, shouldVerifyTargetGroup(ro, newRS, activeSvc))
	})
}

// TestDelayCanaryStableServiceLabelInjection verifies we don't inject pod hash labels to the canary
// or stable service before the pods for them are ready.
func TestDelayCanaryStableServiceLabelInjection(t *testing.T) {
	ro1 := newCanaryRollout("foo", 3, nil, nil, nil, intstr.FromInt(1), intstr.FromInt(1))
	ro1.Spec.Strategy.Canary.CanaryService = "canary"
	ro1.Spec.Strategy.Canary.StableService = "stable"
	canarySvc := newService("canary", 80, ro1.Spec.Selector.MatchLabels, nil)
	stableSvc := newService("stable", 80, ro1.Spec.Selector.MatchLabels, nil)
	ro2 := bumpVersion(ro1)

	f := newFixture(t)
	defer f.Close()
	f.kubeobjects = append(f.kubeobjects, canarySvc, stableSvc)
	f.serviceLister = append(f.serviceLister, canarySvc, stableSvc)

	f.objects = append(f.objects, ro2)
	f.rolloutLister = append(f.rolloutLister, ro2)

	{
		// first ensure we don't update service because new/stable are both not available
		ctrl, _, _ := f.newController(noResyncPeriodFunc)
		roCtx, err := ctrl.newRolloutContext(ro1)
		assert.NoError(t, err)

		roCtx.newRS = newReplicaSetWithStatus(ro1, 3, 0)
		roCtx.stableRS = newReplicaSetWithStatus(ro2, 3, 0)

		err = roCtx.reconcileStableAndCanaryService()
		assert.NoError(t, err)
		_, canaryInjected := canarySvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, canaryInjected)
		_, stableInjected := stableSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, stableInjected)
	}
	{
		// ensure we don't update service because new/stable are both partially available on an adoption of service reconcile
		ctrl, _, _ := f.newController(noResyncPeriodFunc)
		roCtx, err := ctrl.newRolloutContext(ro1)
		assert.NoError(t, err)

		roCtx.newRS = newReplicaSetWithStatus(ro1, 3, 1)
		roCtx.stableRS = newReplicaSetWithStatus(ro2, 3, 1)

		err = roCtx.reconcileStableAndCanaryService()
		assert.NoError(t, err)
		_, canaryInjected := canarySvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, canaryInjected)
		_, stableInjected := stableSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, stableInjected)
	}
	{
		// next ensure we do update service because new/stable are now available
		ctrl, _, _ := f.newController(noResyncPeriodFunc)
		roCtx, err := ctrl.newRolloutContext(ro1)
		assert.NoError(t, err)

		roCtx.newRS = newReplicaSetWithStatus(ro1, 3, 3)
		roCtx.stableRS = newReplicaSetWithStatus(ro2, 3, 3)

		err = roCtx.reconcileStableAndCanaryService()
		assert.NoError(t, err)
		_, canaryInjected := canarySvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.True(t, canaryInjected)
		_, stableInjected := stableSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.True(t, stableInjected)
	}

}

// TestDelayCanaryStableServiceDelayOnAdoptedService verifies allow partial readiness of pods when switching labels
// on an adopted services, but that if there is zero readiness we will not switch
func TestDelayCanaryStableServiceDelayOnAdoptedService(t *testing.T) {
	ro1 := newCanaryRollout("foo", 3, nil, nil, nil, intstr.FromInt(1), intstr.FromInt(1))
	ro1.Spec.Strategy.Canary.CanaryService = "canary"
	ro1.Spec.Strategy.Canary.StableService = "stable"
	//Setup services that are already adopted by rollouts
	stableSvc := newService("stable", 80, ro1.Spec.Selector.MatchLabels, ro1)
	ro2 := bumpVersion(ro1)
	canarySvc := newService("canary", 80, ro1.Spec.Selector.MatchLabels, ro2)

	f := newFixture(t)
	defer f.Close()
	f.kubeobjects = append(f.kubeobjects, canarySvc, stableSvc)
	f.serviceLister = append(f.serviceLister, canarySvc, stableSvc)

	f.objects = append(f.objects, ro2)
	f.rolloutLister = append(f.rolloutLister, ro2)

	t.Run("AdoptedService No Availability", func(t *testing.T) {
		// first ensure we don't update service because new/stable are both not available
		ctrl, _, _ := f.newController(noResyncPeriodFunc)
		roCtx, err := ctrl.newRolloutContext(ro1)
		assert.NoError(t, err)

		roCtx.newRS = newReplicaSetWithStatus(ro1, 3, 0)
		roCtx.stableRS = newReplicaSetWithStatus(ro2, 3, 0)

		err = roCtx.reconcileStableAndCanaryService()
		assert.NoError(t, err)
		canaryHash2, canaryInjected := canarySvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, canaryInjected)
		fmt.Println(canaryHash2)
		stableHash2, stableInjected := stableSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.False(t, stableInjected)
		fmt.Println(stableHash2)
	})
	t.Run("AdoptedService Partial Availability", func(t *testing.T) {
		// ensure we do change selector on partially available replica sets
		ctrl, _, _ := f.newController(noResyncPeriodFunc)
		roCtx, err := ctrl.newRolloutContext(ro1)
		assert.NoError(t, err)

		roCtx.newRS = newReplicaSetWithStatus(ro1, 3, 1)
		roCtx.stableRS = newReplicaSetWithStatus(ro2, 3, 2)

		err = roCtx.reconcileStableAndCanaryService()
		assert.NoError(t, err)
		canaryHash2, canaryInjected := canarySvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.True(t, canaryInjected)
		fmt.Println(canaryHash2)
		stableHash2, stableInjected := stableSvc.Spec.Selector[v1alpha1.DefaultRolloutUniqueLabelKey]
		assert.True(t, stableInjected)
		fmt.Println(stableHash2)
	})

}

// newTargetGroupBindingFor returns a TargetGroupBinding for the given service (port 80) with the
// given target group ARN and target type
func newTargetGroupBindingFor(name, svcName, arn, targetType string) *unstructured.Unstructured {
	return unstructuredutil.StrToUnstructuredUnsafe(fmt.Sprintf(`
apiVersion: elbv2.k8s.aws/v1beta1
kind: TargetGroupBinding
metadata:
  name: %s
  namespace: default
spec:
  serviceRef:
    name: %s
    port: 80
  targetGroupARN: %s
  targetType: %s
`, name, svcName, arn, targetType))
}

// newTargetHealthOutput returns a DescribeTargetHealth output where every given IP is registered
// on port 80 with the given health state
func newTargetHealthOutput(state elbv2types.TargetHealthStateEnum, ips ...string) *elbv2.DescribeTargetHealthOutput {
	thOut := elbv2.DescribeTargetHealthOutput{}
	for _, ip := range ips {
		thOut.TargetHealthDescriptions = append(thOut.TargetHealthDescriptions, elbv2types.TargetHealthDescription{
			Target: &elbv2types.TargetDescription{
				Id:   ptr.To[string](ip),
				Port: ptr.To[int32](80),
			},
			TargetHealth: &elbv2types.TargetHealth{
				State: state,
			},
		})
	}
	return &thOut
}

// newALBCanaryRolloutForTargetGroups returns a promoted ALB canary rollout whose stable service is "stable"
func newALBCanaryRolloutForTargetGroups() *v1alpha1.Rollout {
	ro := newCanaryRollout("foo", 3, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](1), intstr.FromString("25%"), intstr.FromString("25%"))
	ro.Spec.Strategy.Canary.TrafficRouting = &v1alpha1.RolloutTrafficRouting{
		ALB: &v1alpha1.ALBTrafficRouting{
			Ingress:     "ingress",
			RootService: "root",
		},
	}
	ro.Spec.Strategy.Canary.CanaryService = "canary"
	ro.Spec.Strategy.Canary.StableService = "stable"
	return ro
}

// newTargetGroupsHealthContext returns a rolloutContext backed by the fixture's clients, for calling
// the health-aware target group verification directly
func newTargetGroupsHealthContext(f *fixture, ro *v1alpha1.Rollout) *rolloutContext {
	c, _, _ := f.newController(noResyncPeriodFunc)
	return &rolloutContext{
		rollout:        ro,
		log:            logutil.WithRollout(ro),
		reconcilerBase: c.reconcilerBase,
	}
}

// recordedEventMessages returns the "<type> <reason> <message>" strings of the events emitted
// through the rollout context's fake recorder
func recordedEventMessages(roCtx *rolloutContext) []string {
	k8sRecorder := roCtx.recorder.(*record.FakeEventRecorder).Recorder.(*k8srecord.FakeRecorder)
	var events []string
	for {
		select {
		case event := <-k8sRecorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// TestAWSVerifyTargetGroupsHealthy verifies the health-aware target group verification verdicts
func TestAWSVerifyTargetGroupsHealthy(t *testing.T) {
	endpointIPs := []string{"1.2.3.4", "5.6.7.8", "2.4.6.8"}

	setup := func(t *testing.T, tgbs ...*unstructured.Unstructured) (*mocks.ELBv2APIClient, *rolloutContext, *corev1.Service) {
		t.Helper()
		defaults.SetVerifyTargetGroup(true)
		t.Cleanup(func() { defaults.SetVerifyTargetGroup(false) })
		fakeELB := &mocks.ELBv2APIClient{}
		aws.NewClient = aws.FakeNewClientFunc(fakeELB)
		t.Cleanup(func() { aws.NewClient = aws.DefaultNewClientFunc })

		f := newFixture(t)
		t.Cleanup(f.Close)
		ro := newALBCanaryRolloutForTargetGroups()
		svc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: "newhash"}, ro)
		ep := newEndpoints("stable", endpointIPs...)
		f.objects = append(f.objects, ro)
		for _, tgb := range tgbs {
			f.objects = append(f.objects, tgb)
		}
		f.kubeobjects = append(f.kubeobjects, svc, ep)
		return fakeELB, newTargetGroupsHealthContext(f, ro), svc
	}

	t.Run("RegisteredButInitialIsNotReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, endpointIPs...)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotReady, verdict)
		assert.Equal(t, []string{
			"Warning " + conditions.TargetGroupUnverifiedReason + " Service stable (TargetGroup arn::1234) not verified: 0/3 endpoints healthy",
		}, recordedEventMessages(roCtx))
		// the shared verified flag read by the old ReplicaSet scale down is not written
		assert.Nil(t, roCtx.targetsVerified)
	})

	t.Run("RegisteredButInitialIsVerifiedByRegistrationOnly", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, endpointIPs...)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)
		// registration-only verification applies to a promoted canary (stableRS == new hash)
		roCtx.newRS = newReplicaSetWithStatus(roCtx.rollout, 3, 3)
		roCtx.rollout.Status.StableRS = roCtx.newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

		err := roCtx.awsVerifyTargetGroups(svc)
		assert.NoError(t, err)
		assert.True(t, roCtx.areTargetsVerified())
		assert.Equal(t, []string{
			"Normal " + conditions.TargetGroupVerifiedReason + " Service stable (TargetGroup arn::1234) verified: 3 endpoints registered",
		}, recordedEventMessages(roCtx))
	})

	t.Run("AllHealthyIsReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, endpointIPs...)
		// old pods being deregistered are not endpoints of the service, and are ignored
		thOut.TargetHealthDescriptions = append(thOut.TargetHealthDescriptions,
			newTargetHealthOutput(elbv2types.TargetHealthStateEnumDraining, "9.8.7.6", "9.8.7.5").TargetHealthDescriptions...)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictReady, verdict)
		assert.Equal(t, []string{
			"Normal " + conditions.TargetGroupVerifiedReason + " Service stable (TargetGroup arn::1234) verified: 3 endpoints healthy",
		}, recordedEventMessages(roCtx))
		assert.Nil(t, roCtx.targetsVerified)
	})

	for _, state := range []elbv2types.TargetHealthStateEnum{
		elbv2types.TargetHealthStateEnumUnused,
		elbv2types.TargetHealthStateEnumUnavailable,
	} {
		t.Run("OneEndpoint_"+string(state)+"_IsNotReady", func(t *testing.T) {
			fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
			thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, "1.2.3.4", "5.6.7.8")
			thOut.TargetHealthDescriptions = append(thOut.TargetHealthDescriptions,
				newTargetHealthOutput(state, "2.4.6.8").TargetHealthDescriptions...)
			fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

			verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
			assert.NoError(t, err)
			assert.Equal(t, targetGroupVerdictNotReady, verdict)
			assert.Equal(t, []string{
				"Warning " + conditions.TargetGroupUnverifiedReason + " Service stable (TargetGroup arn::1234) not verified: 2/3 endpoints healthy",
			}, recordedEventMessages(roCtx))
		})
	}

	t.Run("UnregisteredEndpointIsNotReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, "1.2.3.4", "5.6.7.8")
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotReady, verdict)
		assert.Equal(t, []string{
			"Warning " + conditions.TargetGroupUnverifiedReason + " Service stable (TargetGroup arn::1234) not verified: 2/3 endpoints healthy",
		}, recordedEventMessages(roCtx))
	})

	t.Run("TwoTargetGroupBindingsOneNotHealthyIsNotReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t,
			newTargetGroupBindingFor("stable-a", "stable", "arn::healthy", "ip"),
			newTargetGroupBindingFor("stable-b", "stable", "arn::initial", "ip"),
		)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.MatchedBy(func(in *elbv2.DescribeTargetHealthInput) bool {
			return *in.TargetGroupArn == "arn::healthy"
		})).Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, endpointIPs...), nil)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.MatchedBy(func(in *elbv2.DescribeTargetHealthInput) bool {
			return *in.TargetGroupArn == "arn::initial"
		})).Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, endpointIPs...), nil)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotReady, verdict)
		// the bindings may be listed in any order, and the check stops at the first not ready one
		assert.Contains(t, recordedEventMessages(roCtx),
			"Warning "+conditions.TargetGroupUnverifiedReason+" Service stable (TargetGroup arn::initial) not verified: 0/3 endpoints healthy")
	})

	t.Run("TwoTargetGroupBindingsAllHealthyIsReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t,
			newTargetGroupBindingFor("stable-a", "stable", "arn::a", "ip"),
			newTargetGroupBindingFor("stable-b", "stable", "arn::b", "ip"),
		)
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(newTargetHealthOutput(elbv2types.TargetHealthStateEnumHealthy, endpointIPs...), nil)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictReady, verdict)
		fakeELB.AssertNumberOfCalls(t, "DescribeTargetHealth", 2)
	})

	t.Run("FlagOffIsNotApplicable", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		defaults.SetVerifyTargetGroup(false)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotApplicable, verdict)
		fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
		assert.Nil(t, recordedEventMessages(roCtx))
		assert.Nil(t, roCtx.targetsVerified)
	})

	t.Run("NoTargetGroupBindingIsNotApplicable", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t)

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotApplicable, verdict)
		fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
		assert.Nil(t, recordedEventMessages(roCtx))
		assert.Nil(t, roCtx.targetsVerified)
	})

	t.Run("InstanceTargetTypeIsNotApplicable", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBindingFor("stable", "stable", "arn::1234", "instance"))

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.NoError(t, err)
		assert.Equal(t, targetGroupVerdictNotApplicable, verdict)
		fakeELB.AssertNotCalled(t, "DescribeTargetHealth", mock.Anything, mock.Anything)
		assert.Nil(t, recordedEventMessages(roCtx))
		assert.Nil(t, roCtx.targetsVerified)
	})

	t.Run("DescribeTargetHealthErrorIsNotReady", func(t *testing.T) {
		fakeELB, roCtx, svc := setup(t, newTargetGroupBinding("stable"))
		fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("boom"))

		verdict, err := roCtx.awsVerifyTargetGroupsHealthy(svc)
		assert.EqualError(t, err, "boom")
		assert.Equal(t, targetGroupVerdictNotReady, verdict)
		assert.Equal(t, []string{
			"Warning " + conditions.TargetGroupVerifyErrorReason + " Failed to verify Service stable (TargetGroup arn::1234): boom",
		}, recordedEventMessages(roCtx))
	})
}

// TestBlueGreenAWSVerifyTargetGroupsReadyWithInitialTargets verifies blue-green promotion keeps
// registration-only target group verification: registered but `initial` targets are verified
func TestBlueGreenAWSVerifyTargetGroupsReadyWithInitialTargets(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("active")
	ep := newEndpoints("active", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, "1.2.3.4", "5.6.7.8", "2.4.6.8")
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

	r1 := newBlueGreenRollout("foo", 3, nil, "active", "")
	r2 := bumpVersion(r1)

	rs1 := newReplicaSetWithStatus(r1, 3, 3)
	rs2 := newReplicaSetWithStatus(r2, 3, 3)

	rs1PodHash := rs1.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	svc := newService("active", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	r2 = updateBlueGreenRolloutStatus(r2, "", rs2PodHash, rs1PodHash, 3, 3, 6, 3, false, true, false)
	r2.Status.Message = "waiting for post-promotion verification to complete"
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	completedCondition, _ := newHealthyCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCond := conditions.NewRolloutCondition(v1alpha1.RolloutCompleted, corev1.ConditionTrue, conditions.RolloutCompletedReason, conditions.RolloutCompletedReason)
	conditions.SetRolloutCondition(&r2.Status, *completedCond)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, svc, ep)
	f.serviceLister = append(f.serviceLister, svc)

	f.expectGetEndpointsAction(ep)
	patchIndex := f.expectPatchRolloutAction(r2) // update status message
	f.run(getKey(r2, t))

	patch := f.getPatchedRollout(patchIndex)
	now := timeutil.MetaNow().UTC().Format(time.RFC3339)
	expectedPatch := fmt.Sprintf(`
	{
		"status":{
			"message":null,
			"phase":"Healthy",
			"stableRS":"%s",
			"duration": {
				"completionStatus": "Promoted",
				"finishedAt": "%s"
			}
		}
	}`, rs2PodHash, now)
	assert.Equal(t, cleanPatch(expectedPatch), patch)
	f.assertEvents([]string{
		conditions.TargetGroupVerifiedReason,
		conditions.RolloutCompletedReason,
	})
}

// TestCanaryAWSVerifyTargetGroupsReadyWithInitialTargets verifies the canary old ReplicaSet scale
// down keeps registration-only target group verification: registered but `initial` targets are verified
func TestCanaryAWSVerifyTargetGroupsReadyWithInitialTargets(t *testing.T) {
	defaults.SetVerifyTargetGroup(true)
	defer defaults.SetVerifyTargetGroup(false)

	// Allow us to fake out the AWS API
	fakeELB := mocks.ELBv2APIClient{}
	aws.NewClient = aws.FakeNewClientFunc(&fakeELB)
	defer func() {
		aws.NewClient = aws.DefaultNewClientFunc
	}()

	f := newFixture(t)
	defer f.Close()

	tgb := newTargetGroupBinding("stable")
	ep := newEndpoints("stable", "1.2.3.4", "5.6.7.8", "2.4.6.8")
	thOut := newTargetHealthOutput(elbv2types.TargetHealthStateEnumInitial, "1.2.3.4", "5.6.7.8", "2.4.6.8")
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(thOut, nil)

	r1 := newCanaryRollout("foo", 3, nil, []v1alpha1.CanaryStep{{
		SetWeight: ptr.To[int32](10),
	}}, ptr.To[int32](0), intstr.FromString("25%"), intstr.FromString("25%"))
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

	rs2PodHash := rs2.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]

	rootSvc := newService("root", 80, map[string]string{"app": "foo"}, nil)
	stableSvc := newService("canary", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	canarySvc := newService("stable", 80, map[string]string{v1alpha1.DefaultRolloutUniqueLabelKey: rs2PodHash}, r2)
	ing := newIngress("ingress", canarySvc, stableSvc)

	r2 = updateCanaryRolloutStatus(r2, rs2PodHash, 6, 3, 6, false)
	r2.Status.Message = ""
	r2.Status.ObservedGeneration = strconv.Itoa(int(r2.Generation))
	r2.Status.StableRS = rs2PodHash
	r2.Status.CurrentStepIndex = ptr.To[int32](1)
	availableCondition, _ := newAvailableCondition(true)
	conditions.SetRolloutCondition(&r2.Status, availableCondition)
	healthyCondition, _ := newHealthyCondition(false)
	conditions.SetRolloutCondition(&r2.Status, healthyCondition)
	progressingCondition, _ := newProgressingCondition(conditions.NewRSAvailableReason, rs2, "")
	conditions.SetRolloutCondition(&r2.Status, progressingCondition)
	completedCondition, _ := newCompletedCondition(true)
	conditions.SetRolloutCondition(&r2.Status, completedCondition)
	_, r2.Status.Canary.Weights = calculateWeightStatus(r2, rs2PodHash, rs2PodHash, 0)

	f.rolloutLister = append(f.rolloutLister, r2)
	f.objects = append(f.objects, r2, tgb)
	f.kubeobjects = append(f.kubeobjects, rs1, rs2, ing, rootSvc, canarySvc, stableSvc, ep)
	f.serviceLister = append(f.serviceLister, rootSvc, canarySvc, stableSvc)
	f.ingressLister = append(f.ingressLister, ingressutil.NewLegacyIngress(ing))

	// Duration is already completed so reconciliation makes no rollout status patch
	finishedAt := timeutil.MetaNow()
	promoted := v1alpha1.CompletionStatusPromoted
	r2.Status.Duration.FinishedAt = &finishedAt
	r2.Status.Duration.CompletionStatus = &promoted

	f.expectGetEndpointsAction(ep)
	scaleDownRSIndex := f.expectPatchReplicaSetAction(rs1)

	f.run(getKey(r2, t))
	f.verifyPatchedReplicaSet(scaleDownRSIndex, 30)
	f.assertEvents([]string{
		conditions.TargetGroupVerifiedReason,
	})
}
