package aws

import (
	"context"
	"testing"

	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	testutil "github.com/argoproj/argo-rollouts/test/util"
	"github.com/argoproj/argo-rollouts/utils/aws/mocks"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	unstructuredutil "github.com/argoproj/argo-rollouts/utils/unstructured"
)

func newFakeClient() (*mocks.ELBv2APIClient, Client) {
	fakeELB := mocks.ELBv2APIClient{}
	awsClient, _ := FakeNewClientFunc(&fakeELB)()
	return &fakeELB, awsClient
}

func TestGetTargetGroupBindingsGVR(t *testing.T) {
	defaults.SetTargetGroupBindingAPIVersion("eks.amazonaws.com/v1")
	gvr, err := GetTargetGroupBindingsGVR()
	assert.NoError(t, err)
	assert.Equal(t, "eks.amazonaws.com", gvr.Group)
	assert.Equal(t, "v1", gvr.Version)
	assert.Equal(t, "targetgroupbindings", gvr.Resource)
	defaults.SetTargetGroupBindingAPIVersion("elbv2.k8s.aws/v1beta1")
}

func TestFindLoadBalancerByDNSName(t *testing.T) {
	// LoadBalancer not found
	{
		fakeELB, c := newFakeClient()
		fakeELB.On("DescribeLoadBalancers", mock.Anything, mock.Anything, mock.Anything).Return(&elbv2.DescribeLoadBalancersOutput{}, nil)
		lb, err := c.FindLoadBalancerByDNSName(context.TODO(), "doesnt-exist")
		assert.NoError(t, err)
		assert.Nil(t, lb)
	}

	// LoadBalancer found
	{
		fakeELB, c := newFakeClient()
		// Mock output
		expectedLB := elbv2types.LoadBalancer{
			LoadBalancerArn: ptr.To[string]("lb-abc123"),
			DNSName:         ptr.To[string]("find-loadbalancer-test-abc-123.us-west-2.elb.amazonaws.com"),
		}
		lbOut := elbv2.DescribeLoadBalancersOutput{
			LoadBalancers: []elbv2types.LoadBalancer{
				expectedLB,
			},
		}
		fakeELB.On("DescribeLoadBalancers", mock.Anything, mock.Anything, mock.Anything).Return(&lbOut, nil)

		lb, err := c.FindLoadBalancerByDNSName(context.TODO(), "find-loadbalancer-test-abc-123.us-west-2.elb.amazonaws.com")
		assert.NoError(t, err)
		assert.Equal(t, expectedLB, *lb)

	}
}

func TestGetNumericTargetPort(t *testing.T) {
	tgb := TargetGroupBinding{
		Spec: TargetGroupBindingSpec{
			ServiceRef: ServiceReference{
				Port: intstr.FromString("web"),
			},
		},
	}
	svc := corev1.Service{
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "web",
					TargetPort: intstr.FromString("http"),
				},
			},
		},
	}
	eps := corev1.Endpoints{
		Subsets: []corev1.EndpointSubset{
			{
				Ports: []corev1.EndpointPort{
					{
						Name: "asdf",
						Port: 1234,
					},
					{
						Name: "http",
						Port: 4567,
					},
				},
			},
		},
	}
	assert.Equal(t, int32(4567), getNumericTargetPort(tgb, svc, eps))
}

func TestGetTargetGroupMetadata(t *testing.T) {
	defaults.SetDescribeTagsLimit(2)
	fakeELB, c := newFakeClient()

	// mock the output
	tgOut := elbv2.DescribeTargetGroupsOutput{
		TargetGroups: []elbv2types.TargetGroup{
			{
				TargetGroupArn: ptr.To[string]("tg-abc123"),
			},
			{
				TargetGroupArn: ptr.To[string]("tg-def456"),
			},
			{
				TargetGroupArn: ptr.To[string]("tg-ghi789"),
			},
		},
	}
	fakeELB.On("DescribeTargetGroups", mock.Anything, mock.Anything).Return(&tgOut, nil)

	mockDescribeTagsCall := fakeELB.On("DescribeTags", mock.Anything, mock.Anything)
	mockDescribeTagsCall.RunFn = func(args mock.Arguments) {
		tagsIn := args[1].(*elbv2.DescribeTagsInput)
		assert.LessOrEqual(t, len(tagsIn.ResourceArns), defaults.GetDescribeTagsLimit())

		tagsOut := elbv2.DescribeTagsOutput{
			TagDescriptions: []elbv2types.TagDescription{},
		}
		for _, arn := range tagsIn.ResourceArns {
			tagsOut.TagDescriptions = append(tagsOut.TagDescriptions, elbv2types.TagDescription{
				ResourceArn: ptr.To[string](arn),
				Tags: []elbv2types.Tag{
					{
						Key:   ptr.To[string]("foo"),
						Value: ptr.To[string]("bar"),
					},
				},
			})
		}

		mockDescribeTagsCall.ReturnArguments = mock.Arguments{&tagsOut, nil}
	}

	listenersOut := elbv2.DescribeListenersOutput{
		Listeners: []elbv2types.Listener{
			{
				ListenerArn:     ptr.To[string]("lst-abc123"),
				LoadBalancerArn: ptr.To[string]("lb-abc123"),
			},
		},
	}
	fakeELB.On("DescribeListeners", mock.Anything, mock.Anything).Return(&listenersOut, nil)

	rulesOut := elbv2.DescribeRulesOutput{
		Rules: []elbv2types.Rule{
			{
				Actions: []elbv2types.Action{
					{
						ForwardConfig: &elbv2types.ForwardActionConfig{
							TargetGroups: []elbv2types.TargetGroupTuple{
								{
									TargetGroupArn: ptr.To[string]("tg-abc123"),
									Weight:         ptr.To[int32](10),
								},
							},
						},
					},
				},
			},
		},
	}
	fakeELB.On("DescribeRules", mock.Anything, mock.Anything).Return(&rulesOut, nil)

	// Test
	tgMeta, err := c.GetTargetGroupMetadata(context.TODO(), "lb-abc123")
	assert.NoError(t, err)
	assert.Len(t, tgMeta, 3)
	assert.Equal(t, "tg-abc123", *tgMeta[0].TargetGroup.TargetGroupArn)
	assert.Equal(t, "bar", tgMeta[0].Tags["foo"])
	assert.Equal(t, int32(10), *tgMeta[0].Weight)

	assert.Equal(t, "tg-def456", *tgMeta[1].TargetGroup.TargetGroupArn)
	assert.Equal(t, "bar", tgMeta[1].Tags["foo"])
	assert.Nil(t, tgMeta[1].Weight)

	assert.Equal(t, "tg-ghi789", *tgMeta[2].TargetGroup.TargetGroupArn)
	assert.Equal(t, "bar", tgMeta[2].Tags["foo"])
	assert.Nil(t, tgMeta[2].Weight)
}

func TestBuildTargetGroupResourceID(t *testing.T) {
	assert.Equal(t, "default/ingress-svc:80", BuildTargetGroupResourceID("default", "ingress", "svc", 80))
}

func TestGetTargetGroupHealth(t *testing.T) {
	fakeELB, c := newFakeClient()
	expectedHealth := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				HealthCheckPort: ptr.To[string]("80"),
				Target:          &elbv2types.TargetDescription{},
				TargetHealth: &elbv2types.TargetHealth{
					State: elbv2types.TargetHealthStateEnumHealthy,
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&expectedHealth, nil)

	// Test
	health, err := c.GetTargetGroupHealth(context.TODO(), "tg-abc123")
	assert.NoError(t, err)
	assert.Equal(t, expectedHealth.TargetHealthDescriptions, health)
}

var testTargetGroupBinding = `
apiVersion: elbv2.k8s.aws/v1beta1
kind: TargetGroupBinding
metadata:
  name: active
  namespace: default
spec:
  serviceRef:
    name: active
    port: 80
  targetGroupARN: arn::1234
  targetType: ip
`

func TestGetTargetGroupBindingsByService(t *testing.T) {
	{
		svc1 := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{},
				Ports: []corev1.ServicePort{{
					Protocol:   "TCP",
					Port:       int32(80),
					TargetPort: intstr.FromInt(80),
				}},
			},
		}
		obj := unstructuredutil.StrToUnstructuredUnsafe(testTargetGroupBinding)
		dynamicClientSet := testutil.NewFakeDynamicClient(obj)
		tgbs, err := GetTargetGroupBindingsByService(context.TODO(), dynamicClientSet, svc1)
		assert.NoError(t, err)
		assert.Equal(t, 80, tgbs[0].Spec.ServiceRef.Port.IntValue())
		assert.Equal(t, "arn::1234", tgbs[0].Spec.TargetGroupARN)
		assert.Equal(t, "ip", string(*tgbs[0].Spec.TargetType))
	}
	{
		svc2 := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "active",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{},
				Ports: []corev1.ServicePort{{
					Protocol:   "TCP",
					Name:       "foo",
					TargetPort: intstr.FromInt(80),
				}},
			},
		}
		obj := unstructuredutil.StrToUnstructuredUnsafe(`
apiVersion: elbv2.k8s.aws/v1beta1
kind: TargetGroupBinding
metadata:
  name: active
  namespace: default
spec:
  serviceRef:
    name: active
    port: foo
  targetGroupARN: arn::1234
  targetType: instance
`)
		dynamicClientSet := testutil.NewFakeDynamicClient(obj)
		tgbs, err := GetTargetGroupBindingsByService(context.TODO(), dynamicClientSet, svc2)
		assert.NoError(t, err)
		assert.Equal(t, "foo", tgbs[0].Spec.ServiceRef.Port.StrVal)
		assert.Equal(t, "arn::1234", tgbs[0].Spec.TargetGroupARN)
		assert.Equal(t, "instance", string(*tgbs[0].Spec.TargetType))
	}
}

func TestVerifyTargetGroupBindingIgnoreInstanceMode(t *testing.T) {
	logCtx := log.NewEntry(log.New())
	_, awsClnt := newFakeClient()
	tgb := TargetGroupBinding{
		Spec: TargetGroupBindingSpec{
			TargetType: (*TargetType)(ptr.To[string]("instance")),
		},
	}
	res, err := VerifyTargetGroupBinding(context.TODO(), logCtx, awsClnt, tgb, nil, nil)
	assert.Nil(t, res)
	assert.NoError(t, err)
}

func TestVerifyTargetGroupBinding(t *testing.T) {
	logCtx := log.NewEntry(log.New())
	tgb := TargetGroupBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: TargetGroupBindingSpec{
			TargetType:     (*TargetType)(ptr.To[string]("ip")),
			TargetGroupARN: "arn::1234",
			ServiceRef: ServiceReference{
				Name: "active",
				Port: intstr.FromInt(80),
			},
		},
	}
	ep := corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active",
			Namespace: metav1.NamespaceDefault,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{
					{
						IP: "1.2.3.4", // registered
					},
					{
						IP: "5.6.7.8", // registered
					},
					{
						IP: "2.4.6.8", // not registered
					},
				},
				Ports: []corev1.EndpointPort{
					{
						Port:     8080,
						Protocol: "TCP",
					},
				},
			},
		},
	}
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{},
			Ports: []corev1.ServicePort{{
				Protocol:   "TCP",
				Port:       int32(80),
				TargetPort: intstr.FromInt(8080),
			}},
		},
	}
	fakeELB, awsClnt := newFakeClient()
	thOut := elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: []elbv2types.TargetHealthDescription{
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("1.2.3.4"),
					Port: ptr.To[int32](8080),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("5.6.7.8"),
					Port: ptr.To[int32](8080),
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("2.4.6.8"), // irrelevant
					Port: ptr.To[int32](8081),       // wrong port
				},
			},
			{
				Target: &elbv2types.TargetDescription{
					Id:   ptr.To[string]("9.8.7.6"), // irrelevant ip
					Port: ptr.To[int32](8080),
				},
			},
		},
	}
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)
	res, err := VerifyTargetGroupBinding(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
	expectedRes := TargetGroupVerifyResult{
		Service:             "active",
		Verified:            false,
		EndpointsRegistered: 2,
		EndpointsTotal:      3,
	}
	assert.Equal(t, expectedRes, *res)
	assert.NoError(t, err)
}

// healthTestTarget is a registered target with the given health state, for health-aware tests
type healthTestTarget struct {
	ip    string
	port  int32
	state elbv2types.TargetHealthStateEnum
}

func newHealthTestFixtures(endpointIPs ...string) (TargetGroupBinding, corev1.Endpoints, corev1.Service) {
	tgb := TargetGroupBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stable",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: TargetGroupBindingSpec{
			TargetType:     (*TargetType)(ptr.To[string]("ip")),
			TargetGroupARN: "arn::1234",
			ServiceRef: ServiceReference{
				Name: "stable",
				Port: intstr.FromInt(80),
			},
		},
	}
	var addresses []corev1.EndpointAddress
	for _, ip := range endpointIPs {
		addresses = append(addresses, corev1.EndpointAddress{IP: ip})
	}
	ep := corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stable",
			Namespace: metav1.NamespaceDefault,
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: addresses,
			Ports:     []corev1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	}
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stable",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{},
			Ports: []corev1.ServicePort{{
				Protocol:   "TCP",
				Port:       int32(80),
				TargetPort: intstr.FromInt(8080),
			}},
		},
	}
	return tgb, ep, svc
}

func newFakeClientWithTargets(targets ...healthTestTarget) Client {
	thOut := elbv2.DescribeTargetHealthOutput{}
	for _, target := range targets {
		thOut.TargetHealthDescriptions = append(thOut.TargetHealthDescriptions, elbv2types.TargetHealthDescription{
			Target: &elbv2types.TargetDescription{
				Id:   ptr.To[string](target.ip),
				Port: ptr.To[int32](target.port),
			},
			TargetHealth: &elbv2types.TargetHealth{
				State: target.state,
			},
		})
	}
	fakeELB, awsClnt := newFakeClient()
	fakeELB.On("DescribeTargetHealth", mock.Anything, mock.Anything).Return(&thOut, nil)
	return awsClnt
}

func TestVerifyTargetGroupBindingHealthy(t *testing.T) {
	logCtx := log.NewEntry(log.New())
	endpointIPs := []string{"1.2.3.4", "5.6.7.8", "2.4.6.8"}

	t.Run("RegisteredButInitialIsNotHealthy", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
		targets := []healthTestTarget{
			{"1.2.3.4", 8080, elbv2types.TargetHealthStateEnumInitial},
			{"5.6.7.8", 8080, elbv2types.TargetHealthStateEnumInitial},
			{"2.4.6.8", 8080, elbv2types.TargetHealthStateEnumInitial},
		}

		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, newFakeClientWithTargets(targets...), tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.Equal(t, TargetGroupVerifyResult{
			Service:             "stable",
			Verified:            false,
			EndpointsRegistered: 3,
			EndpointsHealthy:    0,
			EndpointsTotal:      3,
		}, *res)

		// registration-only verification on the same input still reports verified
		res, err = VerifyTargetGroupBinding(context.TODO(), logCtx, newFakeClientWithTargets(targets...), tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.True(t, res.Verified)
		assert.Equal(t, 3, res.EndpointsRegistered)
		assert.Equal(t, 3, res.EndpointsTotal)
	})

	t.Run("AllHealthyIsVerified", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
		awsClnt := newFakeClientWithTargets(
			healthTestTarget{"1.2.3.4", 8080, elbv2types.TargetHealthStateEnumHealthy},
			healthTestTarget{"5.6.7.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
			healthTestTarget{"2.4.6.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
		)

		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.Equal(t, TargetGroupVerifyResult{
			Service:             "stable",
			Verified:            true,
			EndpointsRegistered: 3,
			EndpointsHealthy:    3,
			EndpointsTotal:      3,
		}, *res)
	})

	// every state other than `healthy` counts as not healthy, with no exception
	notHealthyStates := []elbv2types.TargetHealthStateEnum{
		elbv2types.TargetHealthStateEnumInitial,
		elbv2types.TargetHealthStateEnumUnhealthy,
		elbv2types.TargetHealthStateEnumUnhealthyDraining,
		elbv2types.TargetHealthStateEnumDraining,
		elbv2types.TargetHealthStateEnumUnused,
		elbv2types.TargetHealthStateEnumUnavailable,
	}
	for _, state := range notHealthyStates {
		t.Run("OneEndpoint_"+string(state), func(t *testing.T) {
			tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
			awsClnt := newFakeClientWithTargets(
				healthTestTarget{"1.2.3.4", 8080, elbv2types.TargetHealthStateEnumHealthy},
				healthTestTarget{"5.6.7.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
				healthTestTarget{"2.4.6.8", 8080, state},
			)

			res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
			assert.NoError(t, err)
			assert.False(t, res.Verified)
			assert.Equal(t, 3, res.EndpointsRegistered)
			assert.Equal(t, 2, res.EndpointsHealthy)
			assert.Equal(t, 3, res.EndpointsTotal)
		})
	}

	t.Run("MixedStates", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5", "6.6.6.6")
		targets := []healthTestTarget{
			{"1.1.1.1", 8080, elbv2types.TargetHealthStateEnumHealthy},
			{"2.2.2.2", 8080, elbv2types.TargetHealthStateEnumInitial},
			{"3.3.3.3", 8080, elbv2types.TargetHealthStateEnumDraining},
			{"4.4.4.4", 8080, elbv2types.TargetHealthStateEnumUnhealthy},
			{"5.5.5.5", 8080, elbv2types.TargetHealthStateEnumUnused},
			{"6.6.6.6", 8080, elbv2types.TargetHealthStateEnumUnavailable},
		}

		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, newFakeClientWithTargets(targets...), tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.Equal(t, TargetGroupVerifyResult{
			Service:             "stable",
			Verified:            false,
			EndpointsRegistered: 6,
			EndpointsHealthy:    1,
			EndpointsTotal:      6,
		}, *res)

		// registration-only verification ignores health
		res, err = VerifyTargetGroupBinding(context.TODO(), logCtx, newFakeClientWithTargets(targets...), tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.True(t, res.Verified)
		assert.Equal(t, 6, res.EndpointsRegistered)
		assert.Equal(t, 6, res.EndpointsTotal)
	})

	t.Run("UnregisteredEndpointIsNotHealthy", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
		awsClnt := newFakeClientWithTargets(
			healthTestTarget{"1.2.3.4", 8080, elbv2types.TargetHealthStateEnumHealthy},
			healthTestTarget{"5.6.7.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
		)

		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.Equal(t, TargetGroupVerifyResult{
			Service:             "stable",
			Verified:            false,
			EndpointsRegistered: 2,
			EndpointsHealthy:    2,
			EndpointsTotal:      3,
		}, *res)
	})

	t.Run("NonEndpointTargetsAreIgnored", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
		awsClnt := newFakeClientWithTargets(
			healthTestTarget{"1.2.3.4", 8080, elbv2types.TargetHealthStateEnumHealthy},
			healthTestTarget{"5.6.7.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
			healthTestTarget{"2.4.6.8", 8080, elbv2types.TargetHealthStateEnumHealthy},
			// old pods being deregistered, not endpoints of the service
			healthTestTarget{"9.8.7.6", 8080, elbv2types.TargetHealthStateEnumDraining},
			healthTestTarget{"9.8.7.5", 8080, elbv2types.TargetHealthStateEnumUnhealthy},
			// endpoint IP on another port
			healthTestTarget{"2.4.6.8", 8081, elbv2types.TargetHealthStateEnumInitial},
		)

		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
		assert.NoError(t, err)
		assert.Equal(t, TargetGroupVerifyResult{
			Service:             "stable",
			Verified:            true,
			EndpointsRegistered: 3,
			EndpointsHealthy:    3,
			EndpointsTotal:      3,
		}, *res)
	})

	t.Run("InstanceModeIsNotApplicable", func(t *testing.T) {
		_, awsClnt := newFakeClient()
		tgb := TargetGroupBinding{
			Spec: TargetGroupBindingSpec{
				TargetType: (*TargetType)(ptr.To[string]("instance")),
			},
		}
		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, nil, nil)
		assert.Nil(t, res)
		assert.NoError(t, err)
	})

	t.Run("UnmatchedPortIsNotApplicable", func(t *testing.T) {
		tgb, ep, svc := newHealthTestFixtures(endpointIPs...)
		tgb.Spec.ServiceRef.Port = intstr.FromInt(8443)
		_, awsClnt := newFakeClient()
		res, err := VerifyTargetGroupBindingHealthy(context.TODO(), logCtx, awsClnt, tgb, &ep, &svc)
		assert.Nil(t, res)
		assert.NoError(t, err)
	})
}

func TestIsTargetTypeIP(t *testing.T) {
	assert.True(t, IsTargetTypeIP(TargetGroupBinding{Spec: TargetGroupBindingSpec{TargetType: (*TargetType)(ptr.To[string]("ip"))}}))
	assert.False(t, IsTargetTypeIP(TargetGroupBinding{Spec: TargetGroupBindingSpec{TargetType: (*TargetType)(ptr.To[string]("instance"))}}))
	assert.False(t, IsTargetTypeIP(TargetGroupBinding{}))
}

func TestMatchesServicePort(t *testing.T) {
	svc := corev1.Service{
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("web")}},
		},
	}
	tgbWithPort := func(port intstr.IntOrString) TargetGroupBinding {
		return TargetGroupBinding{Spec: TargetGroupBindingSpec{ServiceRef: ServiceReference{Port: port}}}
	}
	assert.True(t, matchesServicePort(tgbWithPort(intstr.FromInt(80)), svc))
	assert.True(t, matchesServicePort(tgbWithPort(intstr.FromString("http")), svc))
	assert.False(t, matchesServicePort(tgbWithPort(intstr.FromInt(8443)), svc))
	assert.False(t, matchesServicePort(tgbWithPort(intstr.FromString("https")), svc))
	assert.False(t, matchesServicePort(tgbWithPort(intstr.FromString("")), svc))
}
