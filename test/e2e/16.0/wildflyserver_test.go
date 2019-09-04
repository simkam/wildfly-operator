// +build !unit

package e2e

import (
	"testing"

	framework "github.com/operator-framework/operator-sdk/pkg/test"
	"github.com/wildfly/wildfly-operator/pkg/apis"
	wildflyv1alpha1 "github.com/wildfly/wildfly-operator/pkg/apis/wildfly/v1alpha1"
	wildflyframework "github.com/wildfly/wildfly-operator/test/framework"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWildFly16Server(t *testing.T) {
	wildflyServerList := &wildflyv1alpha1.WildFlyServerList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "WildFlyServer",
			APIVersion: "wildfly.org/v1alpha1",
		},
	}
	err := framework.AddToFrameworkScheme(apis.AddToScheme, wildflyServerList)
	if err != nil {
		t.Fatalf("failed to add custom resource scheme to framework: %v", err)
	}
	// run subtests
	t.Run("BasicTest", wildFlyBasicTest)
	t.Run("ClusterTest", wildFlyClusterTest)
	t.Run("SmokeRecoveryScaledownTest", wildFlySmokeRecoveryScaledownTest)
}

func wildFlyBasicTest(t *testing.T) {
	wildflyframework.WildFlyBasicTest(t, "16.0")
}

func wildFlyClusterTest(t *testing.T) {
	wildflyframework.WildFlyClusterTest(t, "16.0")
}

func wildFlySmokeRecoveryScaledownTest(t *testing.T) {
	wildflyframework.WildFlySmokeRecoveryScaledownTest(t, "16.0")
}
