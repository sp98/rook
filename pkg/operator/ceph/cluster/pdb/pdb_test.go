/*
Copyright 2016 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pdb

import (
	"fmt"
	"testing"

	v1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	rookclient "github.com/rook/rook/pkg/client/clientset/versioned/fake"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/operator/test"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	pv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestStart(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{Clientset: clientset, Executor: &exectest.MockExecutor{}}
	pdbController := NewPdbController(context)
	stopChan := make(chan struct{})
	// Start the first time
	err := pdbController.StartWatch(stopChan)
	assert.Nil(t, err)

	// Should not fail if it already exists
	err = pdbController.StartWatch(stopChan)
	assert.Nil(t, err)
}

func TestGetMonPDBSuccess(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{Clientset: clientset, Executor: &exectest.MockExecutor{}}
	pdbController := NewPdbController(context)
	pdbController.createMonPDB(4, monPDB)
	pdb, err := pdbController.GetMonPDB(namespace, monPDB)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, pdb.ObjectMeta.Name, monPDB, "failed to get correct mon PodDisruptionBudget")

}

func TestGetMonPDBFailure(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{Clientset: clientset, Executor: &exectest.MockExecutor{}}
	pdbController := NewPdbController(context)
	pdb, err := pdbController.GetMonPDB(namespace, monPDB)
	if err != nil {
		t.Error(err)
	}

	assert.Nil(t, pdb)
}

func TestGetClusterObjSuccess(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{
		Clientset:     clientset,
		RookClientset: rookclient.NewSimpleClientset(),
	}
	pdbController := NewPdbController(context)
	clusterObj1, _ := pdbController.createCluster(3)
	clusterObj2, _ := pdbController.GetClusterObj()

	assert.Equal(t, clusterObj1.ObjectMeta.Name, clusterObj2.ObjectMeta.Name, "cluster names not matching")

}
func TestGetClusterObjFailure(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{
		Clientset:     clientset,
		RookClientset: rookclient.NewSimpleClientset(),
	}
	pdbController := NewPdbController(context)
	clusterObj, _ := pdbController.GetClusterObj()
	assert.Nil(t, clusterObj)
}

func TestValidMinAvailableMon(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{
		Clientset:     clientset,
		RookClientset: rookclient.NewSimpleClientset(),
	}
	pdbController := NewPdbController(context)
	pdb, _ := pdbController.createMonPDB(3, monPDB)
	pdbController.createCluster(4)
	res := pdbController.ValidateMinAvailableMonCount(pdb)
	assert.True(t, res)

}

func TestInValidMinAvailableMon(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{
		Clientset:     clientset,
		RookClientset: rookclient.NewSimpleClientset(),
	}
	pdbController := NewPdbController(context)
	pdb, _ := pdbController.createMonPDB(3, monPDB)
	pdbController.createCluster(6)
	res := pdbController.ValidateMinAvailableMonCount(pdb)
	assert.False(t, res)

}

func TestValidateMonCountWithOutPDB(t *testing.T) {
	clientset := test.New(1)
	context := &clusterd.Context{
		Clientset:     clientset,
		RookClientset: rookclient.NewSimpleClientset(),
	}
	pdbController := NewPdbController(context)
	pdb, _ := pdbController.createMonPDB(3, "test")
	pdbController.createCluster(6)
	res := pdbController.ValidateMinAvailableMonCount(pdb)
	assert.False(t, res)
}

func (p *PdbController) createMonPDB(minAvailable int, name string) (*pv1beta1.PodDisruptionBudget, error) {
	minValue := intstr.FromInt(minAvailable)
	pdb := &pv1beta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: pv1beta1.PodDisruptionBudgetSpec{
			MinAvailable: &minValue,
		},
	}

	pdbObj, err := p.context.Clientset.PolicyV1beta1().PodDisruptionBudgets(namespace).Create(pdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create pdb with error - %v", err)
	}

	return pdbObj, nil

}

func (p *PdbController) createCluster(monCount int) (*v1.CephCluster, error) {

	cluster := &v1.CephCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rook-ceph",
		},
		Spec: v1.ClusterSpec{
			Mon: v1.MonSpec{
				Count: monCount,
			},
		},
	}

	clusterObj, err := p.context.RookClientset.CephV1().CephClusters(namespace).Create(cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to create pdb with error - %+v", err)
	}

	return clusterObj, nil
}
