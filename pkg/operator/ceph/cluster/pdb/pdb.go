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
	"github.com/coreos/pkg/capnslog"
	v1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	"k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

var (
	logger = capnslog.NewPackageLogger("github.com/rook/rook", "op-pdb")
)

const (
	namespace = "rook-ceph"
	monPDB    = "mon-pdb"
)

//PdbController checks
type PdbController struct {
	context *clusterd.Context
}

//NewPdbController is creates a new PDB controller
func NewPdbController(context *clusterd.Context) *PdbController {
	return &PdbController{
		context: context,
	}
}

// StartWatch starts watcher for Pod Disruption Budgets
func (p *PdbController) StartWatch(stopCh chan struct{}) error {
	logger.Info("Watcher for PDB has started")
	lwPDB := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return p.context.Clientset.PolicyV1beta1().PodDisruptionBudgets(namespace).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return p.context.Clientset.PolicyV1beta1().PodDisruptionBudgets(namespace).Watch(options)
		},
	}

	_, PDBController := cache.NewInformer(
		lwPDB,
		&v1beta1.PodDisruptionBudget{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    p.onAdd,
			UpdateFunc: p.onUpdate,
			DeleteFunc: nil,
		},
	)
	go PDBController.Run(stopCh)

	return nil
}

func (p *PdbController) onAdd(obj interface{}) {
	logger.Debugf("New PodDisruptionBudget object added - %v", obj)
	newPDB, ok := obj.(*v1beta1.PodDisruptionBudget)
	if !ok {
		logger.Warningf("Expected PodDisruptionBudget but handler received %#v", obj)
	}
	p.ValidateMinAvailableMonCount(newPDB)

}

func (p *PdbController) onUpdate(oldObj, newObj interface{}) {
	logger.Debugf("PodDisruptionBudget resource updated")
	newPDB, ok := newObj.(*v1beta1.PodDisruptionBudget)
	if !ok {
		logger.Warningf("Expected PodDistributionBudget but handler received %+v", newObj)
	}
	p.ValidateMinAvailableMonCount(newPDB)

}

//ValidateMinAvailableMonCount validates the minAvaliable Field in the PodDisruptionBudget spec
func (p *PdbController) ValidateMinAvailableMonCount(pdb *v1beta1.PodDisruptionBudget) (bool, error) {
	isValid := false
	if pdb.ObjectMeta.Name == monPDB {
		logger.Debug("New PodDistributionBudget for Mon Found")
		clusterObj, err := p.GetClusterObj()
		if err != nil {
			return isValid, err
		}

		if clusterObj.Spec.Mon.Count != 0 {
			minAvaliableMon := int(pdb.Spec.MinAvailable.IntVal)
			if !p.ValidateMonCount(clusterObj.Spec.Mon.Count, minAvaliableMon) {
				logger.Warningf("Mon Count - %v not consistent with minAvaliable mon in PDB - %v ", clusterObj.Spec.Mon.Count, minAvaliableMon)
				return isValid, nil
			}
			isValid = true
			return isValid, nil
		}

	}

	return isValid, nil
}

//GetMonPDB fetchs the PodDisruptionBudget created for Mons
func (p *PdbController) GetMonPDB(namespace string, name string) (*v1beta1.PodDisruptionBudget, error) {
	pdbOptions := metav1.ListOptions{}
	pdbs, err := p.context.Clientset.PolicyV1beta1().PodDisruptionBudgets(namespace).List(pdbOptions)

	if err != nil {
		logger.Errorf("Error checking for existing Pod Disruption Budgets  - %+v", err)
		return nil, err
	}

	for _, pdb := range pdbs.Items {
		if pdb.ObjectMeta.Name == name {
			logger.Infof("Mon PDB found - %+v", pdb)
			return &pdb, nil
		}

	}

	logger.Infof("PodDisruptionBudget not created for mons")
	return nil, nil
}

// GetClusterObj fetchs the ceph cluster object.
func (p *PdbController) GetClusterObj() (*v1.CephCluster, error) {
	clusterOptions := metav1.GetOptions{}
	clusterobj, err := p.context.RookClientset.CephV1().CephClusters(namespace).Get("rook-ceph", clusterOptions)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Warningf("Failed to retrive cluster object - %v", err)
			return nil, err
		}
	}
	return clusterobj, nil

}

//ValidateMonCount compares mon count in ceph cluster ceph with the minAvaliable field in Mon PodDisributionBudget
func (p *PdbController) ValidateMonCount(monCount int, minAvaliable int) bool {
	return minAvaliable == ((monCount / 2) + 1)

}
