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

//NewPdbController is
func NewPdbController(context *clusterd.Context) *PdbController {
	return &PdbController{
		context: context,
	}
}

// StartWatch is
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
	logger.Debugf("New PodDistributionBudget object added - %v", obj)
	newPDB, ok := obj.(*v1beta1.PodDisruptionBudget)
	if !ok {
		logger.Warningf("Expected PodDistributionBudget but handler received %#v", obj)
	}
	p.validateMonPDB(newPDB)

}

func (p *PdbController) onUpdate(oldObj, newObj interface{}) {
	logger.Infof("Old PDB object update to new value")
	newPDB, ok := newObj.(*v1beta1.PodDisruptionBudget)
	if !ok {
		logger.Warningf("Expected PodDistributionBudget but handler received %+v", newObj)
	}
	p.validateMonPDB(newPDB)

}

func (p *PdbController) validateMonPDB(pdb *v1beta1.PodDisruptionBudget) {
	if pdb.ObjectMeta.Name == monPDB {
		logger.Debug("New PodDistributionBudget for Mon Found")
		clusterObj, _ := p.getClusterObj()
		if clusterObj != nil {
			minAvaliableMon := int(pdb.Spec.MinAvailable.IntVal)
			if !p.ValidateMinAvaliableMon(clusterObj.Spec.Mon.Count, minAvaliableMon) {
				logger.Warningf("Mon Count - %v not consistent with minAvaliable mon in PDB - %v ", clusterObj.Spec.Mon.Count, minAvaliableMon)
			}
		}

	}
}

//GetMonPDB is to
func (p *PdbController) GetMonPDB() (*v1beta1.PodDisruptionBudget, error) {
	pdbOptions := metav1.GetOptions{}
	pdb, err := p.context.Clientset.PolicyV1beta1().PodDisruptionBudgets(namespace).Get(monPDB, pdbOptions)

	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Warningf("Error checking for existing Pod Disruption Budgets  - %+v", err)
			return nil, err
		}
	}
	logger.Infof("Mon PDB found - %+v", pdb)

	return pdb, nil
}

func (p *PdbController) getClusterObj() (*v1.CephCluster, error) {
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

//ValidateMinAvaliableMon is
func (p *PdbController) ValidateMinAvaliableMon(monCount int, minAvaliable int) bool {
	return minAvaliable == ((monCount / 2) + 1)

}
