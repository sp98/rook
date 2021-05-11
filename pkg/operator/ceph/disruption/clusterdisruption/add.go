/*
Copyright 2019 The Rook Authors. All rights reserved.

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

package clusterdisruption

import (
	"reflect"

	"github.com/rook/rook/pkg/operator/ceph/cluster/osd"
	"github.com/rook/rook/pkg/operator/ceph/disruption/controllerconfig"
	"github.com/rook/rook/pkg/operator/k8sutil"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Add adds a new Controller to the Manager based on clusterdisruption.ReconcileClusterDisruption and registers the relevant watches and handlers.
// Read more about how Managers, Controllers, and their Watches, Handlers, Predicates, etc work here:
// https://godoc.org/github.com/kubernetes-sigs/controller-runtime/pkg
func Add(mgr manager.Manager, context *controllerconfig.Context) error {

	// Add the cephv1 scheme to the manager scheme
	mgrScheme := mgr.GetScheme()
	if err := cephv1.AddToScheme(mgr.GetScheme()); err != nil {
		return errors.Wrap(err, "failed to add ceph scheme to manager scheme")
	}

	// This will be used to associate namespaces and cephclusters.
	sharedClusterMap := &ClusterMap{}

	reconcileClusterDisruption := &ReconcileClusterDisruption{
		client:     mgr.GetClient(),
		scheme:     mgrScheme,
		context:    context,
		clusterMap: sharedClusterMap,
	}
	reconciler := reconcile.Reconciler(reconcileClusterDisruption)
	// Create a new controller
	c, err := controller.New(controllerName, mgr, controller.Options{Reconciler: reconciler})
	if err != nil {
		return err
	}

	cephClusterPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			logger.Info("create event from ceph cluster CR")
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldCluster, ok := e.ObjectOld.DeepCopyObject().(*cephv1.CephCluster)
			if !ok {
				return false
			}
			newCluster, ok := e.ObjectNew.DeepCopyObject().(*cephv1.CephCluster)
			if !ok {
				return false
			}
			return !reflect.DeepEqual(oldCluster.Spec, newCluster.Spec)
		},
	}

	// Watch for CephClusters
	err = c.Watch(&source.Kind{Type: &cephv1.CephCluster{}}, &handler.EnqueueRequestForObject{}, cephClusterPredicate)
	if err != nil {
		return err
	}

	// This means that one of the OSD is down due to node drain or any other reason
	osdPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// Do not reconcile when OSD is created
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			osd, ok := e.ObjectNew.DeepCopyObject().(*appsv1.Deployment)
			if !ok {
				return false
			}
			logger.Debugf("osd deployment %q is updated. Unavailable replicas: %d", osd.Name, osd.Status.UnavailableReplicas)
			return osd.Status.UnavailableReplicas > 0
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Do not reconcile when OSD is deleted
			return false
		},
	}

	// Watch for OSD deployment and enqueue the CephCluster in the namespace
	err = c.Watch(
		&source.Kind{Type: &appsv1.Deployment{}},
		handler.EnqueueRequestsFromMapFunc(handler.MapFunc(func(obj client.Object) []reconcile.Request {
			deployment, ok := obj.(*appsv1.Deployment)
			if !ok {
				return []reconcile.Request{}
			}
			labels := deployment.GetLabels()
			appName, ok := labels[k8sutil.AppAttr]
			if !ok || appName != osd.AppName {
				return []reconcile.Request{}
			}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: deployment.Namespace}}
			return []reconcile.Request{req}
		}),
		),
		osdPredicate,
	)
	if err != nil {
		return err
	}

	// enqueues with an empty name that is populated by the reconciler.
	// There is a one-per-namespace limit on CephClusters
	enqueueByNamespace := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(func(obj client.Object) []reconcile.Request {
		// The name will be populated in the reconcile
		namespace := obj.GetNamespace()
		if len(namespace) == 0 {
			logger.Errorf("enqueueByNamespace received an obj without a namespace. %+v", obj)
			return []reconcile.Request{}
		}
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace}}
		return []reconcile.Request{req}
	}),
	)

	// Watch for CephBlockPools and enqueue the CephCluster in the namespace
	err = c.Watch(&source.Kind{Type: &cephv1.CephBlockPool{}}, enqueueByNamespace)
	if err != nil {
		return err
	}

	// Watch for CephFileSystems and enqueue the CephCluster in the namespace
	err = c.Watch(&source.Kind{Type: &cephv1.CephFilesystem{}}, enqueueByNamespace)
	if err != nil {
		return err
	}

	// Watch for CephObjectStores and enqueue the CephCluster in the namespace
	err = c.Watch(&source.Kind{Type: &cephv1.CephObjectStore{}}, enqueueByNamespace)
	if err != nil {
		return err
	}

	return nil
}
