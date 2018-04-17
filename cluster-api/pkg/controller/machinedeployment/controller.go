/*
Copyright 2018 The Kubernetes Authors.

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

package machinedeployment

import (
	"fmt"
	"reflect"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/apiserver-builder/pkg/builders"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"k8s.io/kube-deploy/cluster-api/pkg/apis/cluster/common"
	"k8s.io/kube-deploy/cluster-api/pkg/apis/cluster/v1alpha1"
	"k8s.io/kube-deploy/cluster-api/pkg/client/clientset_generated/clientset"
	listers "k8s.io/kube-deploy/cluster-api/pkg/client/listers_generated/cluster/v1alpha1"
	"k8s.io/kube-deploy/cluster-api/pkg/controller/sharedinformers"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = v1alpha1.SchemeGroupVersion.WithKind("MachineDeployment")

// +controller:group=cluster,version=v1alpha1,kind=MachineDeployment,resource=machinedeployments
type MachineDeploymentControllerImpl struct {
	builders.DefaultControllerFns

	// machineClient a client that knows how to consume Machine resources
	machineClient clientset.Interface

	// lister indexes properties about MachineDeployment
	mdLister listers.MachineDeploymentLister
	msLister listers.MachineSetLister

	mdListerSynced cache.InformerSynced
	msListerSynced cache.InformerSynced
}

// Init initializes the controller and is called by the generated code
// Register watches for additional resource types here.
func (c *MachineDeploymentControllerImpl) Init(arguments sharedinformers.ControllerInitArguments) {
	// Use the lister for indexing machinedeployments labels
	msInformer := arguments.GetSharedInformers().Factory.Cluster().V1alpha1().MachineSets()
	mdInformer := arguments.GetSharedInformers().Factory.Cluster().V1alpha1().MachineDeployments()
	c.msLister = msInformer.Lister()
	c.mdLister = mdInformer.Lister()
	c.msListerSynced = msInformer.Informer().HasSynced
	c.mdListerSynced = mdInformer.Informer().HasSynced

	msInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addMachineSet,
		UpdateFunc: c.updateMachineSet,
		DeleteFunc: c.deleteMachineSet,
	})

	mc, err := clientset.NewForConfig(arguments.GetRestConfig())
	if err != nil {
		glog.Fatalf("error building clientset for machineClient: %v", err)
	}
	c.machineClient = mc
}

// Reconcile handles enqueued messages
func (c *MachineDeploymentControllerImpl) Reconcile(u *v1alpha1.MachineDeployment) error {
	// Deep-copy otherwise we are mutating our cache.
	d := u.DeepCopy()

	everything := metav1.LabelSelector{}
	if reflect.DeepEqual(d.Spec.Selector, &everything) {
		if d.Status.ObservedGeneration < d.Generation {
			d.Status.ObservedGeneration = d.Generation
			c.machineClient.ClusterV1alpha1().MachineDeployments(d.Namespace).UpdateStatus(d)
		}
		return nil
	}

	// List ReplicaSets owned by this Deployment, while reconciling ControllerRef
	// through adoption/orphaning.
	msList, err := c.getMachineSetsForDeployment(d)
	if err != nil {
		return err
	}

	if d.DeletionTimestamp != nil {
		return c.syncStatusOnly(d, msList)
	}

	// Update deployment conditions with an Unknown condition when pausing/resuming
	// a deployment. In this way, we can be sure that we won't timeout when a user
	// resumes a Deployment with a set progressDeadlineSeconds.
	if err = c.checkPausedConditions(d); err != nil {
		return err
	}

	if d.Spec.Paused {
		return c.sync(d, msList)
	}

	switch d.Spec.Strategy.Type {
	case common.RollingUpdateMachineDeploymentStrategyType:
		return c.rolloutRolling(d, msList)
	}
	return fmt.Errorf("unexpected deployment strategy type: %s", d.Spec.Strategy.Type)
}

func (c *MachineDeploymentControllerImpl) Get(namespace, name string) (*v1alpha1.MachineDeployment, error) {
	return c.mdLister.MachineDeployments(namespace).Get(name)
}

// addMachineSet enqueues the deployment that manages a MachineSet when the MachineSet is created.
func (c *MachineDeploymentControllerImpl) addMachineSet(obj interface{}) {
	ms := obj.(*v1alpha1.MachineSet)

	if ms.DeletionTimestamp != nil {
		// On a restart of the controller manager, it's possible for an object to
		// show up in a state that is already pending deletion.
		c.deleteMachineSet(ms)
		return
	}

	// If it has a ControllerRef, that's all that mattems.
	if controllerRef := metav1.GetControllerOf(ms); controllerRef != nil {
		d := c.resolveControllerRef(ms.Namespace, controllerRef)
		if d == nil {
			return
		}
		glog.V(4).Infof("MachineSet %s added.", ms.Name)
		c.Reconcile(d)
		return
	}

	// Otherwise, it's an orphan. Get a list of all matching Deployments and sync
	// them to see if anyone wants to adopt it.
	mds := c.getMachineDeploymentsForMachineSet(ms)
	if len(mds) == 0 {
		return
	}
	glog.V(4).Infof("Orphan MachineSet %s added.", ms.Name)
	for _, d := range mds {
		c.Reconcile(d)
	}
}

// getMachineDeploymentsForMachineSet returns a list of Deployments that potentially
// match a MachineSet.
func (c *MachineDeploymentControllerImpl) getMachineDeploymentsForMachineSet(ms *v1alpha1.MachineSet) []*v1alpha1.MachineDeployment {
	if len(ms.Labels) == 0 {
		return nil
	}

	dList, err := c.mdLister.MachineDeployments(ms.Namespace).List(labels.Everything())
	if err != nil {
		return nil
	}

	var deployments []*v1alpha1.MachineDeployment
	for _, d := range dList {
		selector, err := metav1.LabelSelectorAsSelector(&d.Spec.Selector)
		if err != nil {
			continue
		}
		// If a deployment with a nil or empty selector creeps in, it should match nothing, not everything.
		if selector.Empty() || !selector.Matches(labels.Set(ms.Labels)) {
			continue
		}
		deployments = append(deployments, d)
	}

	return deployments
}

// updateMachineSet figures out what deployment(s) manage a MachineSet when the MachineSet
// is updated and wake them up. If the anything of the MachineSets have changed, we need to
// awaken both the old and new deployments. old and cur must be *v1alpha1.MachineSet
// types.
func (c *MachineDeploymentControllerImpl) updateMachineSet(old, cur interface{}) {
	curMS := cur.(*v1alpha1.MachineSet)
	oldMS := old.(*v1alpha1.MachineSet)
	if curMS.ResourceVersion == oldMS.ResourceVersion {
		// Periodic resync will send update events for all known machine sets.
		// Two different vemsions of the same machine set will always have different RVs.
		return
	}

	curControllerRef := metav1.GetControllerOf(curMS)
	oldControllerRef := metav1.GetControllerOf(oldMS)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if d := c.resolveControllerRef(oldMS.Namespace, oldControllerRef); d != nil {
			c.Reconcile(d)
		}
	}

	// If it has a ControllerRef, that's all that mattems.
	if curControllerRef != nil {
		d := c.resolveControllerRef(curMS.Namespace, curControllerRef)
		if d == nil {
			return
		}
		glog.V(4).Infof("MachineSet %s updated.", curMS.Name)
		c.Reconcile(d)
		return
	}

	// Otherwise, it's an orphan. If anything changed, sync matching controllems
	// to see if anyone wants to adopt it now.
	labelChanged := !reflect.DeepEqual(curMS.Labels, oldMS.Labels)
	if labelChanged || controllerRefChanged {
		mds := c.getMachineDeploymentsForMachineSet(curMS)
		if len(mds) == 0 {
			return
		}
		glog.V(4).Infof("Orphan MachineSet %s updated.", curMS.Name)
		for _, d := range mds {
			c.Reconcile(d)
		}
	}
}

// deleteMachineSet enqueues the deployment that manages a MachineSet when
// the MachineSet is deleted.
func (c *MachineDeploymentControllerImpl) deleteMachineSet(obj interface{}) {
	ms := obj.(*v1alpha1.MachineSet)

	controllerRef := metav1.GetControllerOf(ms)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}
	d := c.resolveControllerRef(ms.Namespace, controllerRef)
	if d == nil {
		return
	}
	glog.V(4).Infof("MachineSet %s deleted.", ms.Name)
	c.Reconcile(d)
}

// resolveControllerRef returns the controller referenced by a ControllerRef,
// or nil if the ControllerRef could not be resolved to a matching controller
// of the correct Kind.
func (c *MachineDeploymentControllerImpl) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *v1alpha1.MachineDeployment {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	d, err := c.mdLister.MachineDeployments(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if d.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return d
}
