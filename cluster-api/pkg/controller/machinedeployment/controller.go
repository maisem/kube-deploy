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
	"sort"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/apiserver-builder/pkg/builders"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/integer"

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

// calculateStatus calculates the latest status for the provided deployment by looking into the provided replica sets.
func calculateStatus(allMSs []*v1alpha1.MachineSet, newMS *v1alpha1.MachineSet, md *v1alpha1.MachineDeployment) v1alpha1.MachineDeploymentStatus {
	var (
		availableReplicas     int32
		totalReplicas         int32
		totalReplicasObserved int32
		readyReplicas         int32
	)
	for _, ms := range allMSs {
		totalReplicas += *ms.Spec.Replicas

		availableReplicas += ms.Status.AvailableReplicas
		readyReplicas += ms.Status.ReadyReplicas
		totalReplicasObserved += ms.Status.Replicas
	}
	unavailableReplicas := totalReplicas - availableReplicas
	// If unavailableReplicas is negative, then that means the Deployment has more available replicas running than
	// desired, e.g. whenever it scales down. In such a case we should simply default unavailableReplicas to zero.
	if unavailableReplicas < 0 {
		unavailableReplicas = 0
	}

	status := v1alpha1.MachineDeploymentStatus{
		ObservedGeneration:  md.Generation,
		Replicas:            totalReplicasObserved,
		UpdatedReplicas:     newMS.Status.Replicas,
		ReadyReplicas:       readyReplicas,
		AvailableReplicas:   availableReplicas,
		UnavailableReplicas: unavailableReplicas,
	}

	return status
}

func (c *MachineDeploymentControllerImpl) rolloutRolling(d *v1alpha1.MachineDeployment, msList []*v1alpha1.MachineSet) error {
	return nil
}

func (c *MachineDeploymentControllerImpl) getAllMachineSetsAndSyncRevision(d *v1alpha1.MachineDeployment, msList []*v1alpha1.MachineSet) (*v1alpha1.MachineSet, []*v1alpha1.MachineSet, error) {
	return nil, nil, nil
}

func (c *MachineDeploymentControllerImpl) syncDeploymentStatus(d *v1alpha1.MachineDeployment, allMSs []*v1alpha1.MachineSet, newMS *v1alpha1.MachineSet) error {
	newStatus := calculateStatus(allMSs, newMS, d)

	if reflect.DeepEqual(d.Status, newStatus) {
		return nil
	}
	return nil
}

func (c *MachineDeploymentControllerImpl) getMachineSetsForDeployment(d *v1alpha1.MachineDeployment) ([]*v1alpha1.MachineSet, error) {
	deploymentSelector, err := metav1.LabelSelectorAsSelector(&d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %v", d.Namespace, d.Name, err)
	}
	return c.msLister.MachineSets(d.Namespace).List(deploymentSelector)
}

func (c *MachineDeploymentControllerImpl) scaleMachineSet(ms *v1alpha1.MachineSet, size int32) error {
	msCopy := ms.DeepCopy()

	if *(msCopy.Spec.Replicas) != size {
		*(msCopy.Spec.Replicas) = size
		_, err := c.machineClient.ClusterV1alpha1().MachineSets(msCopy.Namespace).Update(msCopy)
		return err
	}
	return nil
}

// MachineSetsBySizeOlder sorts a list of MachineSet by size in descending order, using their creation timestamp or name as a tie breaker.
// By using the creation timestamp, this sorts from old to new replica sets.
type MachineSetsBySizeOlder []*v1alpha1.MachineSet

func (o MachineSetsBySizeOlder) Len() int      { return len(o) }
func (o MachineSetsBySizeOlder) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o MachineSetsBySizeOlder) Less(i, j int) bool {
	if *(o[i].Spec.Replicas) == *(o[j].Spec.Replicas) {
		return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
	}
	return *(o[i].Spec.Replicas) > *(o[j].Spec.Replicas)
}

// MachineSetsBySizeNewer sorts a list of MachineSet by size in descending order, using their creation timestamp or name as a tie breaker.
// By using the creation timestamp, this sorts from new to old replica sets.
type MachineSetsBySizeNewer []*v1alpha1.MachineSet

func (o MachineSetsBySizeNewer) Len() int      { return len(o) }
func (o MachineSetsBySizeNewer) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o MachineSetsBySizeNewer) Less(i, j int) bool {
	if *(o[i].Spec.Replicas) == *(o[j].Spec.Replicas) {
		return o[j].CreationTimestamp.Before(&o[i].CreationTimestamp)
	}
	return *(o[i].Spec.Replicas) > *(o[j].Spec.Replicas)
}

// GetProportion will estimate the proportion for the provided replica set using 1. the current size
// of the parent deployment, 2. the replica count that needs be added on the replica sets of the
// deployment, and 3. the total replicas added in the replica sets of the deployment so far.
func GetProportion(ms *v1alpha1.MachineSet, desired, surge, replicasToAdd, replicasAdded int32) int32 {
	if ms == nil || *(ms.Spec.Replicas) == 0 || replicasToAdd == 0 || replicasToAdd == replicasAdded {
		return int32(0)
	}

	msFraction := getMachineSetFraction(ms, desired, surge)
	allowed := replicasToAdd - replicasAdded

	if replicasToAdd > 0 {
		// Use the minimum between the replica set fraction and the maximum allowed replicas
		// when scaling up. This way we ensure we will not scale up more than the allowed
		// replicas we can add.
		return integer.Int32Min(msFraction, allowed)
	}
	// Use the maximum between the replica set fraction and the maximum allowed replicas
	// when scaling down. This way we ensure we will not scale down more than the allowed
	// replicas we can remove.
	return integer.Int32Max(msFraction, allowed)
}

func getMachineSetFraction(ms *v1alpha1.MachineSet, desired, surge int32) int32 {
	totalDesired := desired + surge
	if totalDesired == 0 {
		return -*(ms.Spec.Replicas)
	}
	newSize := float64(*ms.Spec.Replicas*totalDesired) / float64(surge)
	return integer.RoundToInt32(newSize) - *(ms.Spec.Replicas)
}

func (c *MachineDeploymentControllerImpl) scale(d *v1alpha1.MachineDeployment, newMS *v1alpha1.MachineSet, oldMSs []*v1alpha1.MachineSet) error {
	var activeMachineSets []*v1alpha1.MachineSet

	if *newMS.Spec.Replicas == *d.Spec.Replicas {
		// This means that the new machine set already has all the replicas.
		// We should scale down all old machine sets.
		for _, ms := range oldMSs {
			if *ms.Spec.Replicas > 0 {
				if err := c.scaleMachineSet(ms, 0); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if d.Spec.Strategy.Type != common.RollingUpdateMachineDeploymentStrategyType {
		return nil
	}

	curSize := int32(0)
	if *newMS.Spec.Replicas > 0 {
		curSize += *newMS.Spec.Replicas
		activeMachineSets = append(activeMachineSets, newMS)
	}
	for _, ms := range oldMSs {
		if *ms.Spec.Replicas > 0 {
			curSize += *ms.Spec.Replicas
			activeMachineSets = append(activeMachineSets, ms)
		}
	}

	allowedSize := int32(0)
	maxSurge, _ := intstr.GetValueFromIntOrPercent(d.Spec.Strategy.RollingUpdate.MaxSurge, int(*d.Spec.Replicas), true)
	if *(d.Spec.Replicas) > 0 {
		allowedSize = *(d.Spec.Replicas) + int32(maxSurge)
	}

	delta := allowedSize - curSize

	// The additional replicas should be distributed proportionally amongst the active
	// replica sets from the larger to the smaller in size replica set. Scaling direction
	// drives what happens in case we are trying to scale replica sets of the same size.
	// In such a case when scaling up, we should scale up newer replica sets first, and
	// when scaling down, we should scale down older replica sets first.
	switch {
	case delta > 0:
		sort.Sort(MachineSetsBySizeNewer(activeMachineSets))
	case delta < 0:
		sort.Sort(MachineSetsBySizeOlder(activeMachineSets))
	}

	replicasAdded := int32(0)
	nameToSize := make(map[string]int32)
	for _, ms := range activeMachineSets {
		// Estimate proportions if we have replicas to add, otherwise simply populate
		// nameToSize with the current sizes for each replica set.
		if delta != 0 {
			proportion := GetProportion(ms, *d.Spec.Replicas, int32(maxSurge), delta, replicasAdded)

			nameToSize[ms.Name] = *(ms.Spec.Replicas) + proportion
			replicasAdded += proportion
		} else {
			nameToSize[ms.Name] = *(ms.Spec.Replicas)
		}
	}
	if delta != 0 && len(activeMachineSets) > 0 {
		ms := activeMachineSets[0]
		leftover := delta - replicasAdded
		nameToSize[ms.Name] = nameToSize[ms.Name] + leftover
		if nameToSize[ms.Name] < 0 {
			nameToSize[ms.Name] = 0
		}
	}

	for _, ms := range activeMachineSets {
		if err := c.scaleMachineSet(ms, nameToSize[ms.Name]); err != nil {
			// Return as soon as we fail, the deployment is requeued
			return err
		}
	}
	return nil
}

func (c *MachineDeploymentControllerImpl) sync(d *v1alpha1.MachineDeployment, msList []*v1alpha1.MachineSet, scale bool) error {
	newMS, oldMSs, err := c.getAllMachineSetsAndSyncRevision(d, msList)
	if err != nil {
		return err
	}
	if scale {
		if err := c.scale(d, newMS, oldMSs); err != nil {
			return err
		}
	}

	allMSs := append(oldMSs, newMS)
	return c.syncDeploymentStatus(d, allMSs, newMS)
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

	msList, err := c.getMachineSetsForDeployment(d)
	if err != nil {
		return err
	}

	if d.DeletionTimestamp != nil {
		return c.sync(d, msList, false)
	}

	if d.Spec.Paused {
		return c.sync(d, msList, true)
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
