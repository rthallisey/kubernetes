/*
Copyright 2026 The Kubernetes Authors.

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

package rest

import (
	lifecyclev1alpha1 "k8s.io/api/lifecycle/v1alpha1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	lifecycleeventstore "k8s.io/kubernetes/pkg/registry/lifecycle/lifecycleevent/storage"
	lifecycletransitionstore "k8s.io/kubernetes/pkg/registry/lifecycle/lifecycletransition/storage"
)

// RESTStorageProvider is a provider for the lifecycle API group REST storage.
type RESTStorageProvider struct{}

func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(lifecycle.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)

	if storageMap, err := p.v1alpha1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[lifecyclev1alpha1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

func (p RESTStorageProvider) v1alpha1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// lifecycletransitions
	if resource := "lifecycletransitions"; apiResourceConfigSource.ResourceEnabled(lifecyclev1alpha1.SchemeGroupVersion.WithResource(resource)) {
		transitionStorage, err := lifecycletransitionstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = transitionStorage
	}

	// lifecycleevents
	if resource := "lifecycleevents"; apiResourceConfigSource.ResourceEnabled(lifecyclev1alpha1.SchemeGroupVersion.WithResource(resource)) {
		eventStorage, eventStatusStorage, err := lifecycleeventstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = eventStorage
		storage[resource+"/status"] = eventStatusStorage
	}

	return storage, nil
}

func (p RESTStorageProvider) GroupName() string {
	return lifecycle.GroupName
}
