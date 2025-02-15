// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build kubeapiserver && orchestrator
// +build kubeapiserver,orchestrator

package orchestrator

import (
	"time"

	"github.com/DataDog/datadog-agent/pkg/aggregator"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors/inventory"
	"github.com/DataDog/datadog-agent/pkg/orchestrator"
	"github.com/DataDog/datadog-agent/pkg/util/kubernetes/apiserver"
	"github.com/DataDog/datadog-agent/pkg/util/log"

	"k8s.io/client-go/tools/cache"
)

const (
	defaultExtraSyncTimeout = 60 * time.Second
)

// CollectorBundle is a container for a group of collectors. It provides a way
// to easily run them all.
type CollectorBundle struct {
	check            *OrchestratorCheck
	collectors       []collectors.Collector
	extraSyncTimeout time.Duration
	inventory        *inventory.CollectorInventory
	stopCh           chan struct{}
	runCfg           *collectors.CollectorRunConfig
}

// NewCollectorBundle creates a new bundle from the check configuration.
//
// If collectors are declared in the check instance configuration then it'll
// only select those. This needs to match what is found in
// https://github.com/kubernetes/kube-state-metrics/blob/09539977815728349522b58154d800e4b517ec9c/internal/store/builder.go#L176-L206
// in order to share/split easily the collector configuration with the KSM core
// check.
//
// If that's not the case then it'll select all available collectors that are
// marked as stable.
func NewCollectorBundle(chk *OrchestratorCheck) *CollectorBundle {
	bundle := &CollectorBundle{
		check:     chk,
		inventory: inventory.NewCollectorInventory(),
		runCfg: &collectors.CollectorRunConfig{
			APIClient:   chk.apiClient,
			ClusterID:   chk.clusterID,
			Config:      chk.orchestratorConfig,
			MsgGroupRef: chk.groupID,
		},
		stopCh: make(chan struct{}),
	}

	bundle.prepare()

	return bundle
}

// prepare initializes the collector bundle internals before it can be used.
func (cb *CollectorBundle) prepare() {
	cb.prepareCollectors()
	cb.prepareExtraSyncTimeout()
}

// prepareCollectors initializes the bundle collector list.
func (cb *CollectorBundle) prepareCollectors() {
	// No collector configured in the check configuration.
	// Use the list of stable collectors as the default.
	if len(cb.check.instance.Collectors) == 0 {
		cb.collectors = cb.inventory.StableCollectors()
		return
	}

	// Collectors configured in the check configuration.
	// Build the custom list of collectors.
	for _, name := range cb.check.instance.Collectors {
		if collector, err := cb.inventory.CollectorByName(name); err == nil {
			if !collector.Metadata().IsStable {
				_ = cb.check.Warnf("Using unstable collector: %s", name)
			}
			cb.collectors = append(cb.collectors, collector)
		} else {
			_ = cb.check.Warnf("Unsupported collector: %s", name)
		}
	}
}

// prepareExtraSyncTimeout initializes the bundle extra sync timeout.
func (cb *CollectorBundle) prepareExtraSyncTimeout() {
	// No extra timeout set in the check configuration.
	// Use the default.
	if cb.check.instance.ExtraSyncTimeoutSeconds <= 0 {
		cb.extraSyncTimeout = defaultExtraSyncTimeout
		return
	}

	// Custom extra timeout.
	cb.extraSyncTimeout = time.Duration(cb.check.instance.ExtraSyncTimeoutSeconds) * time.Second
}

// Initialize is used to initialize collectors part of the bundle.
// During initialization informers are created, started and their cache is
// synced.
func (cb *CollectorBundle) Initialize() error {
	informersToSync := make(map[apiserver.InformerName]cache.SharedInformer)
	var availableCollectors []collectors.Collector
	// informerSynced is a helper map which makes sure that we don't initialize the same informer twice.
	// i.e. the cluster and nodes resources share the same informer and using both can lead to a race condition activating both concurrently.
	informerSynced := map[cache.SharedInformer]struct{}{}
	for _, collector := range cb.collectors {
		collector.Init(cb.runCfg)
		if !collector.IsAvailable() {
			_ = cb.check.Warnf("Collector %q is unavailable, skipping it", collector.Metadata().Name)
			continue
		}

		availableCollectors = append(availableCollectors, collector)

		informer := collector.Informer()

		if _, found := informerSynced[informer]; !found {
			informersToSync[apiserver.InformerName(collector.Metadata().Name)] = informer
			informerSynced[informer] = struct{}{}
			// we run each enabled informer individually, because starting them through the factory
			// would prevent us from restarting them again if the check is unscheduled/rescheduled
			// see https://github.com/kubernetes/client-go/blob/3511ef41b1fbe1152ef5cab2c0b950dfd607eea7/informers/factory.go#L64-L66

			// TODO: right now we use a stop channel which we don't close, that can lead to resource leaks
			// A recent go-client update https://github.com/kubernetes/kubernetes/pull/104853 changed the behaviour so that
			// we are not able to start informers anymore once they have been stopped. We will need to work around this. Once this is fixed we can properly release the resources during a check.Close().
			go informer.Run(cb.stopCh)
		}
	}

	cb.collectors = availableCollectors

	return apiserver.SyncInformers(informersToSync, cb.extraSyncTimeout)
}

// Run is used to sequentially run all collectors in the bundle.
func (cb *CollectorBundle) Run(sender aggregator.Sender) {
	for _, collector := range cb.collectors {
		runStartTime := time.Now()

		result, err := collector.Run(cb.runCfg)
		if err != nil {
			_ = cb.check.Warnf("Collector %s failed to run: %s", collector.Metadata().Name, err.Error())
			continue
		}

		runDuration := time.Since(runStartTime)
		log.Debugf("Collector %s run stats: listed=%d processed=%d messages=%d duration=%s", collector.Metadata().Name, result.ResourcesListed, result.ResourcesProcessed, len(result.Result.MetadataMessages), runDuration)

		orchestrator.SetCacheStats(result.ResourcesListed, len(result.Result.MetadataMessages), collector.Metadata().NodeType)
		sender.OrchestratorMetadata(result.Result.MetadataMessages, cb.check.clusterID, int(collector.Metadata().NodeType))
		if cb.runCfg.Config.IsManifestCollectionEnabled {
			sender.OrchestratorManifest(result.Result.ManifestMessages, cb.check.clusterID)
		}
	}
}
