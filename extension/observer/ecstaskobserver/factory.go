// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ecstaskobserver // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/ecstaskobserver"

import (
	"context"
	"fmt"
	"net/url"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenthelper"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/extension/extensionhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/ecsutil"
)

const (
	typeStr config.Type = "ecs_task_observer"
)

// NewFactory creates a factory for ECSTaskObserver extension.
func NewFactory() component.ExtensionFactory {
	return extensionhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		createExtension)
}

func createDefaultConfig() config.Extension {
	cfg := defaultConfig()
	return &cfg
}

func createExtension(
	_ context.Context,
	params component.ExtensionCreateSettings,
	cfg config.Extension,
) (component.Extension, error) {
	obsCfg := cfg.(*Config)

	logger := params.TelemetrySettings.Logger
	var metadataProvider ecsutil.MetadataProvider
	var err error
	if obsCfg.Endpoint == "" {
		metadataProvider, err = ecsutil.NewDetectedTaskMetadataProvider(logger)
	} else {
		metadataProvider, err = metadataProviderFromEndpoint(obsCfg, logger)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create ECS Task Observer metadata provider: %w", err)
	}

	e := &ecsTaskObserver{
		config:           obsCfg,
		metadataProvider: metadataProvider,
		telemetry:        params.TelemetrySettings,
	}
	e.Extension = componenthelper.New(componenthelper.WithShutdown(e.Shutdown))
	e.EndpointsWatcher = &observer.EndpointsWatcher{
		Endpointslister: e,
		RefreshInterval: obsCfg.RefreshInterval,
	}

	return e, nil
}

func metadataProviderFromEndpoint(config *Config, logger *zap.Logger) (ecsutil.MetadataProvider, error) {
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed == nil {
		return nil, fmt.Errorf("failed to parse task metadata endpoint: %w", err)
	}

	restClient, err := ecsutil.NewRestClient(*parsed, config.HTTPClientSettings, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECS Task Observer rest client: %w", err)
	}

	return ecsutil.NewTaskMetadataProvider(restClient, logger), nil
}
