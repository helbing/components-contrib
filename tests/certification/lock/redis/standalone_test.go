/*
Copyright 2022 The Dapr Authors
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

package redis_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/lock"
	lock_redis "github.com/dapr/components-contrib/lock/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	lock_loader "github.com/dapr/dapr/pkg/components/lock"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	sidecarName                 = "lock-sidecar"
	bindingName                 = "redis-lock"
	storeName                   = "test-redis-lock"
	standaloneName              = "lock-redis-standalone"
	standaloneDockerComposeYAML = "docker-compose-standalone.yml"
)

func TestStandalone(t *testing.T) {
	logger := logger.NewLogger("dapr.components")

	tryLockWithConfig := func(ctx flow.Context, config map[string]string) (bool, error) {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		lockRequest := &daprsdk.LockRequest{
			ResourceID: config["resource_id"],
		}

		rsp, err := client.TryLockAlpha1(ctx, storeName, lockRequest)
		if err != nil {
			return false, err
		}
		return rsp.Success, nil
	}

	unlockWithConfig := func(ctx flow.Context, config map[string]string) (int32, error) {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		unlockRequest := &daprsdk.UnlockRequest{}

		rsp, err := client.UnlockAlpha1(ctx, storeName, unlockRequest)
		if err != nil {
			return 0, err
		}
		return rsp.StatusCode, nil
	}

	testTryLockAndUnlock := func(ctx flow.Context) error {
		// sleep to avoid metadata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		success, err := tryLockWithConfig(ctx, nil)
		assert.True(t, success)
		assert.NoError(t, err)

		statusCode, err := unlockWithConfig(ctx, nil)
		assert.Equal(t, statusCode, pb.UnlockResponse_SUCCESS)
		assert.NoError(t, err)

		return nil
	}

	flow.New(t, "test redis lock operateions").
		Step(dockercompose.Run(standaloneName, standaloneDockerComposeYAML)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/standalone"),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithLocks(
				lock_loader.New("redis-standalone", func() lock.Store {
					return lock_redis.NewStandaloneRedisLock(logger)
				}),
			))).
		Step("test try lock and unlock", testTryLockAndUnlock).
		Run()
}
