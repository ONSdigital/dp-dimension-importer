// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mocks

import (
	"context"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"sync"
)

var (
	lockIClientMockChecker                        sync.RWMutex
	lockIClientMockGetInstance                    sync.RWMutex
	lockIClientMockGetInstanceDimensionsInBatches sync.RWMutex
	lockIClientMockPatchInstanceDimensions        sync.RWMutex
)

// Ensure, that IClientMock does implement client.IClient.
// If this is not the case, regenerate this file with moq.
var _ client.IClient = &IClientMock{}

// IClientMock is a mock implementation of client.IClient.
//
//     func TestSomethingThatUsesIClient(t *testing.T) {
//
//         // make and configure a mocked client.IClient
//         mockedIClient := &IClientMock{
//             CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error {
// 	               panic("mock out the Checker method")
//             },
//             GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
// 	               panic("mock out the GetInstance method")
//             },
//             GetInstanceDimensionsInBatchesFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, batchSize int, maxWorkers int) (dataset.Dimensions, string, error) {
// 	               panic("mock out the GetInstanceDimensionsInBatches method")
//             },
//             PatchInstanceDimensionsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
// 	               panic("mock out the PatchInstanceDimensions method")
//             },
//         }
//
//         // use mockedIClient in code that requires client.IClient
//         // and then make assertions.
//
//     }
type IClientMock struct {
	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *healthcheck.CheckState) error

	// GetInstanceFunc mocks the GetInstance method.
	GetInstanceFunc func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error)

	// GetInstanceDimensionsInBatchesFunc mocks the GetInstanceDimensionsInBatches method.
	GetInstanceDimensionsInBatchesFunc func(ctx context.Context, serviceAuthToken string, instanceID string, batchSize int, maxWorkers int) (dataset.Dimensions, string, error)

	// PatchInstanceDimensionsFunc mocks the PatchInstanceDimensions method.
	PatchInstanceDimensionsFunc func(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error)

	// calls tracks calls to the methods.
	calls struct {
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// State is the state argument value.
			State *healthcheck.CheckState
		}
		// GetInstance holds details about calls to the GetInstance method.
		GetInstance []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// UserAuthToken is the userAuthToken argument value.
			UserAuthToken string
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// CollectionID is the collectionID argument value.
			CollectionID string
			// InstanceID is the instanceID argument value.
			InstanceID string
			// IfMatch is the ifMatch argument value.
			IfMatch string
		}
		// GetInstanceDimensionsInBatches holds details about calls to the GetInstanceDimensionsInBatches method.
		GetInstanceDimensionsInBatches []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// InstanceID is the instanceID argument value.
			InstanceID string
			// BatchSize is the batchSize argument value.
			BatchSize int
			// MaxWorkers is the maxWorkers argument value.
			MaxWorkers int
		}
		// PatchInstanceDimensions holds details about calls to the PatchInstanceDimensions method.
		PatchInstanceDimensions []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// InstanceID is the instanceID argument value.
			InstanceID string
			// Upserts is the upserts argument value.
			Upserts []*dataset.OptionPost
			// Updates is the updates argument value.
			Updates []*dataset.OptionUpdate
			// IfMatch is the ifMatch argument value.
			IfMatch string
		}
	}
}

// Checker calls CheckerFunc.
func (mock *IClientMock) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("IClientMock.CheckerFunc: method is nil but IClient.Checker was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}{
		Ctx:   ctx,
		State: state,
	}
	lockIClientMockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	lockIClientMockChecker.Unlock()
	return mock.CheckerFunc(ctx, state)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedIClient.CheckerCalls())
func (mock *IClientMock) CheckerCalls() []struct {
	Ctx   context.Context
	State *healthcheck.CheckState
} {
	var calls []struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}
	lockIClientMockChecker.RLock()
	calls = mock.calls.Checker
	lockIClientMockChecker.RUnlock()
	return calls
}

// GetInstance calls GetInstanceFunc.
func (mock *IClientMock) GetInstance(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
	if mock.GetInstanceFunc == nil {
		panic("IClientMock.GetInstanceFunc: method is nil but IClient.GetInstance was just called")
	}
	callInfo := struct {
		Ctx              context.Context
		UserAuthToken    string
		ServiceAuthToken string
		CollectionID     string
		InstanceID       string
		IfMatch          string
	}{
		Ctx:              ctx,
		UserAuthToken:    userAuthToken,
		ServiceAuthToken: serviceAuthToken,
		CollectionID:     collectionID,
		InstanceID:       instanceID,
		IfMatch:          ifMatch,
	}
	lockIClientMockGetInstance.Lock()
	mock.calls.GetInstance = append(mock.calls.GetInstance, callInfo)
	lockIClientMockGetInstance.Unlock()
	return mock.GetInstanceFunc(ctx, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch)
}

// GetInstanceCalls gets all the calls that were made to GetInstance.
// Check the length with:
//     len(mockedIClient.GetInstanceCalls())
func (mock *IClientMock) GetInstanceCalls() []struct {
	Ctx              context.Context
	UserAuthToken    string
	ServiceAuthToken string
	CollectionID     string
	InstanceID       string
	IfMatch          string
} {
	var calls []struct {
		Ctx              context.Context
		UserAuthToken    string
		ServiceAuthToken string
		CollectionID     string
		InstanceID       string
		IfMatch          string
	}
	lockIClientMockGetInstance.RLock()
	calls = mock.calls.GetInstance
	lockIClientMockGetInstance.RUnlock()
	return calls
}

// GetInstanceDimensionsInBatches calls GetInstanceDimensionsInBatchesFunc.
func (mock *IClientMock) GetInstanceDimensionsInBatches(ctx context.Context, serviceAuthToken string, instanceID string, batchSize int, maxWorkers int) (dataset.Dimensions, string, error) {
	if mock.GetInstanceDimensionsInBatchesFunc == nil {
		panic("IClientMock.GetInstanceDimensionsInBatchesFunc: method is nil but IClient.GetInstanceDimensionsInBatches was just called")
	}
	callInfo := struct {
		Ctx              context.Context
		ServiceAuthToken string
		InstanceID       string
		BatchSize        int
		MaxWorkers       int
	}{
		Ctx:              ctx,
		ServiceAuthToken: serviceAuthToken,
		InstanceID:       instanceID,
		BatchSize:        batchSize,
		MaxWorkers:       maxWorkers,
	}
	lockIClientMockGetInstanceDimensionsInBatches.Lock()
	mock.calls.GetInstanceDimensionsInBatches = append(mock.calls.GetInstanceDimensionsInBatches, callInfo)
	lockIClientMockGetInstanceDimensionsInBatches.Unlock()
	return mock.GetInstanceDimensionsInBatchesFunc(ctx, serviceAuthToken, instanceID, batchSize, maxWorkers)
}

// GetInstanceDimensionsInBatchesCalls gets all the calls that were made to GetInstanceDimensionsInBatches.
// Check the length with:
//     len(mockedIClient.GetInstanceDimensionsInBatchesCalls())
func (mock *IClientMock) GetInstanceDimensionsInBatchesCalls() []struct {
	Ctx              context.Context
	ServiceAuthToken string
	InstanceID       string
	BatchSize        int
	MaxWorkers       int
} {
	var calls []struct {
		Ctx              context.Context
		ServiceAuthToken string
		InstanceID       string
		BatchSize        int
		MaxWorkers       int
	}
	lockIClientMockGetInstanceDimensionsInBatches.RLock()
	calls = mock.calls.GetInstanceDimensionsInBatches
	lockIClientMockGetInstanceDimensionsInBatches.RUnlock()
	return calls
}

// PatchInstanceDimensions calls PatchInstanceDimensionsFunc.
func (mock *IClientMock) PatchInstanceDimensions(ctx context.Context, serviceAuthToken string, instanceID string, upserts []*dataset.OptionPost, updates []*dataset.OptionUpdate, ifMatch string) (string, error) {
	if mock.PatchInstanceDimensionsFunc == nil {
		panic("IClientMock.PatchInstanceDimensionsFunc: method is nil but IClient.PatchInstanceDimensions was just called")
	}
	callInfo := struct {
		Ctx              context.Context
		ServiceAuthToken string
		InstanceID       string
		Upserts          []*dataset.OptionPost
		Updates          []*dataset.OptionUpdate
		IfMatch          string
	}{
		Ctx:              ctx,
		ServiceAuthToken: serviceAuthToken,
		InstanceID:       instanceID,
		Upserts:          upserts,
		Updates:          updates,
		IfMatch:          ifMatch,
	}
	lockIClientMockPatchInstanceDimensions.Lock()
	mock.calls.PatchInstanceDimensions = append(mock.calls.PatchInstanceDimensions, callInfo)
	lockIClientMockPatchInstanceDimensions.Unlock()
	return mock.PatchInstanceDimensionsFunc(ctx, serviceAuthToken, instanceID, upserts, updates, ifMatch)
}

// PatchInstanceDimensionsCalls gets all the calls that were made to PatchInstanceDimensions.
// Check the length with:
//     len(mockedIClient.PatchInstanceDimensionsCalls())
func (mock *IClientMock) PatchInstanceDimensionsCalls() []struct {
	Ctx              context.Context
	ServiceAuthToken string
	InstanceID       string
	Upserts          []*dataset.OptionPost
	Updates          []*dataset.OptionUpdate
	IfMatch          string
} {
	var calls []struct {
		Ctx              context.Context
		ServiceAuthToken string
		InstanceID       string
		Upserts          []*dataset.OptionPost
		Updates          []*dataset.OptionUpdate
		IfMatch          string
	}
	lockIClientMockPatchInstanceDimensions.RLock()
	calls = mock.calls.PatchInstanceDimensions
	lockIClientMockPatchInstanceDimensions.RUnlock()
	return calls
}
