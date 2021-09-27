package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-dimension-importer/client"
	"github.com/ONSdigital/dp-dimension-importer/event"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-dimension-importer/store"
	"github.com/ONSdigital/log.go/v2/log"
)

//go:generate moq -out ../mocks/incoming_instance_generated_mocks.go -pkg mocks . CompletedProducer

var (
	errInstanceExists = errors.New("[handler.InstanceEventHandler] instance already exists")
	packageName       = "handler.InstanceEventHandler"
)

// CompletedProducer Producer kafka messages for instances that have been successfully processed.
type CompletedProducer interface {
	Completed(ctx context.Context, e event.InstanceCompleted) error
}

// InstanceEventHandler provides functions for handling DimensionsExtractedEvents.
type InstanceEventHandler struct {
	Store             store.Storer
	DatasetAPICli     *client.DatasetAPI
	Producer          CompletedProducer
	BatchSize         int
	EnablePatchNodeID bool
}

// Handle retrieves the dimensions for specified instanceID from the Import API, creates an MyInstance entity for
// provided instanceID, creates a Dimension entity for each dimension and a relationship to the MyInstance it belongs to
// and makes a PUT request to the Import API with the database ID of each Dimension entity.
func (hdlr *InstanceEventHandler) Handle(ctx context.Context, newInstance event.NewInstance) error {
	err := hdlr.Validate(newInstance)
	if err != nil {
		return err
	}

	logData := log.Data{"instance_id": newInstance.InstanceID, "package": packageName}
	log.Info(ctx, "handling new instance event", logData)
	start := time.Now()

	// retrieve the dimensions from dataset API
	dimensions, err := hdlr.DatasetAPICli.GetDimensions(ctx, newInstance.InstanceID, headers.IfMatchAnyETag)
	if err != nil {
		return fmt.Errorf("DatasetAPICli.GetDimensions returned an error: %w", err)
	}
	if err := ValidateDimensions(dimensions); err != nil {
		return err
	}
	logData["dimensions_count"] = len(dimensions)

	// retrieve the CSV header from the dataset API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.DatasetAPICli.GetInstance(ctx, newInstance.InstanceID)
	if err != nil {
		return fmt.Errorf("dataset api client get instance returned an error: %w", err)
	}
	if err := ValidateInstance(instance); err != nil {
		return err
	}

	// create instance node to the DB if it does not exist already
	if err = hdlr.createInstanceNode(ctx, instance); err != nil {
		if err == errInstanceExists {
			log.Info(ctx, "an instance with this id already exists, ignoring this event", logData)
			return nil // ignoring
		}
		return err
	}

	// insertDimensions to graph db and mongoDB
	if err = hdlr.insertDimensions(ctx, instance, dimensions); err != nil {
		return err
	}

	if err = hdlr.createObservationConstraint(ctx, instance); err != nil {
		return err
	}

	instanceProcessed := event.InstanceCompleted{
		FileURL:    newInstance.FileURL,
		InstanceID: newInstance.InstanceID,
	}

	// produce the kafka message to notify that the dimensions have been successfully imported
	if err := hdlr.Producer.Completed(ctx, instanceProcessed); err != nil {
		return fmt.Errorf("Producer.Completed returned an error: %w", err)
	}

	log.Info(ctx, "instance processing completed successfully", log.Data{"package": packageName, "processing_time": time.Since(start).Seconds()})
	return nil
}

func (hdlr *InstanceEventHandler) Validate(newInstance event.NewInstance) error {
	if hdlr.DatasetAPICli == nil {
		return fmt.Errorf("validation error: %w", client.ErrNoDatasetAPI)
	}
	if hdlr.Store == nil {
		return fmt.Errorf("validation error: %w", client.ErrNoDatastore)
	}
	if len(newInstance.InstanceID) == 0 {
		return fmt.Errorf("validation error: %w", client.ErrInstanceIDEmpty)
	}
	return nil
}

func ValidateInstance(instance *model.Instance) error {
	if instance == nil || instance.DbModel() == nil || len(instance.DbModel().InstanceID) == 0 {
		return fmt.Errorf("validation error: %w", client.ErrInstanceIDEmpty)
	}
	return nil
}

func ValidateDimensions(dimensions []*model.Dimension) error {
	if len(dimensions) == 0 {
		return fmt.Errorf("validation error: %w", client.ErrDimensionNil)
	}
	for _, d := range dimensions {
		if d == nil || d.DbModel() == nil {
			return fmt.Errorf("validation error: %w", client.ErrDimensionNil)
		}
		if len(d.DbModel().DimensionID) == 0 {
			return fmt.Errorf("validation error: %w", client.ErrDimensionIDEmpty)
		}
	}
	return nil
}

// insertDimensions inserts the necessary nodes in the graph database and updates the dimension options in Dataset API
// for all the provided dimensions, in batches of size BatchSize. For each batch:
// - we trigger BatchSize go-routines, each one will insert a dimension node to the graph database
// - when all go-routines finish their execution, we perform one patch call to dataset api to update the order and node_id values
// Once all batches have been processed, a final AddDimensions call is performed
func (hdlr *InstanceEventHandler) insertDimensions(ctx context.Context, instance *model.Instance, dimensions []*model.Dimension) error {
	cache := make(map[string]string)
	wg := &sync.WaitGroup{}
	problem := make(chan error, len(dimensions))

	// waitForBatch is an aux func to block the main thread until all go-routines finish,
	// returning err if any one of them reported an error
	waitForBatch := func() error {
		batchProcessed := make(chan struct{})
		go func() {
			wg.Wait()
			close(batchProcessed)
		}()

		select {
		case <-batchProcessed:
			return nil
		case err := <-problem:
			return err
		}
	}

	// func to process one batch
	processBatch := func(dimensionsBatch []*model.Dimension) error {
		// trigger insertDimension in parallel go-routines (one per item in the batch)
		for _, dimension := range dimensionsBatch {
			wg.Add(1)
			go func(d *model.Dimension) {
				defer wg.Done()
				hdlr.insertDimension(ctx, cache, instance, d, problem)
			}(dimension)
		}

		// wait for all go-routines to finish
		if err := waitForBatch(); err != nil {
			return err
		}

		// set dimension options' order and nodeID for the current batch (one call per batch)
		if err := hdlr.SetOrderAndNodeIDs(ctx, instance.DbModel().InstanceID, dimensionsBatch); err != nil {
			return err
		}
		return nil
	}

	// Get batch splits for provided items
	numFullChunks := len(dimensions) / hdlr.BatchSize
	remainingSize := len(dimensions) % hdlr.BatchSize

	// process full batches
	for i := 0; i < numFullChunks; i++ {
		chunk := dimensions[i*hdlr.BatchSize : (i+1)*hdlr.BatchSize]
		if err := processBatch(chunk); err != nil {
			return err
		}
	}

	// process any remaining
	if remainingSize > 0 {
		lastChunk := dimensions[numFullChunks*hdlr.BatchSize : (numFullChunks*hdlr.BatchSize + remainingSize)]
		if err := processBatch(lastChunk); err != nil {
			return err
		}
	}

	// Add dimensions to graph database
	if err := hdlr.Store.AddDimensions(ctx, instance.DbModel().InstanceID, instance.DbModel().Dimensions); err != nil {
		return fmt.Errorf("AddDimensions returned an error: %w", err)
	}

	return nil
}

// SetOrderAndNodeIDs obtains the codes order from the graph database
// and patches the existing dimension options in dataset API (updating node_id and order values)
// this method assumes that valid non-nil values are provided (have been created or validated by caller)
func (hdlr *InstanceEventHandler) SetOrderAndNodeIDs(ctx context.Context, instanceID string, dimensions []*model.Dimension) error {
	// get a map of codes by codelistID
	codesByCodelistID := map[string][]string{}
	for _, d := range dimensions {
		codelistID := d.CodeListID()
		codesByCodelistID[codelistID] = append(codesByCodelistID[codelistID], d.DbModel().Option)
	}

	t0 := time.Now()

	// get a map of orders by code (one call to dp-graph per codeListID)
	orderByCode := map[string]*int{}
	for codeListID, codes := range codesByCodelistID {
		o, err := hdlr.Store.GetCodesOrder(ctx, codeListID, codes)
		if err != nil {
			err = fmt.Errorf("error while attempting to get dimension order using codes: %w", err)
			log.Error(ctx, "error in setOrderAndNodeIDs while getting orders from the graph database", err, log.Data{
				"instance_id":  instanceID,
				"code_list_id": codeListID,
				"codes":        codes,
			})
			return err
		}
		for k, v := range o {
			orderByCode[k] = v
		}
	}

	log.Info(ctx, "successfully got code orders from graph database", log.Data{"time": time.Since(t0), "num_codelists": len(codesByCodelistID)})

	// create list of updates to send to dataset API patch endpoint
	updates := []*dataset.OptionUpdate{}
	for _, d := range dimensions {
		nodeID := ""
		if hdlr.EnablePatchNodeID {
			nodeID = d.DbModel().NodeID
		}
		order := orderByCode[d.DbModel().Option]

		if nodeID == "" && order == nil {
			continue // no update for the current dimension
		}

		u := &dataset.OptionUpdate{
			Name:   d.DbModel().DimensionID,
			Option: d.DbModel().Option,
		}
		if nodeID != "" {
			u.NodeID = nodeID
		}
		if order != nil {
			u.Order = order
		}
		updates = append(updates, u)
	}

	// Send a patch to dataset api with all the updates in a single call
	// so that the mongodb lock will be acquired only once per batch.
	// The reason is that releasing a lock has been observed in 'develop' environment to take over 40ms.
	if _, err := hdlr.DatasetAPICli.PatchDimensionOption(ctx, instanceID, updates); err != nil {
		err = fmt.Errorf("DatasetAPICli.PatchDimensionOption returned an error: %w", err)
		log.Error(ctx, "patch error in setOrderAndNodeIDs", err, log.Data{
			"instance_id": instanceID,
			"updates":     updates})
		return err
	}
	return nil
}

// insertDimension inserts the dimension to the graph database
// and creates the code relationship if DimensionID is time
// this method assumes that valid non-nil values are provided (have been created or validated by caller)
func (hdlr *InstanceEventHandler) insertDimension(ctx context.Context, cache map[string]string, instance *model.Instance, d *model.Dimension, problem chan error) {
	dbDimension, err := hdlr.Store.InsertDimension(ctx, cache, instance.DbModel().InstanceID, d.DbModel())
	if err != nil {
		err = fmt.Errorf("error while attempting to insert a dimension to the graph database: %w", err)
		log.Error(ctx, "error inserting dimension", err, log.Data{"instance_id": instance.DbModel().InstanceID, "dimension_id": dbDimension.DimensionID})
		problem <- err
		return
	}

	// todo: remove this temp hack once the time codelist / input data has been fixed.
	if dbDimension.DimensionID != "time" {
		if err = hdlr.Store.CreateCodeRelationship(context.Background(), instance.DbModel().InstanceID, d.CodeListID(), dbDimension.Option); err != nil {
			err = fmt.Errorf("error attempting to create relationship to code: %w", err)
			log.Error(ctx, "error attempting to create relationship to code", err, log.Data{"instance_id": instance.DbModel().InstanceID, "dimension_id": dbDimension.DimensionID})
			problem <- err
			return
		}
	}
}

func (hdlr *InstanceEventHandler) createInstanceNode(ctx context.Context, instance *model.Instance) error {

	exists, err := hdlr.Store.InstanceExists(ctx, instance.DbModel().InstanceID)
	if err != nil {
		return fmt.Errorf("instance exists check returned an error: %w", err)
	}

	if exists {
		return errInstanceExists
	}

	if err = hdlr.Store.CreateInstance(ctx, instance.DbModel().InstanceID, instance.DbModel().CSVHeader); err != nil {
		return fmt.Errorf("create instance returned an error: %w", err)
	}

	return nil
}

func (hdlr *InstanceEventHandler) createObservationConstraint(ctx context.Context, instance *model.Instance) error {

	if err := hdlr.Store.CreateInstanceConstraint(ctx, instance.DbModel().InstanceID); err != nil {
		return fmt.Errorf("error while attempting to add the unique observation constraint: %w", err)
	}

	return nil
}
