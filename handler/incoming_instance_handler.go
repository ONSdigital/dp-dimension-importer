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
	errValidationFail = errors.New("[handler.InstanceEventHandler] validation error")
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

	err := hdlr.validate(newInstance)
	if err != nil {
		return err
	}

	logData := log.Data{"instance_id": newInstance.InstanceID, "package": packageName}
	log.Info(ctx, "handling new instance event", logData)

	start := time.Now()

	dimensions, err := hdlr.DatasetAPICli.GetDimensions(ctx, newInstance.InstanceID, headers.IfMatchAnyETag)
	if err != nil {
		return fmt.Errorf("DatasetAPICli.GetDimensions returned an error: %w", err)
	}

	logData["dimensions_count"] = len(dimensions)

	// retrieve the CSV header from the dataset API and attach it to the instance node allowing it to be used after import.
	instance, err := hdlr.DatasetAPICli.GetInstance(ctx, newInstance.InstanceID)
	if err != nil {
		return fmt.Errorf("dataset api client get instance returned an error: %w", err)
	}

	if err = hdlr.createInstanceNode(ctx, instance); err != nil {
		if err == errInstanceExists {
			log.Info(ctx, "an instance with this id already exists, ignoring this event", logData)
			return nil // ignoring
		}
		return err
	}

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

	if err := hdlr.Producer.Completed(ctx, instanceProcessed); err != nil {
		return fmt.Errorf("Producer.Completed returned an error: %w", err)
	}

	log.Info(ctx, "instance processing completed successfully", log.Data{"package": packageName, "processing_time": time.Since(start).Seconds()})
	return nil
}

func (hdlr *InstanceEventHandler) validate(newInstance event.NewInstance) error {

	if hdlr.DatasetAPICli == nil {
		return errors.New("validation error dataset api client required but was nil")
	}
	if hdlr.Store == nil {
		return errors.New("validation error datastore required but was nil")
	}
	if len(newInstance.InstanceID) == 0 {
		return errors.New("validation error instance_id required but was empty")
	}

	return nil
}

func (hdlr *InstanceEventHandler) insertDimensions(ctx context.Context, instance *model.Instance, dimensions []*model.Dimension) error {
	// validate instance
	if instance == nil || instance.DbModel() == nil || len(instance.DbModel().InstanceID) == 0 {
		log.Error(ctx, "nil or empty instanceID provided to patchDimensions", client.ErrInstanceIDEmpty)
		return client.ErrInstanceIDEmpty
	}
	// validate dimensions
	if len(dimensions) == 0 {
		log.Error(ctx, "nil or empty dimensions provided to patchDimensions", client.ErrDimensionNil)
		return client.ErrDimensionNil
	}
	for _, d := range dimensions {
		if d == nil || d.DbModel() == nil {
			return client.ErrDimensionNil
		}
		if len(d.DbModel().DimensionID) == 0 {
			return client.ErrDimensionIDEmpty
		}
	}

	cache := make(map[string]string)
	var wg sync.WaitGroup
	problem := make(chan error, len(dimensions))
	batchProcessed := make(chan bool)
	count := 0

	for _, dimension := range dimensions {
		wg.Add(1)
		count++
		go func(d *model.Dimension) {
			defer wg.Done()
			hdlr.insertDimension(ctx, cache, instance, d, problem)
		}(dimension)

		if count >= hdlr.BatchSize {
			// stop creating go-routins until this full batch (of size BatchSize) finishes
			if err := waitForBatch(&wg, batchProcessed, problem); err != nil {
				return err
			}
			// set dimension options' order and nodeID for the current batch
			if err := hdlr.setOrderAndNodeIDs(ctx, cache, instance.DbModel().InstanceID, dimensions); err != nil {
				return err
			}
			count = 0
		}
	}

	// wait until the last batch (of size <BatchSize) finishes
	err := waitForBatch(&wg, batchProcessed, problem)
	if err != nil {
		return err
	}
	if count > 0 {
		// set dimension options' order and nodeID for the last batch
		if err := hdlr.setOrderAndNodeIDs(ctx, cache, instance.DbModel().InstanceID, dimensions); err != nil {
			return err
		}
	}

	// Add dimensions to graph database
	if err := hdlr.Store.AddDimensions(ctx, instance.DbModel().InstanceID, instance.DbModel().Dimensions); err != nil {
		return fmt.Errorf("AddDimensions returned an error: %w", err)
	}

	return nil
}

func waitForBatch(wg *sync.WaitGroup, batchProcessed chan bool, problem chan error) error {
	go func() {
		wg.Wait()
		batchProcessed <- true
	}()

	select {
	case <-batchProcessed:
		return nil
	case err := <-problem:
		return err
	}
}

// setOrderAndNodeIDs obtains the codes order from the graph database
// and patches the existing dimension options in dataset API (updating node_id and order values)
// this method assumes that valid non-nil values are provided (have been created or validated by caller)
func (hdlr *InstanceEventHandler) setOrderAndNodeIDs(ctx context.Context, cache map[string]string, instanceID string, dimensions []*model.Dimension) error {
	// get a map of codes by codelistID
	codesByCodelistID := map[string][]string{}
	for _, d := range dimensions {
		codelistID := d.CodeListID()
		codesByCodelistID[codelistID] = append(codesByCodelistID[codelistID], d.DbModel().Option)
	}

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
