package neptune

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
)

// ErrEmptyFilter is returned if the provided filter is empty.
var ErrEmptyFilter = errors.New("filter is empty")

func (n *NeptuneDB) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {
	if filter == nil {
		return nil, ErrEmptyFilter
	}

	q := fmt.Sprintf(query.GetInstanceHeader, filter.InstanceID)

	q += buildObservationsQuery(filter)
	q += query.GetObservationSelectRowPart

	if limit != nil {
		q += fmt.Sprintf(query.LimitPart, *limit)
	}

	return n.Pool.OpenCursorCtx(ctx, q, nil, nil)
}

func buildObservationsQuery(f *observation.Filter) string {
	if f.IsEmpty() {
		return fmt.Sprintf(query.GetAllObservationsPart, f.InstanceID)
	}

	q := fmt.Sprintf(query.GetObservationsPart, f.InstanceID)

	for _, dim := range f.DimensionFilters {
		if len(dim.Options) == 0 {
			continue
		}

		for i, opt := range dim.Options {
			dim.Options[i] = fmt.Sprintf("'%s'", opt)
		}

		q += fmt.Sprintf(query.GetObservationDimensionPart, f.InstanceID, dim.Name, strings.Join(dim.Options, ",")) + ","
	}

	//remove trailing comma and close match statement
	q = strings.Trim(q, ",")
	q += ")"

	return q
}

func (n *NeptuneDB) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error {
	if len(observations) == 0 {
		fmt.Println("range should be empty")
		return nil
	}

	//fan out go routines to manage independent requests for each observation in this batch
	//if any report errors, they'll be passed through the problem channel, and handled at the end of processing
	var wg sync.WaitGroup
	problem := make(chan error, len(observations))

	for _, obs := range observations {
		wg.Add(1)
		go func(o models.Observation) {
			defer wg.Done()
			create := fmt.Sprintf(query.CreateObservationPart, instanceID, o.Row, o.RowIndex)
			for _, d := range o.DimensionOptions {
				dimensionName := strings.ToLower(d.DimensionName)
				dimensionLookup := instanceID + "_" + dimensionName + "_" + d.Name

				nodeID, ok := dimensionIDs[dimensionLookup]
				if !ok {
					problem <- fmt.Errorf("no nodeID [%s] found in dimension map", dimensionLookup)
					return
				}

				create += fmt.Sprintf(query.AddObservationRelationshipPart, nodeID, instanceID, d.DimensionName, d.Name)
			}

			create = strings.TrimSuffix(create, ".outV()")

			fmt.Println("query : " + create)
			if _, err := n.exec(create); err != nil {
				problem <- err
				return
			}
		}(*obs)
	}

	wg.Wait()
	numOfErrors := len(problem)
	if numOfErrors > 0 {
		firstError := <-problem
		return errors.Wrapf(errors.New("errors found inserting observation batch"), fmt.Sprintf("[%d] errors found, propagating first", numOfErrors), firstError)
	}

	return nil
}
