package neptune

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/go-ns/log"
)

// InsertDimension node to neptune and create relationships to the instance node.
// Where nodes and relationships already exist, ensure they are upserted.
func (n *NeptuneDB) InsertDimension(ctx context.Context, cache map[string]string, i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	dimensionLabel := fmt.Sprintf("_%s_%s", i.InstanceID, d.DimensionID)

	res, err := n.getVertex(fmt.Sprintf(query.CreateDimensionToInstanceRelationship, i.InstanceID, d.DimensionID, d.Option, i.InstanceID, d.DimensionID, d.Option, i.InstanceID))
	if err != nil {
		return nil, err
	}

	// if len(res) == 0 {
	// 	return nil, errors.New("invalid response from neptune exec during dimension insert")
	// }
	//res.GetID()

	//log.Info("inserted dimension", log.Data{"node_id": id})

	d.NodeID = res.GetID()

	log.Info("inserted dimension", log.Data{"node_id": d.NodeID})

	if _, ok := cache[dimensionLabel]; !ok {
		cache[dimensionLabel] = dimensionLabel
		i.AddDimension(d)
	}
	return d, nil
}
