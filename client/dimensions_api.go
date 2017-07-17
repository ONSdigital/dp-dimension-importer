package client

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"errors"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"encoding/json"
	"github.com/ONSdigital/go-ns/log"
)

const getDimensionsURLFMT = "%s/instance/%s/dimensions"

const instanceIDRequiredErr = ClientError("DimensionsClient.Get requires instanceID")

var httpGet func(string) (*http.Response, error) = http.Get

type DimensionsClientImpl struct {
	DimensionsAddr string
}

func (i DimensionsClientImpl) Get(instanceID string) (*model.Dimensions, error) {
	if len(instanceID) == 0 {
		return nil, instanceIDRequiredErr
	}

	url := fmt.Sprintf(getDimensionsURLFMT, i.DimensionsAddr, instanceID)

	log.Debug("Calling Import API", log.Data{
		"url": url,
	})

	res, err := httpGet(url)

	if err != nil {
		log.Debug("Error calling Import API", nil)
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	if res.StatusCode == 404 {
		return nil, errors.New("InstanceID does not match any import jobs")
	}

	if res.StatusCode == 500 {
		return nil, errors.New("Failed to process the request due to an internal error")
	}

	log.Debug("DimensionsCluent.Get success", log.Data{"instanceID": instanceID})

	var dims model.Dimensions
	err = json.Unmarshal(body, &dims)

	if err != nil {
		log.Debug("Unexpected error while unmarshalling response.", nil)
		return nil, err
	}
	return &dims, nil
}


