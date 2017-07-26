package client

import (
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"io/ioutil"
	"net/http"
	"github.com/ONSdigital/dp-dimension-importer/common"
	"bytes"
)

// import API Get DimensionsKey URL format
const getDimensionsURIFMT = "%s/instances/%s/dimensions"

const putDimensionNodeIDURI = "%s/instances/%s/dimensions/%s/node_id/%s"

// error messages
const okResponseMsg = "Get DimensionsKey success"
const getDimensionsErrMsg = "Get dimensions returned error status"
const unmarshallingErrMsg = "Unexpected error while unmarshalling response"
const unexpectedAPIErrMsg = "Unexpected error returned when calling Import API"
const hostConfigMissingMsg = "DimensionsClient requires an API host to be configured."

// errors
var instanceIDNotFoundErr = errors.New(getDimensionsErrMsg)
var internalServerErr = errors.New(getDimensionsErrMsg)
var unexpectedErr = errors.New(getDimensionsErrMsg)
var missingConfigErr = errors.New("DimensionsClient is missing required configuration.")

// debug data map keys
const urlKey = "url"
const statusCodeKey = "statusCode"
const errDetailsKey = "details"

// Host the host to get the dimensions from.
var Host string

// httpGet abstraction around http.Get to simplify testing / mocking.
var httpGet = http.Get

// respBodyReader abstraction around ioutil.ReadAll to simplify testing / mocking.
var respBodyReader = ioutil.ReadAll

type ImportAPI struct{}

// Get perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (api ImportAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(Host) == 0 {
		log.Debug(hostConfigMissingMsg, nil)
		return nil, missingConfigErr
	}
	if len(instanceID) == 0 {
		return nil, common.ErrInstanceIDRequired
	}

	url := fmt.Sprintf(getDimensionsURIFMT, Host, instanceID)
	data := log.Data{
		common.InstanceIDKey: instanceID,
		urlKey:               url,
	}

	res, err := httpGet(url)
	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unexpectedAPIErrMsg, data)
		return nil, err
	}

	data[statusCodeKey] = res.StatusCode
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		log.Debug(okResponseMsg, data)
	case 404:
		log.Debug(getDimensionsErrMsg, data)
		return nil, instanceIDNotFoundErr
	case 500:
		log.Debug(getDimensionsErrMsg, data)
		return nil, internalServerErr
	default:
		log.Debug(getDimensionsErrMsg, data)
		return nil, unexpectedErr
	}

	body, err := respBodyReader(res.Body)
	if err != nil {
		return nil, err
	}

	var dims []*model.Dimension
	err = json.Unmarshal(body, &dims)

	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unmarshallingErrMsg, data)
		return nil, err
	}
	return dims, nil
}

func (api ImportAPI) UpdateNodeID(instanceID string, d *model.Dimension) error {
	url := fmt.Sprintf(putDimensionNodeIDURI, Host, instanceID, d.Dimension_ID, d.NodeId)

	log.Debug("updating node_id", log.Data{"url": url})

	json, err := json.Marshal(d)
	if err != nil {
		log.Debug("failed to marshall json.", nil)
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(json))
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Debug("PUT node_id returned an error.", nil)
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		return nil
	case 404:
		return errors.New("PUT node_id returned 404")
	case 500:
		return errors.New("PUT node_id returned 500")
	default:
		return errors.New("PUT node_id returned an unexpected error")
	}
	return nil
}
