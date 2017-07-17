package client

type ClientError string

func (err ClientError) Error() string {
	return string(err)
}
