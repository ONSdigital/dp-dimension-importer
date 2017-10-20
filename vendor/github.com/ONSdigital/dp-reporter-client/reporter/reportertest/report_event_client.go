package reportertest

// NotfiyParams struct encapsulating the parameters of a single Notify call
type NotfiyParams struct {
	ID         string
	ErrContext string
	Err        error
}

// NewImportErrorReporterMock create a new NewImportErrorReporterMock
// notifyErrorFunc enables you to customise the result of Notify func to meet the needs of your test case
func NewImportErrorReporterMock(err error) *ImportErrorReporterMock {
	return &ImportErrorReporterMock{
		notifyErrorFunc: func(string, string, error) error {
			return err
		},
		params: make([]NotfiyParams, 0),
	}
}

type ImportErrorReporterMock struct {
	params          []NotfiyParams
	notifyErrorFunc func(string, string, error) error
}

func (m *ImportErrorReporterMock) Notify(id string, errContext string, err error) error {
	m.params = append(m.params, NotfiyParams{
		ID:         id,
		ErrContext: errContext,
		Err:        err,
	})
	return m.notifyErrorFunc(id, errContext, err)
}

// NotifyCalls return a slice of the NotfiyParams passed into each invocation of Notify
func (m *ImportErrorReporterMock) NotifyCalls() []NotfiyParams {
	return m.params
}
