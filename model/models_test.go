package model

import (
	"errors"
	"testing"

	dataset "github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	db "github.com/ONSdigital/dp-graph/v2/models"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInstance_Validate(t *testing.T) {
	Convey("Given Instance is nil", t, func() {
		var i *Instance

		Convey("When Insert is invoked", func() {
			err := i.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance is required but was nil").Error())
			})
		})
	})

	Convey("Given an Instance with an empty instanceID", t, func() {
		i := &Instance{dbInstance: &db.Instance{InstanceID: ""}}

		Convey("When Insert is invoked", func() {
			err := i.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("instance id is required but was empty").Error())
			})
		})
	})
}

func TestDimension_Validate(t *testing.T) {
	Convey("Given a nil dimension", t, func() {
		var d *Dimension

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension is required but was nil").Error())
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID", t, func() {
		d := &Dimension{&db.Dimension{Option: "10"}, "", nil}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension id is required but was empty").Error())
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID and option", t, func() {
		d := &Dimension{&db.Dimension{DimensionID: "", Option: ""}, "", nil}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty").Error())
			})
		})
	})

	Convey("Given a dimension with an empty option", t, func() {
		d := &Dimension{&db.Dimension{DimensionID: "id"}, "", nil}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension value is required but was empty").Error())
			})
		})
	})
}

func TestDimension_New(t *testing.T) {

	emptyDimension := &Dimension{&db.Dimension{}, "", nil}

	Convey("Given a new dimension with a nil pointer dataset API model", t, func() {
		d := NewDimension(nil)

		Convey("It is mapped to an empty DB dimension model", func() {
			So(d, ShouldResemble, emptyDimension)
		})
	})

	Convey("Given a new dimension with empty dataset API model", t, func() {
		d := NewDimension(&dataset.Dimension{})

		Convey("It is mapped to an empty DB dimension model", func() {
			So(d, ShouldResemble, emptyDimension)
		})
	})

	Convey("Given a new dimension with a non-empty dataset API model", t, func() {
		apiDim := &dataset.Dimension{
			DimensionID: "dimID",
			InstanceID:  "instID",
			NodeID:      "nodeID",
			Label:       "lbl",
			Option:      "opt",
			Links:       dataset.Links{},
		}
		d := NewDimension(apiDim)
		expected := &db.Dimension{
			DimensionID: "dimID",
			NodeID:      "nodeID",
			Option:      "opt",
			// Links:       db.Links{},
		}

		Convey("It is mapped to an equivalent database model", func() {
			So(d.DbModel(), ShouldResemble, expected)
			So(d.CodeListID(), ShouldEqual, "")
		})

	})

	Convey("Given a new dimension with a dataset API model with non-empty links", t, func() {
		apiDim := &dataset.Dimension{
			Links: dataset.Links{CodeList: dataset.Link{ID: "myCodeList", URL: "url1"}},
		}
		d := NewDimension(apiDim)
		expected := &db.Dimension{
			DimensionID: "",
			NodeID:      "",
			Option:      "",
		}

		Convey("It is mapped to an equivalent database model and the codeID is obtained from the link", func() {
			So(d.DbModel(), ShouldResemble, expected)
			So(d.CodeListID(), ShouldEqual, "myCodeList")
		})

	})
}

func TestInstance_New(t *testing.T) {

	emptyInstance := &Instance{&db.Instance{}}

	Convey("Given a new instance with a nil pointer dataset API model", t, func() {
		inst := NewInstance(nil)

		Convey("It is mapped to an empty DB instance model", func() {
			So(inst, ShouldResemble, emptyInstance)
		})
	})

	Convey("Given a new instance with an empty dataset API model", t, func() {
		inst := NewInstance(&dataset.Instance{})

		Convey("It is mapped to an empty DB instance model", func() {
			So(inst, ShouldResemble, emptyInstance)
		})
	})

	Convey("Given a new instance with a non-empty API model", t, func() {
		apiInst := &dataset.Instance{
			Version: dataset.Version{
				ID:         "id",
				InstanceID: "shouldBeIgnored",
				State:      dataset.StateCompleted.String()},
		}
		inst := NewInstance(apiInst)
		expected := &db.Instance{
			InstanceID: "id",
		}

		Convey("It is mapped to the expected database model", func() {
			So(inst.DbModel(), ShouldResemble, expected)
		})
	})
}
