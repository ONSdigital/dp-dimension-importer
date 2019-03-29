package model

import (
	"errors"
	"testing"

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
		i := &Instance{InstanceID: ""}

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
		d := &Dimension{Option: "10"}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension id is required but was empty").Error())
			})
		})
	})

	Convey("Given a dimension with an empty dimensionID and option", t, func() {
		d := &Dimension{DimensionID: "", Option: ""}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty").Error())
			})
		})
	})

	Convey("Given a dimension with an empty option", t, func() {
		d := &Dimension{DimensionID: "id"}

		Convey("When validateDimension is called", func() {
			err := d.Validate()

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, errors.New("dimension value is required but was empty").Error())
			})
		})
	})
}
