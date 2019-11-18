package web

import "errors"

var (
	badRequestError             = errors.New("Bad request")
	insuffcientPermissionsError = errors.New("Insuffcient permissions")
)
