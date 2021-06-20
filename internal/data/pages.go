package data

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrInvalidPagesFormat = errors.New("invalid pages format")

type Pages int32

func (r Pages) MarshalJSON() ([]byte, error) {

	jsonValue := fmt.Sprintf("%d pages", r)

	quotedJSONValue := strconv.Quote(jsonValue)

	return []byte(quotedJSONValue), nil
}

func (r *Pages) UnmarshalJSON(jsonValue []byte) error {

	unquotedJSONValue, err := strconv.Unquote(string(jsonValue))
	if err != nil {
		return ErrInvalidPagesFormat
	}

	parts := strings.Split(unquotedJSONValue, " ")

	if len(parts) != 2 || parts[1] != "pages" {
		return ErrInvalidPagesFormat
	}

	i, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return ErrInvalidPagesFormat
	}

	*r = Pages(i)
	return nil
}
