package distboruvka

import (
	"testing"
)

func TestGetGraphDOTFile(t *testing.T) {
	e, _ := GetGraphDOTFile("sample.dot", "2")
	t.Log(e)
}
