package tests

import (
	"reflect"
	"runtime"

	"github.com/zrepl/zrepl/platformtest"
)

type Case func(*platformtest.Context)

func (c Case) String() string {
	return runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name()
}

var Cases = []Case{
	BatchDestroy,
	UndestroyableSnapshotParsing,
	GetNonexistent,
}
