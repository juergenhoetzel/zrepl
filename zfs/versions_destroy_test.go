package zfs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zrepl/zrepl/util/chainlock"
)

type mockBatchDestroy struct {
	mtx                     chainlock.L
	calls                   []string
	commaUnsupported        bool
	undestroyable           string
	randomerror             string
	maxSingleArgumentLength int
}

func (m *mockBatchDestroy) DestroySnapshotsCommaSyntaxSupported() (bool, error) {
	return !m.commaUnsupported, nil
}

func (m *mockBatchDestroy) MaxSingleArgumentLength() int {
	if m.maxSingleArgumentLength == 0 {
		return 1 << 30 // absurdly large
	}
	return m.maxSingleArgumentLength
}

func (m *mockBatchDestroy) Destroy(args []string) error {
	defer m.mtx.Lock().Unlock()
	if len(args) != 1 {
		panic("unexpected use of Destroy")
	}
	a := args[0]
	m.calls = append(m.calls, a)
	if m.commaUnsupported {
		if strings.Contains(a, ",") {
			return fmt.Errorf("unsupported syntax mock error")
		}
	}

	if m.undestroyable != "" && strings.Contains(a, m.undestroyable) {
		return &DestroySnapshotsError{
			Filesystem:    "PLACEHOLDER",
			Undestroyable: []string{m.undestroyable},
			Reason:        []string{"undestroyable reason"},
		}
	}
	if m.randomerror != "" && strings.Contains(a, m.randomerror) {
		return fmt.Errorf("randomerror")
	}
	return nil
}

func TestBatchDestroySnaps(t *testing.T) {

	errs := make([]error, 10)
	nilErrs := func() {
		for i := range errs {
			errs[i] = nil
		}
	}
	opsTemplate := []*DestroySnapOp{
		&DestroySnapOp{"zroot/z", "foo", &errs[0]},
		&DestroySnapOp{"zroot/a", "foo", &errs[1]},
		&DestroySnapOp{"zroot/a", "bar", &errs[2]},
		&DestroySnapOp{"zroot/b", "bar", &errs[3]},
		&DestroySnapOp{"zroot/b", "zab", &errs[4]},
		&DestroySnapOp{"zroot/b", "undestroyable", &errs[5]},
		&DestroySnapOp{"zroot/c", "baz", &errs[6]},
		&DestroySnapOp{"zroot/c", "randomerror", &errs[7]},
		&DestroySnapOp{"zroot/c", "bar", &errs[8]},
		&DestroySnapOp{"zroot/d", "blup", &errs[9]},
	}

	t.Run("single_undestroyable_dataset", func(t *testing.T) {
		nilErrs()
		mock := &mockBatchDestroy{
			commaUnsupported:        false,
			undestroyable:           "undestroyable",
			randomerror:             "randomerror",
		}

		doDestroy(context.TODO(), opsTemplate, mock)

		assert.NoError(t, errs[0])
		assert.NoError(t, errs[1])
		assert.NoError(t, errs[2])
		assert.NoError(t, errs[3])
		assert.NoError(t, errs[4])
		assert.Error(t, errs[5], "undestroyable")
		assert.NoError(t, errs[6])
		assert.Error(t, errs[7], "randomerror")
		assert.NoError(t, errs[8])
		assert.NoError(t, errs[9])

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				"zroot/a@bar,foo", // reordered snaps in lexicographical order
				"zroot/b@bar,undestroyable,zab",
				"zroot/b@bar,zab", // eliminate undestroyables, try others again
				"zroot/b@undestroyable",
				"zroot/c@bar,baz,randomerror",
				"zroot/c@bar", // fallback to single-snapshot on non DestroyError
				"zroot/c@baz",
				"zroot/c@randomerror",
				"zroot/d@blup",
				"zroot/z@foo", // ordered at last position
			},
			mock.calls,
		)
	})

	t.Run("comma_syntax_unsupported", func(t *testing.T) {
		nilErrs()
		mock := &mockBatchDestroy{
			commaUnsupported:        true,
			undestroyable:           "undestroyable",
			randomerror:             "randomerror",
		}

		doDestroy(context.TODO(), opsTemplate, mock)

		assert.NoError(t, errs[0])
		assert.NoError(t, errs[1])
		assert.NoError(t, errs[2])
		assert.NoError(t, errs[3])
		assert.NoError(t, errs[4])
		assert.Error(t, errs[5], "undestroyable")
		assert.NoError(t, errs[6])
		assert.Error(t, errs[7], "randomerror")
		assert.NoError(t, errs[8])
		assert.NoError(t, errs[9])

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				// expect exactly argument order
				"zroot/z@foo",
				"zroot/a@foo",
				"zroot/a@bar",
				"zroot/b@bar",
				"zroot/b@zab",
				"zroot/b@undestroyable",
				"zroot/c@baz",
				"zroot/c@randomerror",
				"zroot/c@bar",
				"zroot/d@blup",
			},
			mock.calls,
		)
	})

	t.Run("empty_ops", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		doDestroy(context.TODO(), nil, mock)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t,mock.calls)
	})

	t.Run("ops_without_snapnames", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		var err error
		ops := []*DestroySnapOp{&DestroySnapOp{"somefs", "", &err}}
		doDestroy(context.TODO(), ops, mock)
		assert.Error(t, err)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t,mock.calls)
	})

	t.Run("ops_without_fsnames", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		var err error
		ops := []*DestroySnapOp{&DestroySnapOp{"", "fsname", &err}}
		doDestroy(context.TODO(), ops, mock)
		assert.Error(t, err)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t,mock.calls)
	})

	t.Run("splits_up_args_at_max_length", func(t *testing.T) {
		mock := &mockBatchDestroy{
			maxSingleArgumentLength: 10,
		}

		var dummy error
		reqs := []*DestroySnapOp{
			// should fit (1111@a,b,c)
			&DestroySnapOp{"1111", "a", &dummy},
			&DestroySnapOp{"1111", "b", &dummy},
			&DestroySnapOp{"1111", "c", &dummy},

			// should split (2222@a,b,c 2222@d,e
			&DestroySnapOp{"2222", "a", &dummy},
			&DestroySnapOp{"2222", "b", &dummy},
			&DestroySnapOp{"2222", "c", &dummy},
			&DestroySnapOp{"2222", "d", &dummy},
			&DestroySnapOp{"2222", "e", &dummy},
		}

		doDestroy(context.TODO(), reqs, mock)

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				"1111@a,b,c",
				"2222@a,b,c",
				"2222@d,e",
			},
			mock.calls,
		)

	})

}

func TestMaxArgumentLengthSupportedOnThisOS(t *testing.T) {
	t.Logf("maxArgumentLength=%v", maxArgumentLength)
	maxArg := bytes.Repeat([]byte("a"), maxArgumentLength)
	cmd := exec.Command("/bin/sh", "-c", "echo -n $1; echo -n $2", "cmdname", string(maxArg), string(maxArg))
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err)
	t.Logf("%v %v", len(maxArg), len(output))
	assert.Equal(t, bytes.Repeat(maxArg, 2), output)
}
