package zfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zrepl/zrepl/util/envconst"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type VersionType string

const (
	Bookmark VersionType = "bookmark"
	Snapshot VersionType = "snapshot"
)

func (t VersionType) DelimiterChar() string {
	switch t {
	case Bookmark:
		return "#"
	case Snapshot:
		return "@"
	default:
		panic(fmt.Sprintf("unexpected VersionType %#v", t))
	}
}

func (t VersionType) String() string {
	return string(t)
}

func DecomposeVersionString(v string) (fs string, versionType VersionType, name string, err error) {
	if len(v) < 3 {
		err = fmt.Errorf("snapshot or bookmark name implausibly short: %s", v)
		return
	}

	snapSplit := strings.SplitN(v, "@", 2)
	bookmarkSplit := strings.SplitN(v, "#", 2)
	if len(snapSplit)*len(bookmarkSplit) != 2 {
		err = fmt.Errorf("dataset cannot be snapshot and bookmark at the same time: %s", v)
		return
	}

	if len(snapSplit) == 2 {
		return snapSplit[0], Snapshot, snapSplit[1], nil
	} else {
		return bookmarkSplit[0], Bookmark, bookmarkSplit[1], nil
	}
}

type FilesystemVersion struct {
	Type VersionType

	// Display name. Should not be used for identification, only for user output
	Name string

	// GUID as exported by ZFS. Uniquely identifies a snapshot across pools
	Guid uint64

	// The TXG in which the snapshot was created. For bookmarks,
	// this is the GUID of the snapshot it was initially tied to.
	CreateTXG uint64

	// The time the dataset was created
	Creation time.Time
}

func (v FilesystemVersion) String() string {
	return fmt.Sprintf("%s%s", v.Type.DelimiterChar(), v.Name)
}

func (v FilesystemVersion) ToAbsPath(p *DatasetPath) string {
	var b bytes.Buffer
	b.WriteString(p.ToString())
	b.WriteString(v.Type.DelimiterChar())
	b.WriteString(v.Name)
	return b.String()
}

type FilesystemVersionFilter interface {
	Filter(t VersionType, name string) (accept bool, err error)
}

func ZFSListFilesystemVersions(fs *DatasetPath, filter FilesystemVersionFilter) (res []FilesystemVersion, err error) {
	listResults := make(chan ZFSListResult)

	promTimer := prometheus.NewTimer(prom.ZFSListFilesystemVersionDuration.WithLabelValues(fs.ToString()))
	defer promTimer.ObserveDuration()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ZFSListChan(ctx, listResults,
		[]string{"name", "guid", "createtxg", "creation"},
		"-r", "-d", "1",
		"-t", "bookmark,snapshot",
		"-s", "createtxg", fs.ToString())

	res = make([]FilesystemVersion, 0)
	for listResult := range listResults {
		if listResult.Err != nil {
			if listResult.Err == io.ErrUnexpectedEOF {
				// Since we specified the fs on the command line, we'll treat this like the filesystem doesn't exist
				return []FilesystemVersion{}, nil
			}
			return nil, listResult.Err
		}

		line := listResult.Fields

		var v FilesystemVersion

		_, v.Type, v.Name, err = DecomposeVersionString(line[0])
		if err != nil {
			return nil, err
		}

		if v.Guid, err = strconv.ParseUint(line[1], 10, 64); err != nil {
			err = errors.Wrap(err, "cannot parse GUID")
			return
		}

		if v.CreateTXG, err = strconv.ParseUint(line[2], 10, 64); err != nil {
			err = errors.Wrap(err, "cannot parse CreateTXG")
			return
		}

		creationUnix, err := strconv.ParseInt(line[3], 10, 64)
		if err != nil {
			err = fmt.Errorf("cannot parse creation date '%s': %s", line[3], err)
			return nil, err
		} else {
			v.Creation = time.Unix(creationUnix, 0)
		}

		accept := true
		if filter != nil {
			accept, err = filter.Filter(v.Type, v.Name)
			if err != nil {
				err = fmt.Errorf("error executing filter: %s", err)
				return nil, err
			}
		}
		if accept {
			res = append(res, v)
		}

	}
	return
}

type DestroySnapOp struct {
	Filesystem string
	Name       string
	ErrOut     *error
}

func (o *DestroySnapOp) String() string {
	return fmt.Sprintf("destroy operation %s@%s", o.Filesystem, o.Name)
}

type batchDestroy struct {
	fs              string
	fullBatch       []*singleDestroy
	fullBatchStderr []byte

	correctedBatch       []*singleDestroy
	correctedBatchStderr []byte

	outOfBAtch []*singleDestroy
}

type singleDestroy struct {
	*DestroySnapOp
}

type destroyer interface {
	Destroy(args []string) error
	DestroySnapshotsCommaSyntaxSupported() (bool, error)
	MaxSingleArgumentLength() int
}

func doDestroy(ctx context.Context, reqs []*DestroySnapOp, e destroyer) {

	writeErr := func(b []*DestroySnapOp, err error) {
		for _, r := range b {
			*r.ErrOut = err
		}
	}

	commaSupported, err := e.DestroySnapshotsCommaSyntaxSupported()
	if err != nil {
		writeErr(reqs, err)
		return
	}

	if !commaSupported {
		for _, r := range reqs {
			*r.ErrOut = e.Destroy([]string{fmt.Sprintf("%s@%s", r.Filesystem, r.Name)})
		}
		return
	}

	sorted := make([]*DestroySnapOp, len(reqs))
	copy(sorted, reqs)
	sort.SliceStable(sorted, func(i, j int) bool {
		// by filesystem, then snap name
		fscmp := strings.Compare(sorted[i].Filesystem, sorted[j].Filesystem)
		if fscmp != 0 {
			return fscmp == -1
		}
		return strings.Compare(sorted[i].Name, sorted[j].Name) == -1
	})

	maxDestroyLen := e.MaxSingleArgumentLength()
	tryBatch := func(batch []*DestroySnapOp) error {
		if len(batch) == 0 {
			return nil
		}
		batchFS := batch[0].Filesystem
		batchNames := make([]string, len(batch))
		for i := range batchNames {
			batchNames[i] = batch[i].Name
			if batchFS != batch[i].Filesystem {
				panic("inconsistent batch")
			}
		}
		batchArg := fmt.Sprintf("%s@%s", batchFS, strings.Join(batchNames, ","))
		if len(batchArg) > maxDestroyLen {
			panic(fmt.Sprintf("batch arg len length exceeds max arg size %v vs %v", len(batchArg), maxDestroyLen))
		}
		return e.Destroy([]string{batchArg})
	}

	destroyedUntil := 0
	for destroyedUntil < len(sorted) {
		batchFS := sorted[destroyedUntil].Filesystem

		batchUntil := destroyedUntil
		var batch []*DestroySnapOp      // TODO sorted list
		batchArgLen := len(batchFS) + 1 // for "@"
		for ; batchArgLen-1 < maxDestroyLen && batchUntil < len(sorted) && sorted[batchUntil].Filesystem == batchFS; batchUntil++ {
			batch = append(batch, sorted[batchUntil])
			batchArgLen += len(sorted[batchUntil].Name) + 1 // for ","
		}
		destroyedUntil = batchUntil // invariant: we will try to handle all snaps in batch in the code below

		fmt.Printf("batch: %v\n", batch)

		err := tryBatch(batch)
		if err == nil {
			continue
		}
		fmt.Printf("batch first err: %s\n", err)

		singleRun := batch // the destroys that will be tried sequentially after the smart error handling below

		if err, ok := err.(*DestroySnapshotsError); ok {
			// eliminate undestroyable datasets from batch and try it once again
			strippedBatch, remaining := make([]*DestroySnapOp, 0, len(batch)), make([]*DestroySnapOp, 0, len(batch))

			for _, b := range batch {
				isUndestroyable := false
				for _, undestroyable := range err.Undestroyable {
					if undestroyable == b.Name {
						isUndestroyable = true
						break
					}
				}
				if isUndestroyable {
					remaining = append(remaining, b)
				} else {
					strippedBatch = append(strippedBatch, b)
				}
			}

			err := tryBatch(strippedBatch)
			if err != nil {
				// retry entire batch if the stripped one fails
				// (it shouldn't because we stripped erronous datasets)
				singleRun = batch // shadow
			} else {
				writeErr(strippedBatch, nil) // these ones worked
				singleRun = remaining        // shadow
			}
			// fallthrough
		}

		// try remaining runs sequentially
		for _, o := range singleRun {
			fmt.Printf("destroy single run %s\n", o)
			*o.ErrOut = e.Destroy([]string{fmt.Sprintf("%s@%s", o.Filesystem, o.Name)})
		}

	}

}

func ZFSDestroyFilesystemVersions(reqs []*DestroySnapOp) {
	doDestroy(context.TODO(), reqs, destroyerImpl{})
}

func ZFSDestroyFilesystemVersion(filesystem *DatasetPath, version *FilesystemVersion) (err error) {

	datasetPath := version.ToAbsPath(filesystem)

	// Sanity check...
	if !strings.ContainsAny(datasetPath, "@#") {
		return fmt.Errorf("sanity check failed: no @ or # character found in %q", datasetPath)
	}

	return ZFSDestroy(datasetPath)
}

type destroyerImpl struct{}

func (d destroyerImpl) Destroy(args []string) error {
	if len(args) != 1 {
		// we have no use case for this at the moment, so let's crash (safer than destroying something unexpectedly)
		panic(fmt.Sprintf("unexpected number of arguments: %v", args))
	}
	// we know that we are only using this for snapshots, so also sanity check for an @ in args[0]
	if !strings.ContainsAny(args[0], "@") {
		panic(fmt.Sprintf("sanity check: expecting '@' in call to Destroy, got %q", args[0]))
	}
	return ZFSDestroy(args[0])
}

func (d destroyerImpl) DestroySnapshotsCommaSyntaxSupported() (bool, error) {
	return envconst.Bool("ZREPL_EXPERIMENTAL_ZFS_COMMA_SYNTAX_SUPPORTED", false), nil // TODO
}

func (d destroyerImpl) MaxSingleArgumentLength() int {
	// Linux execve man page says MAX_ARG_STRLEN = 32 pages
	// Let's hope the other platforms behave similarly
	return envconst.Int("ZREPL_ZFS_MAX_ARGUMENT_LENGTH", os.Getpagesize()<<5)
}
