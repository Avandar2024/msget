package downloader

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPartStateRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "file.part.meta")
	want := newPartState("org/model", "v1", repoFile{Path: "model.bin", Size: 42, SHA256: "abc"}, 2)
	want.Done[0] = true
	want.Received = []int64{21, 7}
	want.Validator = `"etag"`
	if err := writePartState(path, want); err != nil {
		t.Fatal(err)
	}
	var got partState
	if !loadMatchingPartState(path, newPartState("org/model", "v1", repoFile{Path: "model.bin", Size: 42, SHA256: "abc"}, 2), &got) {
		t.Fatal("saved state did not match")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %#v, want %#v", got, want)
	}
}

func TestPartStateRejectsInvalidState(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "file.part.meta")
	expected := newPartState("org/model", "v1", repoFile{Path: "model.bin", Size: 42}, 2)
	for _, data := range []string{
		"not json",
		`{"version":1,"repo":"other","revision":"v1","path":"model.bin","size":42,"parts":2,"done":[false,false]}`,
		`{"version":1,"repo":"org/model","revision":"v1","path":"model.bin","size":42,"parts":2,"done":[false]}`,
	} {
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		if loadMatchingPartState(path, expected, nil) {
			t.Fatalf("accepted invalid state %q", data)
		}
	}
}
