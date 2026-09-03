package downloader

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type partState struct {
	Version   int     `json:"version"`
	Repo      string  `json:"repo"`
	Revision  string  `json:"revision"`
	Path      string  `json:"path"`
	SHA256    string  `json:"sha256,omitempty"`
	Size      int64   `json:"size"`
	Parts     int     `json:"parts"`
	Validator string  `json:"validator,omitempty"`
	Done      []bool  `json:"done,omitempty"`
	Received  []int64 `json:"received,omitempty"`
}

func newPartState(repo, revision string, f repoFile, parts int) partState {
	return partState{Version: 1, Repo: repo, Revision: revision, Path: f.Path, SHA256: f.SHA256, Size: f.Size, Parts: parts, Done: make([]bool, parts), Received: make([]int64, parts)}
}

func loadMatchingPartState(path string, expected partState, dst *partState) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var saved partState
	if json.Unmarshal(data, &saved) != nil || saved.Version != expected.Version || saved.Repo != expected.Repo || saved.Revision != expected.Revision || saved.Path != expected.Path || saved.SHA256 != expected.SHA256 || saved.Size != expected.Size || saved.Parts != expected.Parts || len(saved.Done) != expected.Parts {
		return false
	}
	if len(saved.Received) != expected.Parts {
		saved.Received = make([]int64, expected.Parts)
	}
	if dst != nil {
		*dst = saved
	}
	return true
}

func writePartState(path string, state partState) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(state)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}
