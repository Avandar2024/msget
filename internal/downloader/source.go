package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const (
	SourceAuto       = "auto"
	SourceModelScope = "modelscope"
	SourceHF         = "hf"
)

func (d *Downloader) fileURL(repo, revision, path string) string {
	if d.Source == SourceHF {
		return d.Endpoint + "/" + escapeRepo(repo) + "/resolve/" + url.PathEscape(revision) + "/" + escapeRepo(path)
	}
	q := url.Values{"Revision": {revision}, "FilePath": {path}}
	return d.Endpoint + "/api/v1/models/" + escapeRepo(repo) + "/repo?" + q.Encode()
}

// Keep existing ModelScope checkpoints compatible, but never reuse them for HF.
func (d *Downloader) resumeRepo(repo string) string {
	if d.Source == SourceHF {
		return "hf:" + d.Endpoint + "/" + repo
	}
	return repo
}

func (d *Downloader) listHF(ctx context.Context, repo, revision string) ([]repoFile, error) {
	u := d.Endpoint + "/api/models/" + escapeRepo(repo) + "/revision/" + url.PathEscape(revision) + "?blobs=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	d.headers(req)
	resp, err := d.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("list Hugging Face files: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("list Hugging Face files", resp)
	}
	var result struct {
		Siblings []struct {
			Path string `json:"rfilename"`
			Size int64  `json:"size"`
			LFS  *struct {
				SHA256 string `json:"sha256"`
				Size   int64  `json:"size"`
			} `json:"lfs"`
		} `json:"siblings"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode Hugging Face file list: %w", err)
	}
	files := make([]repoFile, 0, len(result.Siblings))
	for _, sibling := range result.Siblings {
		f := repoFile{Path: sibling.Path, Size: sibling.Size, Type: "blob"}
		if sibling.LFS != nil {
			f.SHA256, f.Size = sibling.LFS.SHA256, sibling.LFS.Size
		}
		files = append(files, f)
	}
	return files, nil
}
