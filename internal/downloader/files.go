package downloader

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (d *Downloader) validExisting(path string, f repoFile) (bool, error) {
	st, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if f.Size > 0 && st.Size() != f.Size {
		return false, nil
	}
	if !d.Verify || f.SHA256 == "" {
		return true, nil
	}
	return hashMatches(path, f.SHA256)
}

func verifyFile(path string, f repoFile, verify bool) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if f.Size > 0 && st.Size() != f.Size {
		return fmt.Errorf("incomplete file: got %d bytes, expected %d", st.Size(), f.Size)
	}
	if verify && f.SHA256 != "" {
		ok, err := hashMatches(path, f.SHA256)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("SHA-256 verification failed")
		}
	}
	return nil
}

func hashMatches(path, expected string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, 1<<20)); err != nil {
		return false, err
	}
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), expected), nil
}

func safeTarget(root, remote string) (string, error) {
	remote = strings.ReplaceAll(remote, "\\", "/")
	clean := filepath.Clean(filepath.FromSlash(remote))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes output directory")
	}
	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes output directory")
	}
	return target, nil
}

func selectedBy(name string, includes, excludes []string) bool {
	if len(includes) > 0 && !matchesAny(name, includes) {
		return false
	}
	return !matchesAny(name, excludes)
}

func matchesAny(name string, patterns []string) bool {
	for _, pattern := range patterns {
		if globMatch(pattern, name) {
			return true
		}
	}
	return false
}

func globMatch(pattern, name string) bool {
	p, s := []rune(pattern), []rune(name)
	type position struct{ pattern, name int }
	seen := make(map[position]bool)
	failed := make(map[position]bool)
	var match func(int, int) bool
	match = func(pi, si int) bool {
		pos := position{pi, si}
		if seen[pos] {
			return !failed[pos]
		}
		seen[pos] = true
		ok := false
		switch {
		case pi == len(p):
			ok = si == len(s)
		case p[pi] == '*':
			double := pi+1 < len(p) && p[pi+1] == '*'
			next := pi + 1
			if double {
				next++
			}
			ok = match(next, si) || si < len(s) && (double || s[si] != '/') && match(pi, si+1)
		case si < len(s) && p[pi] == '?' && s[si] != '/':
			ok = match(pi+1, si+1)
		case si < len(s) && p[pi] == s[si]:
			ok = match(pi+1, si+1)
		}
		failed[pos] = !ok
		return ok
	}
	return match(0, 0)
}

func humanBytes(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := unit, 0
	for value := n / unit; value >= unit && exp < 5; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
