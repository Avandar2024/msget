package downloader

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
		re, err := regexp.Compile(globRegexp(pattern))
		if err == nil && re.MatchString(name) {
			return true
		}
	}
	return false
}

func globRegexp(glob string) string {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(glob); i++ {
		switch glob[i] {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '/':
			b.WriteByte('/')
		default:
			b.WriteString(regexp.QuoteMeta(string(glob[i])))
		}
	}
	b.WriteByte('$')
	return b.String()
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
