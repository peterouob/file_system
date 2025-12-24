package storage

import "testing"

func TestTransformKey(t *testing.T) {
	key := "abcdefg"
	path := FileTransform(key)
	expectFIlePath := "ab"
	expectFileName := "cdefg"

	if path.FilePath != expectFIlePath {
		t.Fatalf("expect FileTransformed path %s, but got %s", expectFileName, path.FilePath)
	}

	if path.FileName != expectFileName {
		t.Fatalf("expect FileTransformed path %s, but got %s", expectFileName, path.FileName)
	}

	expectFullPaht := "ab/cdefg"

	if path.GetFullPath() != expectFullPaht {
		t.Fatalf("expect GetFullPath %s, but got %s", expectFullPaht, path.GetFullPath())
	}
}
