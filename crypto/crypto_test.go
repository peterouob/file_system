package crypto

import (
	"bytes"
	"testing"

	"github.com/peterouob/file_system/utils"
)

func TestCrypto(t *testing.T) {
	payload := "hellopeter"
	src := bytes.NewReader([]byte(payload))
	dst := new(bytes.Buffer)
	key := utils.NewEncryptionKey()
	_, err := CopyEnCrypto(key, src, dst)
	if err != nil {
		t.Fatal(err)
	}

	out := new(bytes.Buffer)
	_, err = CopyDeCrypto(key, dst, out)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out.Bytes(), []byte(payload)) {
		t.Errorf("decode wrong: got %s, want %s", out.String(), payload)
	}
}
