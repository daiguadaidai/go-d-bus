package common

import (
	"testing"
)

func TestEncrypt(t *testing.T) {
	password := "oracle"

	encodePassowrd, err := Encrypt(password)
	if err != nil {
		t.Fatalf("encode password: %v", err)
	}

	t.Logf("encode password: %v", encodePassowrd)
}

func TestDecrypt(t *testing.T) {
	encodePassword := "6fd887932b8419a1ed24b3055cdef5da42382abcc3a427ed11b31afa36d6bcee"

	password, err := Decrypt(encodePassword)
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("decode Password: %v", password)
}
