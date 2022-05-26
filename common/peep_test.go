package common

import (
	"testing"
)

func TestEncrypt(t *testing.T) {
	password := "aaabbbccc"

	encodePassowrd, err := Encrypt(password)
	if err != nil {
		t.Fatalf("encode password: %v", err)
	}

	t.Logf("encode password: %v", encodePassowrd)
}

func TestDecrypt(t *testing.T) {
	encodePassword := "09a1ff3b7af4c471147357dd51d2380459caf78c4b54b9011a17f059857462d1"

	password, err := Decrypt(encodePassword)
	if err != nil {
		t.Errorf("%v", err)
	}

	t.Logf("decode Password: %v", password)
}

type Student struct {
	Id    int64
	Name  string
	Name2 string
}
