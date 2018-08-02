package common

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	math_rand "math/rand"
	"sync"
	"crypto/md5"
)

func RandString(n int) string {
	// 用掩码实现随机字符串
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, math_rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = math_rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// key
const aesTable = "xWduqtCDMLxiDHIMG0FpXzp2LGIehp2s"

var (
	block cipher.Block
	mutex sync.Mutex
)

// AES加密
func Encrypt(origData string) (string, error) {
	src := []byte(origData)
	src = PKCS5Padding(src, aes.BlockSize)
	encryptText := make([]byte, aes.BlockSize+len(src))

	iv := encryptText[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	mode := cipher.NewCBCEncrypter(block, iv)

	mode.CryptBlocks(encryptText[aes.BlockSize:], src)

	return fmt.Sprintf("%x", string(encryptText)), nil
}

// AES解密
func Decrypt(crypted string) (string, error) {
	decryptText, err := hex.DecodeString(crypted)
	if err != nil {
		return "", err
	}
	// 长度不能小于aes.Blocksize
	if len(decryptText) < aes.BlockSize {
		return "", errors.New("crypto/cipher: ciphertext too short")
	}

	iv := decryptText[:aes.BlockSize]
	decryptText = decryptText[aes.BlockSize:]

	// 验证输入参数
	// 必须为aes.Blocksize的倍数
	if len(decryptText)%aes.BlockSize != 0 {
		return "", errors.New("crypto/cipher: ciphertext is not a multiple of the block size")
	}

	mode := cipher.NewCBCDecrypter(block, iv)

	mode.CryptBlocks(decryptText, decryptText)
	decryptText = PKCS5UnPadding(decryptText)
	return string(decryptText), nil
}

func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

/* 通过字符串计算出一个数字的hash值
Params:
	_value: 输入的字符串值
 */
func GenerateHashByString(_value string) int {
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(_value))
	result := Md5Inst.Sum([]byte(""))

	var hashValue int
	for _, data := range result {
		hashValue += int(data)
	}

	return hashValue
}

func init() {
	mutex.Lock()
	defer mutex.Unlock()

	if block != nil {
		return
	}

	cblock, err := aes.NewCipher([]byte(aesTable))
	if err != nil {
		panic("aes.NewCipher: " + err.Error())
	}
	block = cblock
}
