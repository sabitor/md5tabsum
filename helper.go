package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"strings"
)

// encodeBase64 encodes a byte slice using the Base64 algorithm.
func encodeBase64(sourceBytes []byte) string {
	encodedText := base64.StdEncoding.EncodeToString(sourceBytes)
	return encodedText
}

// decodeBase64 decodes a string which was encoded using the Base64 algorithm.
func decodeBase64(encodedText string) ([]byte, error) {
	decodedText, err := base64.StdEncoding.DecodeString(encodedText)
	if err != nil {
		return decodedText, err
	}

	return decodedText, err
}

// encryptAES encrypts a string using AES encryption
func encryptAES(key []byte, plainText string) (string, error) {
	cph, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	encryptedText := encodeBase64(gcm.Seal(nonce, nonce, []byte(plainText), nil))
	return encryptedText, err
}

// decryptAES decrypts a string which was encrypted using AES encryption.
func decryptAES(key []byte, encryptedText string) (string, error) {
	cph, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(encryptedText) < nonceSize {
		return "", errors.New(mm009)
	}
	cipherText, err := decodeBase64(encryptedText)
	if err != nil {
		return "", err
	}
	nonce, encryptedMessage := cipherText[:nonceSize], cipherText[nonceSize:]
	plaintextByteSlice, err := gcm.Open(nil, nonce, encryptedMessage, nil)
	if err != nil {
		return "", err
	}

	plaintextSrting := string(plaintextByteSlice)
	return plaintextSrting, err
}

// instanceName validates the existence of a given DBMS instance name in the instaneToConfig map.
func instanceName(instance string) database {
	if v, ok := instanceConfig[instance]; ok {
		return v
	}
	msg := "key '" + instance + "' doesn't exist"
	panic(msg)
}

// condition calculates whether to write a log message depending on the logging level settings of the configuration file.
func condition(cfgLogLevel, msgLogLevel int) bool {
	return cfgLogLevel >= msgLogLevel // cfgLogLevel contains the setting of an Loglevel config file parameter
}

// formatMsg replaces message wildecards with corresponding parameters.
// A message wildcard is of the format: %<number> (number starts at 1).
func formatMsg(m string, p ...string) string {
	result := m
	for i, v := range p {
		param := "%" + strconv.Itoa(i+1)
		result = strings.Replace(m, param, v, -1)

	}
	return result
}
