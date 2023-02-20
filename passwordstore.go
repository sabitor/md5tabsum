package main

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"md5tabsum/constant"
	"os"
	"strings"

	"golang.org/x/term"
)

// writePasswordStore writes the encrypted instance passwords to the password store.
// The password store location is specified in the config file.
func writePasswordStore(flags int) error {
	f, err := os.OpenFile(gPasswordStore, flags, 0600)
	if err != nil {
		writeLogBasic(constant.STDOUT, err.Error())
		return err
	}
	defer f.Close()

	for k, v := range gInstancePassword {
		record := encryptAES(constant.CIPHERKEY, k+":"+v) + "\n"
		f.Write([]byte(record))
	}
	return err
}

// readPasswordStore reads the encrypted instance passwords and stores them unencrypted in the global instance/password map.
func readPasswordStore() error {
	f, err := os.OpenFile(gPasswordStore, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		record := scanner.Text() // get the line string
		instance, password, _ := strings.Cut(decryptAES(constant.CIPHERKEY, record), ":")
		gInstancePassword[instance] = password
	}

	return err
}

// createInstance creates the password store from scratch based on the configured instances found in the config file.
func createInstance() error {
	var password []byte
	for i := range gDbms {
		fmt.Printf("Enter password for instance %s: ", i)
		password, _ = term.ReadPassword(0)
		fmt.Printf("\n")
		gInstancePassword[i] = string(password)
	}

	err := writePasswordStore(os.O_WRONLY | os.O_CREATE | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// deleteInstance deletes a dedicated entry from the global instance password map.
func deleteInstance(instance *string) error {
	err := readPasswordStore()
	if err != nil {
		return err
	}
	if _, isValid := gInstancePassword[*instance]; !isValid {
		writeLogBasic(constant.STDOUT, "The specified instance doesn't exist in the password store.")
		os.Exit(constant.ERROR)
	}
	delete(gInstancePassword, *instance)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// addInstance adds a dedicated entry in the global instance password map.
func addInstance(instance *string) error {
	err := readPasswordStore()
	if err != nil {
		return err
	}
	if _, isValid := gInstancePassword[*instance]; isValid {
		writeLogBasic(constant.STDOUT, "The specified instance already exists in the password store.")
		os.Exit(constant.ERROR)
	}

	var password []byte
	fmt.Printf("Enter password for instance %s: ", *instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	gInstancePassword[*instance] = string(password)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// updateInstance updates a dedicated entry in the global instance password map.
func updateInstance(instance *string) error {
	err := readPasswordStore()
	if err != nil {
		return err
	}
	if _, isValid := gInstancePassword[*instance]; !isValid {
		writeLogBasic(constant.STDOUT, "The specified instance doesn't exist in the password store.")
		os.Exit(constant.ERROR)
	}

	var password []byte
	fmt.Printf("Enter new password for instance %s: ", *instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	gInstancePassword[*instance] = string(password)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// showInstance lists all instances (without the password) which were found in the global instance password map.
func showInstance() error {
	err := readPasswordStore()
	if err != nil {
		return err
	}

	for k := range gInstancePassword {
		writeLogBasic(constant.STDOUT, k)
	}

	return err
}

// encodeBase64 encodes a byte slice using the Base64 algorithm.
func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// decodeBase64 decodes a string which was encoded using the Base64 algorithm.
func decodeBase64(s string) []byte {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while decoding data.")
		os.Exit(constant.ERROR)
	}

	return data
}

// encryptAES encrypts a string using AES encryption
func encryptAES(key, plainText string) string {
	cph, err := aes.NewCipher([]byte(key))
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while creating a new cipher block.")
		os.Exit(constant.ERROR)
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while returning the 128-bit block.")
		os.Exit(constant.ERROR)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while populating the nonce.")
		os.Exit(constant.ERROR)
	}

	return encodeBase64(gcm.Seal(nonce, nonce, []byte(plainText), nil))
}

// decryptAES decrypts a string which was encrypted using AES encryption
func decryptAES(key, encryptedText string) string {
	cph, err := aes.NewCipher([]byte(key))
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while creating a new cipher block.")
		os.Exit(constant.ERROR)
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while returning the 128-bit block.")
		os.Exit(constant.ERROR)
	}
	nonceSize := gcm.NonceSize()
	if len(encryptedText) < nonceSize {
		writeLogBasic(constant.STDOUT, "Something went wrong while determining the nonce size.")
		os.Exit(constant.ERROR)
	}
	cipherText := decodeBase64(encryptedText)
	nonce, encryptedMessage := cipherText[:nonceSize], cipherText[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encryptedMessage, nil)
	if err != nil {
		writeLogBasic(constant.STDOUT, "Something went wrong while authenticating and decrypting the ciphertext.")
		os.Exit(constant.ERROR)
	}

	return string(plaintext)
}
