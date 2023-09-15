package main

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	// "md5tabsum/log"
	"os"
	"strings"

	sLog "github.com/sabitor/simplelog"
	"golang.org/x/term"
)

// The cipher key has to be either 16, 24 or 32 bytes. Change it accordingly!
const CIPHERKEY = "abcdefghijklmnopqrstuvwxyz012345"

// writePasswordStore writes the encrypted instance:password data for each configured DBMS instance to the password store.
func writePasswordStore(flags int) error {
	f, err := os.OpenFile(passwordStoreFile, flags, 0600)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, err.Error())
		return err
	}
	defer f.Close()

	for k, v := range instancePassword {
		record, err := encryptAES(CIPHERKEY, k+":"+v)
		if err != nil {
			return err
		}
		record += "\n"
		f.Write([]byte(record))
	}
	return err
}

// readPasswordStore reads the encrypted instance:password data and stores them unencrypted in the global instance/password map.
func readPasswordStore() error {
	f, err := os.OpenFile(passwordStoreFile, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		encryptedRecord := scanner.Text() // get the line string
		record, err := decryptAES(CIPHERKEY, encryptedRecord)
		if err != nil {
			return err
		}
		instance, password, _ := strings.Cut(record, ":")
		instancePassword[instance] = password
	}

	return err
}

// createInstance creates the password store from scratch based on the configured instances found in the config file.
func createInstance() error {
	var password []byte
	for i := range instanceToConfig {
		fmt.Printf("Enter password for instance %s: ", i)
		password, _ = term.ReadPassword(0)
		fmt.Printf("\n")
		instancePassword[i] = string(password)
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
	if _, isValid := instancePassword[*instance]; !isValid {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, mm007)
		// os.Exit(ERROR)
		err = errors.New(mm007)
		return err
	}
	delete(instancePassword, *instance)

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
	if _, isValid := instancePassword[*instance]; isValid {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "The specified instance already exists in the password store.")
		// os.Exit(ERROR)
		err = errors.New(mm008)
		return err

	}

	var password []byte
	fmt.Printf("Enter password for instance %s: ", *instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	instancePassword[*instance] = string(password)

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
	if _, isValid := instancePassword[*instance]; !isValid {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "The specified instance doesn't exist in the password store.")
		// os.Exit(ERROR)
		err = errors.New(mm007)
		return err
	}

	var password []byte
	fmt.Printf("Enter new password for instance %s: ", *instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	instancePassword[*instance] = string(password)

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

	for k := range instancePassword {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, k)
		sLog.Write(sLog.STDOUT, k)
	}

	return err
}

// encodeBase64 encodes a byte slice using the Base64 algorithm.
func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// decodeBase64 decodes a string which was encoded using the Base64 algorithm.
func decodeBase64(s string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while decoding data.")
		// os.Exit(ERROR)
		return data, err
	}

	return data, err
}

// encryptAES encrypts a string using AES encryption
func encryptAES(key, plainText string) (string, error) {
	cph, err := aes.NewCipher([]byte(key))
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while creating a new cipher block.")
		// os.Exit(ERROR)
		return "", err
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while creating a new cipher block.")
		// os.Exit(ERROR)
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while populating the nonce.")
		// os.Exit(ERROR)
		return "", err
	}

	encryptedPassword := encodeBase64(gcm.Seal(nonce, nonce, []byte(plainText), nil))
	return encryptedPassword, err
}

// decryptAES decrypts a string which was encrypted using AES encryption
func decryptAES(key, encryptedText string) (string, error) {
	cph, err := aes.NewCipher([]byte(key))
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while creating a new cipher block.")
		// os.Exit(ERROR)
		return "", err
	}
	gcm, err := cipher.NewGCM(cph)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while returning the 128-bit block.")
		// os.Exit(ERROR)
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(encryptedText) < nonceSize {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while determining the nonce size.")
		// os.Exit(ERROR)
		return "", errors.New(mm009)
	}
	cipherText, err := decodeBase64(encryptedText)
	if err != nil {
		return "", err
	}
	nonce, encryptedMessage := cipherText[:nonceSize], cipherText[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encryptedMessage, nil)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "Something went wrong while authenticating and decrypting the ciphertext.")
		// os.Exit(ERROR)
		return "", err
	}

	plainPassword := string(plaintext)
	return plainPassword, err
}
