package main

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/sabitor/simplelog"
	"golang.org/x/term"
)

var (
	passwordStoreFile    string
	passwordStoreKeyFile string
	instancePassword     = make(map[string]string) // store config file instances and their password
)

// readSecretKey reads the secret key from the password store key file into memory.
func readSecretKey() ([]byte, error) {
	secretKey := []byte{}
	encodedSecretKey, err := os.ReadFile(passwordStoreKeyFile)
	if err != nil {
		return secretKey, err
	}
	secretKey, err = decodeBase64(string(encodedSecretKey))
	if err != nil {
		return secretKey, err
	}
	return secretKey, err
}

// writePasswordStore writes AES encrypted password store records into the password store.
// This will be done for each configured and activated DBMS instance in the config file.
func writePasswordStore(flags int) error {
	f, err := os.OpenFile(passwordStoreFile, flags, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	secretKey, err := readSecretKey()
	if err != nil {
		return err
	}
	for k, v := range instancePassword {
		record, err := encryptAES(secretKey, k+":"+v)
		if err != nil {
			return err
		}
		record += "\n"
		f.Write([]byte(record))
	}
	return err
}

// readPasswordStore reads AES encrypted password store records and stores them unencrypted in the global instance/password map.
func readPasswordStore() error {
	var err error
	if _, err = os.Stat(passwordStoreFile); err == nil {
		f, err := os.OpenFile(passwordStoreFile, os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		secretKey, err := readSecretKey()
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			encryptedRecord := scanner.Text() // get the line string
			record, err := decryptAES(secretKey, encryptedRecord)
			if err != nil {
				return err
			}
			instance, password, _ := strings.Cut(record, ":")
			instancePassword[instance] = password
		}
	} else if os.IsNotExist(err) {
		err = errors.New(mm016)
	}

	return err
}

// init initializes the password store based on the configured and activated instances found in the config file.
// A key/value pair per active config file section will be stored in the password store. It will be stored AES encrypted.
// The key value is of the format: <predefined DBMS name>.<instance ID>, the key value is the user password.
func initPWS() error {
	var err error
	if _, err = os.Stat(passwordStoreKeyFile); err != nil {
		if os.IsNotExist(err) {
			// create secret key and store it in the secret key file
			var f *os.File
			f, err = os.OpenFile(passwordStoreKeyFile, os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer f.Close()

			timeStr := time.Now().GoString()
			salt, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
			if err != nil {
				return err
			}
			keyStr := timeStr + salt.String()
			hash := md5.Sum([]byte(keyStr))
			secretKey := hex.EncodeToString(hash[:]) // secret key length: 32 byte
			_, err = f.Write([]byte(encodeBase64([]byte(secretKey))))
			if err != nil {
				return err
			}
		}
	}

	for instance := range instanceActive {
		if _, exists := instancePassword[instance]; !exists {
			fmt.Printf("Enter password for instance %s: ", instance)
			password, _ := term.ReadPassword(0)
			fmt.Printf("\n")
			instancePassword[instance] = string(password)
		}
	}

	err = writePasswordStore(os.O_WRONLY | os.O_CREATE | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// deleteInstance deletes a dedicated entry from the global instance password map.
func deleteInstance(instance string) error {
	var err error
	if _, isValid := instancePassword[instance]; !isValid {
		err = errors.New(mm007)
		return err
	}
	delete(instancePassword, instance)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// addInstance adds a dedicated entry in the global instance password map.
func addInstance(instance string) error {
	var err error
	if _, isValid := instancePassword[instance]; isValid {
		err = errors.New(mm008)
		return err
	}

	var password []byte
	fmt.Printf("Enter password for instance %s: ", instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	instancePassword[instance] = string(password)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// updateInstance updates a dedicated entry in the global instance password map.
func updateInstance(instance string) error {
	var err error
	if _, isValid := instancePassword[instance]; !isValid {
		err = errors.New(mm007)
		return err
	}

	var password []byte
	fmt.Printf("Enter new password for instance %s: ", instance)
	password, _ = term.ReadPassword(0)
	fmt.Printf("\n")
	instancePassword[instance] = string(password)

	err = writePasswordStore(os.O_WRONLY | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// showInstance lists all instances (without the password) which were found in the global instance password map.
func showInstance() {
	for k := range instancePassword {
		simplelog.Write(simplelog.STDOUT, k)
	}
}

// syncPWS synchronizes the password store and the config file.
// All password store entries, which don't have a corresponding config file instance entry, will be deleted.
// For all active config file instances, which don't have an entry in the password store yet, their corresponding password will be added to the password store.
func syncPWS() {
	for instance := range instancePassword {
		if _, exists := instanceConfig[instance]; !exists {
			simplelog.Write(simplelog.STDOUT, formatMsg(mm018, instance))
			deleteInstance(instance)
		}
	}
	for instance := range instanceActive {
		if _, exists := instancePassword[instance]; !exists {
			addInstance(instance)
		}
	}
}
