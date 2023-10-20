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
	instancePassword     = make(map[string]string) // map to store instances and their password
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

// initPWS initializes the password store based on the configured and activated instances found in the config file.
// The following key/value pair per instance section will be stored AES encrypted: <predefined dbms>.<instance ID>:<password>
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

	for i := range instanceToConfig {
		fmt.Printf("Enter password for instance %s: ", i)
		password, _ := term.ReadPassword(0)
		fmt.Printf("\n")
		instancePassword[i] = string(password)
	}

	err = writePasswordStore(os.O_WRONLY | os.O_CREATE | os.O_TRUNC)
	if err != nil {
		return err
	}

	return err
}

// deleteInstance deletes a dedicated entry from the global instance password map.
func deleteInstance(instance string) error {
	err := readPasswordStore()
	if err != nil {
		return err
	}
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
	err := readPasswordStore()
	if err != nil {
		return err
	}
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
	err := readPasswordStore()
	if err != nil {
		return err
	}
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
func showInstance() error {
	err := readPasswordStore()
	if err != nil {
		return err
	}

	for k := range instancePassword {
		simplelog.Write(simplelog.STDOUT, k)
	}

	return err
}
