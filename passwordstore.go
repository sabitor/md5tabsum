package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/sabitor/simplelog"
	"golang.org/x/term"
)

var (
	passwordStoreFile string
	instancePassword  = make(map[string]string) // map to store instances and their password
)

// writePasswordStore writes the encrypted instance:password data for each configured DBMS instance to the password store.
func writePasswordStore(flags int) error {
	f, err := os.OpenFile(passwordStoreFile, flags, 0600)
	if err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, err.Error())
		return err
	}
	defer f.Close()

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
		record, err := decryptAES(secretKey, encryptedRecord)
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
