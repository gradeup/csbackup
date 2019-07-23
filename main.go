package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	awsProfile := flag.String("awsProfile", "default", "a string")
	region := flag.String("region", "us-east-1", "a string")
	bucket := flag.String("bucket", "laptop-db", "a string")
	// host := flag.String("host", "127.0.0.1", "a string")
	// username := flag.String("username", "cassandra", "a string")
	// password := flag.String("password", "cassandra", "a string")
	keyspace := flag.String("keyspace", "", "a string")
	incremental := flag.Bool("incremental", false, "a bool")
	cassandraDataDir := flag.String("cassandraDataDir", "/var/lib/cassandra/data", "a string")

	flag.Parse()

	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(*region)},
		Profile: *awsProfile,
	})
	if err != nil {
		fmt.Print(err.Error())
	}
	svcS3 := s3.New(sess)
	uploader := s3manager.NewUploaderWithClient(svcS3, func(u *s3manager.Uploader) {
		u.MaxUploadParts = 10000       // set to maximum allowed by s3
		u.PartSize = 128 * 1024 * 1024 // 128MB
	})

	fmt.Print("AWS Session created")

	if *incremental == true {
		resp, err := IncrementalSnapshot(svcS3, *cassandraDataDir, *bucket, uploader, *keyspace)
		if err != nil {
			fmt.Print(err.Error())
		}
		fmt.Print(resp)
	} else {
		resp, err := FullSnapshot(svcS3, *cassandraDataDir, *bucket, uploader, *keyspace)
		if err != nil {
			fmt.Print(err.Error())
		}
		fmt.Print(resp)
	}

}

func IncrementalSnapshot(svcS3 *s3.S3, cassandraDataDir string, bucket string, uploader *s3manager.Uploader, keyspace string) (string, error) {
	resp, err := GetBackupFilesForUpload(cassandraDataDir, keyspace)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	_, err = S3UploadFiles(svcS3, cassandraDataDir, resp, bucket, uploader)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	err = ClearBackups(resp)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return "", nil
}

func FullSnapshot(svcS3 *s3.S3, cassandraDataDir string, bucket string, uploader *s3manager.Uploader, keyspace string) (string, error) {
	tag := strconv.FormatInt(time.Now().Unix(), 10)
	var command string
	if keyspace != "" {
		command = "nodetool snapshot -t " + tag + " " + keyspace
	} else {
		command = "nodetool snapshot -t " + tag
	}
	cmd := exec.Command("/bin/sh", "-c", command)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		os.Exit(1)
	}
	fmt.Println("Result: " + out.String())

	resp, err := GetSnapshotFilesForUpload(cassandraDataDir, tag)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	_, err = S3UploadFiles(svcS3, cassandraDataDir, resp, bucket, uploader)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	err = ClearSnapshots(tag)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return tag, nil
}

func ClearSnapshots(tag string) error {
	command := "nodetool clearsnapshot -t " + tag
	cmd := exec.Command("/bin/sh", "-c", command)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		os.Exit(1)
	}
	fmt.Println("Result: " + out.String())
	return nil
}

func ClearBackups(files []string) error {
	for _, file := range files {
		err := os.RemoveAll(file)
		if err != nil {
			fmt.Print(err.Error())
		}
	}
	return nil
}

func GetSnapshotFilesForUpload(cassandraDataDir string, tag string) ([]string, error) {
	paths := []string{}

	keyspaces, err := ioutil.ReadDir(cassandraDataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, keyspace := range keyspaces {
		tables, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name())
		if err != nil {
			log.Fatal(err)
		}
		for _, table := range tables {
			snapshots, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/snapshots")
			if err != nil {
				continue
				log.Fatal(err)
			}
			for _, snapshot := range snapshots {
				if snapshot.Name() == tag {
					files, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/" + "snapshots/" + snapshot.Name())
					if err != nil {
						log.Fatal(err)
					}
					for _, file := range files {
						paths = append(paths, cassandraDataDir+"/"+keyspace.Name()+"/"+table.Name()+"/"+"snapshots/"+snapshot.Name()+"/"+file.Name())
					}
				}
			}
		}
	}

	return paths, nil
}

func GetBackupFilesForUpload(cassandraDataDir string, keyspace_input string) ([]string, error) {
	paths := []string{}

	keyspaces, err := ioutil.ReadDir(cassandraDataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, keyspace := range keyspaces {
		if keyspace_input == "" || keyspace_input == keyspace.Name() {
			tables, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name())
			if err != nil {
				log.Fatal(err)
			}
			for _, table := range tables {
				files, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/backups")
				if err != nil {
					continue
					log.Fatal(err)
				}
				for _, file := range files {
					paths = append(paths, cassandraDataDir+"/"+keyspace.Name()+"/"+table.Name()+"/"+"backups/"+file.Name())
				}
			}
		}
	}

	return paths, nil
}

func S3UploadFiles(svcS3 *s3.S3, cassandraDataDir string, files []string, bucket string, uploader *s3manager.Uploader) (string, error) {
	for _, file := range files {
		err := S3UploadFile(svcS3, file, bucket, uploader)
		if err != nil {
			return "", err
		}
	}
	return "", nil
}

func S3UploadFile(svcS3 *s3.S3, file string, bucket string, uploader *s3manager.Uploader) error {
	t := time.Now()
	fmt.Printf("Uploading %s", file)
	// read bytes from file@host
	r, err := os.Open(file)
	if err != nil {
		return err
	}

	// gzip files before uploading
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		io.Copy(gw, r)
		gw.Close()
		writer.Close()
	}()

	pos := strings.LastIndex(file, "/data") + 5
	key := file[pos:]

	// details of file to upload
	params := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Body:   reader,
		Key:    aws.String(t.Format("2006/01/02/") + key),
	}

	// upload file
	_, err = uploader.Upload(params)
	if err != nil {
		return err
	}
	return nil
}
