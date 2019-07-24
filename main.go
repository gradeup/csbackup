package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
	awsProfile := flag.String("awsProfile", "default", "AWS profile to use for uploading/downloading to/from S3")
	region := flag.String("region", "us-east-1", "AWS region to use")
	bucket := flag.String("bucket", "laptop-db", "AWS S3 Bucket name to use for uploading/downloading Snapshots/Backups")
	command := flag.String("command", "backup", "backup/restore")
	days := flag.Int("days", 0, "days from now to restore, required only for restore, default 0 days")
	host := flag.String("host", "127.0.0.1", "Cassandra host IP")
	username := flag.String("username", "cassandra", "Cassandra username")
	password := flag.String("password", "cassandra", "Cassandra Password")
	keyspace := flag.String("keyspace", "", "Keyspace to backup/restore, leave empty for all keyspaces")
	incremental := flag.Bool("incremental", false, "a bool indicating if incremental backup needs to be taken or snapshot")
	cassandraDataDir := flag.String("cassandraDataDir", "/var/lib/cassandra/data", "a path indicating actual data directory for cassandra installation")
	restoreDataDir := flag.String("restoreDataDir", "/tmp/data", "a temporary path to download restorable snapshots")

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
	downloader := s3manager.NewDownloaderWithClient(svcS3, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
	})

	fmt.Println("AWS Session created")

	if *command == "backup" {
		if *incremental == true {
			resp, err := IncrementalSnapshot(svcS3, *cassandraDataDir, *bucket, uploader, *keyspace)
			if err != nil {
				fmt.Print(err.Error())
			}
			fmt.Print(resp)
		} else {
			resp, err := FullSnapshot(svcS3, *cassandraDataDir, *bucket, uploader, *keyspace, *host, *username, *password)
			if err != nil {
				fmt.Print(err.Error())
			}
			fmt.Print(resp)
		}
	} else {
		resp, err := RestoreByDays(svcS3, *restoreDataDir, *bucket, downloader, *keyspace, *days)
		if err != nil {
			fmt.Print(err.Error())
		}
		fmt.Print(resp)
		fmt.Print("Downloaded Snapshot. To restore move the files to /tmp/data/keyspace_name/table_name/mc-xxxx-data.DB and run sstableloader -d 0.0.0.0 /tmp/data/keyspace_name/table_name")
	}

}

// RestoreByDays downloads the snapshots and backups by keyspace or all keyspaces
func RestoreByDays(svcS3 *s3.S3, cassandraDataDir string, bucket string, downloader *s3manager.Downloader, keyspace string, days int) (string, error) {
	now := time.Now()
	t := now.AddDate(0, 0, -days)
	prefix := t.Format("2006/01/02/")
	fmt.Println("Downloading for " + prefix)
	listObjectsInput := s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	}
	keys, err := svcS3.ListObjects(&listObjectsInput)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// objects := []s3manager.BatchDownloadObject{}
	for _, key := range (*keys).Contents {
		localKey := (*key.Key)[11:]
		if keyspace == "" || strings.Index(localKey, keyspace) == 0 {
			fmt.Println("Downloading " + *key.Key)
			localDir := cassandraDataDir + "/" + localKey
			localDirPos := strings.LastIndex(localDir, "/")
			localDir = localDir[:localDirPos]
			fmt.Println(localDir)
			err := os.MkdirAll(localDir, 0777)
			if err != nil {
				return "", err
			}

			fileName := cassandraDataDir + "/" + localKey

			params := &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    key.Key,
			}
			resp, err := svcS3.GetObject(params)
			if err != nil {
				return "", err
			}

			file, err := os.Create(fileName)
			if err != nil {
				return "", err
			}

			reader, writer := io.Pipe()
			go func() {
				gr, err := gzip.NewReader(resp.Body)
				if err != nil {
					fmt.Print(err.Error())
					return
				}
				io.Copy(writer, gr)
				gr.Close()
				writer.Close()
			}()

			buf := make([]byte, 1024)
			for {
				n, err := reader.Read(buf)
				if err != nil && err != io.EOF {
					return "", err
				}
				if n == 0 {
					break
				}
				if _, writeErr := file.Write(buf[:n]); writeErr != nil {
					return "", err
				}
			}

			if err = reader.Close(); err != nil {
				return "", err
			}
			if err = file.Close(); err != nil {
				return "", err
			}
		}
	}

	return "", nil
}

// IncrementalSnapshot takes the backup by keyspace or all keyspaces
func IncrementalSnapshot(svcS3 *s3.S3, cassandraDataDir string, bucket string, uploader *s3manager.Uploader, keyspace string) (string, error) {
	resp, err := GetBackupFilesForUpload(cassandraDataDir, keyspace)
	if err != nil {
		return "", err
	}

	_, err = S3UploadFiles(svcS3, cassandraDataDir, resp, bucket, uploader)
	if err != nil {
		return "", err
	}

	err = ClearBackups(resp)
	if err != nil {
		return "", err
	}

	return "", nil
}

// FullSnapshot takes the snapshot by keysapce or all keyspaces
func FullSnapshot(svcS3 *s3.S3, cassandraDataDir string, bucket string, uploader *s3manager.Uploader, keyspace string, host string, username string, password string) (string, error) {
	tag := strconv.FormatInt(time.Now().Unix(), 10)
	var command string
	if keyspace != "" {
		command = "nodetool -u " + username + " -pw " + password + " -h " + host + " snapshot -t " + tag + " " + keyspace
	} else {
		command = "nodetool -u " + username + " -pw " + password + " -h " + host + " snapshot -t " + tag
	}
	cmd := exec.Command("/bin/sh", "-c", command)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	fmt.Println(command)
	if err != nil {
		return "", err
	}
	fmt.Println("Result: " + out.String())

	resp, err := GetSnapshotFilesForUpload(cassandraDataDir, tag)
	if err != nil {
		return "", err
	}

	_, err = S3UploadFiles(svcS3, cassandraDataDir, resp, bucket, uploader)
	if err != nil {
		return "", err
	}

	err = ClearSnapshots(tag, host, username, password)
	if err != nil {
		return "", err
	}

	return tag, nil
}

// ClearSnapshots deletes the snapshot by the tag
func ClearSnapshots(tag string, host string, username string, password string) error {
	command := "nodetool clearsnapshot -u " + username + " -pw " + password + " -h " + host + " -t " + tag
	cmd := exec.Command("/bin/sh", "-c", command)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Println("Result: " + out.String())
	return nil
}

// ClearBackups deletes the files uploaded from Backups directory
func ClearBackups(files []string) error {
	for _, file := range files {
		err := os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSnapshotFilesForUpload gets the list of files to be uploaded from the cassandra snapshot directory
func GetSnapshotFilesForUpload(cassandraDataDir string, tag string) ([]string, error) {
	paths := []string{}

	keyspaces, err := ioutil.ReadDir(cassandraDataDir)
	if err != nil {
		return nil, err
	}
	for _, keyspace := range keyspaces {
		tables, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name())
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			snapshots, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/snapshots")
			if err != nil {
				continue
			}
			for _, snapshot := range snapshots {
				if snapshot.Name() == tag {
					files, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/" + "snapshots/" + snapshot.Name())
					if err != nil {
						return nil, err
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

// GetBackupFilesForUpload Gets list of files to be uploaded from the cassandra backups directory
func GetBackupFilesForUpload(cassandraDataDir string, keyspaceInput string) ([]string, error) {
	paths := []string{}

	keyspaces, err := ioutil.ReadDir(cassandraDataDir)
	if err != nil {
		return nil, err
	}
	for _, keyspace := range keyspaces {
		if keyspaceInput == "" || keyspaceInput == keyspace.Name() {
			tables, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name())
			if err != nil {
				return nil, err
			}
			for _, table := range tables {
				files, err := ioutil.ReadDir(cassandraDataDir + "/" + keyspace.Name() + "/" + table.Name() + "/backups")
				if err != nil {
					continue
				}
				for _, file := range files {
					paths = append(paths, cassandraDataDir+"/"+keyspace.Name()+"/"+table.Name()+"/"+"backups/"+file.Name())
				}
			}
		}
	}

	return paths, nil
}

// S3UploadFiles calls S3UploadFile to upload all files
func S3UploadFiles(svcS3 *s3.S3, cassandraDataDir string, files []string, bucket string, uploader *s3manager.Uploader) (string, error) {
	for _, file := range files {
		err := S3UploadFile(svcS3, file, bucket, uploader)
		if err != nil {
			return "", err
		}
	}
	return "", nil
}

// S3UploadFile compresses file and uploads to the specified S3 bucket
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
