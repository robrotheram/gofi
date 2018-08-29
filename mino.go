package main

import "fmt"

func createBuket() {
	err := minioClient.MakeBucket(Settings.BucketName, MINO_BUCKET_LOCATION)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := minioClient.BucketExists(Settings.BucketName)
		if err == nil && exists {
			Logger.Info("We already own %s\n", Settings.BucketName)
		} else {
			Logger.Error(err)
		}
	}

	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Principal": {"AWS": ["*"]},"Resource": ["arn:aws:s3:::` + Settings.BucketName + `/*"],"Sid": ""}]}`

	err = minioClient.SetBucketPolicy(Settings.BucketName, policy)
	if err != nil {
		fmt.Println(err)
		return
	}

	Logger.Info("Successfully created %s\n", Settings.BucketName)
}
