package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/c2h5oh/datasize"
	"golang.org/x/term"
)

const (
	// 1 Mb
	minObjectSizeLimit int64 = 1024 * 1024
	// 400 Mb
	maxObjectSizeLimit int64 = 400 * 1024 * 1024
)

vars (
	bucketName          string
	bucketPrefix        string
	filter              string
	minSizeStr          string
	minSize             int64
	maxSizeStr          string
	maxSize             int64
	printFullObjectPath bool
	delete              bool
)

type Color struct {
	R, G, B int
}

func (c Color) String() string {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm", c.R, c.G, c.B)
}

func main() {
	flag.StringVar(&bucketName, "bucket", "", "S3 bucket name")
	flag.StringVar(&bucketPrefix, "prefix", "", "S3 objects prefix")
	flag.StringVar(&filter, "filter", "", "Filter object key")
	flag.StringVar(&filter, "f", "", "Filter object key")
	flag.StringVar(&minSizeStr, "minsize", "", "Minimum object size")
	flag.StringVar(&maxSizeStr, "maxsize", "", "Maximum object size")
	flag.BoolVar(&printFullObjectPath, "full", false, "Print the full object path")
	flag.BoolVar(&delete, "delete", false, "Delete all objects in the bucket")

	flag.Parse()

	if bucketName == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if maxSizeStr != "" {
		maxSize = int64(datasize.MustParseString(maxSizeStr).Bytes())
	}

	if minSizeStr != "" {
		minSize = int64(datasize.MustParseString(minSizeStr).Bytes())
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalln("error:", err)
	}

	client := s3.NewFromConfig(cfg)

	response, err := client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.Fatalln("Failed to get bucket location, ", err)
	}
	cfg.Region = string(response.LocationConstraint)
	client = s3.NewFromConfig(cfg)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &bucketPrefix,
	})

	white := Color{255, 255, 255}
	darkRed := Color{220, 0, 0}
	isTerm := term.IsTerminal(int(os.Stdout.Fd()))

	var totalDeleteSize int64 = 0
	var totalObjectsDeleted int = 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Fatalln("error:", err)
		}
		if delete {
			var objects []types.ObjectIdentifier
			for _, object := range page.Contents {
				objects = append(objects, types.ObjectIdentifier{Key: aws.String(*object.Key)})
				totalDeleteSize += *object.Size
				totalObjectsDeleted += 1
			}
			client.DeleteObjects(
				context.Background(),
				&s3.DeleteObjectsInput{
					Bucket: &bucketName,
					Delete: &types.Delete{
						Objects: objects,
						Quiet:   aws.Bool(true),
					},
				},
			)
			fmt.Printf("\033[2K\rDeleted %d objects / %s", totalObjectsDeleted, byteCountIEC(totalDeleteSize))
			continue
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			size := *obj.Size

			if !strings.Contains(key, filter) {
				continue
			}
			if (minSize != 0 && size < minSize) || (maxSize != 0 && size > maxSize) {
				continue
			}
			if printFullObjectPath {
				key = fmt.Sprintf("s3://%s/%s", bucketName, key)
			}

			if isTerm {
				var color Color
				switch {
				case size <= minObjectSizeLimit:
					color = white
				case size >= maxObjectSizeLimit:
					color = white
				default:
					factor := float64(size) / float64(maxObjectSizeLimit)
					color = interpolateColor(factor, white, darkRed)
				}
				fmt.Print(color)
			}
			fmt.Printf("%9s ", byteCountIEC(size))
			fmt.Printf("%s %s %s", obj.LastModified.Format(time.DateTime), obj.StorageClass, key)
			if isTerm {
				// Reset colors
				fmt.Print("\033[0m")
			}
			fmt.Println()
		}
	}
}

func interpolateColor(factor float64, c1, c2 Color) Color {
	return Color{
		R: int(float64(c1.R)*(1-factor) + float64(c2.R)*factor),
		G: int(float64(c1.G)*(1-factor) + float64(c2.G)*factor),
		B: int(float64(c1.B)*(1-factor) + float64(c2.B)*factor),
	}
}

func byteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
