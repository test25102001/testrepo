package main

import (
	"fmt"
	"log"
	"math"
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	//part        = 1
	//BUCKET      = "prefectresults"
	//	Merge_Array []*s3.UploadPartCopyOutput
	client *s3.S3
)

const (
	Threshold = (200 * 1024 * 1024)
	Bucket    = "prefectresults"
)

type object struct {
	key         string
	start_bytes int64
	end_bytes   int64
}

var (
	metadata     []object
	part         = 1
	Merged_Array []*s3.UploadPartCopyOutput
	k            = 1
	ptr1Array    []*s3.CompletedPart
)

func copy(uploadid string) {
	part = 1
	Merged_Array = nil

	t := strconv.Itoa(k)
	for _, met := range metadata {

		//iterate in metadata array and create parts
		to_copy := "bytes=" + strconv.Itoa(int(met.start_bytes)-1) + "-" + strconv.Itoa(int(met.end_bytes-1))
		foo, err := client.UploadPartCopy(&s3.UploadPartCopyInput{
			Bucket:          aws.String(Bucket),
			CopySource:      aws.String(url.QueryEscape(Bucket + "/" + met.key)),
			CopySourceRange: aws.String(to_copy),
			PartNumber:      aws.Int64(int64(part)),
			Key:             aws.String("newkey" + t),
			UploadId:        &uploadid,
		})

		if err != nil {
			log.Println(Bucket + "/balraj/output/" + met.key)
			log.Println(err, "copy upload")
		}
		Merged_Array = append(Merged_Array, foo)
		part++

	}
	//log.Println("merged array", Merged_Array)
	for i, cp := range Merged_Array {
		temp := s3.CompletedPart{
			ETag:       cp.CopyPartResult.ETag,
			PartNumber: aws.Int64(int64(i + 1)),
		}
		ptr1Array = append(ptr1Array, &temp)
	}

	complete_upload(uploadid, ptr1Array)

}

func create_multi_upload() *s3.CreateMultipartUploadOutput {
	t := strconv.Itoa(k)
	output, err := client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(Bucket),
		// change key everytime this function called
		Key: aws.String("newkey" + t),
	})

	if err != nil {
		panic(err.Error())
	}

	return output

}

func complete_upload(UploadId string, parts []*s3.CompletedPart) {
	//log.Println("parts", parts)
	t := strconv.Itoa(k)
	final, err := client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(Bucket),
		Key:      aws.String("newkey" + t),
		UploadId: aws.String(UploadId),

		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		log.Print(final)
		log.Print(err)
	}
	log.Printf("uploaded key: %v", *final.Key)
	ptr1Array = nil

	k++

}

func upload_small_parts(m object) {

	log.Printf("found part less than 5 mb at index")
	//initiate this separate part to upload
	t := strconv.Itoa(k)
	output := create_multi_upload()
	to_copy := "bytes=" + strconv.Itoa(int(m.start_bytes)-1) + "-" + strconv.Itoa(int(m.end_bytes-1))
	foo, err := client.UploadPartCopy(&s3.UploadPartCopyInput{
		Bucket:          aws.String(Bucket),
		CopySource:      aws.String(url.QueryEscape(Bucket + "/" + m.key)),
		CopySourceRange: aws.String(to_copy),
		PartNumber:      aws.Int64(1),
		Key:             aws.String("newkey" + t),
		UploadId:        output.UploadId,
	})

	if err != nil {
		log.Println(Bucket + "/balraj/output/" + m.key)
		log.Println(err, "copy upload")
	}
	temp := s3.CompletedPart{
		ETag:       foo.CopyPartResult.ETag,
		PartNumber: aws.Int64(1),
	}
	complete_upload(*output.UploadId, []*s3.CompletedPart{&temp})
	//after uploading remove the part from metadata array

	fmt.Println("after removing small parts", metadata)

}

func Merge() {
	FileSize := 0
	list, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(Bucket),
		Prefix: aws.String("balraj/output/"),
	})

	times := 0

	for _, obj := range list.Contents {
		log.Println("taking...")
		log.Println(*obj.Size, *obj.Key)
		if int(*obj.Size)+FileSize < Threshold {
			temp := object{
				key:         *obj.Key,
				start_bytes: 1,
				end_bytes:   *obj.Size,
			}
			metadata = append(metadata, temp)
			log.Println("added .....", temp)
			FileSize += int(*obj.Size)
			log.Println("****** -- ", FileSize)

			if FileSize == Threshold {
				log.Println("threshold reached....so initiating copy")
				FileSize = 0
				output := create_multi_upload()
				copy(*output.UploadId)

				log.Println(metadata)
				//resetting the array
				metadata = nil
			}
		} else {

			log.Println("else exec")
			remaining := *obj.Size
			prevend := 0

			end := 0
			taken := 0 // 50 60 ----- 100

			for {

				maxAllowed := Threshold - FileSize
				log.Println("Maxallowed is", maxAllowed)

				if remaining > int64(maxAllowed) {
					end = prevend + maxAllowed

				} else {
					end = int(*obj.Size)

				}
				temp := object{
					key:         *obj.Key,
					start_bytes: int64(prevend + 1),
					end_bytes:   int64(end),
				}

				if temp.end_bytes-temp.start_bytes < (5 * 1024 * 1024) {

					upload_small_parts(temp)

				} else {
					metadata = append(metadata, temp)
				}

				log.Println("added .....", temp)

				taken = int(temp.end_bytes-temp.start_bytes) + 1
				FileSize += taken

				log.Println("****** -- ", FileSize)

				if FileSize == Threshold {

					log.Println("threshold reached....merged 200 mb")

					FileSize = 0
					//before uploading upload parts less than 5 mb

					output := create_multi_upload()

					log.Println("metadata ", metadata)
					copy(*output.UploadId)

					metadata = nil

					log.Println("resetting metadata ")
					log.Println("contens of metadata", metadata)
				}
				remaining = int64(math.Abs(float64(remaining) - float64(taken)))
				if remaining == 0 {
					log.Println("break")
					break
				}
				log.Println("remaining", remaining)

				//end = int(remaining)

				prevend = end
				log.Println("------")
				times++

				// if times > 5 {
				// 	return
				// }
			}

		}

		if err != nil {
			log.Println(err)
		}
		///at last iteration copy whatever that is in metadata..that will definitely will be under threshold
		// output := create_multi_upload()
		// copy(*output.UploadId)

	}

	log.Println("after for looop exec ", metadata)
}

func main() {

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("us-west-2"),
		},
		Profile:           "preprod",
		SharedConfigState: session.SharedConfigEnable,
	})
	client = s3.New(sess)

	if err != nil {
		log.Println(err)
	}
	Merge()
}


