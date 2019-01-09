package simplekinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func New(region string) *kinesis.Kinesis {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return kinesis.New(sess)
}

func NewWithEndpoint(region, endpoint string) *kinesis.Kinesis {
	customResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.KinesisServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           endpoint,
				SigningRegion: region,
			}, nil
		}

		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(region),
		EndpointResolver: endpoints.ResolverFunc(customResolver),
	}))
	return kinesis.New(sess)
}
