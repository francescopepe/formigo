module github.com/francescopepe/formigo

go 1.21

require (
	github.com/aws/aws-sdk-go-v2/service/sqs v1.34.5
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.30.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.16 // indirect
	github.com/aws/smithy-go v1.20.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v1.0.0 // Accidentally added
	v1.0.1 // Contains retractions only.
)
