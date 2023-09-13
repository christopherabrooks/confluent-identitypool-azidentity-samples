package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// command-line parameters
var bootstrapServer string
var clusterId string
var topic string
var identityPool string
var tenantId string
var clientId string
var scope string

func init() {
	//map each of the command-line params to variables
	flag.StringVar(&bootstrapServer, "bootstrapserver", "", "Host/port pair to use for establishing the initial connection to the Kafka cluster")
	flag.StringVar(&clusterId, "clusterid", "", "Id of the target cluster")
	flag.StringVar(&topic, "topic", "", "Topic to which to produce messages")
	flag.StringVar(&identityPool, "identitypool", "", "Identity Pool to act as when calling Confluent Cloud")
	flag.StringVar(&tenantId, "tenantid", "", "Id of the Azure AD Tenant to use for authentication")
	flag.StringVar(&clientId, "clientid", "", "Application ID of the Azure AD client application")
	flag.StringVar(&scope, "scope", "", "Scope value representing the requested permissions on the target Azure AD resource application")
}

func validateParameters() {
	if bootstrapServer == "" || clusterId == "" || topic == "" || identityPool == "" || tenantId == "" || clientId == "" || scope == "" {
		fmt.Fprintf(os.Stderr, "Missing required parameters.\n\nUsage:\n")

		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "bootstrapserver", flag.Lookup("bootstrapserver").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "clusterid", flag.Lookup("clusterid").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "topic", flag.Lookup("topic").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "identitypool", flag.Lookup("identitypool").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "tenantid", flag.Lookup("tenantid").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "clientid", flag.Lookup("clientid").Usage)
		fmt.Fprintf(os.Stderr, "  -%s string\n        %s\n", "scope", flag.Lookup("scope").Usage)

		os.Exit(1)
	}
}

func initKafkaConfig() kafka.ConfigMap {
	cm := make(map[string]kafka.ConfigValue)

	cm["security.protocol"] = "SASL_SSL"
	cm["sasl.mechanisms"] = "OAUTHBEARER"
	cm["acks"] = "all"

	cm["bootstrap.servers"] = bootstrapServer

	return cm
}

func handleOAuthBearerTokenRefreshEvent(client kafka.Handle, cred *azidentity.InteractiveBrowserCredential, scope string, logicalCluster string, identityPoolId string) {
	fmt.Fprintf(os.Stderr, "Handling token refresh event\n")
	oauthBearerToken, retrieveErr := retrieveToken(cred, scope, logicalCluster, identityPoolId)
	if retrieveErr != nil {
		fmt.Fprintf(os.Stderr, "%% Token retrieval error: %v\n", retrieveErr)
		client.SetOAuthBearerTokenFailure(retrieveErr.Error())
	} else {
		setTokenError := client.SetOAuthBearerToken(oauthBearerToken)
		if setTokenError != nil {
			fmt.Fprintf(os.Stderr, "%% Error setting token and extensions: %v\n", setTokenError)
			client.SetOAuthBearerTokenFailure(setTokenError.Error())
		} else {
			fmt.Fprintf(os.Stderr, "Fresh token successfully retrieved\n")
		}
	}
}

func retrieveToken(cred *azidentity.InteractiveBrowserCredential, scope string, logicalCluster string, identityPoolId string) (kafka.OAuthBearerToken, error) {

	//get a token using this cred for the requested target application/scope (this should be standard for Confluent Cloud)
	azureADToken, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{scope}})
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to retrieve token %s", err)
	}

	//package the token for use by kafka
	oauthBearerToken := kafka.OAuthBearerToken{
		TokenValue: azureADToken.Token,                                                                    //pass the access token provided by Azure AD
		Expiration: azureADToken.ExpiresOn,                                                                //pass the expiration of token provided by Azure AD
		Extensions: map[string]string{"logicalCluster": logicalCluster, "identityPoolId": identityPoolId}, // pass required context for token, including target cluster and identity pool to use
	}

	return oauthBearerToken, nil
}

func main() {
	flag.Parse()
	validateParameters()

	//initialize producer
	config := initKafkaConfig()
	p, err := kafka.NewProducer(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	//initialize a credential
	cred, err := azidentity.NewInteractiveBrowserCredential(&azidentity.InteractiveBrowserCredentialOptions{
		TenantID: tenantId,
		ClientID: clientId,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing credential: %s\n", err)
		os.Exit(1)
	}

	//Retrieve initial access token to make sure we can sign-in the user
	fmt.Printf("\nSigning in the user in a browser window...\n")

	azureADToken, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{scope}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to authenticate user: %s\n", err)
		os.Exit(1)
	}

	//display the received access token for demo purposes
	fmt.Printf("\nAccess Token returned (user successfully signed in):\n%s\n\n", azureADToken.Token)

	// Token refresh events are posted on the Events channel, instructing
	// the application to refresh its token.

	// define a goroutine background thread to watch for
	// token refresh events and dispatch them to our event handler,
	// and then attach it to the producers events channel
	go func(eventsChan chan kafka.Event) {
		for ev := range eventsChan {
			switch ev.(type) {
			case kafka.OAuthBearerTokenRefresh:
				handleOAuthBearerTokenRefreshEvent(p, cred, scope, clusterId, identityPool)
			default:
				continue
			}
		}
	}(p.Events())

	// route OS termination events to a channel we can monitor
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	// keep producting messages until we get teh signame to stop (ctrl-C)
	run := true
	for run {
		select {
		case sig := <-signalChannel:
			fmt.Printf("\nCaught signal %v: terminating\n", sig)
			run = false
		default:
			value := time.Now().String()
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte("timestamp"),
				Value:          []byte(value),
			}, nil)

			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrQueueFull {
					// Producer queue is full, wait 1s for messages
					// to be delivered then try again.
					time.Sleep(time.Second)
					continue
				}
				fmt.Printf("Failed to produce message: %v\n", err)
			} else {
				fmt.Printf("Produced message: %s\n", value)
			}

			time.Sleep(1 * time.Second)
		}
	}

	p.Close()
}
