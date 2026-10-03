package kafka

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/IBM/sarama"
	signer "github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/prakash/kafbat-ui/internal/models"
	"github.com/xdg-go/scram"
)

type staticCredProvider struct {
	key, secret, token string
}

func (s staticCredProvider) Retrieve(_ context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     s.key,
		SecretAccessKey: s.secret,
		SessionToken:    s.token,
		Source:          "clusters.json",
	}, nil
}

type mskTokenProvider struct {
	region          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

func (p *mskTokenProvider) Token() (*sarama.AccessToken, error) {
	var (
		token string
		err   error
	)

	if p.accessKeyID != "" {
		token, _, err = signer.GenerateAuthTokenFromCredentialsProvider(
			context.Background(),
			p.region,
			staticCredProvider{
				key:    p.accessKeyID,
				secret: p.secretAccessKey,
				token:  p.sessionToken,
			},
		)
	} else {
		token, _, err = signer.GenerateAuthToken(context.Background(), p.region)
	}

	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}

// scramClient wraps xdg-go/scram so sarama can use it.
type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	hashGen scram.HashGeneratorFcn
}

func (c *scramClient) Begin(user, pass, authzID string) error {
	client, err := c.hashGen.NewClient(user, pass, authzID)
	if err != nil {
		return err
	}
	c.Client = client
	c.ClientConversation = c.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}

// normalizeAuthType maps user-friendly aliases to canonical auth types.
func normalizeAuthType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "", "none", "plaintext":
		return "none"
	case "iam", "msk_iam", "aws_msk_iam":
		return "iam"
	case "sasl_scram", "scram", "scram_sha512", "scram-sha-512":
		return "sasl_scram"
	case "sasl_plain", "plain":
		return "sasl_plain"
	default:
		return strings.ToLower(t)
	}
}

func buildSaramaConfig(auth *models.AuthConfig) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Return.Errors = true

	if auth == nil {
		return cfg
	}

	switch normalizeAuthType(auth.Type) {
	case "iam":
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		cfg.Net.SASL.TokenProvider = &mskTokenProvider{
			region:          auth.Region,
			accessKeyID:     auth.AccessKeyID,
			secretAccessKey: auth.SecretAccessKey,
			sessionToken:    auth.SessionToken,
		}

	case "sasl_scram":
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.User = auth.Username
		cfg.Net.SASL.Password = auth.Password
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{hashGen: scram.SHA512}
		}

	case "sasl_plain":
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = auth.Username
		cfg.Net.SASL.Password = auth.Password

	case "none":
		if auth.TLS {
			cfg.Net.TLS.Enable = true
			cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}

	return cfg
}
