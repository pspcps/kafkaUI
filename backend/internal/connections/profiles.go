// Package connections loads operator-owned connection profiles. Requests select IDs,
// never secret paths, arbitrary Vault URLs, cloud keys, or database passwords.
package connections

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
)

var Identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type Profile struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	DestinationType     string `json:"destinationType"`
	Provider            string `json:"provider"`
	SecretName          string `json:"secretName,omitempty"`
	SecretProviderClass string `json:"secretProviderClass,omitempty"`
	CASecretName        string `json:"caSecretName,omitempty"`
	ServiceAccount      string `json:"serviceAccount"`
	Host                string `json:"host,omitempty"`
	Port                int    `json:"port,omitempty"`
	Database            string `json:"database,omitempty"`
	Username            string `json:"username,omitempty"`
	Region              string `json:"region,omitempty"`
}

type View struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	DestinationType string `json:"destinationType"`
	Provider        string `json:"provider"`
}

func (p Profile) View() View { return View{p.ID, p.Name, p.DestinationType, p.Provider} }
func (p Profile) Validate() error {
	if p.ID == "" || p.Name == "" || p.ServiceAccount == "" {
		return errors.New("profile id, name and dedicated serviceAccount are required")
	}
	switch p.DestinationType {
	case "postgresql", "mysql", "mongodb", "sqlserver":
	default:
		return errors.New("unsupported destination type")
	}
	if (p.DestinationType == "postgresql" || p.DestinationType == "mysql") && p.CASecretName == "" {
		return errors.New("verified TLS requires a CA secret for PostgreSQL/MySQL")
	}
	switch p.Provider {
	case "kubernetes_secret":
		if p.SecretName == "" {
			return errors.New("secretName required")
		}
	case "vault", "aws_secrets_manager":
		if p.SecretProviderClass == "" {
			return errors.New("operator-installed CSI SecretProviderClass required")
		}
	case "rds_iam":
		if p.DestinationType != "postgresql" && p.DestinationType != "mysql" {
			return errors.New("RDS IAM supports PostgreSQL/MySQL only")
		}
		if p.Host == "" || p.Port < 1 || p.Port > 65535 || p.Database == "" || p.Username == "" || p.Region == "" {
			return errors.New("RDS IAM endpoint, port, database, username and region required")
		}
	default:
		return errors.New("unsupported credential provider")
	}
	return nil
}

func Load() ([]Profile, error) {
	path := os.Getenv("CONNECTION_PROFILES_FILE")
	if path == "" {
		return []Profile{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("cannot read connection profiles")
	}
	var profiles []Profile
	if json.Unmarshal(raw, &profiles) != nil {
		return nil, errors.New("invalid connection profile JSON")
	}
	seen := map[string]bool{}
	for _, p := range profiles {
		if seen[p.ID] {
			return nil, errors.New("duplicate connection profile ID")
		}
		if err := p.Validate(); err != nil {
			return nil, err
		}
		seen[p.ID] = true
	}
	return profiles, nil
}

func Resolve(id, destinationType string) (*Profile, error) {
	profiles, err := Load()
	if err != nil {
		return nil, err
	}
	for _, p := range profiles {
		if p.ID == id && p.DestinationType == destinationType {
			return &p, nil
		}
	}
	return nil, errors.New("select an approved connection profile for this destination")
}
