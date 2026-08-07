package main

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func generateInstallationToken(cfg Config, installationID int64) (string, error) {
	if cfg.GitHubAppID == "" {
		return "", errors.New("missing GITHUB_APP_ID")
	}

	privateKey, err := parseGitHubPrivateKey(cfg)
	if err != nil {
		return "", err
	}

	jwt, err := createGitHubAppJWT(cfg.GitHubAppID, privateKey)
	if err != nil {
		return "", fmt.Errorf("create GitHub app jwt: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("https://api.github.com/app/installations/%d/access_tokens", installationID), bytes.NewBufferString("{}"))
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+jwt)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("request installation token: %w", err)
	}
	defer response.Body.Close()

	body, _ := io.ReadAll(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("request installation token: github returned status %d", response.StatusCode)
	}

	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decode installation token: %w", err)
	}
	if payload.Token == "" {
		return "", errors.New("github installation token response was empty")
	}
	return payload.Token, nil
}

func parseGitHubPrivateKey(cfg Config) (*rsa.PrivateKey, error) {
	keyMaterial := cfg.GitHubPrivateKey
	if keyMaterial == "" && cfg.GitHubPrivateKeyB64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(cfg.GitHubPrivateKeyB64))
		if err != nil {
			return nil, fmt.Errorf("decode base64 github private key: %w", err)
		}
		keyMaterial = string(decoded)
	}
	if strings.TrimSpace(keyMaterial) == "" {
		return nil, errors.New("missing GitHub private key")
	}

	keyMaterial = strings.ReplaceAll(keyMaterial, "\\n", "\n")
	block, _ := pem.Decode([]byte(keyMaterial))
	if block == nil {
		return nil, errors.New("invalid GitHub private key PEM")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("GitHub private key is not RSA")
	}
	return rsaKey, nil
}

func createGitHubAppJWT(appID string, privateKey *rsa.PrivateKey) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	now := time.Now().Unix()
	claims, err := json.Marshal(map[string]any{
		"iat": now - 30,
		"exp": now + 540,
		"iss": appID,
	})
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload
	hash := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hash[:])
	if err != nil {
		return "", err
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
