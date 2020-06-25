package amqprpc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTLS(t *testing.T) {
	tempCert, tempKey := createCertificateFiles()

	defer func() {
		os.Remove(tempCert)
		os.Remove(tempKey)
	}()

	c := Certificates{tempCert, tempKey, tempCert}

	assert.Contains(t, c.Cert, ".pem", "certificate defined")

	cfg := c.TLSConfig()

	require.NotNil(t, cfg, "config is not nil")
	assert.NotNil(t, cfg.RootCAs, "root CAs exist")
	assert.Equal(t, 1, len(cfg.Certificates), "one certificate parsed")

	c = Certificates{}

	cfg = c.TLSConfig()

	assert.NotNil(t, cfg, "successfully generate TLSConfig")
	assert.Equal(t, 0, len(cfg.Certificates), "no certificates for empty config")
}

func createPrivKey(priv *rsa.PrivateKey) string {
	f, _ := ioutil.TempFile(".", "priv*.key")
	defer f.Close()

	var privateKey = &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	}

	_ = pem.Encode(f, privateKey)

	return f.Name()
}

func createCertificate(priv *rsa.PrivateKey, pub rsa.PublicKey) string {
	f, _ := ioutil.TempFile(".", "pub*.pem")
	defer f.Close()

	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1653),
		Subject: pkix.Name{
			Organization: []string{"UNIT-TESTER"},
			Country:      []string{"SE"},
			Locality:     []string{"Stockholm"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	certificate, _ := x509.CreateCertificate(rand.Reader, cert, cert, &pub, priv)

	certFile := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certificate,
	}

	_ = pem.Encode(f, certFile)

	return f.Name()
}

func createCertificateFiles() (tempCert, tempKey string) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	tempKey = createPrivKey(key)
	tempCert = createCertificate(key, key.PublicKey)

	return tempCert, tempKey
}
