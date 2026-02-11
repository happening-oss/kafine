SHELL := bash

EXTERNAL_LISTENER_ADDR=$(shell ../get-host-ip.sh)
export EXTERNAL_LISTENER_ADDR

brokers := \
	kafka-default-101 kafka-default-102 kafka-default-103 \
	kafka-default-104 kafka-default-105 kafka-default-106

all: \
	kafka-root.crt \
	$(foreach broker,$(brokers),$(broker).keystore.jks) \
	$(foreach broker,$(brokers),$(broker).truststore.jks) \
	kafka-client.crt

# It NEEDS a password, so we'll just use a fixed one.
PASSWORD := password

.DELETE_ON_ERROR:

kafka-root.key:
	openssl genrsa -out kafka-root.key 4096

kafka-root.crt: kafka-root.key
	openssl req -new -x509 -sha256 -subj '/CN=kafka-root' -days 180 -key kafka-root.key -out kafka-root.crt

kafka-client.key:
	openssl genrsa -out kafka-client.key 2048

kafka-client.csr: kafka-client.key
	openssl req -new \
		-key "kafka-client.key" \
		-subj "/CN=kafka-client" \
		-out "kafka-client.csr"

kafka-client.crt: kafka-client.csr kafka-root.key kafka-root.crt
	openssl x509 -req \
		-CA kafka-root.crt -CAkey kafka-root.key \
		-days 90 \
		-in "kafka-client.csr" \
		-out "kafka-client.crt"

%.keystore.jks: kafka-root.crt kafka-root.key
	# create private key
	keytool -genkey \
		-noprompt \
		-alias "$*" \
		-dname "CN=$*" \
		-keystore "$*.keystore.jks" \
		-keyalg RSA \
		-storepass "$(PASSWORD)" \
		-keypass "$(PASSWORD)"

	# create signing request
	keytool -keystore "$*.keystore.jks" \
		-alias "$*" \
		-certreq -file "$*.csr" \
		-storepass "$(PASSWORD)" \
		-keypass "$(PASSWORD)"

	# sign the request; create server cert
	openssl x509 -req \
		-CA kafka-root.crt -CAkey kafka-root.key \
		-CAcreateserial \
		-days 90 \
		-in "$*.csr" \
		-extfile <(echo "subjectAltName=DNS:$*,DNS:localhost,IP:$(EXTERNAL_LISTENER_ADDR)") \
		-out "$*.crt"

	# put the certificates in the keystore
	keytool -keystore "$*.keystore.jks" \
		-noprompt \
		-storepass "$(PASSWORD)" -keypass "$(PASSWORD)" \
		-alias CARoot -import -file "kafka-root.crt"

	keytool -keystore "$*.keystore.jks" \
		-noprompt \
		-storepass "$(PASSWORD)" -keypass "$(PASSWORD)" \
		-alias "$*" -import -file "$*.crt"

	echo "$(PASSWORD)" > "$*_sslkey_creds"
	echo "$(PASSWORD)" > "$*_keystore_creds"

%.truststore.jks: kafka-root.crt kafka-root.key
	keytool -keystore $*.truststore.jks \
		-storepass "$(PASSWORD)" -keypass "$(PASSWORD)" -noprompt \
		-alias CARoot -import -file kafka-root.crt

	echo "$(PASSWORD)" > "$*_truststore_creds"

clean:
	-rm *.key *.crt *.jks *.csr *_creds
