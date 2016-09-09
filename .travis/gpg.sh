#!/usr/bin/env bash

set -e

export GPG_PASSPHRASE=$(echo "$RANDOM$(date)" | md5sum | cut -d\  -f1)

cat >gen-key-script <<EOF
      Key-Type: RSA
      Key-Length: 4096
      Subkey-Type: 1
      Subkey-Length: 4096
      Name-Real: test
      Name-Email: test@foo.bar
      Expire-Date: 2y
      Passphrase: ${GPG_PASSPHRASE}
EOF

set -x

key=$(gpg --batch --gen-key gen-key-script 2>&1 | tail -n1 | cut -d\  -f3)
shred gen-key-script
gpg --send-keys ${key}
while(true); do
    date
    gpg --recv-keys ${key} && break || sleep 30
done

if [ -f "/.dockerenv" ]; then
    shred -v ~/.gnupg/*
    rm -rf ~/.gnupg
fi