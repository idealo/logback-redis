#!/usr/bin/env bash

set -ex

export GPG_PASSPHRASE=$(echo "$RANDOM$(date)" | md5sum | cut -d\  -f1)

cat >gen-key-script <<EOF
      %echo Generating a basic OpenPGP key
      Key-Type: RSA
      Key-Length: 4096
      Subkey-Type: 1
      Subkey-Length: 4096
      Name-Real: test
      Name-Email: test@foo.bar
      Expire-Date: 2y
      Passphrase: ${GPG_PASSPHRASE}
      %commit
      %echo done
EOF

gpg --batch --gen-key gen-key-script 2>&1 | tail -n1 | cut -d\  -f3
gpg -K
export GPG_KEYNAME=$(gpg -K | grep ^sec | cut -d/  -f2 | cut -d\  -f1 | head -n1)
shred gen-key-script
gpg --send-keys ${GPG_KEYNAME}
while(true); do
    date
    gpg --recv-keys ${GPG_KEYNAME} && break || sleep 30
done
