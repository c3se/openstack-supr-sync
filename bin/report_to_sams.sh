#!/usr/bin/env bash
CERTPREFIX=/opt/sams/certificates/cirrus-s1.c3se.chalmers.se-2026-04-09-160449

find /var/spool/openstack-supr-sync -type f -name "*compute*.xml" -exec bash -c 'curl -d @"$1" -X POST --cert "$2".pem --key "$2".key https://accounting.naiss.se:6143/sgas/cr && rm "$1"' _ {} $CERTPREFIX \;

find /var/spool/openstack-supr-sync -type f -name "*storage*.xml" -exec bash -c 'curl -d @"$1" -X POST --cert "$2".pem --key "$2".key https://accounting.naiss.se:6143/sgas/sr && rm "$1"' _ {} $CERTPREFIX \;

