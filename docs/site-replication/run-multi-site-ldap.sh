#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	echo "silo1 ============"
	cat /tmp/silo1_1.log
	cat /tmp/silo1_2.log
	echo "silo2 ============"
	cat /tmp/silo2_1.log
	cat /tmp/silo2_2.log
	echo "silo3 ============"
	cat /tmp/silo3_1.log
	cat /tmp/silo3_2.log

	exit 1
}

cleanup() {
	echo "Cleaning up instances of Silo"
	pkill silo
	pkill -9 silo
	rm -rf /tmp/silo-ldap-idp{1,2,3}
}

cleanup

unset MINIO_KMS_KES_CERT_FILE
unset MINIO_KMS_KES_KEY_FILE
unset MINIO_KMS_KES_ENDPOINT
unset MINIO_KMS_KES_KEY_NAME

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="silo12345"
export MINIO_KMS_AUTO_ENCRYPTION=off
export MINIO_PROMETHEUS_AUTH_TYPE=public
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
export MINIO_IDENTITY_LDAP_SERVER_ADDR="localhost:389"
export MINIO_IDENTITY_LDAP_SERVER_INSECURE="on"
export MINIO_IDENTITY_LDAP_LOOKUP_BIND_DN="cn=admin,dc=min,dc=io"
export MINIO_IDENTITY_LDAP_LOOKUP_BIND_PASSWORD="admin"
export MINIO_IDENTITY_LDAP_USER_DN_SEARCH_BASE_DN="dc=min,dc=io"
export MINIO_IDENTITY_LDAP_USER_DN_SEARCH_FILTER="(uid=%s)"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN="ou=swengg,dc=min,dc=io"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER="(&(objectclass=groupOfNames)(member=%d))"

if [ ! -f ./mc ]; then
	"$(git rev-parse --show-toplevel)/buildscripts/install-mcli.sh" ./mc
fi

silo server --config-dir /tmp/silo-ldap --address ":9001" /tmp/silo-ldap-idp1/{1...4} >/tmp/silo1_1.log 2>&1 &
site1_pid=$!
silo server --config-dir /tmp/silo-ldap --address ":9002" /tmp/silo-ldap-idp2/{1...4} >/tmp/silo2_1.log 2>&1 &
site2_pid=$!
silo server --config-dir /tmp/silo-ldap --address ":9003" /tmp/silo-ldap-idp3/{1...4} >/tmp/silo3_1.log 2>&1 &
site3_pid=$!

export MC_HOST_silo1=http://minio:silo12345@localhost:9001
export MC_HOST_silo2=http://minio:silo12345@localhost:9002
export MC_HOST_silo3=http://minio:silo12345@localhost:9003

./mc ready silo1
./mc ready silo2
./mc ready silo3

./mc admin replicate add silo1 silo2 silo3

./mc idp ldap policy attach silo1 consoleAdmin --user="uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
sleep 5

./mc admin user info silo2 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
./mc admin user info silo3 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
./mc admin policy create silo1 rw ./docs/site-replication/rw.json

sleep 5
./mc admin policy info silo2 rw >/dev/null 2>&1
./mc admin policy info silo3 rw >/dev/null 2>&1

./mc admin policy remove silo3 rw

sleep 10
./mc admin policy info silo1 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin policy info silo2 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin user info silo1 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

./mc admin user info silo2 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

./mc admin user info silo3 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

# LDAP simple user
./mc admin user svcacct add silo2 dillon --access-key testsvc --secret-key testsvc123
if [ $? -ne 0 ]; then
	echo "adding svc account failed, exiting.."
	exit_1
fi

sleep 10

./mc idp ldap policy entities silo1
./mc idp ldap policy entities silo2
./mc idp ldap policy entities silo3

./mc admin service restart silo1
./mc admin service restart silo2
./mc admin service restart silo3

sleep 10

./mc idp ldap policy entities silo1
./mc idp ldap policy entities silo2
./mc idp ldap policy entities silo3

./mc admin user svcacct info silo1 testsvc
if [ $? -ne 0 ]; then
	echo "svc account not mirrored, exiting.."
	exit_1
fi

./mc admin user svcacct info silo2 testsvc
if [ $? -ne 0 ]; then
	echo "svc account not mirrored, exiting.."
	exit_1
fi

./mc admin user svcacct info silo3 testsvc
if [ $? -ne 0 ]; then
	echo "svc account not mirrored, exiting.."
	exit_1
fi

MC_HOST_svc1=http://testsvc:testsvc123@localhost:9001 ./mc ls svc1
MC_HOST_svc2=http://testsvc:testsvc123@localhost:9002 ./mc ls svc2
MC_HOST_svc3=http://testsvc:testsvc123@localhost:9003 ./mc ls svc3

./mc admin user svcacct rm silo1 testsvc
if [ $? -ne 0 ]; then
	echo "removing svc account failed, exiting.."
	exit_1
fi

./mc admin user info silo1 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

./mc admin user info silo2 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

./mc admin user info silo3 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
if [ $? -ne 0 ]; then
	echo "policy mapping missing, exiting.."
	exit_1
fi

sleep 10

./mc admin user svcacct info silo2 testsvc
if [ $? -eq 0 ]; then
	echo "svc account found after delete, exiting.."
	exit_1
fi

./mc admin user svcacct info silo3 testsvc
if [ $? -eq 0 ]; then
	echo "svc account found after delete, exiting.."
	exit_1
fi

./mc mb silo1/newbucket
# copy large upload to newbucket on silo1
truncate -s 17M lrgfile
expected_checksum=$(cat ./lrgfile | md5sum)

./mc cp ./lrgfile silo1/newbucket

# create a bucket bucket2 on silo1.
./mc mb silo1/bucket2

sleep 5
./mc stat --no-list silo2/newbucket
if [ $? -ne 0 ]; then
	echo "expecting bucket to be present. exiting.."
	exit_1
fi

./mc stat --no-list silo3/newbucket
if [ $? -ne 0 ]; then
	echo "expecting bucket to be present. exiting.."
	exit_1
fi

./mc cp README.md silo2/newbucket/

sleep 5
./mc stat --no-list silo1/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

./mc stat --no-list silo3/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

sleep 10
./mc stat --no-list silo3/newbucket/lrgfile
if [ $? -ne 0 ]; then
	echo "expected object to be present, exiting.."
	exit_1
fi
actual_checksum=$(./mc cat silo3/newbucket/lrgfile | md5sum)
if [ "${expected_checksum}" != "${actual_checksum}" ]; then
	echo "replication failed on multipart objects expected ${expected_checksum} got ${actual_checksum}"
	exit
fi
rm ./lrgfile

./mc rm -r --versions --force silo1/newbucket/lrgfile
if [ $? -ne 0 ]; then
	echo "expected object to be present, exiting.."
	exit_1
fi

sleep 5
./mc stat --no-list silo1/newbucket/lrgfile
if [ $? -eq 0 ]; then
	echo "expected object to be deleted permanently after replication, exiting.."
	exit_1
fi

vID=$(./mc stat --no-list silo2/newbucket/README.md --json | jq .versionID)
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi
./mc tag set --version-id "${vID}" silo2/newbucket/README.md "key=val"
if [ $? -ne 0 ]; then
	echo "expecting tag set to be successful. exiting.."
	exit_1
fi
sleep 5

./mc tag remove --version-id "${vID}" silo2/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting tag removal to be successful. exiting.."
	exit_1
fi
sleep 5

replStatus_silo2=$(./mc stat --no-list silo2/newbucket/README.md --json | jq -r .replicationStatus)
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

if [ ${replStatus_silo2} != "COMPLETED" ]; then
	echo "expected tag removal to have replicated, exiting..."
	exit_1
fi

./mc rm silo3/newbucket/README.md
sleep 5

./mc stat --no-list silo2/newbucket/README.md
if [ $? -eq 0 ]; then
	echo "expected file to be deleted, exiting.."
	exit_1
fi

./mc stat --no-list silo1/newbucket/README.md
if [ $? -eq 0 ]; then
	echo "expected file to be deleted, exiting.."
	exit_1
fi

./mc mb --with-lock silo3/newbucket-olock
sleep 5

enabled_silo2=$(./mc stat --json silo2/newbucket-olock | jq -r .ObjectLock.enabled)
if [ $? -ne 0 ]; then
	echo "expected bucket to be mirrored with object-lock but not present, exiting..."
	exit_1
fi

if [ "${enabled_silo2}" != "Enabled" ]; then
	echo "expected bucket to be mirrored with object-lock enabled, exiting..."
	exit_1
fi

enabled_silo1=$(./mc stat --json silo1/newbucket-olock | jq -r .ObjectLock.enabled)
if [ $? -ne 0 ]; then
	echo "expected bucket to be mirrored with object-lock but not present, exiting..."
	exit_1
fi

if [ "${enabled_silo1}" != "Enabled" ]; then
	echo "expected bucket to be mirrored with object-lock enabled, exiting..."
	exit_1
fi

# "Test if most recent tag update is replicated"
./mc tag set silo2/newbucket "key=val1"
if [ $? -ne 0 ]; then
	echo "expecting tag set to be successful. exiting.."
	exit_1
fi

sleep 10

val=$(./mc tag list silo1/newbucket --json | jq -r .tagset | jq -r .key)
if [ "${val}" != "val1" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi
# stop silo1
kill -9 ${site1_pid}
# Update tag on silo2/newbucket when silo1 is down
./mc tag set silo2/newbucket "key=val2"
# create a new bucket on silo2. This should replicate to silo1 after it comes online.
./mc mb silo2/newbucket2
# delete bucket2 on silo2. This should replicate to silo1 after it comes online.
./mc rb silo2/bucket2

# Restart silo1 instance
silo server --config-dir /tmp/silo-ldap --address ":9001" /tmp/silo-ldap-idp1/{1...4} >/tmp/silo1_1.log 2>&1 &
sleep 200

# Test whether most recent tag update on silo2 is replicated to silo1
val=$(./mc tag list silo1/newbucket --json | jq -r .tagset | jq -r .key)
if [ "${val}" != "val2" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi

# Test if bucket created/deleted when silo1 is down healed
diff -q <(./mc ls silo1) <(./mc ls silo2) 1>/dev/null
if [ $? -ne 0 ]; then
	echo "expected 'bucket2' delete and 'newbucket2' creation to have replicated, exiting..."
	exit_1
fi

# force a resync after removing all site replication
./mc admin replicate rm --all --force silo1
./mc rb silo2 --force --dangerous
./mc admin replicate add silo1 silo2
./mc admin replicate resync start silo1 silo2
sleep 30

./mc ls -r --versions silo1/newbucket >/tmp/silo1.txt
./mc ls -r --versions silo2/newbucket >/tmp/silo2.txt

out=$(diff -qpruN /tmp/silo1.txt /tmp/silo2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after replication resync: $out"
	exit 1
fi

cleanup
