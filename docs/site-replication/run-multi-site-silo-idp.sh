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
	rm -rf /tmp/silo-internal-idp{1,2,3}
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

if [ ! -f ./mc ]; then
	"$(git rev-parse --show-toplevel)/buildscripts/install-mcli.sh" ./mc
fi

silo server --config-dir /tmp/silo-internal --address ":9001" http://localhost:9001/tmp/silo-internal-idp1/{1...4} http://localhost:9010/tmp/silo-internal-idp1/{5...8} >/tmp/silo1_1.log 2>&1 &
site1_pid1=$!
silo server --config-dir /tmp/silo-internal --address ":9010" http://localhost:9001/tmp/silo-internal-idp1/{1...4} http://localhost:9010/tmp/silo-internal-idp1/{5...8} >/tmp/silo1_2.log 2>&1 &
site1_pid2=$!

silo server --config-dir /tmp/silo-internal --address ":9002" http://localhost:9002/tmp/silo-internal-idp2/{1...4} http://localhost:9020/tmp/silo-internal-idp2/{5...8} >/tmp/silo2_1.log 2>&1 &
site2_pid1=$!
silo server --config-dir /tmp/silo-internal --address ":9020" http://localhost:9002/tmp/silo-internal-idp2/{1...4} http://localhost:9020/tmp/silo-internal-idp2/{5...8} >/tmp/silo2_2.log 2>&1 &
site2_pid2=$!

silo server --config-dir /tmp/silo-internal --address ":9003" http://localhost:9003/tmp/silo-internal-idp3/{1...4} http://localhost:9030/tmp/silo-internal-idp3/{5...8} >/tmp/silo3_1.log 2>&1 &
site3_pid1=$!
silo server --config-dir /tmp/silo-internal --address ":9030" http://localhost:9003/tmp/silo-internal-idp3/{1...4} http://localhost:9030/tmp/silo-internal-idp3/{5...8} >/tmp/silo3_2.log 2>&1 &
site3_pid2=$!

export MC_HOST_silo1=http://minio:silo12345@localhost:9001
export MC_HOST_silo2=http://minio:silo12345@localhost:9002
export MC_HOST_silo3=http://minio:silo12345@localhost:9003

export MC_HOST_silo10=http://minio:silo12345@localhost:9010
export MC_HOST_silo20=http://minio:silo12345@localhost:9020
export MC_HOST_silo30=http://minio:silo12345@localhost:9030

./mc ready silo1
./mc ready silo2
./mc ready silo3
./mc ready silo10
./mc ready silo20
./mc ready silo30

./mc admin replicate add silo1 silo2

site_enabled=$(./mc admin replicate info silo1)
site_enabled_peer=$(./mc admin replicate info silo10)

[[ $site_enabled =~ "is not enabled" ]] && {
	echo "expected both peers to have same information"
	exit_1
}

[[ $site_enabled_peer =~ "is not enabled" ]] && {
	echo "expected both peers to have same information"
	exit_1
}

./mc admin user add silo1 foobar foo12345

## add foobar-g group with foobar
./mc admin group add silo2 foobar-g foobar

./mc admin policy attach silo1 consoleAdmin --user=foobar
sleep 5

./mc admin user info silo2 foobar

./mc admin group info silo1 foobar-g

./mc admin policy create silo1 rw ./docs/site-replication/rw.json

sleep 5
./mc admin policy info silo2 rw >/dev/null 2>&1

./mc admin replicate status silo1

## Add a new empty site
./mc admin replicate add silo1 silo2 silo3

sleep 10

./mc admin policy info silo3 rw >/dev/null 2>&1

./mc admin policy remove silo3 rw

./mc admin replicate status silo3

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

./mc admin policy info silo3 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin user info silo1 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'silo1', exiting.."
	exit_1
fi

./mc admin user info silo2 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'silo2', exiting.."
	exit_1
fi

./mc admin user info silo3 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'silo3', exiting.."
	exit_1
fi

./mc admin group info silo3 foobar-g
if [ $? -ne 0 ]; then
	echo "group mapping missing on 'silo3', exiting.."
	exit_1
fi

./mc admin user svcacct add silo2 foobar --access-key testsvc --secret-key testsvc123
if [ $? -ne 0 ]; then
	echo "adding svc account failed, exiting.."
	exit_1
fi

./mc admin user svcacct add silo2 minio --access-key testsvc2 --secret-key testsvc123
if [ $? -ne 0 ]; then
	echo "adding root svc account testsvc2 failed, exiting.."
	exit_1
fi

sleep 10

export MC_HOST_rootsvc=http://testsvc2:testsvc123@localhost:9002
./mc ls rootsvc
if [ $? -ne 0 ]; then
	echo "root service account not inherited root permissions, exiting.."
	exit_1
fi

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

./mc admin user svcacct rm silo1 testsvc
if [ $? -ne 0 ]; then
	echo "removing svc account failed, exiting.."
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

err_silo2=$(./mc stat --no-list silo2/newbucket/xxx --json | jq -r .error.cause.message)
if [ $? -ne 0 ]; then
	echo "expecting object to be missing. exiting.."
	exit_1
fi

if [ "${err_silo2}" != "Object does not exist" ]; then
	echo "expected to see Object does not exist error, exiting..."
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
val=$(./mc tag list silo1/newbucket/README.md --version-id "${vID}" --json | jq -r .tagset.key)
if [ "${val}" != "val" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi
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

set -x

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

set +x

# "Test if most recent tag update is replicated"
./mc tag set silo2/newbucket "key=val1"
if [ $? -ne 0 ]; then
	echo "expecting tag set to be successful. exiting.."
	exit_1
fi
sleep 5

val=$(./mc tag list silo1/newbucket --json | jq -r .tagset | jq -r .key)
if [ "${val}" != "val1" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi
# Create user with policy consoleAdmin on silo1
./mc admin user add silo1 foobarx foobar123
if [ $? -ne 0 ]; then
	echo "adding user failed, exiting.."
	exit_1
fi
./mc admin policy attach silo1 consoleAdmin --user=foobarx
if [ $? -ne 0 ]; then
	echo "adding policy mapping failed, exiting.."
	exit_1
fi
sleep 10

# unset policy for foobarx in silo2
./mc admin policy detach silo2 consoleAdmin --user=foobarx
if [ $? -ne 0 ]; then
	echo "unset policy mapping failed, exiting.."
	exit_1
fi

# create a bucket bucket2 on silo1.
./mc mb silo1/bucket2

sleep 10

# Test whether policy detach replicated to silo1
policy=$(./mc admin user info silo1 foobarx --json | jq -r .policyName)
if [ "${policy}" != "null" ]; then
	echo "expected policy detach to have replicated, exiting..."
	exit_1
fi

kill -9 ${site1_pid1} ${site1_pid2}

# Update tag on silo2/newbucket when silo1 is down
./mc tag set silo2/newbucket "key=val2"
# create a new bucket on silo2. This should replicate to silo1 after it comes online.
./mc mb silo2/newbucket2

# delete bucket2 on silo2. This should replicate to silo1 after it comes online.
./mc rb silo2/bucket2

# Restart silo1 instance
silo server --config-dir /tmp/silo-internal --address ":9001" http://localhost:9001/tmp/silo-internal-idp1/{1...4} http://localhost:9010/tmp/silo-internal-idp1/{5...8} >/tmp/silo1_1.log 2>&1 &
silo server --config-dir /tmp/silo-internal --address ":9010" http://localhost:9001/tmp/silo-internal-idp1/{1...4} http://localhost:9010/tmp/silo-internal-idp1/{5...8} >/tmp/silo1_2.log 2>&1 &
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
