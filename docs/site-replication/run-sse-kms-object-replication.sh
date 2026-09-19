#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	echo "silo1 ============"
	cat /tmp/silo1_1.log
	echo "silo2 ============"
	cat /tmp/silo2_1.log
	echo "silo3 ============"
	cat /tmp/silo3_1.log
	echo "silo4 ============"
	cat /tmp/silo4_1.log
	exit 1
}

cleanup() {
	echo -n "Cleaning up instances of Silo ..."
	pkill -9 silo || sudo pkill -9 silo
	pkill -9 kes || sudo pkill -9 kes
	rm -rf ${PWD}/keys
	rm -rf /tmp/silo{1,2,3,4}
	echo "done"
}

cleanup

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="silo12345"
TEST_MINIO_ENC_KEY="MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDA"

# Create certificates for TLS enabled Silo
echo -n "Setup certs for Silo instances ..."
wget -O certgen https://github.com/minio/certgen/releases/latest/download/certgen-linux-amd64 && chmod +x certgen
./certgen --host localhost
mkdir -p /tmp/certs
mv public.crt /tmp/certs || sudo mv public.crt /tmp/certs
mv private.key /tmp/certs || sudo mv private.key /tmp/certs
echo "done"

# Start Silo instances
echo -n "Starting Silo instances ..."
CI=on MINIO_KMS_SECRET_KEY=minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=silo12345 silo server --certs-dir /tmp/certs --address ":9001" --console-address ":10000" /tmp/silo1/{1...4}/disk{1...4} /tmp/silo1/{5...8}/disk{1...4} >/tmp/silo1_1.log 2>&1 &
CI=on MINIO_KMS_SECRET_KEY=minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=silo12345 silo server --certs-dir /tmp/certs --address ":9002" --console-address ":11000" /tmp/silo2/{1...4}/disk{1...4} /tmp/silo2/{5...8}/disk{1...4} >/tmp/silo2_1.log 2>&1 &
echo "done"

if [ ! -f ./mc ]; then
	echo -n "Downloading Silo client ..."
	"$(git rev-parse --show-toplevel)/buildscripts/install-mcli.sh" ./mc
	echo "done"
fi

export MC_HOST_silo1=https://minio:silo12345@localhost:9001
export MC_HOST_silo2=https://minio:silo12345@localhost:9002

./mc ready silo1 --insecure
./mc ready silo2 --insecure

# Prepare data for tests
echo -n "Preparing test data ..."
mkdir -p /tmp/data
echo "Hello from encrypted world" >/tmp/data/encrypted
touch /tmp/data/mpartobj
shred -s 500M /tmp/data/mpartobj
touch /tmp/data/defpartsize
shred -s 500M /tmp/data/defpartsize
touch /tmp/data/custpartsize
shred -s 500M /tmp/data/custpartsize
echo "done"

# Add replication site
./mc admin replicate add silo1 silo2 --insecure
# sleep for replication to complete
sleep 30

# Create bucket in source cluster
echo "Create bucket in source Silo instance"
./mc mb silo1/test-bucket --insecure

# Enable SSE KMS for the bucket
./mc encrypt set sse-kms minio-default-key silo1/test-bucket --insecure

# Load objects to source site
echo "Loading objects to source Silo instance"
./mc cp /tmp/data/encrypted silo1/test-bucket --insecure
./mc cp /tmp/data/mpartobj silo1/test-bucket/mpartobj --enc-c "silo1/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure
./mc cp /tmp/data/defpartsize silo1/test-bucket --insecure
./mc put /tmp/data/custpartsize silo1/test-bucket --insecure --part-size 50MiB
sleep 120

# List the objects from source site
echo "Objects from source instance"
./mc ls silo1/test-bucket --insecure
count1=$(./mc ls silo1/test-bucket/encrypted --insecure | wc -l)
if [ "${count1}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/encrypted not found"
	exit_1
fi
count2=$(./mc ls silo1/test-bucket/mpartobj --insecure | wc -l)
if [ "${count2}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/mpartobj not found"
	exit_1
fi
count3=$(./mc ls silo1/test-bucket/defpartsize --insecure | wc -l)
if [ "${count3}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/defpartsize not found"
	exit_1
fi
count4=$(./mc ls silo1/test-bucket/custpartsize --insecure | wc -l)
if [ "${count4}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/custpartsize not found"
	exit_1
fi

# List the objects from replicated site
echo "Objects from replicated instance"
./mc ls silo2/test-bucket --insecure
repcount1=$(./mc ls silo2/test-bucket/encrypted --insecure | wc -l)
if [ "${repcount1}" -ne 1 ]; then
	echo "BUG: object test-bucket/encrypted not replicated"
	exit_1
fi
repcount2=$(./mc ls silo2/test-bucket/mpartobj --insecure | wc -l)
if [ "${repcount2}" -ne 1 ]; then
	echo "BUG: object test-bucket/mpartobj not replicated"
	exit_1
fi
repcount3=$(./mc ls silo2/test-bucket/defpartsize --insecure | wc -l)
if [ "${repcount3}" -ne 1 ]; then
	echo "BUG: object test-bucket/defpartsize not replicated"
	exit_1
fi
repcount4=$(./mc ls silo2/test-bucket/custpartsize --insecure | wc -l)
if [ "${repcount4}" -ne 1 ]; then
	echo "BUG: object test-bucket/custpartsize not replicated"
	exit_1
fi

# Stat the objects from source site
echo "Stat silo1/test-bucket/encrypted"
./mc stat --no-list silo1/test-bucket/encrypted --insecure --json
stat_out1=$(./mc stat --no-list silo1/test-bucket/encrypted --insecure --json)
src_obj1_algo=$(echo "${stat_out1}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
src_obj1_keyid=$(echo "${stat_out1}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo1/test-bucket/defpartsize"
./mc stat --no-list silo1/test-bucket/defpartsize --insecure --json
stat_out2=$(./mc stat --no-list silo1/test-bucket/defpartsize --insecure --json)
src_obj2_algo=$(echo "${stat_out2}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
src_obj2_keyid=$(echo "${stat_out2}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo1/test-bucket/custpartsize"
./mc stat --no-list silo1/test-bucket/custpartsize --insecure --json
stat_out3=$(./mc stat --no-list silo1/test-bucket/custpartsize --insecure --json)
src_obj3_algo=$(echo "${stat_out3}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
src_obj3_keyid=$(echo "${stat_out3}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo1/test-bucket/mpartobj"
./mc stat --no-list silo1/test-bucket/mpartobj --enc-c "silo1/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out4=$(./mc stat --no-list silo1/test-bucket/mpartobj --enc-c "silo1/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj4_etag=$(echo "${stat_out4}" | jq '.etag')
src_obj4_size=$(echo "${stat_out4}" | jq '.size')
src_obj4_md5=$(echo "${stat_out4}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Stat the objects from replicated site
echo "Stat silo2/test-bucket/encrypted"
./mc stat --no-list silo2/test-bucket/encrypted --insecure --json
stat_out1_rep=$(./mc stat --no-list silo2/test-bucket/encrypted --insecure --json)
rep_obj1_algo=$(echo "${stat_out1_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
rep_obj1_keyid=$(echo "${stat_out1_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo2/test-bucket/defpartsize"
./mc stat --no-list silo2/test-bucket/defpartsize --insecure --json
stat_out2_rep=$(./mc stat --no-list silo2/test-bucket/defpartsize --insecure --json)
rep_obj2_algo=$(echo "${stat_out2_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
rep_obj2_keyid=$(echo "${stat_out2_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo2/test-bucket/custpartsize"
./mc stat --no-list silo2/test-bucket/custpartsize --insecure --json
stat_out3_rep=$(./mc stat --no-list silo2/test-bucket/custpartsize --insecure --json)
rep_obj3_algo=$(echo "${stat_out3_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption"')
rep_obj3_keyid=$(echo "${stat_out3_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"')
echo "Stat silo2/test-bucket/mpartobj"
./mc stat --no-list silo2/test-bucket/mpartobj --enc-c "silo2/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out4_rep=$(./mc stat --no-list silo2/test-bucket/mpartobj --enc-c "silo2/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj4_etag=$(echo "${stat_out4}" | jq '.etag')
rep_obj4_size=$(echo "${stat_out4}" | jq '.size')
rep_obj4_md5=$(echo "${stat_out4}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Check the algo and keyId of replicated objects
if [ "${rep_obj1_algo}" != "${src_obj1_algo}" ]; then
	echo "BUG: Algorithm: '${rep_obj1_algo}' of replicated object: 'silo2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_algo}'"
	exit_1
fi
if [ "${rep_obj1_keyid}" != "${src_obj1_keyid}" ]; then
	echo "BUG: KeyId: '${rep_obj1_keyid}' of replicated object: 'silo2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_keyid}'"
	exit_1
fi
if [ "${rep_obj2_algo}" != "${src_obj2_algo}" ]; then
	echo "BUG: Algorithm: '${rep_obj2_algo}' of replicated object: 'silo2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_algo}'"
	exit_1
fi
if [ "${rep_obj2_keyid}" != "${src_obj2_keyid}" ]; then
	echo "BUG: KeyId: '${rep_obj2_keyid}' of replicated object: 'silo2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_keyid}'"
	exit_1
fi
if [ "${rep_obj3_algo}" != "${src_obj3_algo}" ]; then
	echo "BUG: Algorithm: '${rep_obj3_algo}' of replicated object: 'silo2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_algo}'"
	exit_1
fi
if [ "${rep_obj3_keyid}" != "${src_obj3_keyid}" ]; then
	echo "BUG: KeyId: '${rep_obj3_keyid}' of replicated object: 'silo2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_keyid}'"
	exit_1
fi

# Check the etag, size and md5 of replicated SSEC object
if [ "${rep_obj4_etag}" != "${src_obj4_etag}" ]; then
	echo "BUG: Etag: '${rep_obj4_etag}' of replicated object: 'silo2/test-bucket/mpartobj' doesn't match with source value: '${src_obj4_etag}'"
	exit_1
fi
if [ "${rep_obj4_size}" != "${src_obj4_size}" ]; then
	echo "BUG: Size: '${rep_obj4_size}' of replicated object: 'silo2/test-bucket/mpartobj' doesn't match with source value: '${src_obj4_size}'"
	exit_1
fi
if [ "${src_obj4_md5}" != "${rep_obj4_md5}" ]; then
	echo "BUG: MD5 checksum of object 'silo2/test-bucket/mpartobj' doesn't match with source. Expected: '${src_obj4_md5}', Found: '${rep_obj4_md5}'"
	exit_1
fi

# Check content of replicated objects
./mc cat silo2/test-bucket/encrypted --insecure
./mc cat silo2/test-bucket/mpartobj --enc-c "silo2/test-bucket/mpartobj=${TEST_MINIO_ENC_KEY}" --insecure >/dev/null || exit_1
./mc cat silo2/test-bucket/defpartsize --insecure >/dev/null || exit_1
./mc cat silo2/test-bucket/custpartsize --insecure >/dev/null || exit_1

echo -n "Starting Silo instances with different kms key ..."
CI=on MINIO_KMS_SECRET_KEY=silo3-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=silo12345 silo server --certs-dir /tmp/certs --address ":9003" --console-address ":10000" /tmp/silo3/disk{1...4} >/tmp/silo3_1.log 2>&1 &
CI=on MINIO_KMS_SECRET_KEY=silo4-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=silo12345 silo server --certs-dir /tmp/certs --address ":9004" --console-address ":11000" /tmp/silo4/disk{1...4} >/tmp/silo4_1.log 2>&1 &
echo "done"

export MC_HOST_silo3=https://minio:silo12345@localhost:9003
export MC_HOST_silo4=https://minio:silo12345@localhost:9004

./mc ready silo3 --insecure
./mc ready silo4 --insecure

./mc admin replicate add silo3 silo4 --insecure
./mc mb silo3/bucket --insecure
./mc cp --insecure --enc-kms silo3/bucket=silo3-default-key /tmp/data/encrypted silo3/bucket/x
sleep 10
st=$(./mc stat --json --no-list --insecure silo3/bucket/x | jq -r .replicationStatus)
if [ "${st}" != "FAILED" ]; then
	echo "BUG: Replication succeeded when kms key is different"
	exit_1
fi

cleanup
