#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	echo "silo1 ============"
	cat /tmp/silo1_1.log
	echo "silo2 ============"
	cat /tmp/silo2_1.log

	exit 1
}

cleanup() {
	echo -n "Cleaning up instances of Silo ..."
	pkill silo || sudo pkill silo
	pkill -9 silo || sudo pkill -9 silo
	rm -rf /tmp/silo{1,2}
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
silo server --certs-dir /tmp/certs --address ":9001" --console-address ":10000" /tmp/silo1/{1...4}/disk{1...4} /tmp/silo1/{5...8}/disk{1...4} >/tmp/silo1_1.log 2>&1 &
silo server --certs-dir /tmp/certs --address ":9002" --console-address ":11000" /tmp/silo2/{1...4}/disk{1...4} /tmp/silo2/{5...8}/disk{1...4} >/tmp/silo2_1.log 2>&1 &
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
echo "Hello world" >/tmp/data/plainfile
echo "Hello from encrypted world" >/tmp/data/encrypted
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

# Load objects to source site
echo "Loading objects to source Silo instance"
set -x
./mc cp /tmp/data/plainfile silo1/test-bucket --insecure
./mc cp /tmp/data/encrypted silo1/test-bucket/encrypted --enc-c "silo1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure
./mc cp /tmp/data/defpartsize silo1/test-bucket/defpartsize --enc-c "silo1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure
./mc put /tmp/data/custpartsize silo1/test-bucket/custpartsize --enc-c "silo1/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure --part-size 50MiB
set +x
sleep 120

# List the objects from source site
echo "Objects from source instance"
./mc ls silo1/test-bucket --insecure
count1=$(./mc ls silo1/test-bucket/plainfile --insecure | wc -l)
if [ "${count1}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/plainfile not found"
	exit_1
fi
count2=$(./mc ls silo1/test-bucket/encrypted --insecure | wc -l)
if [ "${count2}" -ne 1 ]; then
	echo "BUG: object silo1/test-bucket/encrypted not found"
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
repcount1=$(./mc ls silo2/test-bucket/plainfile --insecure | wc -l)
if [ "${repcount1}" -ne 1 ]; then
	echo "BUG: object test-bucket/plainfile not replicated"
	exit_1
fi
repcount2=$(./mc ls silo2/test-bucket/encrypted --insecure | wc -l)
if [ "${repcount2}" -ne 1 ]; then
	echo "BUG: object test-bucket/encrypted not replicated"
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

# Stat the SSEC objects from source site
echo "Stat silo1/test-bucket/encrypted"
./mc stat --no-list silo1/test-bucket/encrypted --enc-c "silo1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out1=$(./mc stat --no-list silo1/test-bucket/encrypted --enc-c "silo1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj1_etag=$(echo "${stat_out1}" | jq '.etag')
src_obj1_size=$(echo "${stat_out1}" | jq '.size')
src_obj1_md5=$(echo "${stat_out1}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat silo1/test-bucket/defpartsize"
./mc stat --no-list silo1/test-bucket/defpartsize --enc-c "silo1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out2=$(./mc stat --no-list silo1/test-bucket/defpartsize --enc-c "silo1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj2_etag=$(echo "${stat_out2}" | jq '.etag')
src_obj2_size=$(echo "${stat_out2}" | jq '.size')
src_obj2_md5=$(echo "${stat_out2}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat silo1/test-bucket/custpartsize"
./mc stat --no-list silo1/test-bucket/custpartsize --enc-c "silo1/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out3=$(./mc stat --no-list silo1/test-bucket/custpartsize --enc-c "silo1/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj3_etag=$(echo "${stat_out3}" | jq '.etag')
src_obj3_size=$(echo "${stat_out3}" | jq '.size')
src_obj3_md5=$(echo "${stat_out3}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Stat the SSEC objects from replicated site
echo "Stat silo2/test-bucket/encrypted"
./mc stat --no-list silo2/test-bucket/encrypted --enc-c "silo2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out1_rep=$(./mc stat --no-list silo2/test-bucket/encrypted --enc-c "silo2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj1_etag=$(echo "${stat_out1_rep}" | jq '.etag')
rep_obj1_size=$(echo "${stat_out1_rep}" | jq '.size')
rep_obj1_md5=$(echo "${stat_out1_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat silo2/test-bucket/defpartsize"
./mc stat --no-list silo2/test-bucket/defpartsize --enc-c "silo2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out2_rep=$(./mc stat --no-list silo2/test-bucket/defpartsize --enc-c "silo2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj2_etag=$(echo "${stat_out2_rep}" | jq '.etag')
rep_obj2_size=$(echo "${stat_out2_rep}" | jq '.size')
rep_obj2_md5=$(echo "${stat_out2_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat silo2/test-bucket/custpartsize"
./mc stat --no-list silo2/test-bucket/custpartsize --enc-c "silo2/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out3_rep=$(./mc stat --no-list silo2/test-bucket/custpartsize --enc-c "silo2/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj3_etag=$(echo "${stat_out3_rep}" | jq '.etag')
rep_obj3_size=$(echo "${stat_out3_rep}" | jq '.size')
rep_obj3_md5=$(echo "${stat_out3_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Check the etag and size of replicated SSEC objects
if [ "${rep_obj1_etag}" != "${src_obj1_etag}" ]; then
	echo "BUG: Etag: '${rep_obj1_etag}' of replicated object: 'silo2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_etag}'"
	exit_1
fi
if [ "${rep_obj1_size}" != "${src_obj1_size}" ]; then
	echo "BUG: Size: '${rep_obj1_size}' of replicated object: 'silo2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_size}'"
	exit_1
fi
if [ "${rep_obj2_etag}" != "${src_obj2_etag}" ]; then
	echo "BUG: Etag: '${rep_obj2_etag}' of replicated object: 'silo2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_etag}'"
	exit_1
fi
if [ "${rep_obj2_size}" != "${src_obj2_size}" ]; then
	echo "BUG: Size: '${rep_obj2_size}' of replicated object: 'silo2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_size}'"
	exit_1
fi
if [ "${rep_obj3_etag}" != "${src_obj3_etag}" ]; then
	echo "BUG: Etag: '${rep_obj3_etag}' of replicated object: 'silo2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_etag}'"
	exit_1
fi
if [ "${rep_obj3_size}" != "${src_obj3_size}" ]; then
	echo "BUG: Size: '${rep_obj3_size}' of replicated object: 'silo2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_size}'"
	exit_1
fi

# Check content of replicated SSEC objects
./mc cat silo2/test-bucket/encrypted --enc-c "silo2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure
./mc cat silo2/test-bucket/defpartsize --enc-c "silo2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure >/dev/null || exit_1
./mc cat silo2/test-bucket/custpartsize --enc-c "silo2/test-bucket/custpartsize=${TEST_MINIO_ENC_KEY}" --insecure >/dev/null || exit_1

# Check the MD5 checksums of encrypted objects from source and target
if [ "${src_obj1_md5}" != "${rep_obj1_md5}" ]; then
	echo "BUG: MD5 checksum of object 'silo2/test-bucket/encrypted' doesn't match with source. Expected: '${src_obj1_md5}', Found: '${rep_obj1_md5}'"
	exit_1
fi
if [ "${src_obj2_md5}" != "${rep_obj2_md5}" ]; then
	echo "BUG: MD5 checksum of object 'silo2/test-bucket/defpartsize' doesn't match with source. Expected: '${src_obj2_md5}', Found: '${rep_obj2_md5}'"
	exit_1
fi
if [ "${src_obj3_md5}" != "${rep_obj3_md5}" ]; then
	echo "BUG: MD5 checksum of object 'silo2/test-bucket/custpartsize' doesn't match with source. Expected: '${src_obj3_md5}', Found: '${rep_obj3_md5}'"
	exit_1
fi

cleanup
