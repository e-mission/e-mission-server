cd /src/e-mission-server

#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/storage/db.conf.sample > conf/storage/db.conf
else
    sed "s_localhost_${DB_HOST}_" conf/storage/db.conf.sample > conf/storage/db.conf
fi
cat conf/storage/db.conf

echo "Setting up tests..."
source /opt/conda/etc/profile.d/conda.sh
source setup/setup_tests.sh

echo "Running tests..."
./runAllTests.sh
source setup/teardown_tests.sh
