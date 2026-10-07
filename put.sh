#!/bin/bash

makeLoad1() {

	echo "#!/bin/bash" > load1.sh

	echo "recs1=${recs1}" >> load1.sh
#	echo "recs=$(echo ${recs} | sed 's/,//g')" >> load1.sh
	echo "for((i=0;i<1000;i++)); do" >> load1.sh
	echo "  ./hop-gen -host ${clusterIP} -namespace test -set households -households ${recs1} -max-individuals 4 -max-accounts 4 -max-addresses 2 -workers 8" >> load1.sh echo "done" >> load1.sh

}

makeLoad2() {

	echo "#!/bin/bash" > load2.sh

	echo "recs2=${recs2}" >> load2.sh
#	echo "recs=$(echo ${recs} | sed 's/,//g')" >> load2.sh
	echo "for((i=0;i<4000;i++)); do" >> load2.sh
	echo "  ./hop-gen -host ${clusterIP} -namespace test -set households -households ${recs2} -max-individuals 4 -max-accounts 4 -max-addresses 2 -workers 8 -create-si" >> load2.sh
	echo "done" >> load2.sh

}


#GOOS=linux GOARCH=amd64 go build -o hop-gen.linux ./cmd/hop-generator/.

if [ -e config.json ]; then 
	confFile=config.json
elif [ -e ../config.json ]; then
	confFile=../config.json
else
	echo "Can't find config.json, exiting"
	exit
fi

client=$(jq '.config.clients.clientName' ${confFile})
if [ "$(jq '.config.clients.queryConfig.useNodes' ${confFile})" == "false" ]; then
	nodes=$(jq '.config.clients.nodes' ${confFile})
else
	nodes=$(jq '.config.clients.queryConfig.loadNodes' ${confFile})
	nodesOdd=$(jq '.config.clients.queryConfig.loadNodesOdd' ${confFile})
	nodesEven=$(jq '.config.clients.queryConfig.loadNodesEven' ${confFile})
fi

clusterIP=$(clusterPrivateIp.sh)
clusterIP=$(echo ${clusterIP} | cut -d ' ' -f 1)

recs1=100,000
recs2=50,000

recs1=$(echo ${recs1} | sed "s/,//g")
recs2=$(echo ${recs2} | sed "s/,//g")

#makeLoad1
#makeLoad2

aerolab client attach -n ${client} -l all 'rm -rf client; mkdir client'
aerolab files upload -n ${client} -c -l all bundle.client.tar client/bundle.client.tar
aerolab client attach -n ${client} -l all --parallel 'cd client; tar xvf bundle.client.tar'
aerolab client attach -n ${client} -l all --parallel 'cd client; mv generate.linux generate; mv queryload.linux queryload'

#aerolab files upload -n ${client} -c -l ${nodes} generator.linux generator
#aerolab files upload -n ${client} -c -l ${nodes} builder.linux builder
#aerolab files upload -n ${client} -c -l ${nodes} augmenter.linux augmenter
#aerolab files upload -n ${client} -c -l ${nodesOdd} load1.sh load.sh
#aerolab files upload -n ${client} -c -l ${nodesEven} load2.sh load.sh
#aerolab files upload -n ${client} -c -l ${nodesOdd} registers.json registers.json
#aerolab files upload -n ${client} -c -l ${nodesEven} registers.json registers.json
#aerolab client attach -n ${client} -l ${nodes} --parallel 'chmod +x *.sh'
aerolab client attach -n ${client} -l ${nodes} --parallel 'cd client; chmod +x generate queryload'

