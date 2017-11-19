

# Mongo Benchmarking Project

### Installing dependencies

- On each of the 5 cluster nodes, install MongoDB 3.4.7 as follows:
    + Download MongoDB to the **home** folder of each of the cluster node by running `wget http://downloads.mongodb.org/linux/mongodb-linux-x86_64-3.4.7.tgz`.
    + Copy the script `setup_mongodb.sh` to the **home** folder.
    + Add `/temp/mongodb/bin` to your `PATH`  in your `.bash_profile`.
- You should also have `Python 2.7` and `pip` installed. If you have no sudo access to the server, refer to [this guide](https://gist.github.com/saurabhshri/46e4069164b87a708b39d947e4527298) on how to install pip without sudo access.
- Install the python dependencies using `pip install --user -r requirements.txt`

### Installing & Configuring MongoDB

It's required that you have a server that has passwordless ssh access to the 5 cluster nodes.

- On this server that has passwordless ssh access to all the cluster nodes, get a copy of the project from [here](https://github.com/louislai/MogoTransaction/archive/master.zip) and unzip it anywhere.
- Inside the unzipped project folder, specify your cluster nodes' ip addresses and login details in `config.txt`. Make sure they are in order.
- Inside the project folder, run `./setup_sharding`.


### Setup the data

- Go to any one of your cluster nodes, download the project codes from [here](https://github.com/louislai/MogoTransaction/archive/master.zip) and unzip it inside the node's **home** folder.

- Run `./setup_remote.sh` inside the unzipped project folder. You only need to run this on any **one** node.

### Running benchmarks
- On every cluster node, get a copy of the project from [here](https://github.com/louislai/MogoTransaction/archive/master.zip) and unzip it in the node's **home** folder. It is important that the unzipped project folder is named **MogoTransaction-master**

- On the same server that has passwordless ssh access to all the cluster nodes, get a copy of the project from [here](https://github.com/louislai/MogoTransaction/archive/master.zip) and unzip it anywhere.
- Inside the unzipped project folder, again specify your cluster nodes' ip addresses and login details in `config.txt`.
- Inside the project folder, run `./benchmark.sh <NUMBER_OF_CLIENTS> <TESTING_TYPE> <OPTIONAL_NUMBER_OF_TRANSACTIONS>` to start benchmarking. `NUMBER_OF_CLIENTS` can be 10, 20 or 40. `TESTING_TYPE` can be `1`, denoting majority read & write concern, or `0`, denoting local read & 1-replica write. 
`OPTIONAL_NUMBER_OF_TRANSACTIONS` denotes the number of transactions that you want to run from each transaction file. This can be omitted if you want to run all the transactions in all the files. Log files will be created in your working directory when the benchmark script is running.
