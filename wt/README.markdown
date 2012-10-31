## Dependencies

### Boost
  
    wget http://superb-sea2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.gz
    tar xfvz boost_1_48_0.tar.gz
    cd boost_1_48_0
    ./bootstrap.sh --prefix=/usr/local
    sudo ./b2 install 

### WiredTiger

    git clone git@github.com:wiredtiger/wiredtiger.git
    cd wiredtiger/build_posix
    ../configure
    make
    make install
TODO: Install WiredTiger??
    sudo cp libleveldb.a /usr/local/lib
    sudo cp -r include/leveldb /usr/local/include

## Running WiredTiger MapKeeper Server

After installing all the dependencies, run:

    make

to compile. To start the server, execute the command:

    ./mapkeeper_wt

You can see the complete list of available options by passing `-h` to `mapkeeper_wt` 
command.
  
## Configuration Parameters

There will be many tunable parameters that affects WiredTiger performance.. 

## Related Pages

* [Official WiredTiger Documentation](http://source.wiredtiger.com/)
