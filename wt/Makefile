include ../Makefile.config

EXECUTABLE = mapkeeper_wt

all :
	g++ -ggdb -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
        -lboost_thread-mt -lboost_filesystem -lboost_program_options -lthrift -lthriftnb -levent -I ../thrift/gen-cpp \
	-I ../../wiredtiger/build_posix -L ../../wiredtiger/build_posix/.libs	\
	-lwiredtiger							\
	-L $(THRIFT_DIR)/lib \
        -L ../thrift/gen-cpp -lmapkeeper \
           -Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp			\
           -Wl,-rpath,$(THRIFT_DIR)/lib

run:
	./$(EXECUTABLE) --sync

clean :
	- rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o 

wipe:
	- rm -rf data/*
