CXX = g++
CXXFLAGS += -D HAVE_LONG_INT_64 -Wall -std=c++11 -shared -Wno-unused-value -DODBC64 -fPIC
INCPATH = -I/opt/vertica/sdk/include -I/opt/vertica/sdk/examples/HelperLibraries
VERPATH = /opt/vertica/sdk/include/Vertica.cpp /opt/vertica/sdk/include/BuildInfo.h
UDXLIBNAME = ldblink
UDXLIB = $(UDXLIBNAME).so
UDXSRC = $(UDXLIBNAME).cpp
VSQL = /opt/vertica/bin/vsql

all: prod

debug: CXXFLAGS += -DDBLINK_DEBUG=1 -Og -g
debug: compile

prod: CXXFLAGS += -O3
prod: compile

compile: $(UDXSRC)
	$(CXX) $(CXXFLAGS) $(INCPATH) -o $(UDXLIB) $(UDXSRC) $(VERPATH) -lodbc

install: $(UDXLIB)
	$(VSQL) -f ./install.sql

uninstall:
	$(VSQL) -f ./uninstall.sql

clean:
	rm -f $(UDXLIB)
