all: tsmemseg.exe
tsmemseg.exe: tsmemseg.cpp util.cpp util.hpp
	$(CXX) -Wl,-s -static -std=c++11 -Wall -Wextra -pedantic-errors -o $@ tsmemseg.cpp util.cpp
clean:
	$(RM) tsmemseg.exe
