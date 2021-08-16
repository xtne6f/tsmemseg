all: tsmemseg.exe
tsmemseg.exe: tsmemseg.cpp util.cpp util.hpp id3conv.cpp id3conv.hpp
	$(CXX) -Wl,-s -static -std=c++11 -Wall -Wextra -pedantic-errors -o $@ tsmemseg.cpp util.cpp id3conv.cpp
clean:
	$(RM) tsmemseg.exe
