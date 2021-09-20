CXXFLAGS := -std=c++11 -Wall -Wextra -pedantic-errors -O2 $(CXXFLAGS)
LDFLAGS := -Wl,-s $(LDFLAGS)
ifdef MINGW_PREFIX
  LDFLAGS := -static $(LDFLAGS)
  TARGET ?= tsmemseg.exe
else
  LDFLAGS := -lpthread $(LDFLAGS)
  TARGET ?= tsmemseg
endif

all: $(TARGET)
$(TARGET): tsmemseg.cpp util.cpp util.hpp id3conv.cpp id3conv.hpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(TARGET_ARCH) -o $@ tsmemseg.cpp util.cpp id3conv.cpp
clean:
	$(RM) $(TARGET)
