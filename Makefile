CXXFLAGS := -std=c++11 -Wall -Wextra -pedantic-errors -O2 $(CXXFLAGS)
LDFLAGS := -Wl,-s $(LDFLAGS)
ifdef MINGW_PREFIX
  LDFLAGS := -static $(LDFLAGS)
  TARGET ?= tsmemseg.exe
else
  LDFLAGS := -pthread $(LDFLAGS)
  TARGET ?= tsmemseg
endif

all: $(TARGET)
$(TARGET): tsmemseg.cpp util.cpp util.hpp mp4fragmenter.cpp mp4fragmenter.hpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(TARGET_ARCH) -o $@ tsmemseg.cpp util.cpp mp4fragmenter.cpp
clean:
	$(RM) $(TARGET)
