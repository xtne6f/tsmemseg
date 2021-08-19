tsmemseg - In-memory transport stream segmenter mainly for HLS

Usage:

tsmemseg [-i inittime][-t time][-a acc_timeout][-r readrate][-f fill_readrate][-s seg_num][-m max_kbytes] seg_name

-i inittime (seconds), 0<=range<=60, default=0
  Initial segment duration. Segment is cut on a key (NAL-IDR) packet.

-t time (seconds), 0<=range<=60, default=2
  Duration other than the initial segment.

-a acc_timeout (seconds), 0<=range<=600, default=10
  Quit when the named-pipes of this tool have not been accessed for more than acc_timeout. 0 means no quit.

-r readrate (percent), 0 or 20<=range<=500, default=0
  Read speed from standard input compared to media timestamp (PTS). 0 means unlimited.

-f fill_readrate (percent), 0 or 20<=range<=750, default=1.5*readrate
  Initial read speed until all segments are available. 0 means unlimited.

-s seg_num, 2<=range<=99, default=8
  The number of segment entries to be created.

-m max_kbytes (kbytes), 32<=range<=32768, default=4096
  Maximum size of each segment. If segment length exceeds this limit, the segment is forcibly cut whether on a key packet or not.

seg_name
  Used for the name pattern of named-pipes used to access segments.
  Available characters are 0-9, A-Z, a-z, '_'. Maximum length is 65.
  For instance, if "foo123_" is specified, the name pattern of named-pipes is "\\.\pipe\tsmemseg_foo123_??".

Description:

Standard input to this tool is assumed to be an MPEG transport stream which contains a single PMT stream, and a single MPEG-4 AVC video stream
with appropriate keyframe interval. This is such as a stream that is encoded using FFmpeg.
This tool does not output any files. Users can access each segment via Windows named-pipe (typically, using fopen("rb")).
Information corresponding to HLS playlist file (.m3u8) can be obtained via "\\.\pipe\tsmemseg_{seg_name}00". (hereinafter "listing pipe")
Actual segment data (MPEG-TS) can be obtained via between "\\.\pipe\tsmemseg_{seg_name}01" and "\\.\pipe\tsmemseg_{seg_name}{seg_num}". (hereinafter "segment pipe")
For example, this tool is intended to be used in server-side scripts on web servers.

Specification of "listing pipe":

"listing pipe" contains the following binary data in 16 bytes units. All values are written in little endian.
The 0th byte of the first 16 bytes unit stores the number of following 16 bytes units. This is the same value as seg_num.
The sequence of 4-7th bytes stores the UNIX time when this list was updated.
8th stores whether this list will be updated later (0) or it has been no longer updated (1).

Subsequent 16 bytes units contain information about each segment. Newly updated segment is stored backward.
The 0th byte of the units stores the index of the segment pointed to. The range is between 1 and seg_num.
4-6th stores the sequential number of segment.
7th stores whether segment is available (0) or unavailable (1).
8-11th stores the duration of segment in milliseconds.
All other unused bytes are initialized with 0.

Specification of "segment pipe":

"segment pipe" contains MPEG-TS packets in 188 bytes units.
The first unit is always TS-NULL packet and its payload contains the following information.
The sequence of 4-6th bytes (immediately after TS-NULL header) stores the sequential number of this segment.
7th stores whether this segment is available (0) or unavailable (1).
8-11th stores the number of following 188 bytes units. These units are the MPEG-TS stream itself.
All other unused bytes are initialized with 0.

Notes:

This tool currently only supports Windows.

Licensed under MIT.
