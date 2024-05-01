tsmemseg - In-memory transport stream segmenter mainly for HLS/LL-HLS

Usage:

tsmemseg [-4][-i inittime][-t time][-p ptime][-a acc_timeout][-c cmd][-r readrate][-f fill_readrate][-s seg_num][-m max_kbytes][-g dir][-d flags] seg_name

-4
  Convert to fragmented MP4.

-i inittime (seconds), 0<=range<=60, default=1
  Initial segment duration. Segment is cut on a key (NAL-IRAP) packet.

-t time (seconds), 0<=range<=60, default=2
  Duration other than the initial segment.

-p ptime (seconds), 0<=range<=60, default=0.5
  Target duration for partial segments (MP4 fragments).

-a acc_timeout (seconds), 0<=range<=600, default=10
  Quit when the named-pipes/FIFOs of this tool have not been accessed for more than acc_timeout. 0 means no quit.

-c cmd
  Run command once when this tool is closing or access-timedout. "cmd" string is passed to system() C function.

-r readrate (percent), 0 or 20<=range<=500, default=0
  Read speed from standard input compared to media timestamp (PTS). 0 means unlimited.

-f fill_readrate (percent), 0 or 20<=range<=750, default=1.5*readrate
  Initial read speed until all segments are available. 0 means unlimited.

-s seg_num, 2<=range<=99, default=8
  The number of segment entries to be created.

-m max_kbytes (kbytes), 32<=range<=32768, default=4096
  Maximum size of each segment. If segment length exceeds this limit, the segment is forcibly cut whether on a key packet or not.

-g dir, default=""
  Specify the directory for creating FIFOs. If not specified, created in "/tmp" with 0600 permission.
  This option is ignored on Windows.

-d flags, range=0 or 1 or 3, default=0
  Convert ARIB caption/superimpose streams to an ID3 timed-metadata stream that https://github.com/monyone/aribb24.js can recognize.
  This feature is enabled when "flags" is 1 or 3.
  If "flags" is 3, this feature treats unknown MPEG2 private data streams as ARIB superimpose.

seg_name
  Used for the name pattern of named-pipes/FIFOs used to access segments, or "-" (stdout).
  If "-" is specified, simply prints stream to standard output. -a -c -r -f -s options are ignored.
  In other cases, available characters are 0-9, A-Z, a-z, '_'. Maximum length is 65.
  For instance, if "foo123_" is specified, the name pattern of named-pipes/FIFOs is "\\.\pipe\tsmemseg_foo123_??" or "/tmp/tsmemseg_foo123_??.fifo".

Description:

Standard input to this tool is assumed to be an MPEG transport stream which contains a single PMT stream, and a single MPEG-4 AVC / HEVC video stream
with appropriate keyframe interval and/or a single ADTS audio stream. This is such as a stream that is encoded using FFmpeg.
This tool does not output any files. Users can access each segment via Windows named-pipe or Unix FIFO (typically, using fopen("rb")).
Information corresponding to HLS playlist file (.m3u8) can be obtained via "\\.\pipe\tsmemseg_{seg_name}00" or "/tmp/tsmemseg_{seg_name}00.fifo". (hereinafter "listing pipe")
Actual segment data (MPEG-TS or fragmented MP4) can be obtained via between "tsmemseg_{seg_name}01" and "tsmemseg_{seg_name}{seg_num}". (hereinafter "segment pipe")
For FIFOs, exclusive lock (flock(LOCK_EX)) should be obtained if simultaneous access is possible.
For example, this tool is intended to be used in server-side scripts on web servers.

Specification of "listing pipe":

"listing pipe" contains the following binary data in 16 bytes units. All values are written in little endian.
The 0th byte of the first 16 bytes unit stores the number of following 16 bytes units. This is the same value as seg_num.
The sequence of 4-7th bytes stores the UNIX time when this list was updated.
8th stores whether this list will be updated later (0) or it has been no longer updated (1).
9th stores whether the available last segment is "incomplete" (1, means additional MP4 fragments will be added later) or not (0).
10th stores whether each segment is MPEG-TS (0) or MP4 (1).
12-15th stores byte length of extra readable area after the subsequent units.

Subsequent 16 bytes units contain information about each segment. Newly updated segment is stored backward.
The 0th byte of the units stores the index of the segment pointed to. The range is between 1 and seg_num.
2nd stores the number of MP4 fragments in this segment. Information about each fragment can be got from each 16 bytes unit (explained later) in the extra readable area.
4-6th stores the sequential number of segment.
7th stores whether segment is available (0) or unavailable (1).
8-11th stores the duration of segment in milliseconds.
12-15th stores sum of all past segment durations up to this segment in 10-milliseconds.

Information about MP4 fragments (16 bytes units) are placed in the extra readable area.
The 0-3rd byte of the units stores the duration of fragment in milliseconds.
Besides the fragment information, if there is space in the extra readable area, it is MP4 header box (ftyp/moov).

For Unix FIFO only, there is a 64-bytes field preceding the information units to store the seg_name.
The field can be used as a signature to prevent multiple processes from reading the FIFO in a race condition.

All other unused bytes are initialized with 0.

 0         1 2        3 4 5 6             7           8                 9          0   1 2 3 4 5
+----------------------+-----------------------------+----------------------------------+-----------------+
|seg_name field (Unix FIFO only)                                                                          :
+----------------------+-----------------------------+----------------------------------+-----------------+
:                                                                                                         :
+----------------------+-----------------------------+----------------------------------+-----------------+
:                                                                                                         :
+----------------------+-----------------------------+----------------------------------+-----------------+
:                                                                                                         |
+----------------------+-----------------------------+----------------------------------+-----------------+
|seg_num   0 0        0|UNIX_time_updated            |no_longer_updated incomplete MP4 0|extra_area_length|
+----------------------+-----------------------------+----------------------------------+-----------------+
|seg_index 0 frag_num 0|sequential_number unavailable|seg_duration_msec                 |sum_of_durations |
+----------------------+-----------------------------+----------------------------------+-----------------+
...
+----------------------+-----------------------------+----------------------------------+-----------------+
|frag_duration_msec    |0 0 0             0          |0                 0          0   0|0 0 0 0          |
+----------------------+-----------------------------+----------------------------------+-----------------+
...
+----------------------+-----------------------------+----------------------------------+-----------------+
|ftyp/moov                                                                                                :
...

Specification of "segment pipe":

"segment pipe" contains MPEG-TS packets or MP4 moof boxes.
The first unit is always TS-NULL packet and its payload contains the following information.
The sequence of 4-6th bytes (immediately after TS-NULL header) stores the sequential number of this segment.
7th stores whether this segment is available (0) or unavailable (1).
8-11th stores the number of following 188 bytes units (MPEG-TS) or bytes (MP4). These are the MPEG-TS/MP4 stream itself.
12th stores whether this segment is MPEG-TS (0) or MP4 (1).
32-35th, and subsequent 4 bytes units (until its value is 0) store the size of all fragments contained in the stream.

For Unix FIFO only, there is a 188-bytes field preceding the information packet to store the seg_name.

All other unused bytes are initialized with 0.

 0    1    2    3    4 5 6             7           8 9 0 1                  2   3 4 5
(The following 188 bytes are Unix FIFO only)
+-------------------+-----------------------------+------------------------+-----------+
|0x47 0x1f 0xff 0x10|seg_name field                                                    :
...(188 bytes)
+-------------------+-----------------------------+------------------------+-----------+
|0x47 0x1f 0xff 0x10|sequential_number unavailable|number_of_units_or_bytes|MP4 0 0 0  |
+-------------------+-----------------------------+------------------------+-----------+
|0    0    0    0   |0 0 0             0          |0 0 0 0                 |0   0 0 0  |
+-------------------+-----------------------------+------------------------+-----------+
|0    0    0    0   |0 0 0             0          |0 0 0 0                 |0   0 0 0  |
+-------------------+-----------------------------+------------------------+-----------+
|0    0    0    0   |0 0 0             0          |0 0 0 0                 |0   0 0 0  |
+-------------------+-----------------------------+------------------------+-----------+
|frag_size_0        |frag_size_1                  |frag_size_2             |frag_size_3|
+-------------------+-----------------------------+------------------------+-----------+
...(188 bytes)
+-------------------+-----------------------------+------------------------+-----------+
|MPEG-TS/MP4 stream                                                                    :
...

Notes:

This tool currently only supports Windows and Linux.

Licensed under MIT.

https://github.com/monyone/node-arib-subtitle-timedmetadater was very helpful in implementing "-d" option.

https://github.com/monyone/biim was very very helpful in implementing "-4" option.
