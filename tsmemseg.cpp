#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <fcntl.h>
#include <io.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include "id3conv.hpp"
#include "util.hpp"

namespace
{
constexpr uint32_t SEGMENT_COUNT_EMPTY = 0x1000000;
constexpr size_t SEGMENTS_MAX = 100;

using lock_recursive_mutex = std::lock_guard<std::recursive_mutex>;

struct SEGMENT_PIPE_CONTEXT
{
    HANDLE h;
    OVERLAPPED ol;
    bool initialized;
    bool connected;
};

struct SEGMENT_CONTEXT
{
    SEGMENT_PIPE_CONTEXT pipes[2];
    std::vector<uint8_t> buf;
    std::vector<uint8_t> backBuf;
    uint32_t segCount;
    uint32_t segDurationMsec;
};

int64_t GetMsecTick()
{
    LARGE_INTEGER f, c;
    if (QueryPerformanceFrequency(&f) && QueryPerformanceCounter(&c)) {
        return c.QuadPart / f.QuadPart * 1000 + c.QuadPart % f.QuadPart * 1000 / f.QuadPart;
    }
    return 0;
}

uint32_t GetCurrentUnixTime()
{
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    int64_t ll = ft.dwLowDateTime | (static_cast<int64_t>(ft.dwHighDateTime) << 32);
    return static_cast<uint32_t>((ll - 116444736000000000) / 10000000);
}

void Worker(SEGMENT_CONTEXT *segments, std::vector<HANDLE> events, std::recursive_mutex &bufLock, std::atomic_uint32_t &lastAccessTick)
{
    for (;;) {
        DWORD result = WaitForMultipleObjects(static_cast<DWORD>(events.size()), events.data(), FALSE, INFINITE);
        if (result < WAIT_OBJECT_0 + 1 || result >= WAIT_OBJECT_0 + events.size()) {
            break;
        }
        lastAccessTick = static_cast<uint32_t>(GetMsecTick());

        HANDLE olEvent = events[result - WAIT_OBJECT_0];
        SEGMENT_CONTEXT &seg = segments[(result - WAIT_OBJECT_0 - 1) / 2];
        SEGMENT_PIPE_CONTEXT &pipe = seg.pipes[(result - WAIT_OBJECT_0 - 1) % 2];
        {
            SEGMENT_PIPE_CONTEXT &subPipe = seg.pipes[(result - WAIT_OBJECT_0) % 2];
            lock_recursive_mutex lock(bufLock);

            // seg.backBuf is used only when seg.buf is in use, so this will be the rare case.
            if (!seg.backBuf.empty() && !subPipe.connected) {
                // Swap and clear the back buffer.
                seg.buf.swap(seg.backBuf);
                std::vector<uint8_t>().swap(seg.backBuf);
            }
        }
        if (pipe.connected) {
            // Complete an asynchronous pipe write
            FlushFileBuffers(pipe.h);
            DisconnectNamedPipe(pipe.h);

            lock_recursive_mutex lock(bufLock);
            pipe.connected = false;
        }
        else if (pipe.initialized) {
            {
                lock_recursive_mutex lock(bufLock);
                pipe.connected = true;
            }
            // Start an asynchronous pipe write
            OVERLAPPED olZero = {};
            pipe.ol = olZero;
            pipe.ol.hEvent = olEvent;
            if (!WriteFile(pipe.h, seg.buf.data(),
                           static_cast<DWORD>(seg.buf.size()), nullptr, &pipe.ol) &&
                GetLastError() != ERROR_IO_PENDING) {
                DisconnectNamedPipe(pipe.h);

                lock_recursive_mutex lock(bufLock);
                pipe.connected = false;
            }
        }
        if (!pipe.connected) {
            // Start connecting
            OVERLAPPED olZero = {};
            pipe.ol = olZero;
            pipe.ol.hEvent = olEvent;
            if (!ConnectNamedPipe(pipe.h, &pipe.ol)) {
                DWORD err = GetLastError();
                if (err == ERROR_PIPE_CONNECTED) {
                    SetEvent(pipe.ol.hEvent);
                }
                else if (err != ERROR_IO_PENDING) {
                    CloseHandle(pipe.h);
                    pipe.h = INVALID_HANDLE_VALUE;
                    ResetEvent(pipe.ol.hEvent);
                }
            }
        }
        pipe.initialized = true;
    }

    // Cancel all pending IOs
    for (size_t i = 0; i < events.size() - 1; ++i) {
        if (segments[i / 2].pipes[i % 2].h != INVALID_HANDLE_VALUE &&
            segments[i / 2].pipes[i % 2].initialized) {
            if (CancelIo(segments[i / 2].pipes[i % 2].h)) {
                WaitForSingleObject(events[i + 1], INFINITE);
            }
        }
    }
}

void ClearSegmentsAndEvents(std::vector<SEGMENT_CONTEXT> &segments, std::vector<HANDLE> &events)
{
    while (!segments.empty()) {
        for (size_t i = 0; i < 2; ++i) {
            if (segments.back().pipes[i].h != INVALID_HANDLE_VALUE) {
                CloseHandle(segments.back().pipes[i].h);
            }
        }
        segments.pop_back();
    }
    while (!events.empty()) {
        if (events.back()) {
            CloseHandle(events.back());
        }
        events.pop_back();
    }
}

bool CheckAccessTimeout(int64_t tick, const std::atomic_uint32_t *lastAccessTicks, size_t n, uint32_t accessTimeoutMsec)
{
    for (size_t i = 0; i < n; ++i) {
        if (static_cast<uint32_t>(tick) - lastAccessTicks[i] < accessTimeoutMsec) {
            return false;
        }
    }
    return true;
}

void WriteUint32(uint8_t *buf, uint32_t n)
{
    buf[0] = static_cast<uint8_t>(n);
    buf[1] = static_cast<uint8_t>(n >> 8);
    buf[2] = static_cast<uint8_t>(n >> 16);
    buf[3] = static_cast<uint8_t>(n >> 24);
}

void AssignSegmentList(std::vector<uint8_t> &buf, const std::vector<SEGMENT_CONTEXT> &segments, size_t segIndex)
{
    buf.assign(segments.size() * 16, 0);
    WriteUint32(&buf[0], static_cast<uint32_t>(segments.size() - 1));
    WriteUint32(&buf[4], GetCurrentUnixTime());
    for (size_t i = segIndex, j = 1; j < segments.size(); ++j) {
        WriteUint32(&buf[j * 16], static_cast<uint32_t>(i));
        WriteUint32(&buf[j * 16 + 4], segments[i].segCount);
        WriteUint32(&buf[j * 16 + 8], segments[i].segDurationMsec);
        i = i % (segments.size() - 1) + 1;
    }
}

void WriteSegmentHeader(std::vector<uint8_t> &buf, uint32_t segCount)
{
    // NULL TS header
    buf[0] = 0x47;
    buf[1] = 0x01;
    buf[2] = 0xff;
    buf[3] = 0x10;
    WriteUint32(&buf[4], segCount);
    WriteUint32(&buf[8], static_cast<uint32_t>(buf.size() / 188 - 1));
}
}

int main(int argc, char **argv)
{
    uint32_t targetDurationMsec = 0;
    uint32_t nextTargetDurationMsec = 2000;
    uint32_t accessTimeoutMsec = 10000;
    int readRatePerMille = -1;
    int nextReadRatePerMille = 0;
    size_t segNum = 8;
    size_t segMaxBytes = 4096 * 1024;
    const char *destName = "";
    CID3Converter id3conv;

    for (int i = 1; i < argc; ++i) {
        char c = '\0';
        if (argv[i][0] == '-' && argv[i][1] && !argv[i][2]) {
            c = argv[i][1];
        }
        if (c == 'h') {
            fprintf(stderr, "Usage: tsmemseg [-i inittime][-t time][-a acc_timeout][-r readrate][-f fill_readrate][-s seg_num][-m max_kbytes][-d flags] seg_name\n");
            return 2;
        }
        bool invalid = false;
        if (i < argc - 1) {
            if (c == 'i' || c == 't') {
                double sec = strtod(argv[++i], nullptr);
                invalid = !(0 <= sec && sec <= 60);
                if (!invalid) {
                    uint32_t &msec = c == 'i' ? targetDurationMsec : nextTargetDurationMsec;
                    msec = static_cast<uint32_t>(sec * 1000);
                }
            }
            else if (c == 'a') {
                double sec = strtod(argv[++i], nullptr);
                invalid = !(0 <= sec && sec <= 600);
                if (!invalid) {
                    accessTimeoutMsec = static_cast<uint32_t>(sec * 1000);
                }
            }
            else if (c == 'r' || c == 'f') {
                double percent = strtod(argv[++i], nullptr);
                invalid = !(0 <= percent && percent <= 1000);
                if (!invalid) {
                    int &perMille = c == 'f' ? readRatePerMille : nextReadRatePerMille;
                    perMille = static_cast<int>(percent * 10);
                    invalid = perMille != 0 && perMille < 100;
                }
            }
            else if (c == 's') {
                segNum = static_cast<size_t>(strtol(argv[++i], nullptr, 10));
                invalid = segNum < 2 || SEGMENTS_MAX <= segNum;
            }
            else if (c == 'm') {
                segMaxBytes = static_cast<size_t>(strtol(argv[++i], nullptr, 10) * 1024);
                invalid = segMaxBytes < 32 * 1024 || 32 * 1024 * 1024 < segMaxBytes;
            }
            else if (c == 'd') {
                id3conv.SetOption(static_cast<int>(strtol(argv[++i], nullptr, 10)));
            }
        }
        else {
            destName = argv[i];
            for (size_t j = 0; destName[j]; ++j) {
                c = destName[j];
                if (j >= 65 || ((c < '0' || '9' < c) && (c < 'A' || 'Z' < c) && (c < 'a' || 'z' < c) && c != '_')) {
                    destName = "";
                    break;
                }
            }
            invalid = !destName[0];
        }
        if (invalid) {
            fprintf(stderr, "Error: argument %d is invalid.\n", i);
            return 1;
        }
    }
    if (!destName[0]) {
        fprintf(stderr, "Error: not enough arguments.\n");
        return 1;
    }
    if (readRatePerMille < 0) {
        readRatePerMille = nextReadRatePerMille * 3 / 2;
    }

#if 0
    // for testing
    FILE *fp = fopen("test.m2t", "rb");
#else
    FILE *fp = stdin;
    if (_setmode(_fileno(fp), _O_BINARY) < 0) {
        fprintf(stderr, "Error: _setmode.\n");
        return 1;
    }
#endif

    // segments.front() is segment list, the others are segments.
    std::vector<SEGMENT_CONTEXT> segments;
    // events.front() is stop-event, the others are used for asynchronous writing of segments.
    std::vector<HANDLE> events(1, CreateEvent(nullptr, TRUE, FALSE, nullptr));

    if (events.front()) {
        while (segments.size() < 1 + segNum) {
            SEGMENT_CONTEXT seg = {};
            char pipeName[128];
            sprintf(pipeName, "\\\\.\\pipe\\tsmemseg_%s%02d", destName, static_cast<int>(segments.size()));
            // Create 2 pipes for simultaneous access
            size_t createdCount = 0;
            for (; createdCount < 2; ++createdCount) {
                events.push_back(CreateEvent(nullptr, TRUE, TRUE, nullptr));
                if (!events.back()) {
                    break;
                }
                seg.pipes[createdCount].h = CreateNamedPipeA(pipeName, PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED, 0, 2, 48128, 0, 0, nullptr);
                if (seg.pipes[createdCount].h == INVALID_HANDLE_VALUE) {
                    break;
                }
            }
            if (createdCount < 2) {
                if (createdCount == 1) {
                    CloseHandle(seg.pipes[0].h);
                }
                break;
            }
            seg.segCount = SEGMENT_COUNT_EMPTY;
            if (!segments.empty()) {
                seg.buf.assign(188, 0);
                WriteSegmentHeader(seg.buf, seg.segCount);
            }
            segments.push_back(std::move(seg));
        }
    }
    if (segments.size() < 1 + segNum) {
        ClearSegmentsAndEvents(segments, events);
        fprintf(stderr, "Error: pipe creation failed.\n");
        return 1;
    }
    AssignSegmentList(segments.front().buf, segments, 1);

    int64_t baseTick = GetMsecTick();
    std::recursive_mutex bufLock;
    std::vector<std::thread> threads;
    std::atomic_uint32_t lastAccessTicks[SEGMENTS_MAX / 20];

    // Create a thread for every 20 segments
    for (size_t i = 0; i < segments.size(); i += 20) {
        std::vector<HANDLE> eventsForThread;
        eventsForThread.push_back(events.front());
        eventsForThread.insert(eventsForThread.end(), events.begin() + 1 + i * 2, events.begin() + std::min(1 + (i + 20) * 2, events.size()));
        lastAccessTicks[i / 20] = static_cast<uint32_t>(baseTick);
        threads.emplace_back(Worker, segments.data() + i, std::move(eventsForThread), std::ref(bufLock), std::ref(lastAccessTicks[i / 20]));
    }

    // Index of the next segment to be overwritten (between 1 and "segNum")
    size_t segIndex = 1;
    // Sequence count of segments
    uint32_t segCount = 0;
    // PID of the packet to determine segmentation (Currently this is AVC_VIDEO)
    int keyPid = 0;
    // AVC-NAL's parsing state
    int nalState = 0;
    // Map of PID and unit-start position. The second of the pair is the last unit-start immediately before "keyPid" unit-start.
    std::unordered_map<int, std::pair<size_t, size_t>> unitStartMap;
    // Packets accumulating for next segmentation
    std::vector<uint8_t> packets;
    std::vector<uint8_t> backPackets;

    unsigned int syncError = 0;
    unsigned int forcedSegmentationError = 0;
    int64_t entireDurationMsec = 0;
    uint32_t durationMsecResidual45khz = 0;
    uint32_t pts45khz = 0;
    uint32_t lastPts45khz = 0;
    bool ptsInitialized = false;
    bool isFirstKey = true;
    PAT pat = {};
    uint8_t buf[188 * 16];
    size_t bufCount = 0;
    size_t nRead;
    while ((nRead = fread(buf + bufCount, 1, sizeof(buf) - bufCount, fp)) != 0) {
        bufCount += nRead;

        bool accessTimedout = false;
        for (;;) {
            int64_t nowTick = GetMsecTick();
            if (accessTimeoutMsec != 0 && CheckAccessTimeout(nowTick, lastAccessTicks, threads.size(), accessTimeoutMsec)) {
                accessTimedout = true;
                break;
            }
            if (readRatePerMille != nextReadRatePerMille &&
                std::find_if(segments.begin() + 1, segments.end(),
                    [](const SEGMENT_CONTEXT &a) { return a.segCount == SEGMENT_COUNT_EMPTY; }) == segments.end()) {
                // All segments are not empty
                readRatePerMille = nextReadRatePerMille;
                // Rebase
                baseTick = nowTick;
                entireDurationMsec = 0;
            }
            if (readRatePerMille > 0) {
                // Check reading speed
                uint32_t ptsDiff = pts45khz - lastPts45khz;
                if (!(ptsDiff >> 31) && entireDurationMsec + ptsDiff / 45 > (nowTick - baseTick) * readRatePerMille / 1000) {
                    // Too fast
                    Sleep(10);
                    continue;
                }
            }
            break;
        }
        if (accessTimedout) {
            break;
        }

        for (const uint8_t *packet = buf; packet < buf + sizeof(buf) && packet + 188 <= buf + bufCount; packet += 188) {
            if (extract_ts_header_sync(packet) != 0x47) {
                // Resynchronization is not implemented.
                ++syncError;
                continue;
            }
            id3conv.AddPacket(packet);
        }
        for (auto itPacket = id3conv.GetPackets().cbegin(); itPacket != id3conv.GetPackets().end(); itPacket += 188) {
            const uint8_t *packet = &*itPacket;
            int unitStart = extract_ts_header_unit_start(packet);
            int pid = extract_ts_header_pid(packet);
            int counter = extract_ts_header_counter(packet);
            if (unitStart) {
                unitStartMap.emplace(pid, std::pair<size_t, size_t>(0, SIZE_MAX)).first->second.first = packets.size();
            }
            int payloadSize = get_ts_payload_size(packet);
            const uint8_t *payload = packet + 188 - payloadSize;

            bool isKey = false;
            if (pid == 0) {
                extract_pat(&pat, payload, payloadSize, unitStart, counter);
            }
            else if (pid == pat.first_pmt.pmt_pid) {
                extract_pmt(&pat.first_pmt, payload, payloadSize, unitStart, counter);
            }
            else if (pid == pat.first_pmt.first_video_pid &&
                     pat.first_pmt.first_video_stream_type == AVC_VIDEO) {
                if (unitStart) {
                    for (auto it = unitStartMap.begin(); it != unitStartMap.end(); ++it) {
                        it->second.second = it->second.first;
                    }
                    keyPid = pid;
                    nalState = 0;
                    if (payloadSize >= 9 && payload[0] == 0 && payload[1] == 0 && payload[2] == 1) {
                        int ptsDtsFlags = payload[7] >> 6;
                        int pesHeaderLength = payload[8];
                        if (ptsDtsFlags >= 2 && payloadSize >= 14) {
                            pts45khz = (static_cast<uint32_t>((payload[9] >> 1) & 7) << 29) |
                                       (payload[10] << 21) |
                                       (((payload[11] >> 1) & 0x7f) << 14) |
                                       (payload[12] << 6) |
                                       ((payload[13] >> 2) & 0x3f);
                            if (!ptsInitialized) {
                                lastPts45khz = pts45khz;
                                ptsInitialized = true;
                            }
                        }
                        if (9 + pesHeaderLength < payloadSize) {
                            if (contains_nal_idr(&nalState, payload + 9 + pesHeaderLength, payloadSize - (9 + pesHeaderLength))) {
                                isKey = !isFirstKey;
                                isFirstKey = false;
                            }
                        }
                    }
                }
                else if (pid == keyPid) {
                    if (contains_nal_idr(&nalState, payload, payloadSize)) {
                        isKey = !isFirstKey;
                        isFirstKey = false;
                    }
                }
            }

            if (isKey || packets.size() + 188 > segMaxBytes) {
                uint32_t ptsDiff = pts45khz - lastPts45khz;
                if (ptsDiff >> 31) {
                    // PTS went back, rare case.
                    ptsDiff = 0;
                }
                if (!isKey || ptsDiff >= targetDurationMsec * 45) {
                    lock_recursive_mutex lock(bufLock);

                    SEGMENT_CONTEXT &seg = segments[segIndex];
                    segIndex = segIndex % segNum + 1;
                    seg.segCount = (++segCount) & 0xffffff;
                    seg.segDurationMsec = ptsDiff / 45;
                    durationMsecResidual45khz += ptsDiff % 45;
                    seg.segDurationMsec += durationMsecResidual45khz / 45;
                    durationMsecResidual45khz %= 45;
                    entireDurationMsec += seg.segDurationMsec;

                    std::vector<uint8_t> &segBuf =
                        !seg.backBuf.empty() || seg.pipes[0].connected || seg.pipes[1].connected ? seg.backBuf : seg.buf;
                    segBuf.assign(188, 0);

                    backPackets.clear();
                    if (isKey) {
                        size_t keyUnitStartPos = unitStartMap[keyPid].second;
                        // Bring PAT and PMT to the front
                        int bringState = 0;
                        for (size_t i = 0; i < packets.size() && i < keyUnitStartPos && bringState < 2; i += 188) {
                            int p = extract_ts_header_pid(&packets[i]);
                            if (p == 0 || p == pat.first_pmt.pmt_pid) {
                                bringState = p == 0 ? 1 : bringState == 1 ? 2 : bringState;
                                segBuf.insert(segBuf.end(), packets.begin() + i, packets.begin() + i + 188);
                            }
                        }
                        bringState = 0;
                        for (size_t i = 0; i < packets.size(); i += 188) {
                            if (i < keyUnitStartPos) {
                                int p = extract_ts_header_pid(&packets[i]);
                                if ((p == 0 || p == pat.first_pmt.pmt_pid) && bringState < 2) {
                                    bringState = p == 0 ? 1 : bringState == 1 ? 2 : bringState;
                                    // Already inserted
                                }
                                else {
                                    auto it = unitStartMap.find(p);
                                    if (it == unitStartMap.end() || i < std::min(it->second.first, it->second.second)) {
                                        segBuf.insert(segBuf.end(), packets.begin() + i, packets.begin() + i + 188);
                                    }
                                    else {
                                        backPackets.insert(backPackets.end(), packets.begin() + i, packets.begin() + i + 188);
                                    }
                                }
                            }
                            else {
                                backPackets.insert(backPackets.end(), packets.begin() + i, packets.begin() + i + 188);
                            }
                        }
                    }
                    else {
                        // Packets have been accumulated over the limit, simply segment everything.
                        segBuf.insert(segBuf.end(), packets.begin(), packets.end());
                        ++forcedSegmentationError;
                    }
                    packets.swap(backPackets);

                    WriteSegmentHeader(segBuf, seg.segCount);

                    SEGMENT_CONTEXT &segfr = segments.front();
                    std::vector<uint8_t> &segfrBuf =
                        !segfr.backBuf.empty() || segfr.pipes[0].connected || segfr.pipes[1].connected ? segfr.backBuf : segfr.buf;
                    AssignSegmentList(segfrBuf, segments, segIndex);

                    lastPts45khz = pts45khz;
                    targetDurationMsec = nextTargetDurationMsec;
                }
                unitStartMap.clear();
            }
            packets.insert(packets.end(), packet, packet + 188);
        }
        id3conv.ClearPackets();

        if (bufCount >= 188 && bufCount % 188 != 0) {
            std::copy(buf + bufCount / 188 * 188, buf + bufCount, buf);
        }
        bufCount %= 188;
    }

    if (syncError) {
        fprintf(stderr, "Warning: %u sync error happened.\n", syncError);
    }
    if (forcedSegmentationError) {
        fprintf(stderr, "Warning: %u forced segmentation happened.\n", forcedSegmentationError);
    }
    while (accessTimeoutMsec != 0 && !CheckAccessTimeout(GetMsecTick(), lastAccessTicks, threads.size(), accessTimeoutMsec)) {
        Sleep(100);
    }
    SetEvent(events.front());
    while (!threads.empty()) {
        threads.back().join();
        threads.pop_back();
    }
    ClearSegmentsAndEvents(segments, events);
    return 0;
}
