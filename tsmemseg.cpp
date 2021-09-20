#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <io.h>
#include <stdexcept>
#else
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <condition_variable>
#endif
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
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

class CManualResetEvent
{
public:
#ifdef _WIN32
    CManualResetEvent(bool initialState = false) {
        m_h = CreateEvent(nullptr, FALSE, initialState, nullptr);
        if (!m_h) throw std::runtime_error("");
    }
    ~CManualResetEvent() { CloseHandle(m_h); }
    void Set() { SetEvent(m_h); }
    HANDLE Handle() { return m_h; }
    bool WaitOne(std::chrono::milliseconds rel) {
        return WaitForSingleObject(m_h, static_cast<DWORD>(rel.count())) == WAIT_OBJECT_0;
    }
#else
    CManualResetEvent() : m_state(false) {}
    void Set() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_state = true;
        }
        m_cond.notify_all();
    }
    bool WaitOne(std::chrono::milliseconds rel) {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_cond.wait_for(lock, rel, [this]() { return m_state; });
    }
#endif
    CManualResetEvent(const CManualResetEvent &) = delete;
    CManualResetEvent &operator=(const CManualResetEvent &) = delete;

private:
#ifdef _WIN32
    HANDLE m_h;
#else
    bool m_state;
    std::mutex m_mutex;
    std::condition_variable m_cond;
#endif
};

struct SEGMENT_PIPE_CONTEXT
{
#ifdef _WIN32
    HANDLE h;
    OVERLAPPED ol;
    bool initialized;
#else
    int fd;
    size_t written;
#endif
    bool connected;
};

struct SEGMENT_CONTEXT
{
    char path[128];
    SEGMENT_PIPE_CONTEXT pipes[2];
    std::vector<uint8_t> buf;
    std::vector<uint8_t> backBuf;
    uint32_t segCount;
    uint32_t segDurationMsec;
};

void SleepFor(std::chrono::milliseconds rel)
{
#ifdef _WIN32
    // MSVC sleep_for() is buggy
    Sleep(static_cast<DWORD>(rel.count()));
#else
    std::this_thread::sleep_for(rel);
#endif
}

int64_t GetMsecTick()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

uint32_t GetCurrentUnixTime()
{
    return static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

void ClosingRunner(const char *closingCmd, CManualResetEvent &stopEvent, std::atomic_uint32_t &lastAccessTick, uint32_t accessTimeoutMsec)
{
    while (accessTimeoutMsec == 0 || static_cast<uint32_t>(GetMsecTick()) - lastAccessTick < accessTimeoutMsec) {
        if (stopEvent.WaitOne(std::chrono::milliseconds(1000))) {
            break;
        }
    }
    system(closingCmd);
}

#ifdef _WIN32
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
#else
void Worker(std::vector<SEGMENT_CONTEXT> &segments, CManualResetEvent &stopEvent, std::recursive_mutex &bufLock, std::atomic_uint32_t &lastAccessTick)
{
    for (;;) {
        int64_t tick = GetMsecTick();
        bool connected = false;
        for (auto it = segments.begin(); it != segments.end(); ++it) {
            SEGMENT_PIPE_CONTEXT &pipe = it->pipes[0];
            if (!pipe.connected) {
                {
                    lock_recursive_mutex lock(bufLock);

                    // it->backBuf is used only when it->buf is in use, so this will be the rare case.
                    if (!it->backBuf.empty()) {
                        // Swap and clear the back buffer.
                        it->buf.swap(it->backBuf);
                        std::vector<uint8_t>().swap(it->backBuf);
                    }
                }
                // Start connecting
                pipe.fd = open(it->path, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
                if (pipe.fd >= 0) {
                    lastAccessTick = static_cast<uint32_t>(tick);
                    pipe.written = 0;

                    lock_recursive_mutex lock(bufLock);
                    pipe.connected = true;
                }
            }
            connected = connected || pipe.connected;
        }

        // Sleep for 50 msec
        tick += 50;

        while (connected) {
            connected = false;
            for (auto it = segments.begin(); it != segments.end(); ++it) {
                SEGMENT_PIPE_CONTEXT &pipe = it->pipes[0];
                if (pipe.connected) {
                    ssize_t n = 0;
                    while (pipe.written < it->buf.size() &&
                           (n = write(pipe.fd, it->buf.data() + pipe.written, it->buf.size() - pipe.written)) > 0) {
                        pipe.written += n;
                    }
                    if (pipe.written < it->buf.size() && n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        connected = true;
                    }
                    else {
                        close(pipe.fd);

                        lock_recursive_mutex lock(bufLock);
                        pipe.connected = false;
                    }
                }
            }
            // Sleep a little
            if (GetMsecTick() >= tick || stopEvent.WaitOne(std::chrono::milliseconds(1))) {
                break;
            }
        }
        if (stopEvent.WaitOne(std::chrono::milliseconds(std::max<int64_t>(tick - GetMsecTick(), 1)))) {
            break;
        }
    }

    // Close all files
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        if (it->pipes[0].connected) {
            close(it->pipes[0].fd);
        }
    }
}
#endif

void CloseSegments(const std::vector<SEGMENT_CONTEXT> &segments)
{
    for (auto it = segments.rbegin(); it != segments.rend(); ++it) {
#ifdef _WIN32
        for (size_t i = 0; i < 2; ++i) {
            if (it->pipes[i].h != INVALID_HANDLE_VALUE) {
                CloseHandle(it->pipes[i].h);
            }
        }
#else
        unlink(it->path);
#endif
    }
}

#ifndef _WIN32
const std::vector<SEGMENT_CONTEXT> *g_signalParam;

void SignalHandler(int signum)
{
    // Unlink all fifo files.
    CloseSegments(*g_signalParam);

    struct sigaction sigact = {};
    sigact.sa_handler = SIG_DFL;
    sigaction(signum, &sigact, nullptr);
    raise(signum);
}
#endif

void WriteUint32(uint8_t *buf, uint32_t n)
{
    buf[0] = static_cast<uint8_t>(n);
    buf[1] = static_cast<uint8_t>(n >> 8);
    buf[2] = static_cast<uint8_t>(n >> 16);
    buf[3] = static_cast<uint8_t>(n >> 24);
}

void AssignSegmentList(std::vector<uint8_t> &buf, const std::vector<SEGMENT_CONTEXT> &segments, size_t segIndex, bool endList)
{
    buf.assign(segments.size() * 16, 0);
    WriteUint32(&buf[0], static_cast<uint32_t>(segments.size() - 1));
    WriteUint32(&buf[4], GetCurrentUnixTime());
    buf[8] = endList;
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
    const char *closingCmd = "";
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
            fprintf(stderr, "Usage: tsmemseg [-i inittime][-t time][-a acc_timeout][-c cmd][-r readrate][-f fill_readrate][-s seg_num][-m max_kbytes][-d flags] seg_name\n");
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
            else if (c == 'c') {
                closingCmd = argv[++i];
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
#ifdef _WIN32
    if (_setmode(_fileno(fp), _O_BINARY) < 0) {
        fprintf(stderr, "Error: _setmode.\n");
        return 1;
    }
#endif
#endif

    // segments.front() is segment list, the others are segments.
    std::vector<SEGMENT_CONTEXT> segments;
    CManualResetEvent stopEvent;
#ifdef _WIN32
    // Used for asynchronous writing of segments.
    std::vector<std::unique_ptr<CManualResetEvent>> events;
#endif

    while (segments.size() < 1 + segNum) {
        SEGMENT_CONTEXT seg = {};
#ifdef _WIN32
        sprintf(seg.path, "\\\\.\\pipe\\tsmemseg_%s%02d", destName, static_cast<int>(segments.size()));
        // Create 2 pipes for simultaneous access
        size_t createdCount = 0;
        for (; createdCount < 2; ++createdCount) {
            events.emplace_back(new CManualResetEvent(true));
            seg.pipes[createdCount].h = CreateNamedPipeA(seg.path, PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED, 0, 2, 48128, 0, 0, nullptr);
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
#else
        sprintf(seg.path, "/tmp/tsmemseg_%s%02d.fifo", destName, static_cast<int>(segments.size()));
        if (mkfifo(seg.path, S_IRWXU) != 0) {
            break;
        }
#endif
        seg.segCount = SEGMENT_COUNT_EMPTY;
        if (!segments.empty()) {
            seg.buf.assign(188, 0);
            WriteSegmentHeader(seg.buf, seg.segCount);
        }
        segments.push_back(std::move(seg));
    }
    if (segments.size() < 1 + segNum) {
        CloseSegments(segments);
        fprintf(stderr, "Error: pipe/fifo creation failed.\n");
        return 1;
    }
    AssignSegmentList(segments.front().buf, segments, 1, false);

#ifndef _WIN32
    struct sigaction sigact = {};
    sigact.sa_handler = SignalHandler;
    g_signalParam = &segments;
    sigaction(SIGHUP, &sigact, nullptr);
    sigaction(SIGINT, &sigact, nullptr);
    sigaction(SIGTERM, &sigact, nullptr);
    sigact.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sigact, nullptr);
#endif

    int64_t baseTick = GetMsecTick();
    std::recursive_mutex bufLock;
    std::thread closingRunnerThread;
    std::vector<std::thread> threads;
    std::atomic_uint32_t lastAccessTick(static_cast<uint32_t>(baseTick));

    if (closingCmd[0]) {
        closingRunnerThread = std::thread(ClosingRunner, closingCmd, std::ref(stopEvent), std::ref(lastAccessTick), accessTimeoutMsec);
    }

#ifdef _WIN32
    // Create a thread for every 20 segments
    for (size_t i = 0; i < segments.size(); i += 20) {
        std::vector<HANDLE> eventsForThread;
        eventsForThread.push_back(stopEvent.Handle());
        for (size_t j = i * 2; j < (i + 20) * 2 && j < events.size(); ++j) {
            eventsForThread.push_back(events[j]->Handle());
        }
        threads.emplace_back(Worker, segments.data() + i, std::move(eventsForThread), std::ref(bufLock), std::ref(lastAccessTick));
    }
#else
    // Use one thread
    threads.emplace_back(Worker, std::ref(segments), std::ref(stopEvent), std::ref(bufLock), std::ref(lastAccessTick));
#endif

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
            if (accessTimeoutMsec != 0 && static_cast<uint32_t>(nowTick) - lastAccessTick >= accessTimeoutMsec) {
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
                    SleepFor(std::chrono::milliseconds(10));
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
                    AssignSegmentList(segfrBuf, segments, segIndex, false);

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

    {
        lock_recursive_mutex lock(bufLock);

        // End list
        SEGMENT_CONTEXT &segfr = segments.front();
        std::vector<uint8_t> &segfrBuf =
            !segfr.backBuf.empty() || segfr.pipes[0].connected || segfr.pipes[1].connected ? segfr.backBuf : segfr.buf;
        AssignSegmentList(segfrBuf, segments, segIndex, true);
    }

    if (syncError) {
        fprintf(stderr, "Warning: %u sync error happened.\n", syncError);
    }
    if (forcedSegmentationError) {
        fprintf(stderr, "Warning: %u forced segmentation happened.\n", forcedSegmentationError);
    }
    while (accessTimeoutMsec != 0 && static_cast<uint32_t>(GetMsecTick()) - lastAccessTick < accessTimeoutMsec) {
        SleepFor(std::chrono::milliseconds(100));
    }
    stopEvent.Set();
    while (!threads.empty()) {
        threads.back().join();
        threads.pop_back();
    }
    if (closingRunnerThread.joinable()) {
        closingRunnerThread.join();
    }
    CloseSegments(segments);
    return 0;
}
