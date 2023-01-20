#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <io.h>
#include <stdexcept>
#else
#include <errno.h>
#include <signal.h>
#include <sys/select.h>
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
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include "mp4fragmenter.hpp"
#include "util.hpp"

namespace
{
constexpr uint32_t SEGMENT_COUNT_EMPTY = 0x1000000;
constexpr size_t SEGMENTS_MAX = 100;
// Maximum number of fragments per segment (38 is the configurable maximum)
constexpr size_t MP4_FRAG_MAX_NUM = 20;

using lock_recursive_mutex = std::lock_guard<std::recursive_mutex>;

class CManualResetEvent
{
public:
#ifdef _WIN32
    CManualResetEvent(bool initialState = false) {
        m_h = CreateEvent(nullptr, TRUE, initialState, nullptr);
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
    int segDurationMsec;
    int64_t segTimeMsec;
    std::vector<int> fragDurationsMsec;
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
                    {
                        lock_recursive_mutex lock(bufLock);
                        pipe.connected = true;
                    }
#if defined(F_GETPIPE_SZ) && defined(F_SETPIPE_SZ)
                    int pipeBufSize = fcntl(pipe.fd, F_GETPIPE_SZ);
                    if (pipeBufSize > 0 && pipeBufSize < static_cast<int>(it->buf.size() / 2)) {
                        // Buffer is too small, expand up to 5 times.
                        fcntl(pipe.fd, F_SETPIPE_SZ, std::min(static_cast<int>(it->buf.size()), pipeBufSize * 5));
                    }
#endif
                }
            }
            connected = connected || pipe.connected;
        }

        // Sleep for 50 msec
        tick += 50;

        while (connected) {
            connected = false;
            fd_set wfd;
            FD_ZERO(&wfd);
            int maxfd = -1;
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
                        maxfd = std::max(maxfd, pipe.fd);
                        if (maxfd < FD_SETSIZE) {
                            FD_SET(pipe.fd, &wfd);
                        }
                    }
                    else {
                        close(pipe.fd);

                        lock_recursive_mutex lock(bufLock);
                        pipe.connected = false;
                    }
                }
            }
            if (connected) {
                if (maxfd < FD_SETSIZE) {
                    // Wait for writable
                    timeval tv = {};
                    tv.tv_usec = static_cast<long>(std::max<int64_t>(tick - GetMsecTick(), 0) * 1000);
                    if (tv.tv_usec <= 0 || tv.tv_usec >= 1000000 ||
                        select(maxfd + 1, &wfd, nullptr, nullptr, &tv) < 0 ||
                        stopEvent.WaitOne(std::chrono::milliseconds(0))) {
                        break;
                    }
                }
                else {
                    // Sleep a little
                    if (GetMsecTick() >= tick || stopEvent.WaitOne(std::chrono::milliseconds(1))) {
                        break;
                    }
                }
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

void AssignSegmentList(std::vector<uint8_t> &buf, const std::vector<SEGMENT_CONTEXT> &segments, size_t segIndex,
                       bool endList, bool incomplete, bool isMp4, const std::vector<uint8_t> &mp4Header)
{
    buf.assign(segments.size() * 16, 0);
    WriteUint32(&buf[0], static_cast<uint32_t>(segments.size() - 1));
    WriteUint32(&buf[4], GetCurrentUnixTime());
    buf[8] = endList;
    buf[9] = incomplete;
    buf[10] = isMp4;
    for (size_t i = segIndex, j = 1; j < segments.size(); ++j) {
        WriteUint32(&buf[j * 16], static_cast<uint32_t>(i));
        WriteUint32(&buf[j * 16 + 2], static_cast<uint32_t>(segments[i].fragDurationsMsec.size()));
        WriteUint32(&buf[j * 16 + 4], segments[i].segCount);
        WriteUint32(&buf[j * 16 + 8], segments[i].segDurationMsec);
        WriteUint32(&buf[j * 16 + 12], static_cast<uint32_t>(segments[i].segTimeMsec / 10));
        for (size_t k = 0; k < segments[i].fragDurationsMsec.size(); ++k) {
            buf.insert(buf.end(), 16, 0);
            WriteUint32(&buf[buf.size() - 16], segments[i].fragDurationsMsec[k]);
        }
        i = i % (segments.size() - 1) + 1;
    }
    buf.insert(buf.end(), mp4Header.begin(), mp4Header.end());
    WriteUint32(&buf[12], static_cast<uint32_t>(buf.size() - segments.size() * 16));
}

void WriteSegmentHeader(std::vector<uint8_t> &buf, uint32_t segCount, bool isMp4, const std::vector<size_t> &fragSizes)
{
    // NULL TS header
    buf[0] = 0x47;
    buf[1] = 0x01;
    buf[2] = 0xff;
    buf[3] = 0x10;
    WriteUint32(&buf[4], segCount);
    WriteUint32(&buf[8], static_cast<uint32_t>((buf.size() - 188) / (isMp4 ? 1 : 188)));
    buf[12] = isMp4;
    if (isMp4) {
        size_t remainSize = buf.size() - 188;
        size_t i = 0;
        for (; i + 1 < std::min(fragSizes.size(), MP4_FRAG_MAX_NUM) && remainSize >= fragSizes[i]; ++i) {
            WriteUint32(&buf[i * 4 + 32], static_cast<uint32_t>(fragSizes[i]));
            remainSize -= fragSizes[i];
        }
        WriteUint32(&buf[i * 4 + 32], static_cast<uint32_t>(remainSize));
    }
}

std::vector<uint8_t> &SelectWritableSegmentBuffer(SEGMENT_CONTEXT &seg)
{
    return !seg.backBuf.empty() || seg.pipes[0].connected || seg.pipes[1].connected ? seg.backBuf : seg.buf;
}

void ProcessSegmentation(FILE *fp, bool enableFragmentation, uint32_t targetDurationMsec, uint32_t nextTargetDurationMsec,
                         uint32_t targetFragDurationMsec, size_t segMaxBytes, size_t fragMaxBytes, unsigned int &syncError,
                         const std::function<bool (int64_t)> &onRead,
                         const std::function<bool (bool, bool, int64_t, const PMT &, std::vector<uint8_t> &)> &onSegmentOrFragment)
{
    // PID of the packet to determine segmentation (AVC_VIDEO or H_265_VIDEO or audio stream)
    int keyPid = 0;
    // AVC-NAL's parsing state
    int nalState = 0;

    struct UNIT_START_POSITION
    {
        size_t lastPos;
        // The last unit-start immediately before "keyPid" unit-start
        size_t beforeKeyStart;
        // The last unit-start immediately before "keyPid" unit-start marked for fragmentation
        size_t beforeMarkedKeyStart;
    };
    // Map of PID and unit-start position
    std::unordered_map<int, UNIT_START_POSITION> unitStartMap;
    // Packets accumulating for next segmentation
    std::vector<uint8_t> packets;
    std::vector<uint8_t> backPackets;
    std::vector<uint8_t> workPackets;

    size_t segBytes = 0;
    int64_t pts = -1;
    int64_t lastSegPts = -1;
    int64_t lastFragPts = -1;
    // PTS marking for fragmentation
    int64_t markedFragPts = -1;
    bool firstAudioPacketArrived = false;
    bool isFirstKey = true;
    PAT pat = {};
    uint8_t buf[188 * 16];
    size_t bufCount = 0;
    size_t nRead;

    while ((nRead = fread(buf + bufCount, 1, sizeof(buf) - bufCount, fp)) != 0) {
        bufCount += nRead;

        if (onRead) {
            int64_t ptsDiff = (0x200000000 + pts - lastSegPts) & 0x1ffffffff;
            if (ptsDiff >= 0x100000000) {
                // PTS went back.
                ptsDiff = 0;
            }
            if (onRead(ptsDiff)) {
                break;
            }
        }

        for (const uint8_t *packet = buf; packet < buf + sizeof(buf) && packet + 188 <= buf + bufCount; packet += 188) {
            if (extract_ts_header_sync(packet) != 0x47) {
                // Resynchronization is not implemented.
                ++syncError;
                continue;
            }
            int unitStart = extract_ts_header_unit_start(packet);
            int pid = extract_ts_header_pid(packet);
            int counter = extract_ts_header_counter(packet);
            if (unitStart) {
                UNIT_START_POSITION unitStartPos = {SIZE_MAX, SIZE_MAX, SIZE_MAX};
                unitStartMap.emplace(pid, unitStartPos).first->second.lastPos = packets.size();
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
            else if (pid == pat.first_pmt.first_video_pid) {
                if (unitStart) {
                    keyPid = pid;
                }
            }
            else if (pid == pat.first_pmt.first_adts_audio_pid) {
                if (unitStart && pat.first_pmt.first_video_pid == 0) {
                    keyPid = pid;
                }
                firstAudioPacketArrived = true;
            }

            if (keyPid != 0 && pid == keyPid &&
                (pid == pat.first_pmt.first_adts_audio_pid ||
                 (pid == pat.first_pmt.first_video_pid &&
                  (pat.first_pmt.first_video_stream_type == AVC_VIDEO ||
                   pat.first_pmt.first_video_stream_type == H_265_VIDEO)))) {
                bool h265 = pat.first_pmt.first_video_stream_type == H_265_VIDEO;
                if (unitStart) {
                    bool markForFrag = false;
                    int64_t ptsDiff = (0x200000000 + pts - lastFragPts) & 0x1ffffffff;
                    // Defer fragmentation until the arrival of first audio packet.
                    if ((pat.first_pmt.first_adts_audio_pid == 0 || firstAudioPacketArrived) &&
                        markedFragPts < 0 && lastFragPts >= 0 &&
                        (ptsDiff < 0x100000000 ? ptsDiff : 0) / 90 >= targetFragDurationMsec)
                    {
                        markForFrag = true;
                        markedFragPts = pts;
                    }

                    for (auto it = unitStartMap.begin(); it != unitStartMap.end(); ++it) {
                        it->second.beforeKeyStart = it->second.lastPos;
                        if (markForFrag) {
                            it->second.beforeMarkedKeyStart = it->second.beforeKeyStart;
                        }
                    }
                    if (payloadSize >= 9 && payload[0] == 0 && payload[1] == 0 && payload[2] == 1) {
                        int ptsDtsFlags = payload[7] >> 6;
                        int pesHeaderLength = payload[8];
                        if (ptsDtsFlags >= 2 && payloadSize >= 14) {
                            pts = get_pes_timestamp(payload + 9);
                            if (lastSegPts < 0) {
                                lastSegPts = pts;
                                lastFragPts = pts;
                            }
                        }
                        if (pid == pat.first_pmt.first_video_pid) {
                            nalState = 0;
                            if (9 + pesHeaderLength < payloadSize) {
                                if (contains_nal_irap(&nalState, payload + 9 + pesHeaderLength, payloadSize - (9 + pesHeaderLength), h265)) {
                                    isKey = !isFirstKey;
                                    isFirstKey = false;
                                }
                            }
                        }
                        else {
                            // Always treat as key.
                            isKey = !isFirstKey;
                            isFirstKey = false;
                        }
                    }
                }
                else if (pid == pat.first_pmt.first_video_pid) {
                    if (contains_nal_irap(&nalState, payload, payloadSize, h265)) {
                        isKey = !isFirstKey;
                        isFirstKey = false;
                    }
                }
            }

            bool forceSegment = (segMaxBytes != 0 && packets.size() + segBytes + 188 > segMaxBytes) ||
                                packets.size() + 188 > fragMaxBytes;
            // Avoid making the last fragment too small.
            int64_t markedPtsDiff = (0x200000000 + pts - markedFragPts) & 0x1ffffffff;
            bool createFragment = enableFragmentation && markedFragPts >= 0 &&
                                  (markedPtsDiff < 0x100000000 ? markedPtsDiff : 0) / 90 >= targetFragDurationMsec / 4;
            if (isKey || forceSegment || createFragment) {
                int64_t ptsDiff = (0x200000000 + pts - lastSegPts) & 0x1ffffffff;
                if (ptsDiff >= 0x100000000) {
                    // PTS went back, rare case.
                    ptsDiff = 0;
                }
                bool isSegmentKey = isKey && ptsDiff >= targetDurationMsec * 90;
                if (isSegmentKey || forceSegment || createFragment) {
                    workPackets.clear();
                    backPackets.clear();

                    if (isKey || !forceSegment) {
                        size_t keyUnitStartPos = isKey ? unitStartMap[keyPid].beforeKeyStart :
                            unitStartMap[keyPid].beforeMarkedKeyStart;
                        // Bring PAT and PMT to the front
                        int bringState = 0;
                        for (size_t i = 0; i < packets.size() && i < keyUnitStartPos && bringState < 2; i += 188) {
                            int p = extract_ts_header_pid(&packets[i]);
                            if (p == 0 || p == pat.first_pmt.pmt_pid) {
                                bringState = p == 0 ? 1 : bringState == 1 ? 2 : bringState;
                                workPackets.insert(workPackets.end(), packets.begin() + i, packets.begin() + i + 188);
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
                                    if (it == unitStartMap.end() ||
                                        i < std::min(it->second.lastPos, isKey ? it->second.beforeKeyStart : it->second.beforeMarkedKeyStart)) {
                                        workPackets.insert(workPackets.end(), packets.begin() + i, packets.begin() + i + 188);
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
                        workPackets.assign(packets.begin(), packets.end());
                    }
                    packets.swap(backPackets);

                    if (!isSegmentKey && !forceSegment) {
                        // fragment
                        lastFragPts = markedFragPts;
                        segBytes += workPackets.size();
                    }
                    else {
                        // segment
                        lastFragPts = pts;
                        lastSegPts = pts;
                        targetDurationMsec = nextTargetDurationMsec;
                        segBytes = 0;
                    }
                    markedFragPts = -1;

                    if (onSegmentOrFragment(isSegmentKey, forceSegment, ptsDiff, pat.first_pmt, workPackets)) {
                        return;
                    }
                    unitStartMap.clear();
                }
            }
            packets.insert(packets.end(), packet, packet + 188);
        }

        if (bufCount >= 188 && bufCount % 188 != 0) {
            std::copy(buf + bufCount / 188 * 188, buf + bufCount, buf);
        }
        bufCount %= 188;
    }
}
}

int main(int argc, char **argv)
{
    bool isMp4 = false;
    uint32_t targetDurationMsec = 1000;
    uint32_t nextTargetDurationMsec = 2000;
    uint32_t targetFragDurationMsec = 500;
    uint32_t accessTimeoutMsec = 10000;
    const char *closingCmd = "";
    int readRatePerMille = -1;
    int nextReadRatePerMille = 0;
    size_t segNum = 8;
    size_t segMaxBytes = 4096 * 1024;
    const char *destName = "";
    CMp4Fragmenter mp4frag;

    for (int i = 1; i < argc; ++i) {
        char c = '\0';
        if (argv[i][0] == '-' && argv[i][1] && !argv[i][2]) {
            c = argv[i][1];
        }
        if (c == 'h') {
            fprintf(stderr, "Usage: tsmemseg [-4][-i inittime][-t time][-p ptime][-a acc_timeout][-c cmd][-r readrate][-f fill_readrate][-s seg_num][-m max_kbytes] seg_name\n");
            return 2;
        }
        bool invalid = false;
        if (i < argc - 1) {
            if (c == '4') {
                isMp4 = true;
            }
            else if (c == 'i' || c == 't' || c == 'p') {
                double sec = strtod(argv[++i], nullptr);
                invalid = !(0 <= sec && sec <= 60);
                if (!invalid) {
                    uint32_t &msec = c == 'i' ? targetDurationMsec : c == 't' ? nextTargetDurationMsec : targetFragDurationMsec;
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
        }
        else {
            destName = argv[i];
            for (size_t j = 0; destName[j]; ++j) {
                c = destName[j];
                if (j >= 65 ||
                    ((j > 0 || c != '-' || destName[1]) &&
                     (c < '0' || '9' < c) && (c < 'A' || 'Z' < c) && (c < 'a' || 'z' < c) && c != '_')) {
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

    if (destName[0] == '-') {
        FILE *wfp = stdout;
#ifdef _WIN32
        if (_setmode(_fileno(wfp), _O_BINARY) < 0) {
            fprintf(stderr, "Error: _setmode.\n");
            return 1;
        }
#endif
        unsigned int syncError = 0;
        unsigned int forcedSegmentationError = 0;
        bool wroteHeader = false;

        ProcessSegmentation(fp, isMp4, targetDurationMsec, nextTargetDurationMsec, targetFragDurationMsec, 0, segMaxBytes, syncError, nullptr,
            [&, wfp, isMp4](bool isKey, bool forceSegment, int64_t ptsDiff, const PMT &pmt, std::vector<uint8_t> &packets) -> bool
        {
            static_cast<void>(ptsDiff);

            if (!isKey && forceSegment) {
                ++forcedSegmentationError;
            }
            if (isMp4) {
                mp4frag.AddPackets(packets, pmt, !isKey && forceSegment);
                if (!wroteHeader && !mp4frag.GetHeader().empty()) {
                    wroteHeader = true;
                    if (fwrite(mp4frag.GetHeader().data(), 1, mp4frag.GetHeader().size(), wfp) != mp4frag.GetHeader().size()) {
                        return true;
                    }
                }
                if (fwrite(mp4frag.GetFragments().data(), 1, mp4frag.GetFragments().size(), wfp) != mp4frag.GetFragments().size()) {
                    return true;
                }
                mp4frag.ClearFragments();
            }
            else {
                if (fwrite(packets.data(), 1, packets.size(), wfp) != packets.size()) {
                    return true;
                }
            }
            fflush(wfp);
            return false;
        });

        if (syncError) {
            fprintf(stderr, "Warning: %u sync error happened.\n", syncError);
        }
        if (forcedSegmentationError) {
            fprintf(stderr, "Warning: %u forced segmentation happened.\n", forcedSegmentationError);
        }
        return 0;
    }

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
            WriteSegmentHeader(seg.buf, seg.segCount, isMp4, mp4frag.GetFragmentSizes());
        }
        segments.push_back(std::move(seg));
    }
    if (segments.size() < 1 + segNum) {
        CloseSegments(segments);
        fprintf(stderr, "Error: pipe/fifo creation failed.\n");
        return 1;
    }
    AssignSegmentList(segments.front().buf, segments, 1, false, false, isMp4, mp4frag.GetHeader());

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
    // The last segment is incomplete
    bool segIncomplete = false;

    unsigned int syncError = 0;
    unsigned int forcedSegmentationError = 0;
    int64_t entireDurationMsec = 0;
    int64_t entireDurationFromBaseMsec = 0;
    int64_t durationMsecResidual = 0;

    ProcessSegmentation(fp, isMp4, targetDurationMsec, nextTargetDurationMsec, targetFragDurationMsec, segMaxBytes, segMaxBytes, syncError,
        [&, accessTimeoutMsec, nextReadRatePerMille](int64_t ptsDiff) -> bool
    {
        for (;;) {
            int64_t nowTick = GetMsecTick();
            if (accessTimeoutMsec != 0 && static_cast<uint32_t>(nowTick) - lastAccessTick >= accessTimeoutMsec) {
                return true;
            }
            if (readRatePerMille != nextReadRatePerMille &&
                std::find_if(segments.begin() + 1, segments.end(),
                    [](const SEGMENT_CONTEXT &a) { return a.segCount == SEGMENT_COUNT_EMPTY; }) == segments.end()) {
                // All segments are not empty
                readRatePerMille = nextReadRatePerMille;
                // Rebase
                baseTick = nowTick;
                entireDurationFromBaseMsec = 0;
            }
            if (readRatePerMille > 0) {
                // Check reading speed
                if (entireDurationFromBaseMsec + ptsDiff / 90 > (nowTick - baseTick) * readRatePerMille / 1000) {
                    // Too fast
                    SleepFor(std::chrono::milliseconds(10));
                    continue;
                }
            }
            break;
        }
        return false;
    },
        [&, isMp4, segNum](bool isKey, bool forceSegment, int64_t ptsDiff, const PMT &pmt, std::vector<uint8_t> &packets) -> bool
    {
        if (isMp4) {
            mp4frag.AddPackets(packets, pmt, !isKey && forceSegment);
        }

        lock_recursive_mutex lock(bufLock);

        SEGMENT_CONTEXT &seg = segments[segIncomplete ? (segIndex + segNum - 2) % segNum + 1 : segIndex];
        if (!segIncomplete) {
            segIndex = segIndex % segNum + 1;
            seg.segCount = (++segCount) & 0xffffff;
        }
        segIncomplete = !isKey && !forceSegment;
        seg.segDurationMsec = static_cast<int>((ptsDiff + durationMsecResidual) / 90);
        seg.segTimeMsec = entireDurationMsec;
        if (!segIncomplete) {
            durationMsecResidual = (ptsDiff + durationMsecResidual) % 90;
            entireDurationMsec += seg.segDurationMsec;
            entireDurationFromBaseMsec += seg.segDurationMsec;
        }

        std::vector<uint8_t> &segBuf = SelectWritableSegmentBuffer(seg);
        segBuf.assign(188, 0);

        if (isMp4) {
            seg.fragDurationsMsec = mp4frag.GetFragmentDurationsMsec();
            // Limit the total number of fragments
            size_t undeterminedSize = 0;
            for (size_t i = seg.fragDurationsMsec.size(); i >= MP4_FRAG_MAX_NUM; --i) {
                if (segIncomplete) {
                    undeterminedSize += mp4frag.GetFragmentSizes()[i - 1];
                }
                if (i > MP4_FRAG_MAX_NUM) {
                    seg.fragDurationsMsec[i - 2] += seg.fragDurationsMsec.back();
                    seg.fragDurationsMsec.pop_back();
                }
                else if (segIncomplete) {
                    // In incomplete state, duration of the limited fragment is undetermined, remote it too
                    seg.fragDurationsMsec.pop_back();
                }
            }
            segBuf.insert(segBuf.end(), mp4frag.GetFragments().begin(), mp4frag.GetFragments().end() - undeterminedSize);
        }
        else {
            segBuf.insert(segBuf.end(), packets.begin(), packets.end());
        }

        WriteSegmentHeader(segBuf, seg.segCount, isMp4, mp4frag.GetFragmentSizes());
        if (!segIncomplete) {
            mp4frag.ClearFragments();
        }
        std::vector<uint8_t> &segfrBuf = SelectWritableSegmentBuffer(segments.front());
        AssignSegmentList(segfrBuf, segments, segIndex, false, segIncomplete, isMp4, mp4frag.GetHeader());
        return false;
    });

    {
        lock_recursive_mutex lock(bufLock);

        // End list
        std::vector<uint8_t> &segfrBuf = SelectWritableSegmentBuffer(segments.front());
        AssignSegmentList(segfrBuf, segments, segIndex, true, false, isMp4, mp4frag.GetHeader());
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
