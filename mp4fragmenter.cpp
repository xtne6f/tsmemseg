#include "mp4fragmenter.hpp"
#include <algorithm>

namespace
{
const uint8_t RESERVED_0 = 0;
const uint8_t PRE_DEFINED_0 = 0;

void PushUshort(std::vector<uint8_t> &data, uint32_t n)
{
    data.push_back((n >> 8) & 0xff);
    data.push_back(n & 0xff);
}

void PushUint(std::vector<uint8_t> &data, uint32_t n)
{
    PushUshort(data, n >> 16);
    PushUshort(data, n & 0xffff);
}

void PushUint64(std::vector<uint8_t> &data, uint64_t n)
{
    PushUint(data, n >> 32);
    PushUint(data, n & 0xffffffff);
}

void PushString(std::vector<uint8_t> &data, const char *s)
{
    for (size_t i = 0; s[i]; ++i) {
        data.push_back(s[i]);
    }
}

void WriteUint(uint8_t *data, uint32_t n)
{
    data[0] = (n >> 24) & 0xff;
    data[1] = (n >> 16) & 0xff;
    data[2] = (n >> 8) & 0xff;
    data[3] = n & 0xff;
}

template<class P>
void PushBox(std::vector<uint8_t> &data, const char *type, P pushProc, uint32_t flagsOrBox = 0xffffffff)
{
    size_t i = data.size();
    PushUint(data, 0);
    PushString(data, type);
    if (flagsOrBox != 0xffffffff) {
        PushUint(data, flagsOrBox);
    }
    pushProc(data);
    WriteUint(&data[i], static_cast<uint32_t>(data.size() - i));
}

template<class P>
void PushFullBox(std::vector<uint8_t> &data, const char *type, uint32_t flags, P pushProc)
{
    PushBox(data, type, pushProc, flags);
}

template<class P>
void ParseNals(const uint8_t *payload, size_t payloadSize, P onNalProc)
{
    size_t nalPos = 0;
    for (size_t i = 2;;) {
        if (i >= payloadSize || (payload[i] == 1 && payload[i - 1] == 0 && payload[i - 2] == 0)) {
            if (nalPos != 0) {
                onNalProc(payload + nalPos, (i >= payloadSize ? payloadSize : i - 2 - (payload[i - 3] == 0)) - nalPos);
            }
            if (i >= payloadSize) {
                break;
            }
            nalPos = i + 1;
            i += 3;
        }
        else if (payload[i] > 0) {
            i += 3;
        }
        else {
            ++i;
        }
    }
}

std::vector<uint8_t> EbspToRbsp(const std::vector<uint8_t> &src)
{
    std::vector<uint8_t> dest;
    for (size_t i = 0; i < src.size(); ++i) {
        if (i < 2 || i + 1 == src.size() || src[i - 2] != 0 || src[i - 1] != 0 || src[i] != 3 || src[i + 1] > 3) {
            dest.push_back(src[i]);
        }
    }
    return dest;
}

int ReadUegBits(const uint8_t *data, size_t &pos)
{
    // Reads up to 61bits
    for (int n = 0; n < 31; ++n) {
        if (read_bool(data, pos)) {
            return read_bits(data, pos, n) - 1 + (1 << n);
        }
    }
    return 0;
}

int ReadSegBits(const uint8_t *data, size_t &pos)
{
    int r = ReadUegBits(data, pos);
    return (r >> 1) + (r & 1 ? 1 : -r);
}

bool SyncAdtsPayload(std::vector<uint8_t> &workspace, const uint8_t *payload, size_t lenBytes)
{
    if (!workspace.empty() && workspace[0] == 0) {
        // No need to resync
        workspace.insert(workspace.end(), payload, payload + lenBytes);
        workspace[0] = 0xff;
    }
    else {
        // Resync
        workspace.insert(workspace.end(), payload, payload + lenBytes);
        size_t i = 0;
        for (; i < workspace.size(); ++i) {
            if (workspace[i] == 0xff && (i + 1 >= workspace.size() || (workspace[i + 1] & 0xf0) == 0xf0)) {
                break;
            }
        }
        workspace.erase(workspace.begin(), workspace.begin() + i);
        if (workspace.size() < 2) {
            return false;
        }
    }
    return true;
}
}

const int CMp4Fragmenter::VIDEO_TRACK_ID = 1;
const int CMp4Fragmenter::AUDIO_TRACK_ID = 2;

CMp4Fragmenter::CMp4Fragmenter()
    : m_fragmentCount(0)
    , m_fragmentDurationResidual(0)
    , m_videoPts(-1)
    , m_videoDts(-1)
    , m_videoDecodeTime(0)
    , m_videoDecodeTimeDts(-1)
    , m_audioPts(-1)
    , m_audioDecodeTime(0)
    , m_audioDecodeTimePts(-1)
    , m_codecWidth(-1)
    , m_parallelismType(0)
    , m_numTemporalLayers(1)
    , m_temporalIDNestingFlag(false)
    , m_aacProfile(-1)
{
}

void CMp4Fragmenter::AddPackets(const std::vector<uint8_t> &packets, const PMT &pmt, bool packetsMaybeNotEndAtUnitStart)
{
    int64_t baseVideoDts = -1;
    int64_t baseAudioPts = -1;
    m_emsg.clear();
    m_videoMdat.clear();
    m_audioMdat.clear();
    m_videoSampleInfos.clear();
    m_audioSampleSizes.clear();

    for (size_t i = 0; i < packets.size(); i += 188) {
        const uint8_t *packet = &packets[i];
        int unitStart = extract_ts_header_unit_start(packet);
        int pid = extract_ts_header_pid(packet);
        int counter = extract_ts_header_counter(packet);
        int payloadSize = get_ts_payload_size(packet);
        const uint8_t *payload = packet + 188 - payloadSize;

        if (pid != 0 &&
            (pid == pmt.first_video_pid ||
             pid == pmt.first_adts_audio_pid ||
             pid == pmt.first_id3_metadata_pid)) {
            auto &pesPair = pid == pmt.first_video_pid ? m_videoPes :
                            pid == pmt.first_adts_audio_pid ? m_audioPes : m_id3Pes;
            int &pesCounter = pesPair.first;
            std::vector<uint8_t> &pes = pesPair.second;

            if (unitStart) {
                pesCounter = counter;
                if (pes.size() >= 6 && pes[0] == 0 && pes[1] == 0 && pes[2] == 1) {
                    size_t pesPacketLength = (pes[4] << 8) | pes[5];
                    if (pesPacketLength == 0 && &pesPair == &m_videoPes) {
                        // Video PES has been accumulated
                        AddVideoPes(pes, pmt.first_video_stream_type == H_265_VIDEO);
                        if (baseVideoDts < 0) {
                            baseVideoDts = m_videoDts;
                        }
                    }
                }
                pes.assign(payload, payload + payloadSize);
            }
            else if (!pes.empty()) {
                pesCounter = (pesCounter + 1) & 0x0f;
                if (pesCounter == counter) {
                    pes.insert(pes.end(), payload, payload + payloadSize);
                }
                else {
                    // Ignore packets until the next unit-start
                    pes.clear();
                }
            }
            if (pes.size() >= 6) {
                size_t pesPacketLength = (pes[4] << 8) | pes[5];
                if (pesPacketLength != 0 && pes.size() >= 6 + pesPacketLength) {
                    // PES has been accumulated
                    pes.resize(6 + pesPacketLength);
                    if (pes[0] == 0 && pes[1] == 0 && pes[2] == 1) {
                        if (&pesPair == &m_videoPes) {
                            AddVideoPes(pes, pmt.first_video_stream_type == H_265_VIDEO);
                            if (baseVideoDts < 0) {
                                baseVideoDts = m_videoDts;
                            }
                        }
                        else if (&pesPair == &m_audioPes) {
                            AddAudioPes(pes);
                            if (baseAudioPts < 0) {
                                baseAudioPts = m_audioPts;
                            }
                        }
                        else {
                            AddID3Pes(pes);
                        }
                    }
                    pes.clear();
                }
            }
        }
    }

    std::vector<uint8_t> &pes = m_videoPes.second;
    if (pes.size() >= 6 && pes[0] == 0 && pes[1] == 0 && pes[2] == 1) {
        size_t pesPacketLength = (pes[4] << 8) | pes[5];
        if (pesPacketLength == 0 && !packetsMaybeNotEndAtUnitStart) {
            // Video PES has been accumulated (Assuming packets are split at the unit start.)
            AddVideoPes(pes, pmt.first_video_stream_type == H_265_VIDEO);
            if (baseVideoDts < 0) {
                baseVideoDts = m_videoDts;
            }
            pes.clear();
        }
    }

    if (m_moov.empty()) {
        if ((pmt.first_video_pid == 0 || m_codecWidth >= 0) &&
            (pmt.first_adts_audio_pid == 0 || m_aacProfile >= 0)) {
            PushBox(m_moov, "ftyp", [](std::vector<uint8_t> &data) {
                PushString(data, "isom");
                PushUint(data, 1);
                PushString(data, "isom");
                PushString(data, "avc1");
            });
            PushMoov(m_moov);
        }
    }
    if (!m_moov.empty()) {
        size_t fragSize = m_fragments.size();
        int fragDurationMsec = 0;
        m_fragments.insert(m_fragments.end(), m_emsg.begin(), m_emsg.end());
        if (!m_videoSampleInfos.empty() || !m_audioSampleSizes.empty()) {
            // Increment playback position
            if (baseVideoDts >= 0 && m_videoDecodeTimeDts >= 0) {
                int64_t diff = (0x200000000 + baseVideoDts - m_videoDecodeTimeDts) & 0x1ffffffff;
                m_videoDecodeTime += diff < 0x100000000 ? diff : 0;
                m_videoDecodeTimeDts = baseVideoDts;
            }
            if (baseAudioPts >= 0 && m_audioDecodeTimePts >= 0) {
                int64_t diff = (0x200000000 + baseAudioPts - m_audioDecodeTimePts) & 0x1ffffffff;
                m_audioDecodeTime += diff < 0x100000000 ? diff : 0;
                m_audioDecodeTimePts = baseAudioPts;
            }

            // Adjust difference between video/audio playback positions
            if (m_videoDecodeTimeDts < 0 && baseVideoDts >= 0) {
                if (m_audioDecodeTimePts >= 0) {
                    int64_t diff = (0x200000000 + m_audioDecodeTime + baseVideoDts - m_audioDecodeTimePts) & 0x1ffffffff;
                    m_videoDecodeTime = std::min<int64_t>(diff < 0x100000000 ? diff : 0, 900000);
                }
                else if (baseAudioPts >= 0) {
                    int64_t diff = (0x200000000 + baseVideoDts - baseAudioPts) & 0x1ffffffff;
                    m_videoDecodeTime = std::min<int64_t>(diff < 0x100000000 ? diff : 0, 900000);
                }
                m_videoDecodeTimeDts = baseVideoDts;
            }
            if (m_audioDecodeTimePts < 0 && baseAudioPts >= 0) {
                if (m_videoDecodeTimeDts >= 0) {
                    int64_t diff = (0x200000000 + m_videoDecodeTime + baseAudioPts - m_videoDecodeTimeDts) & 0x1ffffffff;
                    m_audioDecodeTime = std::min<int64_t>(diff < 0x100000000 ? diff : 0, 900000);
                }
                m_audioDecodeTimePts = baseAudioPts;
            }

            std::pair<int, int> duration;
            PushMoof(m_fragments, duration, m_fragmentCount);
            if (duration.first > 0) {
                int64_t num = static_cast<int64_t>(duration.first) * 1000 + m_fragmentDurationResidual;
                fragDurationMsec = static_cast<int>(num / duration.second);
                m_fragmentDurationResidual = static_cast<int>(num % duration.second);
            }
        }
        fragSize = m_fragments.size() - fragSize;
        if (fragSize > 0) {
            m_fragmentSizes.push_back(fragSize);
            m_fragmentDurationsMsec.push_back(fragDurationMsec);
        }
    }
}

void CMp4Fragmenter::ClearFragments()
{
    m_fragments.clear();
    m_fragmentSizes.clear();
    m_fragmentDurationsMsec.clear();
}

void CMp4Fragmenter::AddVideoPes(const std::vector<uint8_t> &pes, bool h265)
{
    int streamID = pes[3];
    if ((streamID & 0xf0) == 0xe0 && pes.size() >= 9) {
        size_t payloadPos = 9 + pes[8];
        if (payloadPos < pes.size()) {
            int64_t lastDts = m_videoDts;
            int ptsDtsFlags = pes[7] >> 6;
            if (ptsDtsFlags >= 2 && pes.size() >= 14) {
                m_videoPts = get_pes_timestamp(&pes[9]);
                m_videoDts = m_videoPts;
                if (ptsDtsFlags == 3 && pes.size() >= 19) {
                    m_videoDts = get_pes_timestamp(&pes[14]);
                }
            }

            bool parameterChanged = false;
            bool isKey = false;
            size_t sampleSize = 0;
            ParseNals(&pes[payloadPos], pes.size() - payloadPos,
                      [this, h265, &parameterChanged, &isKey, &sampleSize](const uint8_t *nal, size_t len) {
                if (len > 0) {
                    int nalUnitType = h265 ? (nal[0] >> 1) & 0x3f : nal[0] & 0x1f;
                    if (h265 && nalUnitType == 32) {
                        if (m_vps.size() != len || !std::equal(nal, nal + len, m_vps.begin())) {
                            if (m_moov.empty()) {
                                m_vps.assign(nal, nal + len);
                                ParseVps(m_vps);
                            }
                            else {
                                parameterChanged = true;
                            }
                        }
                    }
                    else if (nalUnitType == (h265 ? 33 : 7)) {
                        if (m_sps.size() != len || !std::equal(nal, nal + len, m_sps.begin())) {
                            if (m_moov.empty()) {
                                m_sps.assign(nal, nal + len);
                                if (!(h265 ? ParseH265Sps(m_sps) : ParseSps(m_sps))) {
                                    m_codecWidth = -1;
                                }
                            }
                            else {
                                parameterChanged = true;
                            }
                        }
                    }
                    else if (nalUnitType == (h265 ? 34 : 8)) {
                        if (m_pps.size() != len || !std::equal(nal, nal + len, m_pps.begin())) {
                            if (m_moov.empty()) {
                                m_pps.assign(nal, nal + len);
                                if (h265) {
                                    ParseH265Pps(m_pps);
                                }
                            }
                            else {
                                parameterChanged = true;
                            }
                        }
                    }
                    else if (nalUnitType == (h265 ? 35 : 9)) {
                        // Drop AUD
                    }
                    else if (h265 ? (nalUnitType == 39 || nalUnitType == 40) : (nalUnitType == 6)) {
                        // Drop SEI
                    }
                    else {
                        if (h265 ? (nalUnitType >= 16 && nalUnitType <= 21) : (nalUnitType == 5)) {
                            // IRAP (BLA or CRA or IDR)
                            isKey = true;
                        }
                        else if (!h265 && nalUnitType == 1) {
                            // Non-IDR
                            // Emulation prevention should not appear unless first_mb_in_slice value is huge
                            if (len >= 5 && (nal[1] != 0 || nal[2] != 0 || nal[3] != 3)) {
                                uint8_t sliceIntro[16] = {};
                                std::copy(nal + 1, nal + 5, sliceIntro);
                                size_t pos = 0;
                                // first_mb_in_slice
                                ReadUegBits(sliceIntro, pos);
                                int sliceType = ReadUegBits(sliceIntro, pos);
                                if (sliceType == 2 || sliceType == 4 || sliceType == 7 || sliceType == 9) {
                                    // I or SI picture
                                    isKey = true;
                                }
                            }
                        }
                        sampleSize += 4 + len;
                        PushUint(m_videoMdat, static_cast<uint32_t>(len));
                        m_videoMdat.insert(m_videoMdat.end(), nal, nal + len);
                    }
                }
            });

            if (m_moov.empty()) {
                m_h265 = h265;
            }
            else if (m_h265 != h265) {
                parameterChanged = true;
            }

            if (m_codecWidth < 0 || parameterChanged) {
                m_videoMdat.clear();
                m_videoSampleInfos.clear();
            }
            else {
                VIDEO_SAMPLE_INFO info;
                info.sampleSize = static_cast<uint32_t>(sampleSize);
                info.isKey = isKey;
                int64_t diff = (0x200000000 + m_videoDts - lastDts) & 0x1ffffffff;
                info.sampleDuration = lastDts < 0 || diff > 900000 ? -1 : static_cast<int>(diff);
                diff = (0x200000000 + m_videoPts - m_videoDts) & 0x1ffffffff;
                info.compositionTimeOffsets = diff > 900000 ? 0 : static_cast<int>(diff);
                m_videoSampleInfos.push_back(info);
            }
        }
    }
}

void CMp4Fragmenter::AddAudioPes(const std::vector<uint8_t> &pes)
{
    int streamID = pes[3];
    if ((streamID & 0xe0) == 0xc0 && pes.size() >= 9) {
        size_t payloadPos = 9 + pes[8];
        if (payloadPos < pes.size() && SyncAdtsPayload(m_workspace, &pes[payloadPos], pes.size() - payloadPos)) {
            int ptsDtsFlags = pes[7] >> 6;
            if (ptsDtsFlags >= 2 && pes.size() >= 14) {
                m_audioPts = get_pes_timestamp(&pes[9]);
            }
            while (m_workspace.size() > 0) {
                if (m_workspace[0] != 0xff) {
                    // Need to resync
                    m_workspace.clear();
                    break;
                }
                if (m_workspace.size() < 7) {
                    break;
                }
                if ((m_workspace[1] & 0xf0) != 0xf0) {
                    m_workspace.clear();
                    break;
                }

                // ADTS header
                size_t pos = 12;
                pos += 3;
                bool protectionAbsent = read_bool(m_workspace.data(), pos);
                int profile = read_bits(m_workspace.data(), pos, 2);
                int samplingFrequencyIndex = read_bits(m_workspace.data(), pos, 4);
                ++pos;
                int channelConfiguration = read_bits(m_workspace.data(), pos, 3);
                pos += 4;
                size_t frameLenBytes = read_bits(m_workspace.data(), pos, 13);
                size_t headerSize = protectionAbsent ? 7 : 9;
                if (frameLenBytes < headerSize) {
                    m_workspace.clear();
                    break;
                }
                if (m_workspace.size() < frameLenBytes) {
                    break;
                }

                if (m_moov.empty() && samplingFrequencyIndex < 13) {
                    static const int SAMPLING_FREQUENCY[13] = {
                        96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350
                    };
                    m_aacProfile = profile;
                    m_samplingFrequency = SAMPLING_FREQUENCY[samplingFrequencyIndex];
                    m_samplingFrequencyIndex = samplingFrequencyIndex;
                    m_channelConfiguration = channelConfiguration;
                }
                if (m_aacProfile == profile &&
                    m_samplingFrequencyIndex == samplingFrequencyIndex &&
                    m_channelConfiguration == channelConfiguration)
                {
                    m_audioMdat.insert(m_audioMdat.end(), m_workspace.begin() + headerSize, m_workspace.begin() + frameLenBytes);
                    m_audioSampleSizes.push_back(static_cast<uint16_t>(frameLenBytes - headerSize));
                }
                m_workspace.erase(m_workspace.begin(), m_workspace.begin() + frameLenBytes);
            }

            if (!m_workspace.empty()) {
                // This 0 means synchronized 0xff.
                m_workspace[0] = 0;
            }
        }
    }
}

void CMp4Fragmenter::AddID3Pes(const std::vector<uint8_t> &pes)
{
    const uint8_t PRIVATE_STREAM_1 = 0xbd;

    int streamID = pes[3];
    if (streamID == PRIVATE_STREAM_1 && pes.size() >= 14) {
        size_t payloadPos = 9 + pes[8];
        int ptsDtsFlags = pes[7] >> 6;
        if (payloadPos < pes.size() && ptsDtsFlags >= 2) {
            // Sync with media time
            int64_t emsgTime = m_videoDecodeTimeDts >= 0 ? m_videoDecodeTime : m_audioDecodeTime;
            int64_t mediaTimePts = m_videoDecodeTimeDts >= 0 ? m_videoDecodeTimeDts : m_audioDecodeTimePts;
            if (mediaTimePts >= 0) {
                int64_t diff = (0x200000000 + get_pes_timestamp(&pes[9]) - mediaTimePts) & 0x1ffffffff;
                emsgTime += std::min<int64_t>(diff < 0x100000000 ? diff : 0, 900000);
            }
            PushFullBox(m_emsg, "emsg", 0x01000000, [payloadPos, emsgTime, &pes](std::vector<uint8_t> &data) {
                PushUint(data, 90000);
                PushUint64(data, emsgTime);
                PushUint(data, 0xffffffff);
                PushUint(data, 0);
                PushString(data, "https://aomedia.org/emsg/ID3");
                data.push_back(0);
                data.push_back(0);
                data.insert(data.end(), pes.begin() + payloadPos, pes.end());
            });
        }
    }
}

void CMp4Fragmenter::PushMoov(std::vector<uint8_t> &data) const
{
    PushBox(data, "moov", [this](std::vector<uint8_t> &data) {
        PushFullBox(data, "mvhd", 0x00000000, [](std::vector<uint8_t> &data) {
            PushUint(data, 0);
            PushUint(data, 0);
            PushUint(data, 1000);
            PushUint(data, 0);
            PushUint(data, 0x00010000);
            PushUshort(data, 0x0100);
            PushUshort(data, RESERVED_0);
            PushUint(data, RESERVED_0);
            PushUint(data, RESERVED_0);
            // Unity matrix
            PushUint(data, 0x00010000);
            PushUint(data, 0);
            PushUint(data, 0);
            PushUint(data, 0);
            PushUint(data, 0x00010000);
            PushUint(data, 0);
            PushUint(data, 0);
            PushUint(data, 0);
            PushUint(data, 0x40000000);
            for (int i = 0; i < 6; ++i) {
                PushUint(data, PRE_DEFINED_0);
            }
            PushUint(data, AUDIO_TRACK_ID + 1);
        });

        if (m_codecWidth >= 0) {
            PushBox(data, "trak", [this](std::vector<uint8_t> &data) {
                PushFullBox(data, "tkhd", 0x00000003, [this](std::vector<uint8_t> &data) {
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, VIDEO_TRACK_ID);
                    PushUint(data, RESERVED_0);
                    PushUint(data, 0);
                    PushUint(data, RESERVED_0);
                    PushUint(data, RESERVED_0);
                    PushUshort(data, 0);
                    PushUshort(data, 0);
                    PushUshort(data, 0);
                    PushUshort(data, RESERVED_0);
                    // Unity matrix
                    PushUint(data, 0x00010000);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0x00010000);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0x40000000);
                    PushUshort(data, (m_codecWidth * m_sarWidth + (m_sarHeight - 1)) / m_sarHeight);
                    PushUshort(data, 0);
                    PushUshort(data, m_codecHeight);
                    PushUshort(data, 0);
                });
                PushBox(data, "mdia", [this](std::vector<uint8_t> &data) {
                    PushFullBox(data, "mdhd", 0x00000000, [](std::vector<uint8_t> &data) {
                        PushUint(data, 0);
                        PushUint(data, 0);
                        PushUint(data, 90000);
                        PushUint(data, 0);
                        // "und"
                        PushUshort(data, 0x55c4);
                        PushUshort(data, PRE_DEFINED_0);
                    });
                    PushFullBox(data, "hdlr", 0x00000000, [](std::vector<uint8_t> &data) {
                        PushUint(data, PRE_DEFINED_0);
                        PushString(data, "vide");
                        PushUint(data, RESERVED_0);
                        PushUint(data, RESERVED_0);
                        PushUint(data, RESERVED_0);
                        PushString(data, "Video Handler");
                        data.push_back(0);
                    });
                    PushBox(data, "minf", [this](std::vector<uint8_t> &data) {
                        PushFullBox(data, "vmhd", 0x00000001, [](std::vector<uint8_t> &data) {
                            PushUshort(data, 0);
                            PushUshort(data, 0);
                            PushUshort(data, 0);
                            PushUshort(data, 0);
                        });
                        PushBox(data, "dinf", [](std::vector<uint8_t> &data) {
                            PushFullBox(data, "dref", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 1);
                                PushFullBox(data, "url ", 0x00000001, [](std::vector<uint8_t> &data) {
                                    static_cast<void>(data);
                                });
                            });
                        });
                        PushBox(data, "stbl", [this](std::vector<uint8_t> &data) {
                            PushFullBox(data, "stsd", 0x00000000, [this](std::vector<uint8_t> &data) {
                                PushUint(data, 1);
                                PushBox(data, m_h265 ? "hvc1" : "avc1", [this](std::vector<uint8_t> &data) {
                                    for (int i = 0; i < 6; ++i) {
                                        data.push_back(RESERVED_0);
                                    }
                                    PushUshort(data, 1);
                                    PushUshort(data, PRE_DEFINED_0);
                                    PushUshort(data, RESERVED_0);
                                    PushUint(data, PRE_DEFINED_0);
                                    PushUint(data, PRE_DEFINED_0);
                                    PushUint(data, PRE_DEFINED_0);
                                    PushUshort(data, m_codecWidth);
                                    PushUshort(data, m_codecHeight);
                                    PushUshort(data, 72);
                                    PushUshort(data, 0);
                                    PushUshort(data, 72);
                                    PushUshort(data, 0);
                                    PushUint(data, RESERVED_0);
                                    PushUshort(data, 1);
                                    // Empty compressorname
                                    for (int i = 0; i < 32; ++i) {
                                        data.push_back(0);
                                    }
                                    PushUshort(data, 24);
                                    PushUshort(data, 0xffff);
                                    if (m_h265) {
                                        PushBox(data, "hvcC", [this](std::vector<uint8_t> &data) {
                                            data.push_back(1);
                                            data.push_back(static_cast<uint8_t>((m_generalProfileSpace << 6) | (m_generalTierFlag << 5) | m_generalProfileIdc));
                                            data.insert(data.end(), m_generalProfileCompatibilityFlags,
                                                        m_generalProfileCompatibilityFlags + sizeof(m_generalProfileCompatibilityFlags));
                                            data.insert(data.end(), m_generalConstraintIndicatorFlags,
                                                        m_generalConstraintIndicatorFlags + sizeof(m_generalConstraintIndicatorFlags));
                                            data.push_back(static_cast<uint8_t>(m_generalLevelIdc));
                                            PushUshort(data, 0xf000 | m_minSpatialSegmentationIdc);
                                            data.push_back(static_cast<uint8_t>(0xfc | m_parallelismType));
                                            data.push_back(static_cast<uint8_t>(0xfc | m_chromaFormatIdc));
                                            data.push_back(static_cast<uint8_t>(0xf8 | m_bitDepthLumaMinus8));
                                            data.push_back(static_cast<uint8_t>(0xf8 | m_bitDepthChromaMinus8));
                                            data.push_back(0);
                                            data.push_back(0);
                                            data.push_back(((m_numTemporalLayers & 0x07) << 3) | (m_temporalIDNestingFlag << 2) | 3);
                                            data.push_back(3);
                                            data.push_back(0x80 | 32);
                                            data.push_back(0);
                                            data.push_back(1);
                                            PushUshort(data, static_cast<uint32_t>(m_vps.size()));
                                            data.insert(data.end(), m_vps.begin(), m_vps.end());
                                            data.push_back(0x80 | 33);
                                            data.push_back(0);
                                            data.push_back(1);
                                            PushUshort(data, static_cast<uint32_t>(m_sps.size()));
                                            data.insert(data.end(), m_sps.begin(), m_sps.end());
                                            data.push_back(0x80 | 34);
                                            data.push_back(0);
                                            data.push_back(1);
                                            PushUshort(data, static_cast<uint32_t>(m_pps.size()));
                                            data.insert(data.end(), m_pps.begin(), m_pps.end());
                                        });
                                    }
                                    else {
                                        PushBox(data, "avcC", [this](std::vector<uint8_t> &data) {
                                            data.push_back(1);
                                            data.push_back(m_sps[1]);
                                            data.push_back(m_sps[2]);
                                            data.push_back(m_sps[3]);
                                            data.push_back(0xff);
                                            data.push_back(0xe1);
                                            PushUshort(data, static_cast<uint32_t>(m_sps.size()));
                                            data.insert(data.end(), m_sps.begin(), m_sps.end());
                                            data.push_back(1);
                                            PushUshort(data, static_cast<uint32_t>(m_pps.size()));
                                            data.insert(data.end(), m_pps.begin(), m_pps.end());
                                            if (m_sps[3] != 66 && m_sps[3] != 77 && m_sps[3] != 88) {
                                                data.push_back(static_cast<uint8_t>(0xfc | m_chromaFormatIdc));
                                                data.push_back(static_cast<uint8_t>(0xf8 | m_bitDepthLumaMinus8));
                                                data.push_back(static_cast<uint8_t>(0xf8 | m_bitDepthChromaMinus8));
                                                data.push_back(0);
                                            }
                                        });
                                    }
                                });
                            });
                            PushFullBox(data, "stts", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stsc", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stsz", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stco", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                        });
                    });
                });
            });
        }

        if (m_aacProfile >= 0) {
            PushBox(data, "trak", [this](std::vector<uint8_t> &data) {
                PushFullBox(data, "tkhd", 0x00000003, [](std::vector<uint8_t> &data) {
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, AUDIO_TRACK_ID);
                    PushUint(data, RESERVED_0);
                    PushUint(data, 0);
                    PushUint(data, RESERVED_0);
                    PushUint(data, RESERVED_0);
                    PushUshort(data, 0);
                    PushUshort(data, 1);
                    PushUshort(data, 0x0100);
                    PushUshort(data, RESERVED_0);
                    // Unity matrix
                    PushUint(data, 0x00010000);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0x00010000);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0x40000000);
                    PushUint(data, 0);
                    PushUint(data, 0);
                });
                PushBox(data, "mdia", [this](std::vector<uint8_t> &data) {
                    PushFullBox(data, "mdhd", 0x00000000, [this](std::vector<uint8_t> &data) {
                        PushUint(data, 0);
                        PushUint(data, 0);
                        PushUint(data, m_samplingFrequency);
                        PushUint(data, 0);
                        // "und"
                        PushUshort(data, 0x55c4);
                        PushUshort(data, PRE_DEFINED_0);
                    });
                    PushFullBox(data, "hdlr", 0x00000000, [](std::vector<uint8_t> &data) {
                        PushUint(data, PRE_DEFINED_0);
                        PushString(data, "soun");
                        PushUint(data, RESERVED_0);
                        PushUint(data, RESERVED_0);
                        PushUint(data, RESERVED_0);
                        PushString(data, "Audio Handler");
                        data.push_back(0);
                    });
                    PushBox(data, "minf", [this](std::vector<uint8_t> &data) {
                        PushFullBox(data, "smhd", 0x00000000, [](std::vector<uint8_t> &data) {
                            PushUshort(data, 0);
                            PushUshort(data, RESERVED_0);
                        });
                        PushBox(data, "dinf", [](std::vector<uint8_t> &data) {
                            PushFullBox(data, "dref", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 1);
                                PushFullBox(data, "url ", 0x00000001, [](std::vector<uint8_t> &data) {
                                    static_cast<void>(data);
                                });
                            });
                        });
                        PushBox(data, "stbl", [this](std::vector<uint8_t> &data) {
                            PushFullBox(data, "stsd", 0x00000000, [this](std::vector<uint8_t> &data) {
                                PushUint(data, 1);
                                PushBox(data, "mp4a", [this](std::vector<uint8_t> &data) {
                                    for (int i = 0; i < 6; ++i) {
                                        data.push_back(RESERVED_0);
                                    }
                                    PushUshort(data, 1);
                                    PushUint(data, RESERVED_0);
                                    PushUint(data, RESERVED_0);
                                    PushUshort(data, m_channelConfiguration);
                                    PushUshort(data, 16);
                                    PushUint(data, RESERVED_0);
                                    PushUshort(data, m_samplingFrequency);
                                    PushUshort(data, 0);
                                    PushFullBox(data, "esds", 0x00000000, [this](std::vector<uint8_t> &data) {
                                        // ES_Descriptor {
                                        data.push_back(0x03);
                                        data.push_back(25);
                                        PushUshort(data, 1);
                                        data.push_back(0);
                                        // DecoderConfigDescriptor {
                                        data.push_back(0x04);
                                        data.push_back(17);
                                        data.push_back(0x40);
                                        data.push_back(0x15);
                                        data.push_back(0);
                                        data.push_back(0);
                                        data.push_back(0);
                                        PushUint(data, 0);
                                        PushUint(data, 0);
                                        // DecoderSpecificInfo {
                                        data.push_back(0x05);
                                        data.push_back(2);
                                        // (AudioSpecificConfig)
                                        data.push_back(static_cast<uint8_t>(((m_aacProfile + 1) << 3) | (m_samplingFrequencyIndex >> 1)));
                                        data.push_back(static_cast<uint8_t>(((m_samplingFrequencyIndex & 0x01) << 7) | (m_channelConfiguration << 3)));
                                        // }}
                                        // SLConfigDescriptor {
                                        data.push_back(0x06);
                                        data.push_back(1);
                                        data.push_back(2);
                                        // }}
                                    });
                                });
                            });
                            PushFullBox(data, "stts", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stsc", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stsz", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                                PushUint(data, 0);
                            });
                            PushFullBox(data, "stco", 0x00000000, [](std::vector<uint8_t> &data) {
                                PushUint(data, 0);
                            });
                        });
                    });
                });
            });
        }

        PushBox(data, "mvex", [this](std::vector<uint8_t> &data) {
            if (m_codecWidth >= 0) {
                PushFullBox(data, "trex", 0x00000000, [](std::vector<uint8_t> &data) {
                    PushUint(data, VIDEO_TRACK_ID);
                    PushUint(data, 1);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                });
            }
            if (m_aacProfile >= 0) {
                PushFullBox(data, "trex", 0x00000000, [](std::vector<uint8_t> &data) {
                    PushUint(data, AUDIO_TRACK_ID);
                    PushUint(data, 1);
                    PushUint(data, 0);
                    PushUint(data, 0);
                    PushUint(data, 0);
                });
            }
        });
    });
}

void CMp4Fragmenter::PushMoof(std::vector<uint8_t> &data, std::pair<int, int> &fragDuration, uint32_t &fragCount) const
{
    fragDuration.first = 0;
    fragDuration.second = 1;

    if (!m_videoSampleInfos.empty()) {
        size_t moofBegin = data.size();
        size_t offsetFieldPos = 0;
        ++fragCount;
        PushBox(data, "moof", [this, fragCount, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
            PushFullBox(data, "mfhd", 0x00000000, [fragCount](std::vector<uint8_t> &data) {
                PushUint(data, fragCount);
            });
            PushBox(data, "traf", [this, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
                PushFullBox(data, "tfhd", 0x00000000, [](std::vector<uint8_t> &data) {
                    PushUint(data, VIDEO_TRACK_ID);
                });
                PushFullBox(data, "tfdt", 0x01000000, [this](std::vector<uint8_t> &data) {
                    PushUint64(data, m_videoDecodeTime);
                });
                PushFullBox(data, "trun", 0x00000f01, [this, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
                    PushUint(data, static_cast<uint32_t>(m_videoSampleInfos.size()));
                    offsetFieldPos = data.size();
                    PushUint(data, 0);
                    for (auto it = m_videoSampleInfos.begin(); it != m_videoSampleInfos.end(); ++it) {
                        auto itDuration = std::find_if(it, m_videoSampleInfos.end(),
                                                       [](const VIDEO_SAMPLE_INFO &a) { return a.sampleDuration >= 0; });
                        int duration = itDuration == m_videoSampleInfos.end() ? 3000 : itDuration->sampleDuration;
                        fragDuration.first += duration;
                        fragDuration.second = 90000;
                        PushUint(data, duration);
                        PushUint(data, it->sampleSize);
                        PushUint(data, it->isKey ? 0x02400000 : 0x01010000);
                        PushUint(data, it->compositionTimeOffsets);
                    }
                });
            });
        });

        PushBox(data, "mdat", [this, moofBegin, offsetFieldPos](std::vector<uint8_t> &data) {
            WriteUint(&data[offsetFieldPos], static_cast<uint32_t>(data.size() - moofBegin));
            data.insert(data.end(), m_videoMdat.begin(), m_videoMdat.end());
        });
    }

    if (!m_audioSampleSizes.empty()) {
        size_t moofBegin = data.size();
        size_t offsetFieldPos = 0;
        ++fragCount;
        PushBox(data, "moof", [this, fragCount, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
            PushFullBox(data, "mfhd", 0x00000000, [fragCount](std::vector<uint8_t> &data) {
                PushUint(data, fragCount);
            });
            PushBox(data, "traf", [this, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
                PushFullBox(data, "tfhd", 0x00000028, [](std::vector<uint8_t> &data) {
                    PushUint(data, AUDIO_TRACK_ID);
                    PushUint(data, 1024);
                    PushUint(data, 0x02000000);
                });
                PushFullBox(data, "tfdt", 0x01000000, [this](std::vector<uint8_t> &data) {
                    PushUint64(data, m_audioDecodeTime * m_samplingFrequency / 90000);
                });
                PushFullBox(data, "trun", 0x00000201, [this, &fragDuration, &offsetFieldPos](std::vector<uint8_t> &data) {
                    PushUint(data, static_cast<uint32_t>(m_audioSampleSizes.size()));
                    offsetFieldPos = data.size();
                    PushUint(data, 0);
                    for (size_t i = 0; i < m_audioSampleSizes.size(); ++i) {
                        PushUint(data, m_audioSampleSizes[i]);
                    }
                    if (m_codecWidth < 0) {
                        fragDuration.first = static_cast<int>(1024 * m_audioSampleSizes.size());
                        fragDuration.second = m_samplingFrequency;
                    }
                });
            });
        });

        PushBox(data, "mdat", [this, moofBegin, offsetFieldPos](std::vector<uint8_t> &data) {
            WriteUint(&data[offsetFieldPos], static_cast<uint32_t>(data.size() - moofBegin));
            data.insert(data.end(), m_audioMdat.begin(), m_audioMdat.end());
        });
    }
}

bool CMp4Fragmenter::ParseSps(const std::vector<uint8_t> &ebspSps)
{
    std::vector<uint8_t> rbspSps = EbspToRbsp(ebspSps);
    size_t lenBits = rbspSps.size() * 8;
    // 512bits overrun area
    rbspSps.insert(rbspSps.end(), 64, 0);
    const uint8_t *sps = rbspSps.data();
    // for debug
    int r;
    static_cast<void>(r);

    size_t pos = 8;
    int profileIdc = read_bits(sps, pos, 8);
    pos += 16;
    r = ReadUegBits(sps, pos);

    if (pos > lenBits) {
        return false;
    }
    m_chromaFormatIdc = 1;
    m_bitDepthLumaMinus8 = 0;
    m_bitDepthChromaMinus8 = 0;
    static const int HAS_CHROMA_INFO[12] = {100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134};
    if (std::find(HAS_CHROMA_INFO, HAS_CHROMA_INFO + 12, profileIdc) != HAS_CHROMA_INFO + 12) {
        m_chromaFormatIdc = ReadUegBits(sps, pos);
        if (m_chromaFormatIdc == 3) {
            ++pos;
        }
        m_bitDepthLumaMinus8 = ReadUegBits(sps, pos);
        m_bitDepthChromaMinus8 = ReadUegBits(sps, pos);
        ++pos;
        if (read_bool(sps, pos)) {
            int scalingListCount = m_chromaFormatIdc != 3 ? 8 : 12;
            for (int i = 0; i < scalingListCount; ++i) {
                if (read_bool(sps, pos)) {
                    int count = i < 6 ? 16 : 64;
                    int lastScale = 8;
                    while (--count >= 0 && lastScale != 0) {
                        if (pos > lenBits) {
                            return false;
                        }
                        int deltaScale = ReadSegBits(sps, pos);
                        lastScale = (lastScale + deltaScale) & 0xff;
                    }
                }
            }
        }
    }

    if (pos > lenBits) {
        return false;
    }
    r = ReadUegBits(sps, pos);
    int picOrderCntType = ReadUegBits(sps, pos);
    if (picOrderCntType == 0) {
        r = ReadUegBits(sps, pos);
    }
    else if (picOrderCntType == 1) {
        ++pos;
        r = ReadSegBits(sps, pos);
        r = ReadSegBits(sps, pos);
        int numRefFramesInPicOrderCntCycle = ReadUegBits(sps, pos);
        for (int i = 0; i < numRefFramesInPicOrderCntCycle; ++i) {
            if (pos > lenBits) {
                return false;
            }
            r = ReadSegBits(sps, pos);
        }
    }

    r = ReadUegBits(sps, pos);
    ++pos;
    int picWidthInMbsMinus1 = ReadUegBits(sps, pos);
    int picHeightInMapUnitsMinus1 = ReadUegBits(sps, pos);
    bool frameMbsOnlyFlag = read_bool(sps, pos);
    if (!frameMbsOnlyFlag) {
        ++pos;
    }
    ++pos;

    if (pos > lenBits) {
        return false;
    }
    int frameCropLeftOffset = 0;
    int frameCropRightOffset = 0;
    int frameCropTopOffset = 0;
    int frameCropBottomOffset = 0;
    if (read_bool(sps, pos)) {
        frameCropLeftOffset = ReadUegBits(sps, pos);
        frameCropRightOffset = ReadUegBits(sps, pos);
        frameCropTopOffset = ReadUegBits(sps, pos);
        frameCropBottomOffset = ReadUegBits(sps, pos);
    }

    m_sarWidth = 1;
    m_sarHeight = 1;
    if (read_bool(sps, pos)) {
        // VUI
        if (read_bool(sps, pos)) {
            int aspectRatioIdc = read_bits(sps, pos, 8);
            static const int SAR_W_TABLE[17] = {1, 1, 12, 10, 16, 40, 24, 20, 32, 80, 18, 15, 64, 160, 4, 3, 2};
            static const int SAR_H_TABLE[17] = {1, 1, 11, 11, 11, 33, 11, 11, 11, 33, 11, 11, 33, 99, 3, 2, 1};
            if (aspectRatioIdc < 17) {
                m_sarWidth = SAR_W_TABLE[aspectRatioIdc];
                m_sarHeight = SAR_H_TABLE[aspectRatioIdc];
            }
            else if (aspectRatioIdc == 255) {
                m_sarWidth = read_bits(sps, pos, 16);
                m_sarHeight = std::max(read_bits(sps, pos, 16), 1);
            }
        }
    }

    m_codecWidth = (picWidthInMbsMinus1 + 1) * 16;
    m_codecHeight = (2 - frameMbsOnlyFlag) * ((picHeightInMapUnitsMinus1 + 1) * 16);
    int cropUnitX = (m_chromaFormatIdc == 0 || m_chromaFormatIdc == 3 ? 1 : 2);
    int cropUnitY = (m_chromaFormatIdc == 1 ? 2 : 1) * (2 - frameMbsOnlyFlag);
    m_codecWidth -= (frameCropLeftOffset + frameCropRightOffset) * cropUnitX;
    m_codecHeight -= (frameCropTopOffset + frameCropBottomOffset) * cropUnitY;

    return pos <= lenBits;
}

bool CMp4Fragmenter::ParseH265Sps(const std::vector<uint8_t> &ebspSps)
{
    std::vector<uint8_t> rbspSps = EbspToRbsp(ebspSps);
    size_t lenBits = rbspSps.size() * 8;
    // 512bits overrun area
    rbspSps.insert(rbspSps.end(), 64, 0);
    const uint8_t *sps = rbspSps.data();
    size_t pos = 16;
    // for debug
    int r;
    static_cast<void>(r);

    pos += 4;
    int maxSubLayersMinus1 = read_bits(sps, pos, 3);
    m_temporalIDNestingFlag = read_bool(sps, pos);

    m_generalProfileSpace = read_bits(sps, pos, 2);
    m_generalTierFlag = read_bool(sps, pos);
    m_generalProfileIdc = read_bits(sps, pos, 5);
    for (int i = 0; i < 4; ++i) {
        m_generalProfileCompatibilityFlags[i] = read_bits(sps, pos, 8) & 0xff;
    }
    for (int i = 0; i < 6; ++i) {
        m_generalConstraintIndicatorFlags[i] = read_bits(sps, pos, 8) & 0xff;
    }
    m_generalLevelIdc = read_bits(sps, pos, 8);

    bool subLayerProfilePresentFlag[8];
    bool subLayerLevelPresentFlag[8];
    for (int i = 0; i < maxSubLayersMinus1; ++i) {
        subLayerProfilePresentFlag[i] = read_bool(sps, pos);
        subLayerLevelPresentFlag[i] = read_bool(sps, pos);
    }
    if (maxSubLayersMinus1 > 0) {
        for (int i = maxSubLayersMinus1; i < 8; ++i) {
            pos += 2;
        }
    }
    for (int i = 0; i < maxSubLayersMinus1; ++i) {
        if (subLayerProfilePresentFlag[i]) {
            pos += 88;
        }
        if (subLayerLevelPresentFlag[i]) {
            pos += 8;
        }
    }

    if (pos > lenBits) {
        return false;
    }
    r = ReadUegBits(sps, pos);
    m_chromaFormatIdc = ReadUegBits(sps, pos);
    if (m_chromaFormatIdc == 3) {
        ++pos;
    }
    int picWidthInLumaSamples = ReadUegBits(sps, pos);
    int picHeightInLumaSamples = ReadUegBits(sps, pos);
    int leftOffset = 0;
    int rightOffset = 0;
    int topOffset = 0;
    int bottomOffset = 0;

    if (pos > lenBits) {
        return false;
    }
    if (read_bool(sps, pos)) {
        leftOffset = ReadUegBits(sps, pos);
        rightOffset = ReadUegBits(sps, pos);
        topOffset = ReadUegBits(sps, pos);
        bottomOffset = ReadUegBits(sps, pos);
    }
    m_bitDepthLumaMinus8 = ReadUegBits(sps, pos);
    m_bitDepthChromaMinus8 = ReadUegBits(sps, pos);
    int log2MaxPicOrderCntLsbMinus4 = ReadUegBits(sps, pos);
    bool subLayerOrderingInfoPresentFlag = read_bool(sps, pos);
    for (int i = 0; i <= (subLayerOrderingInfoPresentFlag ? maxSubLayersMinus1 : 0); ++i) {
        if (pos > lenBits) {
            return false;
        }
        r = ReadUegBits(sps, pos);
        r = ReadUegBits(sps, pos);
        r = ReadUegBits(sps, pos);
    }

    if (pos > lenBits) {
        return false;
    }
    r = ReadUegBits(sps, pos);
    r = ReadUegBits(sps, pos);
    r = ReadUegBits(sps, pos);
    r = ReadUegBits(sps, pos);
    r = ReadUegBits(sps, pos);
    r = ReadUegBits(sps, pos);

    if (pos > lenBits) {
        return false;
    }
    if (read_bool(sps, pos)) {
        if (read_bool(sps, pos)) {
            // sps_scaling_list_data
            for (int i = 0; i < 4; ++i) {
                for (int j = 0; j < (i == 3 ? 2 : 6); ++j) {
                    if (pos > lenBits) {
                        return false;
                    }
                    if (read_bool(sps, pos)) {
                        int coefNum = std::min(64, 1 << (4 + (i << 1)));
                        if (i > 1) {
                            r = ReadSegBits(sps, pos);
                        }
                        while (--coefNum >= 0) {
                            if (pos > lenBits) {
                                return false;
                            }
                            r = ReadSegBits(sps, pos);
                        }
                    }
                    else {
                        r = ReadUegBits(sps, pos);
                    }
                }
            }
        }
    }

    if (pos > lenBits) {
        return false;
    }
    pos += 2;
    if (read_bool(sps, pos)) {
        pos += 8;
        r = ReadUegBits(sps, pos);
        r = ReadUegBits(sps, pos);
        ++pos;
    }
    int numShortTermRefPicSets = ReadUegBits(sps, pos);
    int numDeltaPocs = 0;
    for (int i = 0; i < numShortTermRefPicSets; ++i) {
        if (pos > lenBits) {
            return false;
        }
        bool interRefPicSetPredictionFlag = false;
        if (i != 0) {
            interRefPicSetPredictionFlag = read_bool(sps, pos);
        }
        if (interRefPicSetPredictionFlag) {
            if (i == numShortTermRefPicSets) {
                r = ReadUegBits(sps, pos);
            }
            read_bool(sps, pos);
            r = ReadUegBits(sps, pos);
            int nextNumDeltaPocs = 0;
            for (int j = 0; j <= numDeltaPocs; ++j) {
                if (pos > lenBits) {
                    return false;
                }
                bool usedByCurrPicFlag = read_bool(sps, pos);
                bool useDeltaFlag = false;
                if (!usedByCurrPicFlag) {
                    useDeltaFlag = read_bool(sps, pos);
                }
                if (usedByCurrPicFlag || useDeltaFlag) {
                    ++nextNumDeltaPocs;
                }
            }
            numDeltaPocs = nextNumDeltaPocs;
        }
        else {
            int numNegativePics = ReadUegBits(sps, pos);
            int numPositivePics = ReadUegBits(sps, pos);
            numDeltaPocs = numNegativePics + numPositivePics;
            for (int j = 0; j < numDeltaPocs; ++j) {
                if (pos > lenBits) {
                    return false;
                }
                r = ReadUegBits(sps, pos);
                read_bool(sps, pos);
            }
        }
    }
    if (read_bool(sps, pos)) {
        int numLongTermRefPicsSps = ReadUegBits(sps, pos);
        while (--numLongTermRefPicsSps >= 0) {
            pos += log2MaxPicOrderCntLsbMinus4 + 4;
            ++pos;
        }
    }

    m_minSpatialSegmentationIdc = 0;
    m_sarWidth = 1;
    m_sarHeight = 1;

    if (pos > lenBits) {
        return false;
    }
    pos += 2;
    if (read_bool(sps, pos)) {
        // VUI
        if (read_bool(sps, pos)) {
            int aspectRatioIdc = read_bits(sps, pos, 8);
            static const int SAR_W_TABLE[17] = {1, 1, 12, 10, 16, 40, 24, 20, 32, 80, 18, 15, 64, 160, 4, 3, 2};
            static const int SAR_H_TABLE[17] = {1, 1, 11, 11, 11, 33, 11, 11, 11, 33, 11, 11, 33, 99, 3, 2, 1};
            if (aspectRatioIdc < 17) {
                m_sarWidth = SAR_W_TABLE[aspectRatioIdc];
                m_sarHeight = SAR_H_TABLE[aspectRatioIdc];
            }
            else if (aspectRatioIdc == 255) {
                m_sarWidth = read_bits(sps, pos, 16);
                m_sarHeight = std::max(read_bits(sps, pos, 16), 1);
            }
        }
        if (read_bool(sps, pos)) {
            ++pos;
        }
        if (read_bool(sps, pos)) {
            pos += 4;
            if (read_bool(sps, pos)) {
                pos += 24;
            }
        }

        if (pos > lenBits) {
            return false;
        }
        if (read_bool(sps, pos)) {
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
        }
        pos += 3;
        if (read_bool(sps, pos)) {
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
        }

        if (pos > lenBits) {
            return false;
        }
        if (read_bool(sps, pos)) {
            // vui_timing_info
            pos += 64;
            if (read_bool(sps, pos)) {
                r = ReadUegBits(sps, pos);
            }
            if (read_bool(sps, pos)) {
                // vui_hrd_parameters
                bool subPicHrdParamsPresentFlag = false;
                bool nalHrdParametersPresentFlag = read_bool(sps, pos);
                bool vclHrdParametersPresentFlag = read_bool(sps, pos);
                if (nalHrdParametersPresentFlag || vclHrdParametersPresentFlag) {
                    subPicHrdParamsPresentFlag = read_bool(sps, pos);
                    if (subPicHrdParamsPresentFlag) {
                        pos += 19;
                    }
                    pos += 8;
                    if (subPicHrdParamsPresentFlag) {
                        pos += 4;
                    }
                    pos += 15;
                }
                for (int i = 0; i <= maxSubLayersMinus1; ++i) {
                    if (pos > lenBits) {
                        return false;
                    }
                    bool fixedPicRateGeneralFlag = read_bool(sps, pos);
                    bool fixedPicRateWithinCvsFlag = false;
                    int cpbCnt = 1;
                    if (!fixedPicRateGeneralFlag) {
                        fixedPicRateWithinCvsFlag = read_bool(sps, pos);
                    }
                    bool lowDelayHrdFlag = false;
                    if (fixedPicRateWithinCvsFlag) {
                        r = ReadSegBits(sps, pos);
                    }
                    else {
                        lowDelayHrdFlag = read_bool(sps, pos);
                    }
                    if (!lowDelayHrdFlag) {
                        cpbCnt = ReadUegBits(sps, pos) + 1;
                    }
                    for (int j = 0; j < nalHrdParametersPresentFlag + vclHrdParametersPresentFlag; ++j) {
                        for (int k = 0; k < cpbCnt; ++k) {
                            if (pos > lenBits) {
                                return false;
                            }
                            r = ReadUegBits(sps, pos);
                            r = ReadUegBits(sps, pos);
                            if (subPicHrdParamsPresentFlag) {
                                r = ReadUegBits(sps, pos);
                                r = ReadUegBits(sps, pos);
                            }
                            ++pos;
                        }
                    }
                }
            }
        }

        if (pos > lenBits) {
            return false;
        }
        if (read_bool(sps, pos)) {
            pos += 3;
            m_minSpatialSegmentationIdc = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
            r = ReadUegBits(sps, pos);
        }
    }

    int subWC = m_chromaFormatIdc == 1 || m_chromaFormatIdc == 2 ? 2 : 1;
    int subHC = m_chromaFormatIdc == 1 ? 2 : 1;
    m_codecWidth = picWidthInLumaSamples - (leftOffset + rightOffset) * subWC;
    m_codecHeight = picHeightInLumaSamples - (topOffset + bottomOffset) * subHC;

    return pos <= lenBits;
}

bool CMp4Fragmenter::ParseVps(const std::vector<uint8_t> &ebspVps)
{
    std::vector<uint8_t> rbspVps = EbspToRbsp(ebspVps);
    size_t lenBits = rbspVps.size() * 8;
    // 512bits overrun area
    rbspVps.insert(rbspVps.end(), 64, 0);
    const uint8_t *vps = rbspVps.data();
    size_t pos = 16;

    pos += 12;
    m_numTemporalLayers = read_bits(vps, pos, 3) + 1;
    m_temporalIDNestingFlag = read_bool(vps, pos);

    return pos <= lenBits;
}

bool CMp4Fragmenter::ParseH265Pps(const std::vector<uint8_t> &ebspPps)
{
    std::vector<uint8_t> rbspPps = EbspToRbsp(ebspPps);
    size_t lenBits = rbspPps.size() * 8;
    // 512bits overrun area
    rbspPps.insert(rbspPps.end(), 64, 0);
    const uint8_t *pps = rbspPps.data();
    size_t pos = 16;
    // for debug
    int r;
    static_cast<void>(r);

    r = ReadUegBits(pps, pos);
    r = ReadUegBits(pps, pos);
    pos += 7;
    r = ReadUegBits(pps, pos);
    r = ReadUegBits(pps, pos);
    r = ReadSegBits(pps, pos);
    pos += 2;

    if (pos > lenBits) {
        return false;
    }
    if (read_bool(pps, pos)) {
        r = ReadUegBits(pps, pos);
    }
    r = ReadSegBits(pps, pos);
    r = ReadSegBits(pps, pos);
    pos += 4;
    bool tilesEnabledFlag = read_bool(pps, pos);
    bool entropyCodingSyncEnabledFlag = read_bool(pps, pos);
    m_parallelismType = entropyCodingSyncEnabledFlag ? (tilesEnabledFlag ? 0 : 3) : (tilesEnabledFlag ? 2 : 1);

    return pos <= lenBits;
}
