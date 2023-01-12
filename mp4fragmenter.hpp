#ifndef INCLUDE_MP4FRAGMENTER_HPP
#define INCLUDE_MP4FRAGMENTER_HPP

#include "util.hpp"
#include <stdint.h>
#include <utility>
#include <vector>

class CMp4Fragmenter
{
public:
    CMp4Fragmenter();
    void AddPackets(std::vector<uint8_t> &packets, const PMT &pmt, bool packetsMaybeNotEndAtUnitStart);
    void ClearFragments();
    const std::vector<uint8_t> &GetFragments() const { return m_fragments; }
    const std::vector<size_t> &GetFragmentSizes() const { return m_fragmentSizes; }
    const std::vector<int> &GetFragmentDurationsMsec() const { return m_fragmentDurationsMsec; }
    const std::vector<uint8_t> &GetHeader() const { return m_moov; }

private:
    void AddVideoPes(const std::vector<uint8_t> &pes, bool h265);
    void AddAudioPes(const std::vector<uint8_t> &pes);
    void AddID3Pes(const std::vector<uint8_t> &pes);
    void PushMoov(std::vector<uint8_t> &data) const;
    void PushMoof(std::vector<uint8_t> &data, std::pair<int, int> &fragDuration, uint32_t fragCount) const;
    bool ParseSps(const std::vector<uint8_t> &ebspSps);
    bool ParseH265Sps(const std::vector<uint8_t> &ebspSps);
    bool ParseVps(const std::vector<uint8_t> &ebspVps);
    bool ParseH265Pps(const std::vector<uint8_t> &ebspPps);

    static const int VIDEO_TRACK_ID;
    static const int AUDIO_TRACK_ID;

    uint32_t m_fragmentCount;
    int m_fragmentDurationResidual;
    std::vector<uint8_t> m_fragments;
    std::vector<size_t> m_fragmentSizes;
    std::vector<int> m_fragmentDurationsMsec;
    std::pair<int, std::vector<uint8_t>> m_videoPes;
    std::pair<int, std::vector<uint8_t>> m_audioPes;
    std::pair<int, std::vector<uint8_t>> m_id3Pes;

    int64_t m_videoPts;
    int64_t m_videoDts;
    int64_t m_videoDecodeTime;
    int64_t m_videoDecodeTimeDts;

    int64_t m_audioPts;
    int64_t m_audioDecodeTime;
    int64_t m_audioDecodeTimePts;
    std::vector<uint8_t> m_workspace;
    std::vector<uint8_t> m_emsg;
    std::vector<uint8_t> m_videoMdat;
    std::vector<uint8_t> m_audioMdat;
    std::vector<uint8_t> m_moov;

    // These members are valid if (m_codecWidth >= 0)
    int m_codecWidth;
    int m_codecHeight;
    int m_sarWidth;
    int m_sarHeight;
    int m_chromaFormatIdc;
    int m_bitDepthLumaMinus8;
    int m_bitDepthChromaMinus8;
    bool m_h265;
    int m_generalProfileSpace;
    bool m_generalTierFlag;
    int m_generalProfileIdc;
    int m_generalLevelIdc;
    uint8_t m_generalProfileCompatibilityFlags[4];
    uint8_t m_generalConstraintIndicatorFlags[6];
    int m_minSpatialSegmentationIdc;
    int m_parallelismType;
    int m_numTemporalLayers;
    bool m_temporalIDNestingFlag;
    std::vector<uint8_t> m_vps;
    std::vector<uint8_t> m_sps;
    std::vector<uint8_t> m_pps;

    struct VIDEO_SAMPLE_INFO
    {
        uint32_t sampleSize;
        bool isKey;
        int sampleDuration;
        int compositionTimeOffsets;
    };
    std::vector<VIDEO_SAMPLE_INFO> m_videoSampleInfos;

    // These members are valid if (m_aacProfile >= 0)
    int m_aacProfile;
    int m_samplingFrequency;
    int m_samplingFrequencyIndex;
    int m_channelConfiguration;
    std::vector<uint16_t> m_audioSampleSizes;
};

#endif
