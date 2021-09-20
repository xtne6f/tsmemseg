#ifndef INCLUDE_UTIL_HPP
#define INCLUDE_UTIL_HPP

#include <stdint.h>

constexpr uint8_t AVC_VIDEO = 0x1b;

struct PSI
{
    int table_id;
    int section_length;
    int version_number;
    int current_next_indicator;
    int continuity_counter;
    int data_count;
    uint8_t data[1024];
};

struct PMT
{
    int pmt_pid;
    int program_number;
    int version_number;
    int pcr_pid;
    int first_video_stream_type;
    int first_video_pid;
    PSI psi;
};

struct PAT
{
    int transport_stream_id;
    int version_number;
    PMT first_pmt;
    PSI psi;
};

uint32_t calc_crc32(const uint8_t *data, int data_size, uint32_t crc = 0xffffffff);
int extract_psi(PSI *psi, const uint8_t *payload, int payload_size, int unit_start, int counter);
void extract_pat(PAT *pat, const uint8_t *payload, int payload_size, int unit_start, int counter);
void extract_pmt(PMT *pmt, const uint8_t *payload, int payload_size, int unit_start, int counter);
int contains_nal_idr(int *nal_state, const uint8_t *payload, int payload_size);
int get_ts_payload_size(const uint8_t *packet);

inline int extract_ts_header_sync(const uint8_t *packet) { return packet[0]; }
inline int extract_ts_header_unit_start(const uint8_t *packet) { return !!(packet[1] & 0x40); }
inline int extract_ts_header_pid(const uint8_t *packet) { return ((packet[1] & 0x1f) << 8) | packet[2]; }
inline int extract_ts_header_adaptation(const uint8_t *packet) { return (packet[3] >> 4) & 0x03; }
inline int extract_ts_header_counter(const uint8_t *packet) { return packet[3] & 0x0f; }

#endif
