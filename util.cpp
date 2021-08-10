#include "util.hpp"
#include <algorithm>

namespace
{
void extract_psi(PSI *psi, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    if (unit_start) {
        psi->continuity_counter = 0x20 | counter;
        psi->data_count = psi->version_number = 0;
    }
    else {
        psi->continuity_counter = (psi->continuity_counter + 1) & 0x2f;
        if (psi->continuity_counter != (0x20 | counter)) {
            psi->continuity_counter = psi->data_count = psi->version_number = 0;
            return;
        }
    }
    if (psi->data_count + payload_size <= static_cast<int>(sizeof(psi->data))) {
        std::copy(payload, payload + payload_size, psi->data + psi->data_count);
        psi->data_count += payload_size;
    }
    // TODO: CRC32

    // If psi->version_number != 0, these fields are valid.
    if (psi->data_count >= 1) {
        int pointer = psi->data[0];
        if (psi->data_count >= pointer + 4) {
            int section_length = ((psi->data[pointer + 2] & 0x03) << 8) | psi->data[pointer + 3];
            if (section_length >= 3 && psi->data_count >= pointer + 4 + section_length) {
                const uint8_t *table = psi->data + 1 + pointer;
                psi->pointer_field          = pointer;
                psi->table_id               = table[0];
                psi->section_length         = section_length;
                psi->version_number         = 0x20 | ((table[5] >> 1) & 0x1f);
                psi->current_next_indicator = table[5] & 0x01;
            }
        }
    }
}
}

void extract_pat(PAT *pat, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    extract_psi(&pat->psi, payload, payload_size, unit_start, counter);

    if (pat->psi.version_number &&
        pat->psi.current_next_indicator &&
        pat->psi.table_id == 0 &&
        pat->psi.section_length >= 5)
    {
        // Update PAT
        const uint8_t *table = pat->psi.data + 1 + pat->psi.pointer_field;
        pat->transport_stream_id = (table[3] << 8) | table[4];
        pat->version_number = pat->psi.version_number;

        // Check the first PMT
        static const PMT pmt_zero = {};
        int pid = 0;
        int pos = 3 + 5;
        while (pos + 3 < 3 + pat->psi.section_length - 4/*CRC32*/) {
            int program_number = (table[pos] << 8) | (table[pos + 1]);
            if (program_number != 0) {
                pid = ((table[pos + 2] & 0x1f) << 8) | table[pos + 3];
                if (pat->first_pmt.pmt_pid != pid) {
                    pat->first_pmt = pmt_zero;
                    pat->first_pmt.pmt_pid = pid;
                }
                break;
            }
            pos += 4;
        }
        if (pid == 0) {
            pat->first_pmt = pmt_zero;
        }
    }
}

void extract_pmt(PMT *pmt, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    extract_psi(&pmt->psi, payload, payload_size, unit_start, counter);

    if (pmt->psi.version_number &&
        pmt->psi.current_next_indicator &&
        pmt->psi.table_id == 2 &&
        pmt->psi.section_length >= 9)
    {
        // Update PMT
        uint8_t *table = pmt->psi.data + 1 + pmt->psi.pointer_field;
        pmt->program_number = (table[3] << 8) | table[4];
        pmt->version_number = pmt->psi.version_number;
        pmt->pcr_pid = ((table[8] & 0x1f) << 8) | table[9];
        int program_info_length = ((table[10] & 0x03) << 8) | table[11];

        pmt->first_video_pid = 0;
        int pos = 3 + 9 + program_info_length;
        while (pos + 4 < 3 + pmt->psi.section_length - 4/*CRC32*/) {
            int stream_type = table[pos];
            if (stream_type == AVC_VIDEO) {
                pmt->first_video_stream_type = stream_type;
                pmt->first_video_pid = (table[pos + 1] & 0x1f) << 8 | table[pos + 2];
                break;
            }
            int es_info_length = (table[pos + 3] & 0x03) << 8 | table[pos + 4];
            pos += 5 + es_info_length;
        }
    }
}

int contains_nal_idr(int *nal_state, const uint8_t *payload, int payload_size)
{
    for (int i = 0; i < payload_size; ++i) {
        // 0,1,2: Searching for NAL start code
        if ((*nal_state == 0 || *nal_state == 1) && payload[i] == 0) {
            ++*nal_state;
        }
        else if (*nal_state == 2 && payload[i] <= 1) {
            if (payload[i] == 1) {
                // 3: Found NAL start code
                ++*nal_state;
            }
        }
        else if (*nal_state == 3) {
            int nal_unit_type = payload[i] & 0x1f;
            if (nal_unit_type == 5) {
                // 4: Stop searching
                ++*nal_state;
                return 1;
            }
            *nal_state = 0;
        }
        else if (*nal_state >= 4) {
            break;
        }
        else {
            *nal_state = 0;
        }
    }
    return 0;
}

int get_ts_payload_size(const uint8_t *packet)
{
    int adaptation = extract_ts_header_adaptation(packet);
    if (adaptation & 1) {
        if (adaptation == 3) {
            int adaptation_length = packet[4];
            if (adaptation_length <= 183) {
                return 183 - adaptation_length;
            }
        }
        else {
            return 184;
        }
    }
    return 0;
}
