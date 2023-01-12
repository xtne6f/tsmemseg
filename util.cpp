#include "util.hpp"
#include <algorithm>

uint32_t calc_crc32(const uint8_t *data, int data_size, uint32_t crc)
{
    for (int i = 0; i < data_size; ++i) {
        uint32_t c = ((crc >> 24) ^ data[i]) << 24;
        for (int j = 0; j < 8; ++j) {
            c = (c << 1) ^ (c & 0x80000000 ? 0x04c11db7 : 0);
        }
        crc = (crc << 8) ^ c;
    }
    return crc;
}

int extract_psi(PSI *psi, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    int copy_pos = 0;
    int copy_size = payload_size;
    int done = 1;
    if (unit_start) {
        if (payload_size < 1) {
            psi->continuity_counter = psi->data_count = psi->version_number = 0;
            return 1;
        }
        int pointer = payload[0];
        psi->continuity_counter = (psi->continuity_counter + 1) & 0x2f;
        if (pointer > 0 && psi->continuity_counter == (0x20 | counter)) {
            copy_pos = 1;
            copy_size = pointer;
            // Call the function again
            done = 0;
        }
        else {
            psi->continuity_counter = 0x20 | counter;
            psi->data_count = psi->version_number = 0;
            copy_pos = 1 + pointer;
            copy_size -= copy_pos;
        }
    }
    else {
        psi->continuity_counter = (psi->continuity_counter + 1) & 0x2f;
        if (psi->continuity_counter != (0x20 | counter)) {
            psi->continuity_counter = psi->data_count = psi->version_number = 0;
            return 1;
        }
    }
    if (copy_size > 0 && copy_pos + copy_size <= payload_size) {
        copy_size = std::min(copy_size, static_cast<int>(sizeof(psi->data)) - psi->data_count);
        std::copy(payload + copy_pos, payload + copy_pos + copy_size, psi->data + psi->data_count);
        psi->data_count += copy_size;
    }

    // If psi->version_number != 0, these fields are valid.
    if (psi->data_count >= 3) {
        int section_length = ((psi->data[1] & 0x03) << 8) | psi->data[2];
        if (psi->data_count >= 3 + section_length &&
            calc_crc32(psi->data, 3 + section_length) == 0 &&
            section_length >= 3)
        {
            psi->table_id = psi->data[0];
            psi->section_length = section_length;
            psi->version_number = 0x20 | ((psi->data[5] >> 1) & 0x1f);
            psi->current_next_indicator = psi->data[5] & 0x01;
        }
    }
    return done;
}

void extract_pat(PAT *pat, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    int done;
    do {
        done = extract_psi(&pat->psi, payload, payload_size, unit_start, counter);
        if (pat->psi.version_number &&
            pat->psi.current_next_indicator &&
            pat->psi.table_id == 0 &&
            pat->psi.section_length >= 5)
        {
            // Update PAT
            const uint8_t *table = pat->psi.data;
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
    while (!done);
}

void extract_pmt(PMT *pmt, const uint8_t *payload, int payload_size, int unit_start, int counter)
{
    int done;
    do {
        done = extract_psi(&pmt->psi, payload, payload_size, unit_start, counter);
        if (pmt->psi.version_number &&
            pmt->psi.current_next_indicator &&
            pmt->psi.table_id == 2 &&
            pmt->psi.section_length >= 9)
        {
            // Update PMT
            const uint8_t *table = pmt->psi.data;
            pmt->program_number = (table[3] << 8) | table[4];
            pmt->version_number = pmt->psi.version_number;
            pmt->pcr_pid = ((table[8] & 0x1f) << 8) | table[9];
            int program_info_length = ((table[10] & 0x03) << 8) | table[11];

            pmt->first_video_pid = 0;
            pmt->first_adts_audio_pid = 0;
            pmt->first_id3_metadata_pid = 0;

            int pos = 3 + 9 + program_info_length;
            while (pos + 4 < 3 + pmt->psi.section_length - 4/*CRC32*/) {
                int stream_type = table[pos];
                int pid = ((table[pos + 1] & 0x1f) << 8) | table[pos + 2];
                if ((stream_type == AVC_VIDEO || stream_type == H_265_VIDEO) && pmt->first_video_pid == 0) {
                    pmt->first_video_stream_type = stream_type;
                    pmt->first_video_pid = pid;
                }
                else if (stream_type == ADTS_TRANSPORT && pmt->first_adts_audio_pid == 0) {
                    pmt->first_adts_audio_pid = pid;
                }
                else if (stream_type == PES_ID3_METADATA && pmt->first_id3_metadata_pid == 0) {
                    pmt->first_id3_metadata_pid = pid;
                }
                int es_info_length = ((table[pos + 3] & 0x03) << 8) | table[pos + 4];
                pos += 5 + es_info_length;
            }
        }
    }
    while (!done);
}

int contains_nal_irap(int *nal_state, const uint8_t *payload, int payload_size, bool h_265)
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
            int nal_unit_type = h_265 ? (payload[i] >> 1) & 0x3f : payload[i] & 0x1f;
            if (h_265 ? (nal_unit_type == 19 || nal_unit_type == 20 || nal_unit_type == 21) : (nal_unit_type == 5)) {
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

int64_t get_pes_timestamp(const uint8_t *data_5bytes)
{
    return (data_5bytes[4] >> 1) |
           (data_5bytes[3] << 7) |
           ((data_5bytes[2] & 0xfe) << 14) |
           (data_5bytes[1] << 22) |
           (static_cast<int64_t>(data_5bytes[0] & 0x0e) << 29);
}
