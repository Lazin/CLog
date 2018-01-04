#include <iostream>
#include <string>
#include <stdint.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

#include <lz4.h>
#include <roaring64map.hh>

size_t write_int(FILE* fp, int i) {
    return fwrite(&i, sizeof(i), 1, fp);
}

size_t write_bin(FILE* fp, const void* array, size_t arrayBytes) {
    return fwrite(array, 1, arrayBytes, fp);
}


class LZ4Compressor {
    std::string path_;
    enum {
        BLOCK_SIZE = 0x2000,
        //NUM_TUPLES = 255,
        NUM_TUPLES = 340,
        FRAME_TUPLE_SIZE = sizeof(uint64_t)*3,
    };
    union Frame {
        char block[BLOCK_SIZE];
        struct Partition {
            uint32_t size;
            uint64_t ids[NUM_TUPLES];
            uint64_t timestamps[NUM_TUPLES];
            //uint64_t mantissa[NUM_TUPLES];
            //uint64_t exponent[NUM_TUPLES];
            double values[NUM_TUPLES];
        } part;
    } frames_[2];

    char write_buf_[LZ4_COMPRESSBOUND(BLOCK_SIZE)];

    int pos_;
    LZ4_stream_t stream_;
    FILE* file_;
    size_t file_size_;
    size_t max_file_size_;
    Roaring64Map bitmap_;

    void clear(int i) {
        memset(&frames_[i], 0, BLOCK_SIZE);
    }

    void write(int i) {
        Frame& frame = frames_[i];
        // Do write
        int out_bytes = LZ4_compress_fast_continue(&stream_, frame.block, write_buf_, BLOCK_SIZE, sizeof(write_buf_), 1);
        if(out_bytes <= 0) {
            throw std::runtime_error("LZ4 error");
        }
        file_size_ += write_int(file_, out_bytes);
        file_size_ += write_bin(file_, write_buf_, static_cast<size_t>(out_bytes));
    }
public:
    LZ4Compressor(const char* file_name) {
        path_ = file_name;
        pos_  = 0;
        clear(0);
        clear(1);
        LZ4_resetStream(&stream_);
        file_ = fopen(file_name, "wb");
        file_size_ = 0;
        max_file_size_ = 1024*1024*256;  // 256MB
    }

    size_t file_size() const {
        return file_size_;
    }

    ~LZ4Compressor() {
        fclose(file_);
    }

    bool append(uint64_t id, uint64_t timestamp, double value) {
        bitmap_.add(id);
        union {
            double x;
            uint64_t d;
        } bits;
        bits.x = value;
        //uint64_t mantissa = bits.d & ((1ull << 53) - 1);
        //uint16_t exponent = bits.d >> 10;
        Frame& frame = frames_[pos_];
        frame.part.ids[frame.part.size] = id;
        frame.part.timestamps[frame.part.size] = timestamp;
        //frame.part.mantissa[frame.part.size] = mantissa;
        //frame.part.exponent[frame.part.size] = exponent;
        frame.part.values[frame.part.size] = value;
        frame.part.size++;
        if (frame.part.size == NUM_TUPLES) {
            write(pos_);
            pos_ = (pos_ + 1) % 2;
            clear(pos_);
        }
        return file_size_ < max_file_size_;
    }
};

int main()
{
    //for (std::string line; std::getline(std::cin, line);) {
    //}
    LZ4Compressor compressor("/tmp/log.lz4");
    int nids = 1000;
    std::vector<uint64_t> ids;
    for (int i = 0; i < nids; i++) {
        ids.push_back(i + 1000);
    }
    std::vector<double> basis;
    std::vector<double> delta;
    for (int i = 0; i < nids; i++) {
        basis.push_back(static_cast<double>(rand() % 100000)/1000.0);
        delta.push_back(static_cast<double>(rand() % 100)/1000.0);
    }
    int64_t nvalues = 10000;
    int64_t total   = 0;
    for (int64_t i = 0; i < nvalues; i++) {
        for (int j = 0; j < nids; j++) {
            uint64_t id  = ids[j];
            double value = basis[j];
            basis[j] += delta[j];
            compressor.append(id, i + 10000000, value);
            total++;
        }
    }
    std::cout << total << " values had been written" << std::endl;
    return 0;
}
