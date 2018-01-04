#include <iostream>
#include <string>
#include <stdint.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <memory>
#include <deque>
#include <sstream>

#include <lz4.h>
#include <roaring64map.hh>

static void fclose_(FILE* f) {
    fclose(f);
}

size_t write_int(FILE* fp, int i) {
    return fwrite(&i, sizeof(i), 1, fp);
}

size_t write_bin(FILE* fp, const void* array, size_t arrayBytes) {
    return fwrite(array, 1, arrayBytes, fp);
}


class LZ4Volume {
    std::string path_;
    enum {
        BLOCK_SIZE = 0x2000,
        FRAME_TUPLE_SIZE = sizeof(uint64_t)*3,
        NUM_TUPLES = (BLOCK_SIZE - sizeof(uint32_t)) / FRAME_TUPLE_SIZE,
    };
    union Frame {
        char block[BLOCK_SIZE];
        struct Partition {
            uint32_t size;
            uint64_t ids[NUM_TUPLES];
            uint64_t timestamps[NUM_TUPLES];
            double values[NUM_TUPLES];
        } part;
    } frames_[2];

    char write_buf_[LZ4_COMPRESSBOUND(BLOCK_SIZE)];

    int pos_;
    LZ4_stream_t stream_;
    std::unique_ptr<FILE, int (*)(FILE*)> file_;
    size_t file_size_;
    const size_t max_file_size_;
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
        // TODO: I/O errors should be handled
        file_size_ += write_int(file_.get(), out_bytes);
        file_size_ += write_bin(file_.get(), write_buf_, static_cast<size_t>(out_bytes));
    }
public:
    LZ4Volume(const char* file_name)
        : path_(file_name)
        , pos_(0)
        , file_((FILE*)fopen(file_name, "wb"), &fclose_)
        , file_size_(0)
        , max_file_size_(1024*1024*256)  // 256MB
    {
        clear(0);
        clear(1);
        LZ4_resetStream(&stream_);
    }

    size_t file_size() const {
        return file_size_;
    }

    bool append(uint64_t id, uint64_t timestamp, double value) {
        bitmap_.add(id);
        union {
            double x;
            uint64_t d;
        } bits;
        bits.x = value;
        Frame& frame = frames_[pos_];
        frame.part.ids[frame.part.size] = id;
        frame.part.timestamps[frame.part.size] = timestamp;
        frame.part.values[frame.part.size] = value;
        frame.part.size++;
        if (frame.part.size == NUM_TUPLES) {
            write(pos_);
            pos_ = (pos_ + 1) % 2;
            clear(pos_);
        }
        return file_size_ < max_file_size_;
    }

    const std::string get_path() const {
        return path_;
    }

    void delete_file() {
        file_.reset();
        remove(path_.c_str());
        // TODO: log volume->get_path() deleted
    }
};

class InputLog {
    std::deque<std::unique_ptr<LZ4Volume>> volumes_;
    std::string root_dir_;
    size_t volume_counter_;
    const size_t max_volumes_;

    std::string get_volume_name() {
        // TODO: use boost filesystem
        std::stringstream fmt;
        fmt << root_dir_ << "/" << inputlog << volume_counter_ << ".ils";
        return fmt.str();
    }

    void add_volume(std::string path) {
        std::unique_ptr<LZ4Volume> volume(new LZ4Volume(path.c_str()));
        volumes_.push_front(std::move(volume));
        volume_counter_++;
    }

    void remove_last_volume() {
        auto volume = std::move(volumes_.back());
        volumes_.pop_back();
        volume->delete_file();
    }
public:
    InputLog(const char* rootdir, size_t nvol)
        : root_dir_(rootdir)
        , volume_counter_(0)
        , max_volumes_(nvol)
    {
        std::string path = get_volume_name();
        add_volume(path);
    }

    /** Delete all files.
      */
    void delete_files() {
        for (auto& it: volumes_) {
            it->delete_file();
        }
    }

    /** Append data point to the log.
      * Return true on oveflow. Parameter `stale_ids` will be filled with ids that will leave the
      * input log on next rotation. Rotation should be triggered manually.
      */
    bool append(uint64_t id, uint64_t timestamp, double value, std::vector<uint64_t>* stale_ids) {
    }

    void rotate() {
        remove_last_volume();
        std::string path = get_volume_name();
        add_volume(path);
    }
};

int main()
{
    LZ4Volume compressor("/tmp/log.lz4");
    int nids = 1000;
    std::vector<uint64_t> ids;
    for (int i = 0; i < nids; i++) {
        ids.push_back(i + 1000);
    }
    std::vector<double> basis;
    std::vector<double> delta;
    for (int i = 0; i < nids; i++) {
        basis.push_back(static_cast<double>(rand() % 100000) / 1000.0);
        delta.push_back(static_cast<double>(rand() % 100) / 1000.0);
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
