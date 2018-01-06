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
#include <apr.h>
#include <apr_file_io.h>
#include <apr_general.h>

namespace {
// Namespace for APR related stuff

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;

void panic_on_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        throw std::runtime_error(std::string(msg) + " " + error_message);
    }
}

void _close_apr_file(apr_file_t* file) {
    apr_file_close(file);
}

AprPoolPtr _make_apr_pool() {
    apr_pool_t* mem_pool = NULL;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    panic_on_error(status, "Can't create APR pool");
    AprPoolPtr pool(mem_pool, &apr_pool_destroy);
    return std::move(pool);
}

AprFilePtr _open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_WRITE|APR_BINARY|APR_CREATE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

AprFilePtr _open_file_ro(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_BINARY, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

size_t _get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    panic_on_error(status, "Can't get file info");
    return static_cast<size_t>(info.size);
}

size_t _write_frame(AprFilePtr& file, uint32_t size, void* array) {
    size_t outsize = 0;
    iovec io[2] = {
        {&size, sizeof(uint32_t)},
        {array, size},
    };
    apr_status_t status = apr_file_writev_full(file.get(), io, 2, &outsize);
    panic_on_error(status, "Can't write frame to file");
    return outsize;
}

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
    AprPoolPtr pool_;
    AprFilePtr file_;
    size_t file_size_;
    const size_t max_file_size_;
    Roaring64Map bitmap_;
    const bool is_read_only_;

    void clear(int i) {
        memset(&frames_[i], 0, BLOCK_SIZE);
    }

    void write(int i) {
        assert(!is_read_only_);
        Frame& frame = frames_[i];
        // Do write
        int out_bytes = LZ4_compress_fast_continue(&stream_,
                                                   frame.block,
                                                   write_buf_,
                                                   BLOCK_SIZE,
                                                   sizeof(write_buf_),
                                                   1);
        if(out_bytes <= 0) {
            throw std::runtime_error("LZ4 error");
        }
        file_size_ += _write_frame(file_, static_cast<uint32_t>(out_bytes), write_buf_);
    }
public:
    /**
     * @brief Create empty volume
     * @param file_name is string that contains volume file name
     * @param volume_size is a maximum allowed volume size
     */
    LZ4Volume(const char* file_name, size_t volume_size)
        : path_(file_name)
        , pos_(0)
        , pool_(_make_apr_pool())
        , file_(_open_file(file_name, pool_.get()))
        , file_size_(0)
        , max_file_size_(volume_size)
        , is_read_only_(false)
    {
        clear(0);
        clear(1);
        LZ4_resetStream(&stream_);
    }

    /**
     * @brief Read existing volume
     * @param file_name volume file name
     */
    LZ4Volume(const char* file_name)
        : path_(file_name)
        , pos_(0)
        , pool_(_make_apr_pool())
        , file_(_open_file_ro(file_name, pool_.get()))
        , file_size_(_get_file_size(file_.get()))
        , max_file_size_(0)
        , is_read_only_(true)
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
        return file_size_ >= max_file_size_;
    }

    const std::string get_path() const {
        return path_;
    }

    void delete_file() {
        file_.reset();
        remove(path_.c_str());
    }

    const Roaring64Map& get_index() const {
        return bitmap_;
    }
};


class InputLog {
    std::deque<std::unique_ptr<LZ4Volume>> volumes_;
    std::string root_dir_;
    size_t volume_counter_;
    const size_t max_volumes_;
    const size_t volume_size_;

    std::string get_volume_name() {
        // TODO: use boost filesystem
        std::stringstream fmt;
        fmt << root_dir_ << "/" << "inputlog" << volume_counter_ << ".ils";
        return fmt.str();
    }

    void add_volume(std::string path) {
        std::unique_ptr<LZ4Volume> volume(new LZ4Volume(path.c_str(), volume_size_));
        volumes_.push_front(std::move(volume));
        volume_counter_++;
    }

    void remove_last_volume() {
        auto volume = std::move(volumes_.back());
        volumes_.pop_back();
        volume->delete_file();
    }

public:
    InputLog(const char* rootdir, size_t nvol, size_t svol)
        : root_dir_(rootdir)
        , volume_counter_(0)
        , max_volumes_(nvol)
        , volume_size_(svol)
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
        bool result = volumes_.front()->append(id, timestamp, value);
        if (result && volumes_.size() == max_volumes_) {
            // Extract stale ids
            assert(volumes_.size() > 0);
            std::vector<const Roaring64Map*> remaining;
            for (size_t i = 0; i < volumes_.size() - 1; i++) {
                // move from newer to older volumes
                remaining.push_back(&volumes_.at(i)->get_index());
            }
            Roaring64Map sum = Roaring64Map::fastunion(remaining.size(), remaining.data());
            auto stale = volumes_.back()->get_index() - sum;
            for (auto it = stale.begin(); it != stale.end(); it++) {
                stale_ids->push_back(*it);
            }
        }
        return result;
    }

    void rotate() {
        if (volumes_.size() >= max_volumes_) {
            remove_last_volume();
        }
        std::string path = get_volume_name();
        add_volume(path);
    }
};

int main()
{
    apr_initialize();
    InputLog logger("/tmp", 5, 10*1024*1024);
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
    int64_t nitems = 10000;
    int64_t total  = 1;
    int64_t basets = 10000000;
    // push single item
    std::vector<uint64_t> outids;
    bool res = logger.append(42, basets, 0.1234, &outids);
    assert(!res);
    int rotation = 0;
    for (int64_t i = 0; i < nitems; i++) {
        for (int j = 0; j < nids; j++) {
            uint64_t id  = ids[j];
            double value = basis[j];
            basis[j] += delta[j];
            if (logger.append(id, i + basets, value, &outids)) {
                if (outids.size() != 1) {
                    std::cout << outids.size() << " elements is out" << std::endl;
                } else {
                    std::cout << outids.at(0) << " is out" << std::endl;
                }
                outids.clear();
                std::cout << "rotation " << rotation << std::endl;
                rotation++;
                logger.rotate();
                std::cout << std::endl;
            }
            total++;
        }
    }
    std::cout << total << " values had been written" << std::endl;
    return 0;
}
