/*
 * DirectFile.h
 *
 *  Created on: Apr 9, 2024
 *      Author: mad
 */

#ifndef INCLUDE_DIRECTFILE_H_
#define INCLUDE_DIRECTFILE_H_

#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <stdexcept>
#include <algorithm>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>


namespace mad {

class DirectFile {
public:
	/*
	 * Thread local buffer used to align memory address.
	 * Should be re-used between calls from the same thread, to avoid alloc / free overhead.
	 */
	struct buffer_t
	{
		uint8_t* data = nullptr;

		~buffer_t() {
			if(data) {
				::free(data);
				data = nullptr;
			}
		}
	};

	/*
	 * Note: read_flag needs to be true if file has existing content that needs to be preserved!
	 */
	DirectFile(	const std::string& file_path, bool read_flag, bool write_flag, bool create_flag = false,
				int log_page_size = 12, size_t buffer_size = 1024 * 1024)
		:	read_flag(read_flag),
			write_flag(write_flag),
			log_page_size(log_page_size),
			page_size(1 << log_page_size),
			align_mask(page_size - 1),
			buffer_size(buffer_size)
	{
		int flags = 0;
		if(write_flag) {
			flags |= O_RDWR;
		} else {
			flags |= O_WRONLY;
		}
		if(create_flag) {
			flags |= O_CREAT;
		}
		const auto mode = 0644;

		fd = ::open(file_path.c_str(), flags | O_DIRECT, mode);
		if(fd < 0) {
			fd = ::open(file_path.c_str(), flags, mode);	// fall back to sucks IO
		} else {
			direct_flag = true;
		}
		if(fd < 0) {
			throw std::runtime_error("open() failed with: " + std::string(std::strerror(errno)));
		}
	}

	~DirectFile() {
		close();
	}

	/*
	 * Note: thread-safe
	 * Note: `buffer` should be default initialized and re-used between calls from the same thread.
	 */
	void write(const void* data, const size_t length, const uint64_t offset, buffer_t& buffer)
	{
		if(!buffer.data) {
			buffer.data = (uint8_t*)::aligned_alloc(page_size, buffer_size);
		}

		size_t total = 0;
		const auto src = (const uint8_t*)data;
		{
			const auto offset_mod = offset & align_mask;
			if(offset_mod) {
				// handle unaligned start address
				const auto count = std::min<size_t>(page_size - offset_mod, length);
				{
					std::lock_guard<std::mutex> lock(mutex);
					::memcpy(get_page(offset) + offset_mod, data, count);
				}
				total += count;
			}
		}

		while(total < length)
		{
			size_t count = std::min<size_t>(length - total, buffer_size);
			if(count >= page_size) {
				count &= ~size_t(align_mask);	// align count to page size

				::memcpy(buffer.data, src + total, count);

				const auto addr = offset + total;
				if(::pwrite(fd, buffer.data, count, addr) != count) {
					throw std::runtime_error("pwrite() failed with: " + std::string(std::strerror(errno)));
				}
				const auto begin = addr >> log_page_size;
				const auto end = (addr + count) >> log_page_size;

				std::lock_guard<std::mutex> lock(mutex);

				// discard any cached pages that we just over-wrote
				for(auto iter = cache.lower_bound(begin); iter != cache.end();)
				{
					if(iter->first < end) {
						::free(iter->second);
						iter = cache.erase(iter);
					} else {
						break;
					}
				}
			} else {
				// final unaligned tail
				std::lock_guard<std::mutex> lock(mutex);
				::memcpy(get_page(offset + total), src + total, count);
			}
			total += count;
		}
	}

	/* Flush all cached pages to file.
	 * Note: thread-safe
	 */
	void flush()
	{
		if(fd < 0) {
			return;
		}
		std::lock_guard<std::mutex> lock(mutex);

		for(const auto& entry : cache) {
			if(::pwrite(fd, entry.second, page_size, entry.first * page_size) != page_size) {
				throw std::runtime_error("pwrite() on flush failed with: " + std::string(std::strerror(errno)));
			}
			::free(entry.second);
		}
		cache.clear();

		read_flag = true;
	}

	/*
	 * Flush cache and close file.
	 * Note: NOT thread-safe
	 */
	void close()
	{
		if(fd >= 0) {
			flush();
			if(::close(fd) < 0) {
				throw std::runtime_error("close() failed with: " + std::string(std::strerror(errno)));
			}
			fd = -1;
		}
	}

	// returns true when actually using Direct IO
	bool is_direct() const {
		return direct_flag;
	}

protected:
	uint8_t* get_page(const uint64_t address)
	{
		const auto index = address >> log_page_size;
		auto& page = cache[index];
		if(!page) {
			page = (uint8_t*)::aligned_alloc(page_size, page_size);
			if(read_flag) {
				const auto ret = ::pread(fd, page, page_size, index * page_size);
				if(ret <= 0) {
					::memset(page, 0, page_size);
				} else {
					::memset(page + ret, 0, page_size - ret);
				}
			} else {
				::memset(page, 0, page_size);
			}
		}
		return page;
	}

private:
	bool read_flag;
	const bool write_flag;
	const int log_page_size;
	const uint32_t page_size;
	const uint32_t align_mask;
	const size_t buffer_size;

	int fd = -1;
	bool direct_flag = false;

	std::mutex mutex;
	std::map<uint64_t, uint8_t*> cache;

};


} // mad

#endif /* INCLUDE_DIRECTFILE_H_ */
