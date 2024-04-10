/*
 * test_write.cpp
 *
 *  Created on: Apr 10, 2024
 *      Author: mad
 */

#include <mad/DirectFile.h>

#include <cmath>
#include <cstdio>
#include <iostream>
#include <random>
#include <vector>
#include <chrono>

inline
int64_t get_time_micros() {
	return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}


int main(int argc, char** argv)
{
	if(argc < 2) {
		return -1;
	}
	const std::string path(argv[1]);

	::remove(path.c_str());

	const uint64_t file_size = uint64_t(argc > 2 ? atoi(argv[2]) : 1024) * 1024 * 1024;

	std::cout << "File: " << path << std::endl;
	std::cout << "Size: " << file_size / pow(1024, 3) << " GiB" << std::endl;

	std::default_random_engine generator;

	std::vector<uint64_t> data(1 << 21);
	for(auto& v : data) {
		v = generator();
	}
	const size_t data_size = data.size() * 8;

	const auto time_begin = get_time_micros();
	{
		mad::DirectFile file(path, false, true, true);

		std::cout << "Direct IO: " << (file.is_direct() ? "yes" : "no") << std::endl;

		mad::DirectFile::buffer_t buffer;

		for(uint64_t offset = 0; offset < file_size;)
		{
			const auto src = offset % data_size;

			auto count = generator() % data_size;
			count = std::min(count, file_size - offset);
			count = std::min(count, data_size - src);

			file.write(((const uint8_t*)data.data()) + src, count, offset, buffer);

			if(offset % 16 == 1) {
				file.flush();
				std::cout << "Flushed at offset " << offset << " (" << offset / double(file_size) << ")" << std::endl;
			}
			offset += count;
		}
		file.close();
	}
	const auto time_end = get_time_micros();

	const auto elapsed = (time_end - time_begin) / 1e6;
	std::cout << "Took " << elapsed << " sec, " << file_size / elapsed / pow(1024, 2) << " MiB/s" << std::endl;

	{
		FILE* file = fopen(path.c_str(), "rb");

		std::vector<uint8_t> buffer(1024 * 1024);

		for(uint64_t offset = 0; offset < file_size;)
		{
			const auto count = std::min(buffer.size(), file_size - offset);

			if(::fread(buffer.data(), 1, count, file) != count) {
				throw std::logic_error("fread() failed at offset " + std::to_string(offset));
			}
			const auto src = ((const uint8_t*)data.data()) + (offset % data_size);

			if(::memcmp(buffer.data(), src, count)) {
				std::cerr << "ERROR: wrong data at offset " << offset << std::endl;
			}
			offset += count;
		}
		fclose(file);
	}
	std::cout << "Verify passed" << std::endl;

	return 0;
}


