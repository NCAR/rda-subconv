#include <iostream>
#include <subconv.hpp>

namespace subconv {

void InputDataSource::initialize(std::string posix_filename)
{
  if (posix_filename != posix.filename) {
    posix.ifs.reset(new std::ifstream(posix_filename.c_str()));
    if (!posix.ifs->is_open()) {
	throw std::runtime_error("InputDataSource::initialize(): error opening "+posix_filename+" for input");
    }
    posix.filename=posix_filename;
  }
  else {
    posix.ifs->seekg(0,std::ios::beg);
  }
  type=Type::_POSIX;
}

void InputDataSource::initialize(std::shared_ptr<s3::Session>& s3_session,std::string bucket,std::string key)
{
  s3.session=s3_session;
  s3.bucket=bucket;
  s3.key=key;
  type=Type::_S3;
}

void InputDataSource::read(off_t offset,size_t num_bytes)
{
  if (num_bytes > BUF_LEN) {
    BUF_LEN=num_bytes*2;
    if (BUF_LEN < 1024) {
        // set the buffer to at least 1 KB, otherwise there have been problems
        //  with freeing too small of a buffer - sometimes core dumps
        BUF_LEN=1024;
    }
    read_buffer.reset(new unsigned char[BUF_LEN]);
  }
  std::unique_ptr<Timer> timer(nullptr);
  if (args.get_timings) {
    timer.reset(new Timer);
    timer->start();
  }
  switch (type) {
    case Type::_POSIX: {
	posix.ifs->seekg(offset,std::ios_base::beg);
	posix.ifs->read(reinterpret_cast<char *>(read_buffer.get()),num_bytes);
	break;
    }
    case Type::_S3: {
	std::stringstream range_bytes_ss;
	range_bytes_ss << offset << "-" << (offset+num_bytes-1);
	std::string error;
	auto num_tries=0;
	while (num_tries < 3 && !s3.session->download_range(s3.bucket,s3.key,range_bytes_ss.str(),read_buffer,BUF_LEN,error)) {
	  ++num_tries;
	}
	if (num_tries == 3) {
	  throw std::runtime_error("InputDataSource::read(): error getting bytes "+range_bytes_ss.str()+" from "+s3.bucket+"/"+s3.key);
	}
	break;
    }
    default: {}
  }
  if (args.get_timings) {
    timer->stop();
    timing_data.read+=timer->elapsed_time();
    timing_data.read_bytes+=num_bytes;
    ++timing_data.num_reads;
  }
}

} // end namespace subconv
