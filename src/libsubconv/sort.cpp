#include <subconv.hpp>
#include <bsort.hpp>

namespace subconv {

void sort_to_nc_order(std::string input_filename, std::string output_filename) {
  std::vector<SortData> sort_list;
  std::unique_ptr<unsigned char []> buffer;
  int buffer_capacity = 0;
  InputGRIBStream istream;
  if (!istream.open(input_filename)) {
    throw std::runtime_error("sort(): unable to open " + input_filename +
        " for input");
  }
  int num_bytes;
  while ( (num_bytes = istream.peek()) > 0) {
    if (num_bytes > buffer_capacity) {
      buffer_capacity = num_bytes;
      buffer.reset(new unsigned char[buffer_capacity]);
    }
    auto offset = istream.current_record_offset();
    istream.read(buffer.get(), buffer_capacity);
    GRIB2Message msg;
    msg.fill(buffer.get(), Grid::HEADER_ONLY);
    auto grid = msg.grid(0);
    GRIB2Grid *g2 = reinterpret_cast<GRIB2Grid *>(grid);
    sort_list.emplace_back(grid->valid_date_time(), 0, g2->discipline() *
        1000000 + g2->parameter_category() * 1000 + g2->parameter(), offset,
        num_bytes);
  }
  istream.close();
  binary_sort(sort_list,
      [](const SortData& left, const SortData& right) -> bool {
        if (left.m_datetime < right.m_datetime) {
          return true;
        }
        if (left.m_datetime > right.m_datetime) {
          return false;
        }
        if (left.m_param < right.m_param) {
          return true;
        }
        if (left.m_param > right.m_param) {
          return false;
        }
        if (left.m_lvl < right.m_lvl) {
          return true;
        }
        return false;
      });
  std::ofstream ofs(output_filename.c_str());
  if (!ofs.is_open()) {
    throw std::runtime_error("sort(): unable to open output file");
  }
  std::ifstream ifs(input_filename.c_str());
  for (const auto& item : sort_list) {
    ifs.seekg(item.m_offset, std::ios::beg);
    ifs.read(reinterpret_cast<char *>(buffer.get()), item.m_num_bytes);
    ofs.write(reinterpret_cast<char *>(buffer.get()), item.m_num_bytes);
  }
  ifs.close();
  ofs.close();
}

} // end namespace subconv
