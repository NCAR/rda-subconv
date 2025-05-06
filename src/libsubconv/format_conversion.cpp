#include <signal.h>
#include <sys/stat.h>
#include <subconv.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

using grid_to_netcdf::GridData;
using grid_to_netcdf::HouseKeeping;
using grid_to_netcdf::convert_grib_file_to_netcdf;
using UniqueVariableEntry = NetCDF::UniqueVariableEntry;
using std::runtime_error;
using std::unique_ptr;
using strutils::split;
using strutils::to_lower;

namespace subconv {

void do_conversion(ThreadData& thread_data) {
  unique_ptr<Timer> timer = nullptr;
  if (args.get_timings) {
    timer.reset(new Timer);
    timer->start();
  }
  GridData grid_data;
  grid_data.parameter_mapper = thread_data.parameter_mapper;
  thread_data.insert_filenames.clear();
  thread_data.wget_filenames.clear();
  auto fileinfo = split(thread_data.f_attach, "<!>");
  auto input_filename = args.download_directory + fileinfo[0];
  if (to_lower(request_values.ofmt) == "netcdf") {
    if (fileinfo[1] == "WMO_GRIB1" || fileinfo[1] == "WMO_GRIB2") {
      auto output_filename = input_filename + ".nc";
      InputGRIBStream istream;
      istream.open(input_filename.c_str());
      OutputNetCDFStream onc;
      if (!onc.open(output_filename + ".TMP")) {
        throw runtime_error("Error opening " + output_filename + " for output");
      }
      grid_data.subset_definition.latitude.south = request_values.slat;
      grid_data.subset_definition.latitude.north = request_values.nlat;
      grid_data.subset_definition.longitude.west = request_values.wlon;
      grid_data.subset_definition.longitude.east = request_values.elon;
      grid_data.path_to_gauslat_lists = args.SHARE_DIRECTORY + "/GRIB";
      HouseKeeping hk;
      hk.include_parameter_set = thread_data.include_parameter_codes_set;
      write_netcdf_header_from_grib_file(istream, onc, grid_data, hk,
            args.SHARE_DIRECTORY + "/metadata/LevelTables");
      if (grid_data.record_flag < 0) {
        Buffer buffer;
        void *msg = nullptr;
        if (fileinfo[1] == "WMO_GRIB1") {
          msg = new GRIBMessage;
        } else if (fileinfo[1] == "WMO_GRIB2") {
          msg = new GRIB2Message;
        }
        int len;
        while ( (len = istream.peek()) > 0) {
          buffer.allocate(len);
          istream.read(&buffer[0], len);
          size_t num_grids = 0;
          if (fileinfo[1] == "WMO_GRIB1") {
            reinterpret_cast<GRIBMessage *>(msg)->fill(&buffer[0], false);
            num_grids = reinterpret_cast<GRIBMessage *>(msg)->number_of_grids();
          } else if (fileinfo[1] == "WMO_GRIB2") {
            reinterpret_cast<GRIB2Message *>(msg)->fill(&buffer[0], false);
            num_grids = reinterpret_cast<GRIB2Message *>(msg)->
                number_of_grids();
          }
          for (size_t n = 0; n < num_grids; ++n) {
            Grid *grid = nullptr;
            if (fileinfo[1] == "WMO_GRIB1") {
              grid = reinterpret_cast<GRIBMessage *>(msg)->grid(n);
            } else if (fileinfo[1] == "WMO_GRIB2") {
              grid = reinterpret_cast<GRIB2Message *>(msg)->grid(n);
            }
            if (is_selected_parameter(thread_data, grid)) {
              Grid::Format grid_format{ Grid::Format::not_set };
              if (fileinfo[1] == "WMO_GRIB1") {
                grid_format = Grid::Format::grib;
              } else if (fileinfo[1] == "WMO_GRIB2") {
                grid_format = Grid::Format::grib2;
              }
              convert_grid_to_netcdf(grid, grid_format, &onc, grid_data,
                  args.SHARE_DIRECTORY + "/metadata/LevelTables");
            }
          }
        }
        if (msg != nullptr) {
          if (fileinfo[1] == "WMO_GRIB1") {
            delete reinterpret_cast<GRIBMessage *>(msg);
          } else if (fileinfo[1] == "WMO_GRIB2") {
            delete reinterpret_cast<GRIB2Message *>(msg);
          }
        }
      } else {
        convert_grib_file_to_netcdf(input_filename, onc,
            grid_data.ref_date_time, grid_data.cell_methods,
            grid_data.subset_definition, *thread_data.parameter_mapper,
            args.SHARE_DIRECTORY + "/metadata/LevelTables",
            hk.unique_variable_table, grid_data.record_flag);
      }
      istream.close();
      if (!onc.close()) {
        auto e = "Error: " + myerror + " for file " + output_filename;
        myerror = "";
        throw runtime_error(e);
      }
      system(("mv " + output_filename + ".TMP " + output_filename).c_str());
      struct stat buf;
      stat(output_filename.c_str(), &buf);
      thread_data.write_bytes = buf.st_size;
      thread_data.output_format = "netCDF";
      thread_data.insert_filenames.emplace_back(fileinfo[0].substr(1) + ".nc");
      thread_data.wget_filenames.emplace_back(fileinfo[0].substr(1) + ".nc" +
          request_values.ancillary.compression);
      for (const auto& key : hk.unique_variable_table.keys()) {
        UniqueVariableEntry ve;
        hk.unique_variable_table.found(key, ve);           
        ve.free_memory();
      }
    } else {
      throw runtime_error("Error: unable to convert from '" + fileinfo[1] +
          "'");
    }
    system(("rm -f " + input_filename).c_str());
  }
  if (args.get_timings) {
    timer->stop();
    thread_data.timing_data.thread = timer->elapsed_time();
  }
}

} // end namespace subconv
