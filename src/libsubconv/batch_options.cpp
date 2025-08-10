#include <iomanip>
#include <sstream>
#include <subconv.hpp>
#include <strutils.hpp>

using std::setfill;
using std::setw;
using std::string;
using std::stringstream;

namespace subconv {

string batch_options(const Directives& directives) {
  auto cpus_per_task = 0;
  string mem;
  if (request_values.ofmt.empty()) {
    if (request_values.nlat < 99. && request_values.elon < 999.) {
      cpus_per_task = lround(args.num_threads * 0.85);
      mem = "500M";
    } else {
      cpus_per_task = args.num_threads / 3;
      mem = "200M";
    }
  } else {
    cpus_per_task = lround(args.num_threads * 0.85);
    mem = "1G";
  }
  if (cpus_per_task == 0) {
    cpus_per_task = 1;
  }
  auto time_minutes = 0;
  if ( (timing_data.num_reads & 0x40000000) == 0x40000000) {
    timing_data.num_reads = timing_data.num_reads & 0x2fffffff;
    if (request_values.ofmt.empty()) {
      time_minutes = timing_data.num_reads;
    } else {
      time_minutes = timing_data.num_reads * 5;
    }
  } else {
    if (request_values.ofmt.empty()) {
      time_minutes = timing_data.num_reads / 5 / 60;
    } else {
      time_minutes = timing_data.num_reads / 60;
    }
  }
  if (args.num_threads == 0) {
    throw std::runtime_error("batch_options(): divide by zero: "
        "args.num_threads was not set");
  }
  time_minutes /= args.num_threads;
  if (time_minutes == 0) {
    ++time_minutes;
  }

  // set priority in the range of 1 to 10
  int decile = lroundf(time_minutes / 144.);
  if (request_values.ancillary.user_country != "UNITED.STATES") {
    decile += 5;
  }
  auto priority = strutils::itos(std::min(std::max(decile, 1), 10));
  if (rdadb_server.update("dssdb.dsrqst", "priority = " + priority, "rindex "
      "= " + args.rqst_index) < 0) {
    throw std::runtime_error("batch_options(): unable to set priority (" +
        priority + ") for request index " + args.rqst_index);
  }
  short hr, min;
  time_minutes = std::min(time_minutes, 1410);
  hr = time_minutes / 60;
  min = (time_minutes % 60);
  if (min < 30) {
    min = 30;
  } else {
    ++hr;
    min = 0;
  }
  stringstream batch_options;
  switch (args.batch_type) {
    case 'B': {
      batch_options << "--cpus-per-task=" << cpus_per_task * 2 << " --mem=" <<
          mem << " -t " << hr << ":" << setw(2) << setfill('0') << min << ":00";
      break;
    }
    case 'Q': {
      stringstream select;
      select << "1:ncpus=" << cpus_per_task << ":mem=" << mem;
      batch_options << "-l select=";
      if (directives.host_restrict.empty()) {
        batch_options << select.str();
      } else {
        for (size_t n = 0; n < directives.host_restrict.size(); ++n) {
          if (n > 0) {
            batch_options << "+";
          }
          batch_options << select.str() << ":host=" <<
              directives.host_restrict[n];
        }
      }
      batch_options << ",walltime=" << setw(2) << setfill('0') << hr << ":" <<
          setw(2) << min << ":00";
      break;
    }
  }
  return batch_options.str();
}

} // end namespace subconv
