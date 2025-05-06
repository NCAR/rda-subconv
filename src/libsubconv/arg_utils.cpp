#include <iostream>
#include <subconv.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using std::cerr;
using std::endl;

namespace subconv {

void check_usage(int argc)
{
  if (argc < 3 || argc > 6) {
    cerr << "USAGE: subconv [OPTIONS...] REQUEST_NUMBER DIRECTORY" << endl;
    cerr << "\nSYNOPSIS:" << endl;
    cerr << "    Process a dsrqst request by performing some combination of "
        "parameter," << endl;
    cerr << "    temporal, and spatial subsetting. Also, possibly perform a "
        "format conversion" << endl;
    cerr << "    from the native data format to some other data format." << endl;
    cerr << "\nREQUIRED:" << endl;
    cerr << "  REQUEST_NUMBER\n    the six-digit dsrqst request number" << endl;
    cerr << "  DIRECTORY\n    full path to directory where request files will "
        "reside" << endl;
    cerr << "\nOPTIONS:" << endl;
    cerr << "  -I    ignore volume and allow large request to process" << endl;
    cerr << "  -n <num_threads>\n        number of threads to use during "
        "processing" << endl;
    cerr << "  -R    ignore user restrictions and process request" << endl;
    cerr << "  -t    turn on internal timings" << endl;
    cerr << "\n-OR-\n" << endl;
    cerr << "USAGE: subconv -B | -Q REQUEST_NUMBER" << endl;
    cerr << "\nSYNOPSIS:" << endl;
    cerr << "    Return a string of \"sbatch\" options that would be "
        "appropriate for the given" << endl;
    cerr << "    request number." << endl;
    cerr << "\nREQUIRED:" << endl;
    cerr << "  -B    indicates that this is an \"sbatch\" options run" << endl;
    cerr << "  -Q    indicates that this is an \"qsub\" options run" << endl;
    cerr << "  REQUEST_NUMBER\n        the six-digit dsrqst request number" <<
        endl;
    cerr << "\n-OR-\n" << endl;
    cerr << "USAGE: subconv -T RINFO_STRING" << endl;
    cerr << "\nSYNOPSIS:" << endl;
    cerr << "    Test a request by using the \"subsetting string\" that would "
        "be placed in" << endl;
    cerr << "    the \"rinfo\" field of dssdb.dsrqst for the request. This is "
        "used by users" << endl;
    cerr << "    who want to use automated subset request submissions so that "
        "they can know" << endl;
    cerr << "    whether or not their requests are valid before they actually "
        "submit them." << endl;
    cerr << "\nREQUIRED:" << endl;
    cerr << "  -T    indicates that this is a \"test\" run" << endl;
    cerr << "  RINFO_STRING\n        the full colon-separated \"subsetting "
        "string\" containing the dataset" << endl;
    cerr << "        number, temporal range, parameters, levels, etc." << endl;
    exit(1);
  }
}

void parse_args(int argc, char **argv)
{
  auto parts = strutils::split(unixutils::unix_args_string(argc, argv, '%'),
      "%");
  size_t next = 0;
  while (next < parts.size() && parts[next][0] == '-') {
    if (parts[next] == "-B") {
	args.rqst_index = parts[++next];
	args.batch_type = 'B';
	args.is_test = true;
    } else if (parts[next] == "-I") {
	args.ignore_volume = true;
    } else if (parts[next] == "-n") {
	args.num_threads = std::stoi(parts[++next]);
    } else if (parts[next] == "-Q") {
	args.rqst_index = parts[++next];
	args.batch_type = 'Q';
	args.is_test = true;
    } else if (parts[next] == "-R") {
	args.ignore_restrictions = true;
    } else if (parts[next] == "-t") {
	args.get_timings = true;
	args.main_timer.start();
    } else if (parts[next] == "-T") {
	args.rinfo = parts[++next];
	args.download_directory = "x";
	args.is_test = true;
    }
    ++next;
  }
  if (args.batch_type == 0x0 && !args.is_test) {
    args.rqst_index = parts[next++];
    args.download_directory = parts[next++];
  }
}

} // end namespace subconv
