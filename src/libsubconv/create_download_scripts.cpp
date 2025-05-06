#include <subconv.hpp>
#include <strutils.hpp>
#include <web/web.hpp>

using std::string;
using std::vector;
using strutils::replace_all;

namespace subconv {

void create_download_scripts(const vector<string>& wget_list, string
    dsrqst_root) {
  putenv(const_cast<char *>(http_cookie.c_str()));
  putenv(const_cast<char *>("SERVER_NAME=rda.ucar.edu"));
  auto web_download_directory = args.download_directory;
  replace_all(web_download_directory, dsrqst_root, "");
  if (request_values.ancillary.tarflag != "Y") {
    std::ofstream download_script_fs((args.download_directory + "/unix-wget." +
        args.rqst_index + ".csh").c_str());
    create_wget_script(wget_list, "https://request.rda.ucar.edu",
        web_download_directory, "csh", &download_script_fs);
    download_script_fs.close();
    download_script_fs.clear();
    download_script_fs.open((args.download_directory + "/unix-curl." + args.
        rqst_index + ".csh").c_str());
    create_curl_script(wget_list, "https://request.rda.ucar.edu",
        web_download_directory,"csh",&download_script_fs);
    download_script_fs.close();
    download_script_fs.clear();
    download_script_fs.open((args.download_directory + "/dos-wget." + args.
        rqst_index + ".bat").c_str());
    create_wget_script(wget_list, "https://request.rda.ucar.edu",
        web_download_directory,"bat",&download_script_fs);
    download_script_fs.close();
    download_script_fs.clear();
  }
}

} // end namespace subconv
