#include <subconv.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::string;

namespace subconv {

void terminate(string stdout_message, string stderr_message) {
  if (args.is_test) {
    cout << stdout_message << endl;
    throw std::runtime_error("");
  } else {
    throw std::runtime_error(stderr_message);
  }
}

} // end namespace subconv
