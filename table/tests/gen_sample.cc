


#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

using namespace std;

const char* path = 0;
//const char* path = "./samples_large.txt";
std::string gen() {
	char arr[51] = { 0 };
	int sz = max(std::rand() % 50, 4);
	for (int i = 0; i < sz; i++) {
		// ascii: 33 ~ 126
		int random_variable = std::rand() % 94 + 33;
		arr[i] = random_variable;
	}
	return arr;
}

int main(int argc, char* argv[]) {
	bool simple_key = false;
	if (argc == 1) {
		path = "./samples_simple.txt";
	} else {
		path = "./samples_large.txt";
	}
	//std::srand(std::time(0)); // use current time as seed for random generator
	ofstream fo(path);
  char carr[17] = { 0 };
	for (int i = 0; i < 10; i++) {
    carr[7] = i;
    for (int j = 0; j < 10; j++) {
      carr[15] = j;
      //fo << carr << '\n';
      fo.write(carr, 16);
      fo << '\n';
    }
	}
  fo.close();
	//std::cout << "Random value on [0 " << RAND_MAX << "]: " 
	//        << random_variable << '\n';
	return 0;
}

