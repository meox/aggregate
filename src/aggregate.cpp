#include <iostream>

#include <thread>
#include <vector>
#include <map>
#include <unordered_map>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <experimental/string_view>
#include <xxhash.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

/*
 *  Aggregator  (written by Gian Lorenzo Meocci <glmeocci@gmail.com>)
 *
 */


#define VERSION "1.4.0"


using namespace boost::filesystem;
using namespace std;
namespace std_exp = std::experimental;


typedef pair<int64_t, bool> pval_t;

template <typename T>
struct mapval_t
{
	std::vector<std::pair<T, bool>> sum_val;
	std::vector<std::string> key_val;
};


class Reader
{
public:
	Reader(const std::string& fname)
	{
		struct stat sb;
		int fd = open(fname.c_str(), O_RDONLY);
		fstat(fd, &sb);
		fsize = sb.st_size;
		addr = static_cast<char*>(mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
	}

	std_exp::string_view get_line()
	{
		if (addr == nullptr || end_reached)
			return "";
		
		const auto end_line_pos = get_end_line();
		if (end_line_pos != std::string::npos)
		{
			const auto diff = end_line_pos - p_buffer;
			std_exp::string_view r = std_exp::string_view(&addr[p_buffer], diff);
			p_buffer += diff+1;
			return r;
		}
		else
		{
			return std_exp::string_view(&addr[p_buffer], fsize - p_buffer);
		}
	}

	bool is_finished() const { return end_reached; }
	
	~Reader() { munmap(addr, fsize); }

private:
	inline size_t get_end_line()
	{
		for (size_t i = p_buffer; i < fsize; i++)
			if (addr[i] == end_line)
				return i;

		end_reached = true;
		return std::string::npos;
	}

	char* addr{nullptr};
	size_t fsize;
	size_t p_buffer{0};

	bool end_reached{false};
	constexpr const static char end_line{'\n'};
};

inline
std::vector<std_exp::string_view> split(const std_exp::string_view& line, char sep)
{
	static size_t reservation = 0;
	std::vector<std_exp::string_view> v;
	if (reservation != 0)
		v.reserve(reservation);

	const size_t l = line.length();
	size_t last = 0, i = 0, c = 0;
	for (; i < l; i++)
	{
		if (line[i] == sep)
		{
			v.push_back(std_exp::string_view(&line[last], (i - last)));
			i++;
			last = i;
			c++;
		}
	}

	//std::cerr << "last: " << last << ", i = " << i << std::endl;
	if (i - last > 1)
		v.push_back(std_exp::string_view(&line[last], (i - last)));
	else
		v.push_back(std_exp::string_view(""));

	reservation = c + 1;
	return v;
}


template <typename F>
void splitter(const string& fname, const string& separator, F fun, size_t skip_line)
{
	Reader reader{fname};
	size_t skipped{0};

	while (!reader.is_finished() && skipped < skip_line)
	{
		reader.get_line();
		skipped++;
	}

	while (!reader.is_finished())
	{
		const std_exp::string_view line = reader.get_line();
		if (line.empty())
			continue;

		fun(split(line, separator[0]));
	}
}


std::map<uint32_t, uint32_t> get_index_uint32(const string& index)
{
	std::map<uint32_t, uint32_t> indexs;
	const auto sep = boost::is_any_of(";");

	size_t pos{};
	std::vector<std::string> k_strs;
	boost::split(k_strs, index, sep);

	for(const auto& k : k_strs)
	{
		auto p = k.find("-");
		if(p != string::npos)
		{
			const auto b = stoull(k.substr(0, p));
			const auto e = stoull(k.substr(p+1));
			for (size_t n = b; n <= e; n++)
				indexs[n] = pos++;
		}
		else
		{
			indexs[std::stoul(k)] = pos++;
		}
	}

	return indexs;
}


std::vector<std::string> get_index_string(const std::string& index)
{
	vector<string> indexs;
	const auto sep = boost::is_any_of(";");

	vector<string> k_strs;
	boost::split(k_strs, index, sep);

	for(const auto& k : k_strs)
	{
		auto p = k.find("-");
		if(p != string::npos)
		{
			const auto b = stoull(k.substr(0, p));
			const auto e = stoull(k.substr(p+1));
			for(size_t n = b; n <= e; n++)
				indexs.push_back(std::to_string(n));
		}
		else
			indexs.push_back(k);
	}

	return indexs;
}


class BuildKey
{
public:
	BuildKey(const std::map<uint32_t, uint32_t>& key_index) : _key_index(key_index)
	{
		state = XXH64_createState();
	}

	uint64_t hash(const std::vector<std_exp::string_view>& line)
	{
		XXH64_reset(state, 0);
		for (const auto k : _key_index)
			XXH64_update(state, line[k.first].data(), line[k.first].size());
		return XXH64_digest(state);
	}

	~BuildKey()
	{
		XXH64_freeState(state);
	}

private:
	XXH64_state_t* state;
	const std::map<uint32_t, uint32_t>& _key_index;
};


int64_t fast_atol(const std_exp::string_view& str)
{
	size_t i;
	int64_t val{};
	const size_t l{str.length()};
	bool negative{false};

	if (l == 0) return 0;

	switch(str[0])
	{
		case '-':
		{
			i = 1;
			negative = true;
			break;
		}
		case '+':
		{
			i = 1;
			break;
		}
		default:
		{
			i = 0;
		}
	}

	for (; i < l; i++)
		val = val*10 + (str[i] - '0');

	if (negative)
		return -val;
	else
		return val;
}


void help();
void dry_run(
	const vector<string>& fnames,
	std::map<uint32_t, uint32_t>& keys_fields,
	std::map<uint32_t, uint32_t>& sum_fields,
	std::vector<std::string>& proj_fields,
	const map<string, string>& registers,
	const string& output_header,
	const string& input_sep);



int main(int argc, char* argv[])
{
	std::map<uint32_t, uint32_t> sum_fields;
	std::map<uint32_t, uint32_t> keys_fields;
	std::vector<std::string> proj_fields;

	std::map<std::string, std::string> registers;

	bool dry_run_exec{false};
	size_t skip_line{0};
	std::string output_header{};

	std::vector<std::string> header_lines{};
	std::vector<std::string> fnames{};
	int64_t no_value{-1};
	std::string input_sep{","}, output_sep{","};
	std::string output_file{"out.csv"};

	if (argc == 1)
	{
		help();
		return 0;
	}

	int i{};
	while(i < argc)
	{
		if (strcmp(argv[i], "--help") == 0)
		{
			help();
			return 0;
		}
		else if (strcmp(argv[i], "--version") == 0)
		{
			cout << "Aggregation Tool " << VERSION << " compiled on " << __DATE__ << "@" << __TIME__ << endl << endl;
			return 0;
		}
		else if (strcmp(argv[i], "-k") == 0)
			keys_fields = get_index_uint32(argv[++i]);
		else if (strcmp(argv[i], "-s") == 0)
			sum_fields = get_index_uint32(argv[++i]);
		else if (strcmp(argv[i], "-p") == 0)
			proj_fields = get_index_string(argv[++i]);
		else if (strcmp(argv[i], "--skip-line") == 0)
			skip_line = stoi(argv[++i]);
		else if (strcmp(argv[i], "-f") == 0)
			fnames.push_back(argv[++i]);
		else if (strcmp(argv[i], "--set-header") == 0)
			output_header = argv[++i];
		else if (strcmp(argv[i], "--no-value") == 0)
			no_value = std::stoi(std::string{argv[++i]});
		else if (strcmp(argv[i], "--path") == 0)
		{
			const string f_path = argv[++i];
			for_each(directory_iterator(f_path), directory_iterator(), [&fnames](directory_entry& p){
				if (is_regular_file(p) && p.path().extension() == ".csv")
				{
					fnames.push_back(p.path().native());
				}
			});
		}
		else if (strcmp(argv[i], "--dry-run") == 0)
			dry_run_exec = true;
		else if (strcmp(argv[i], "--input-sep") == 0)
			input_sep = argv[++i];
		else if (strcmp(argv[i], "--output-sep") == 0)
			output_sep = argv[++i];
		else if (strcmp(argv[i], "--output-file") == 0)
			output_file = argv[++i];
		else if (strcmp(argv[i], "-r") == 0)
		{
			string args = argv[++i];
			auto p = args.find(":");
			if (p != string::npos)
			{
				registers[args.substr(0, p)] = args.substr(p+1);
			}
		}
		i++;
	}


	if (proj_fields.empty()) { std::cerr << "Projection fields list is empty!" << std::endl; exit(1); }
	if (sum_fields.empty())  { std::cerr << "Aggregation fields list is empty!" << std::endl; exit(1); }
	if (keys_fields.empty()) { std::cerr << "Key fields list is empty!" << std::endl; exit(1); }
	if (fnames.empty())      { std::cerr << "No files selected" << std::endl; exit(1); }


	if (dry_run_exec)
	{
		dry_run(fnames, keys_fields, sum_fields, proj_fields, registers, output_header, input_sep);
		return 0;
	}

	const auto non_valid = std::make_pair(0, false);
	
	BuildKey key_builder{keys_fields};
	std::unordered_map<uint64_t, mapval_t<int64_t>> map_object;

	//( value , is_valid )
	std::vector<std::pair<int64_t, bool>> partial(sum_fields.size());
	const size_t ksize = keys_fields.size();

	for (const auto& fname : fnames)
	{
		splitter(fname, input_sep, [&map_object, &partial, &sum_fields, &ksize, &keys_fields, &no_value, &non_valid, &key_builder](const std::vector<std_exp::string_view>& v)
		{
			for (const auto& index : sum_fields)
			{
				int64_t n = fast_atol(v[index.first]);
				//std::cerr << "f: " << index.first << ", s: " << index.second << ", n: " << n << std::endl;
				if(n != no_value)
					partial[index.second] = make_pair(n, true);
				else
					partial[index.second] = non_valid;
			}
			
			const uint64_t key = key_builder.hash(v);
			
			auto it = map_object.find(key);
			if (it != map_object.end())
			{
				//exists
				std::transform(
					it->second.sum_val.begin(), it->second.sum_val.end(),
					partial.begin(), it->second.sum_val.begin(),
					[](const pval_t& a, const pval_t& b)
					{
						if (a.second == true && b.second == true)
							return std::make_pair(a.first + b.first, true);
						else if (a.second == true && b.second == false)
							return std::make_pair(a.first, true);
						else if (a.second == false && b.second == true)
							return std::make_pair(b.first, true);
						else
							return std::make_pair(static_cast<int64_t>(0), false);
					}
				);
			}
			else
			{
				auto& obj = map_object[key];
				obj.key_val.resize(ksize);
				for (const auto& index : keys_fields)
					obj.key_val[index.second] = v[index.first].to_string();

				obj.sum_val = partial;
			}
		}, skip_line);
	}

	// save
	std::ofstream fout{output_file};

	if (!output_header.empty())
		fout << output_header << endl;


	auto get = [&sum_fields, &keys_fields, &no_value](uint32_t k, const mapval_t<int64_t>& mval, auto printer) {
		const auto it = sum_fields.find(k);
		if (it != sum_fields.end())
		{
			// is a sum fields
			const auto& val = mval.sum_val.at(it->second);
			if (val.second)
				printer(val.first);
			else
				printer(no_value);
		}
		else
		{
			const auto jt = keys_fields.find(k);
			printer(mval.key_val.at(jt->second));
		}
	};


	auto print = [&output_sep, &fout](const auto& val_print, bool& flag) {
		if (flag) { fout << val_print; flag = false; }
		else
			fout << output_sep << val_print;
	};


	std::vector<uint32_t> proj_fields_n;
	std::transform(
		proj_fields.begin(),
		proj_fields.end(),
		std::back_inserter(proj_fields_n),
		[](const std::string& e) -> unsigned long {
			try {
				return std::stoul(e);
			} catch (...) {
				return 0;
			}
		}
	);

	//show aggregate
	for (const auto& o : map_object)
	{
		size_t j{};
		bool f{true};
		for (const auto& e : proj_fields)
		{
			if (e[0] == '%')
				print(registers[e], f);
			else
				get(proj_fields_n[j], o.second, [&print, &f](const auto& v){ print(v, f); });
			j++;
		}

		fout << '\n';
	}

	fout.close();
}



/*
 * Dry-RUN
 */

void dry_run (
	const std::vector<std::string>& fnames,
	std::map<uint32_t, uint32_t>& keys_fields,
	std::map<uint32_t, uint32_t>& sum_fields,
	vector<string>& proj_fields,
	const map<string,string>& registers,
	const string& output_header,
	const string& input_sep)
{
	// show files
	for (const auto& f : fnames)
		std::cout << "Reading file: " << f << endl;
	std::cout << endl;

	//open first file
	{
		std::string line;
		std::vector<std::string> strs;

		std::ifstream f{fnames[0]};
		const auto sep = boost::is_any_of(input_sep);
		std::getline(f, line);
		boost::split(strs, line, sep);

		if (strs.size() == 1)
		{
			std::cout << "Bad separator: size = " << strs.size() << endl;
			return;
		}

		//string key = build_key(keys_fields, strs);
		std::cout << "#fields:\t" << strs.size() << endl;
		std::cout << "keys size:\t" << keys_fields.size() << endl;
		std::cout << "aggr size:\t" << sum_fields.size() << endl;
		std::cout << "prj size:\t" << proj_fields.size() << endl;
		//std::cout << "Key:\t" << key << endl;
		std::cout << "Output Header:\t" << output_header << endl;

		// list of element not use in projection
		{
			bool found{false};
			std::cout << "#fields not use in projection: ";
			for(size_t i = 0; i < strs.size(); i++)
			{
				if (find(proj_fields.begin(), proj_fields.end(), to_string(i)) == proj_fields.end())
				{
					found = true;
					std::cout << i << " ";
				}
			}
			if (!found) cout << 0 << endl;
			std::cout << endl;
		}

		// check errors
		if(keys_fields.size() > strs.size())
			std::cout << "Keys field list is too big: (" << keys_fields.size() << ">" << strs.size() << ")" << endl;

		if(sum_fields.size() > strs.size())
			std::cout << "Aggregation field list is too big: (" << sum_fields.size() << ">" << strs.size() << ")" << endl;


		for (const auto& k: proj_fields)
		{
			if (k.find("%") == 0 && registers.find(k) == registers.end())
				std::cout << "Register " << k << " used but not initialized" << endl;
		}


		// key elements & aggregation elements should be differents
		{
			std::vector<uint32_t> s_fields;
			std::vector<uint32_t> k_fields;
			std::vector<uint32_t> diff;

			std::for_each(sum_fields.begin(), sum_fields.end(), [&s_fields](const auto& e){ s_fields.push_back(e.first); });
			std::for_each(keys_fields.begin(), keys_fields.end(), [&k_fields](const auto& e){ k_fields.push_back(e.first); });

			std::set_intersection(
				k_fields.begin(), k_fields.end(),
				s_fields.begin(), s_fields.end(), back_inserter(diff));

			if (!diff.empty())
			{
				std::cout << "Elements that are both present in keys and in aggregation: ";
				for (const auto& e : diff)
					std::cout << e << " ";
				std::cout << endl;
			}
		}

		{
			const auto bad_keys = std::count_if(keys_fields.begin(), keys_fields.end(), [&strs](const auto& e){ return e.first >= strs.size(); });
			const auto bad_sum = std::count_if(sum_fields.begin(), sum_fields.end(), [&strs](const auto& e){ return e.first >= strs.size(); });
			const auto bad_prj = std::count_if(proj_fields.begin(), proj_fields.end(), [&strs](const string& e){
				if (e.find("%") == string::npos)
					return stoull(e) >= strs.size();
				return false;
			});

			if (bad_keys > 0)
				cout << "There are " << bad_keys << " elements in Keys list that exceded the total number of fields" << endl;
			if (bad_sum > 0)
				cout << "There are " << bad_sum << " elements in Aggregation list that exceded the total number of fields" << endl;
			if (bad_prj > 0)
				cout << "There are " << bad_prj << " elements in Projection list that exceded the total number of fields" << endl;
		}

		cout << endl;
	}
}


void help()
{
	cout << "Aggregation Tool " << VERSION << " compiled on " << __DATE__ << "@" << __TIME__ << endl << endl;
	cout << "aggregate [options]" << endl << endl;
	cout << " -k               are the keys-elements used for aggregation" << endl;
	cout << " -s               are the sums-elements used for aggregation" << endl;
	cout << " -p               are the sums-elements used for projection" << endl;
	cout << " -r               specify a register ex.: -r %t:123; you can use that register inside a projection list" << endl;
	cout << " --skip-line      number of rows (starting from head) to skip" << endl;
	cout << " -f               is the file to load (coudl be used serveral times)" << endl;
	cout << " --path           is the path where to find csv input files" << endl;
	cout << " --input-sep      is the csv input separator" << endl;
	cout << " --output-sep     is the csv output separator" << endl;
	cout << " --output-file    is the output file" << endl;
	cout << " --no-value       specify witch is the \"no value\" (default: \"-1\")" << endl;
	cout << " --set-header     specify the header to use for the output csv" << endl;
	cout << " --dry-run        execute some test on input parameter" << endl;
	cout << " --help           print this help and exit" << endl;
	cout << " --version        print the version number and exit" << endl;

	cout << endl;
	cout << "ex: ./aggregate -r %t:1982 -k \"2-20\" -s \"21-35\" -p \"%t;1-35\" --skip-line 1 --path /mnt/disk-master/BI_SUB_IUCS_STATS_RAW/ --output-file out.csv" << endl;
	cout << endl;
}

