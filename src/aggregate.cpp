#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <xxhash.h>



/*
 *  Aggregator  (written by Gian Lorenzo Meocci <glmeocci@gmail.com>)
 *
 */


#define VERSION "1.3.0"


using namespace boost::filesystem;
using namespace std;


typedef pair<int64_t, bool> pval_t;

template <typename T>
struct mapval_t
{
    vector<pair<T, bool>> sum_val;
    map<uint32_t, string> prj_val;
};


template <typename F>
void splitter(const string& fname, const string& separator, F fun, size_t skip_line)
{
    ifstream f{fname};
    string line;
    const auto sep = boost::is_any_of(separator);
    size_t skipped{0};

    while(f && skipped < skip_line)
    {
        getline(f, line);
        skipped++;
    }

    while(f)
    {
        getline(f, line);
        
        if (!line.empty())
        {
            vector<string> strs;
            boost::split(strs, line, sep);
            fun(strs);
        }
    }
}


vector<uint32_t> get_index_uint32(const string& index)
{
    vector<uint32_t> indexs;
    const auto sep = boost::is_any_of(";");

    vector<string> k_strs;
    boost::split(k_strs, index, sep);

    for(const auto& k : k_strs)
    {
        auto p = k.find("-");
        if(p != string::npos)
        {
            const auto b = stoi(k.substr(0, p));
            const auto e = stoi(k.substr(p+1));
            for(size_t n = b; n <= e; n++)
                indexs.push_back(n);
        }
        else
            indexs.push_back(std::stoi(k));
    }

    return indexs;
}

vector<string> get_index_string(const string& index)
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
            const auto b = stoi(k.substr(0, p));
            const auto e = stoi(k.substr(p+1));
            for(size_t n = b; n <= e; n++)
                indexs.push_back(to_string(n));
        }
        else
            indexs.push_back(k);
    }

    return indexs;
}

class BuildKey
{
public:
    BuildKey(const vector<uint32_t>& key_index) : _key_index(key_index)
    {
        state = XXH64_createState();
    }

    uint64_t hash(const vector<string>& line)
    {
        XXH64_reset(state, 0);
        for (const auto k : _key_index)
        {
            //cout << (state != nullptr) << " " << k << " " << line[k] << " " << line[k].size() << std::endl;
            XXH64_update(state, (void*)line[k].c_str(), line[k].size());
        }
        return XXH64_digest(state);
    }

    ~BuildKey()
    {
        XXH64_freeState(state);
    }

private:
    XXH64_state_t* state;
    const vector<uint32_t>& _key_index;
};


void help();
void dry_run(
    const vector<string>& fnames,
    vector<uint32_t>& keys_fields,
    vector<uint32_t>& sum_fields,
    vector<string>& proj_fields,
    const map<string, string>& registers,
    const string& output_header,
    const string& input_sep);



int main(int argc, char* argv[])
{
    unordered_map<uint64_t, mapval_t<int64_t>> map_object;

    vector<uint32_t> sum_fields;
    vector<uint32_t> keys_fields;
    vector<string> proj_fields;

    map<string, string> registers;

    BuildKey key_builder{keys_fields};

    bool dry_run_exec{false}, multi_thread{false};
    size_t skip_line{0};
    string output_header{};

    vector<string> header_lines{};
    vector<string> fnames{};
    int64_t no_value{-1};
    string input_sep{","}, output_sep{","};
    string output_file{"out.csv"};

    size_t i{};
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


    if (proj_fields.empty()) { cerr << "Projection fields list is empty!" << endl; exit(1); }
    if (sum_fields.empty())  { cerr << "Aggregation fields list is empty!" << endl; exit(1); }
    if (keys_fields.empty()) { cerr << "Key fields list is empty!" << endl; exit(1); }
    if (fnames.empty())      { cerr << "No files selected" << endl; exit(1); }


    if (dry_run_exec)
    {
        dry_run(fnames, keys_fields, sum_fields, proj_fields, registers, output_header, input_sep);
        return 0;
    }

    const auto non_valid = make_pair(0, false);

    //( value , is_valid )
    vector<pair<int64_t, bool>> partial(sum_fields.size());
    
    for (const auto& fname : fnames)
    {
        splitter(fname, input_sep, [&map_object, &sum_fields, &proj_fields, &keys_fields, &no_value, &non_valid, &partial, &key_builder](const vector<string>& v)
        {
            map<uint32_t, string> partial_prj;

            size_t j{};
            for(const auto& index : sum_fields)
            {
                int64_t n = std::stol(v[index]);
                if(n != no_value)
                    partial[j] = make_pair(n, true);
                else
                    partial[j] = non_valid;
                j++;
            }
            
            for(const auto& index : keys_fields)
                partial_prj[index] = v[index];


            const uint64_t key = key_builder.hash(v);
            auto it = map_object.find(key);
            if(it != map_object.end())
            {
                //exists
                transform(
                    it->second.sum_val.begin(), it->second.sum_val.end(),
                    partial.begin(), it->second.sum_val.begin(),
                    [](const pval_t& a, const pval_t& b)
                    {
                        if (a.second == true && b.second == true)
                            return make_pair(a.first + b.first, true);
                        else if (a.second == true && b.second == false)
                            return make_pair(a.first, true);
                        else if (a.second == false && b.second == true)
                            return make_pair(b.first, true);
                        else
                            return make_pair(static_cast<int64_t>(0), false);
                    }
                );
            }
            else
            {
                map_object[key].sum_val = partial;
                map_object[key].prj_val = partial_prj;
            }
        }, skip_line);
    }


    // save
    ofstream fout{output_file};

    if (!output_header.empty())
        fout << output_header << endl;


    auto get = [&sum_fields, &keys_fields, &no_value](uint32_t k, const mapval_t<int64_t>& o, auto printer) {
        const auto it = find(sum_fields.begin(), sum_fields.end(), k);
        if (it != sum_fields.end())
        {
            // is a sum fields
            const auto v = o.sum_val[it - sum_fields.begin()];
            if (v.second)
                printer(v.first);
            else
                printer(no_value);
        }
        else
        {
            const auto kt = find(keys_fields.begin(), keys_fields.end(), k);
            printer(o.prj_val.at(*kt));
        }
    };


    auto print = [&output_sep, &fout](const auto& v, bool& f) {
        if (f) { fout << v; f = false; }
        else
            fout << output_sep << v;
    };


    std::vector<uint32_t> proj_fields_n;
    std::transform(proj_fields.begin(), proj_fields.end(), std::back_inserter(proj_fields_n), [](const auto& e){ return std::stoi(e); });


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
                get(proj_fields_n[j], o.second, [&](const auto& v){ print(v, f); });
            j++;
        }

        fout << "\n";
    }

    fout.close();
}



/*
 * Dry-RUN
 */

void dry_run (
    const vector<string>& fnames,
    vector<uint32_t>& keys_fields,
    vector<uint32_t>& sum_fields,
    vector<string>& proj_fields,
    const map<string,string>& registers,
    const string& output_header,
    const string& input_sep)
{
    // show files
    for(const auto& f : fnames)
    {
        cout << "Reading file: " << f << endl;
    }
    cout << endl;


    //open first file
    {
        string line;
        vector<string> strs;

        ifstream f{fnames[0]};
        const auto sep = boost::is_any_of(input_sep);
        getline(f, line);
        boost::split(strs, line, sep);

        if (strs.size() == 1)
        {
            cout << "Bad separator: size = " << strs.size() << endl;
            return;
        }

        //string key = build_key(keys_fields, strs);
        cout << "#fields:\t" << strs.size() << endl;
        cout << "keys size:\t" << keys_fields.size() << endl;
        cout << "aggr size:\t" << sum_fields.size() << endl;
        cout << "prj size:\t" << proj_fields.size() << endl;
        //cout << "Key:\t" << key << endl;
        cout << "Output Header:\t" << output_header << endl;

        // list of element not use in projection
        {
            bool found{false};
            cout << "#fields not use in projection: ";
            for(size_t i = 0; i < strs.size(); i++)
            {
                if (find(proj_fields.begin(), proj_fields.end(), to_string(i)) == proj_fields.end())
                {
                    found = true;
                    cout << i << " ";
                }
            }
            if (!found) cout << 0 << endl;
            cout << endl;
        }

        // check errors
        if(keys_fields.size() > strs.size())
            cout << "Keys field list is too big: (" << keys_fields.size() << ">" << strs.size() << ")" << endl;

        if(sum_fields.size() > strs.size())
            cout << "Aggregation field list is too big: (" << sum_fields.size() << ">" << strs.size() << ")" << endl;


        for (const auto& k: proj_fields)
        {
            if (k.find("%") == 0 && registers.find(k) == registers.end())
                cout << "Register " << k << " used but not initialized" << endl;
        }


        // key elements & aggregation elements should be differents
        {
            vector<uint32_t> diff;
            sort(keys_fields.begin(), keys_fields.end());
            sort(sum_fields.begin(), sum_fields.end());
            set_intersection(
                keys_fields.begin(), keys_fields.end(),
                sum_fields.begin(), sum_fields.end(), back_inserter(diff));

            if (!diff.empty())
            {
                cout << "Elements that are both present in keys and in aggregation: ";
                for (const auto& e : diff)
                    cout << e << " ";
                cout << endl;
            }
        }

        {
            const auto bad_keys = count_if(keys_fields.begin(), keys_fields.end(), [&strs](const uint32_t& e){ return e >= strs.size(); });
            const auto bad_sum = count_if(sum_fields.begin(), sum_fields.end(), [&strs](const uint32_t& e){ return e >= strs.size(); });
            const auto bad_prj = count_if(proj_fields.begin(), proj_fields.end(), [&strs](const string& e){
                if (e.find("%") == string::npos)
                    return stoi(e) >= strs.size();
                return false;
            });

            if(bad_keys > 0)
                cout << "There are " << bad_keys << " elements in Keys list that exceded the total number of fields";
            if(bad_sum > 0)
                cout << "There are " << bad_sum << " elements in Aggregation list that exceded the total number of fields";
            if(bad_prj > 0)
                cout << "There are " << bad_prj << " elements in Projection list that exceded the total number of fields";
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
    cout << "ex: ./aggregate -r %t:1982 -k \"2-20\" -s \"21-35\" -p \"%t;1-35\" --skip-line 1 --path /ssd/BI_SUB_UP_ACT_RAW --reuse-skipped --output-file out.csv" << endl;
    cout << endl;
}

