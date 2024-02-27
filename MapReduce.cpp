#include <iostream>
#include <algorithm>
#include <ctime>
#include <cstring>
#include <fstream>
#include <mutex>
#include <numeric>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace std;
int file_count = 0;    // 분할한 파일 개수

// Map 인터페이스
class imapper {
public:
    virtual void map(const string& input, unordered_map<string, int>& output) = 0;
    virtual ~imapper() = default;
};

// Reduce 인터페이스
class ireducer {
public:
    virtual void reduce(const unordered_map<string, int>& intermediate) = 0;
    virtual ~ireducer() = default;
};

// Wordcount - Map 클래스
class wordcountmapper : public imapper {
public:
    void map(const string& input, unordered_map<string, int>& output) override {
        istringstream iss(input);
        string word;

        while (iss >> word) {
            // 각 단어의 카운트를 증가시킵니다.
            output[word]++;
        }
    }
};

// Wordcount - Reduce 클래스
class wordcountreducer : public ireducer {
public:
    void reduce(const unordered_map<string, int>& intermediate) override {

        // output 파일 설정
        ofstream out;

        // 임시 파일 생성
        file_count++;
        string file_name = "split_" + to_string(file_count) + ".txt";
        out.open(file_name);

        // 데이터를 파일에 추가
        for (const auto& entry : intermediate) {
            out << entry.first << " " << entry.second << '\n';
        }
        out.close();
    }
};

struct concurrentqueue {
    unordered_map<string, int> data;
};

struct heapnode {
    string sentence;
    int index;
    heapnode(string a, int b) : sentence(a), index(b) {}
    bool operator<(const heapnode& rhs) const {
        return (sentence.compare(rhs.sentence) > 0);
    }
};


void enqueue(concurrentqueue& queue, const string& key, int value) {
    queue.data[key] += value;
}


void mapthread(imapper* mapper, const vector<string>& lines, concurrentqueue& queue) {
    for (const auto& line : lines) {
        mapper->map(line, queue.data);
    }
}


// Map-Reduce 함수
concurrentqueue mapreduce(vector<string> data, int split_count)
{
    // 1. Map 작업
    // map 객체 생성
    imapper* mapper = new wordcountmapper();

    // 스레드 및 큐 생성
    const int numthreads = thread::hardware_concurrency();
    vector<thread> threads;
    vector<concurrentqueue> queues(numthreads);

    // 입력받은 줄 수에 맞게 벡터 크기 재조정
    data.reserve(data.size());

    // 입력받은 줄 수에 맞게 큐 크기 재조정
    for (auto& queue : queues)
        queue.data.reserve(data.size());

    // numthread 개수만큼 스레드 생성 후 mapthread 함수 실행
    // mapper, lines는 인자 역할, queues는 결과 저장 역할.
    for (int i = 0; i < numthreads; ++i) {
        threads.emplace_back(mapthread, mapper, cref(data), ref(queues[i]));
    }

    // 스레드 실행
    for (auto& thread : threads) {
        thread.join();
    }

    // 여러 큐의 데이터를 하나의 큐로 통합
    concurrentqueue combinedqueue;
    for (const auto& threadqueue : queues) {
        for (const auto& entry : threadqueue.data) {
            enqueue(combinedqueue, entry.first, entry.second);
        }
    }

    // 2. Reduce 작업
    // reduce 객체 생성
    ireducer* reducer = new wordcountreducer();

    reducer->reduce(combinedqueue.data);
    delete reducer;
    delete mapper;

    return combinedqueue;

}


// 외부 정렬을 위한 파일 분할 함수
int file_split(string input_name, int MEMORY_SIZE)
{
    ifstream in;
    in.open(input_name.c_str());
    if (!in.is_open()) {
        cout << "Input file 에러\n";
        exit(-1);
    }

    vector<string> data;
    data.clear();

    // input 파일 크기 측정
    int input_size;
    in.seekg(0, in.end);
    input_size = in.tellg();
    in.seekg(0, in.beg);
    cout << "파일 크기 : " << input_size << " bytes\n\n";


    int using_memory = 0;   // 현재 사용 메모리
    int count = 0;

    cout << "1. input 파일 분할 작업 수행중...\n";

    while (!in.eof())
    {
        // 텍스트 파일에서 한 줄씩 읽어옴
        string line;
        getline(in, line);

        // 특수문자 제거
        auto replace_special = [](char& c) {
            if (!isalnum(c) && !isspace(c))
                c = ' ';
        };
        for_each(line.begin(), line.end(), replace_special);

        // 메모리 용량보다 작을 경우 data 벡터에 저장.
        if (using_memory + line.size() < MEMORY_SIZE) {
            using_memory += line.size() + 1;
            data.push_back(line);
        }

        // 메모리 용량보다 클 경우 데이터 정렬 후 텍스트 파일로 추출
        else {

            // MapReduce 작업
            concurrentqueue map_result = mapreduce(data, file_count);
            cout << file_count << "번째 파일 분할 및 map 작업 중...\n";

            data.clear();
            using_memory = line.size();
        }
    }


    // 버퍼에 남은 데이터를 텍스트 파일에 추가
    if (data.size() > 0) {

        // MapReduce 작업
        concurrentqueue map_result = mapreduce(data, file_count);

        cout << file_count << "번째 파일 분할 및 map 작업 완료\n";
    }

    return file_count;
}


void merge(int start, int end, int location) {

    int split_count = end - start + 1;      // 반복 한 번에 처리할 파일 개수


    // input 파일 설정
    // 파일의 병합 개수가 달라질 수 있으므로 벡터 사용
    vector<ifstream> in(split_count);
    for (int i = 0; i < split_count; i++) {
        string file_name = "split_" + to_string(start + i) + ".txt";
        in[i].open(file_name);
    }

    // output 파일 설정
    ofstream out;
    string file_name = "split_" + to_string(location) + ".txt";
    out.open(file_name);

    // 우선순위 큐 사용 - 최소 힙 이용
    priority_queue<heapnode, vector<heapnode> > heap;

    // 텍스트 파일에서 한 줄씩 읽어옴
    for (int i = 0; i < split_count; i++) {
        string line;
        if (!in[i].eof()) {
            getline(in[i], line);
            heap.push(heapnode(line, i));
        }
    }

    cout << start << "~" << end << "번 파일 병합\n";

    while (!heap.empty()) {
        string sentence = heap.top().sentence;
        int index = heap.top().index;
        heap.pop();

        out << sentence << endl;

        if (!in[index].eof()) {
            getline(in[index], sentence);
            heap.push(heapnode(sentence, index));
        }
    }

    for (int i = 0; i < split_count; i++) {
        in[i].close();
    }

    out.close();
}


void external_merge(int split_count) {

    int start = 1;
    int end = split_count;
    int merge_count = 0;

    cout << "2. 외부 정렬 작업 수행중...\n";

    // 외부 정렬
    while (start < end) {
        merge_count++;
        cout << "병합 " << merge_count << "주기 작업중...\n";
        int merge_location = end;
        int distance = 4;     // 반복 한 번에 처리할 파일 개수 
        int time = (end - start + 1) / distance + 1;    // 필요한 반복 횟수
        if ((end - start + 1) / time < distance) {
            distance = (end - start + 1) / time + 1;
        }
        // 반복
        while (start <= end) {
            // 병합 범위 결정
            int mid = min(start + distance, end);
            merge_location++;
            merge(start, mid, merge_location);
            start = mid + 1;
        }
        end = merge_location;
    }
    cout << "병합 작업 완료\n";

    string file_name = "split_" + to_string(start) + ".txt";
    rename(file_name.c_str(), "merge.txt");

    cout << "임시 파일 삭제 중...\n";
    for (int i = 1; i < end; i++) {
        string file_name = "split_" + to_string(i) + ".txt";
        remove(file_name.c_str());
    }
}


void duplicate_check()
{
    cout << "3. 중복 제거 작업 수행중...\n";

    ifstream in("merge.txt");

    vector<pair<string, int>> data;
    string line;

    while (getline(in, line))
    {
        istringstream iss(line);
        string key;
        int value;

        if (iss >> key >> value) {
            auto it = find_if(data.begin(), data.end(), [&key](const pair<string, int>& element) {
                return element.first == key;
            });

            if (it != data.end())
                it->second += value;
            else
                data.emplace_back(key, value);

        }
    }

    sort(data.begin(), data.end(),
        [](const pair<string, int>& a, const pair<string, int>& b) {
            return a.first < b.first;
        });

    // output 파일 설정
    ofstream out;
    out.open("output.txt");

    // 데이터를 파일에 추가
    for (const auto& entry : data) {
        out << entry.first << " : " << entry.second << '\n';
    }
    
    out.close();
    in.close();
    remove("merge.txt");
}


int main() {

    // 메모리 할당 체크
    int MEMORY_SIZE;
    cout << "메모리 할당 크기 (단위 = MB) : ";
    cin >> MEMORY_SIZE;
    MEMORY_SIZE = MEMORY_SIZE * 1024 * 1000;   // 메모리 할당 크기(1MB)

    // input 파일 설정
    string input_name;
    cout << "Input 파일명 : ";
    cin >> input_name;


    const clock_t begin_time = clock();

    // 임시 파일 분할
    int split_count = file_split(input_name, MEMORY_SIZE);
    cout << "분할한 파일 개수 : " << split_count << "\n";
    cout << "임시 파일 분할에 걸린 시간 : " << float(clock() - begin_time) / CLOCKS_PER_SEC << "s\n\n";
    
    // 외부 정렬
    external_merge(split_count);
    cout << "외부 정렬에 걸린 시간 : " << float(clock() - begin_time) / CLOCKS_PER_SEC << "s\n\n";

    // 중복 제거
    duplicate_check();

    // 경과 시간 출력
    cout << "프로그램 총 동작 시간 : " << float(clock() - begin_time) / CLOCKS_PER_SEC << "s\n";

    return 0;
}
