/*@brief: A simplified implementation (without using locks or CAS atomic operations) 
*         of the SPMC LMAX Disruptor design from the paper :
*         "Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads" 
*         by Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, Andrew Stewart.
* @Email: <lyapium@proton.me>
* @Date : 20 July, 2024
*/
#include <iostream>
#include <thread>
#include <string>
#include <numeric>
#include <vector>
#include <chrono>
#include <atomic>

//Make the block address aligned to exactly fit on a cacheline of 64 bytes
struct alignas(64) Block
{
    uint8_t data[63];
    uint8_t size_;
};
class RingBuffer
{
public:
    ~RingBuffer() { delete[] buffer; }
    //size must be power of 2 to enable efficient remainder computaion 
    RingBuffer(uint32_t power = 8) 
    {
        uint32_t size = (1 << power);
        buffer = new Block[size];
        l = size - 1;
    }
    void Write(uint32_t block_idx, void* data, uint8_t size)
    {
        uint32_t idx = block_idx & l;
        buffer[idx].size_ = size;
        std::memcpy(buffer[idx].data, data, size);
    }
    void Read(uint32_t block_idx, void* data, uint8_t& size) const
    {
        uint32_t idx = block_idx & l;
        size = buffer[idx].size_;
        std::memcpy(data, buffer[idx].data, size);
    }
private:
    Block* buffer;
    uint32_t l;
};
//Align Counter type so that each counter fits on a single cacheline of 64 bytes to
//prevent false sharing
struct alignas(64) Counter
{
    std::atomic<uint64_t> sequence;
};
Counter sequence = { 0 };
const int numConsumers = 4;
Counter consumer_sequences[numConsumers] = { 0 };
const uint32_t p = 8;
const uint32_t size_ = 1 << p;
const uint64_t ull_max = (std::numeric_limits<uint64_t>::max)();
RingBuffer buffer(p);
bool producer_running = true, consumer_running = true;
void producer()
{
    while (producer_running)
    {
        bool cont = true;
        for (int i = 0; i < numConsumers; ++i)
        {
            auto diff = sequence.sequence - consumer_sequences[i].sequence;
            if (diff >= size_)
            {
                cont = false;
                break;
            }
        }
        if (!cont)
        {
            continue;
        }
        if (sequence.sequence == ull_max)
        {
            std::cout << "completed 1 epoch: " << ull_max << "\n";
            uint64_t c = ull_max / size_ - 2;
            for (int i = 0; i < numConsumers; ++i)
            {
                consumer_sequences[i].sequence -= c * size_;
            }
            sequence.sequence -= c * size_;
        }
        std::string message = "Message: " + std::to_string(sequence.sequence);
        buffer.Write((uint32_t)sequence.sequence, (void*)message.c_str(), message.size() + 1);
        sequence.sequence.fetch_add(1, std::memory_order_release);
    }
}
void consumer(const int consumer_id, const int next_consumer_id)
{
    
    while (consumer_running || consumer_sequences[consumer_id].sequence < 
        sequence.sequence.load(std::memory_order_acquire))
    {
        if(consumer_sequences[consumer_id].sequence
            < sequence.sequence.load(std::memory_order_acquire)
            && (next_consumer_id == -1 ||
                consumer_sequences[consumer_id].sequence
                < consumer_sequences[next_consumer_id].sequence.load(std::memory_order_acquire))
            )
        {
            alignas(64) uint8_t data[64];
            uint8_t data_size;
            buffer.Read((uint32_t)consumer_sequences[consumer_id].sequence, (void*)data, data_size);
            consumer_sequences[consumer_id].sequence.fetch_add(1, std::memory_order_release);
        }
    }
}
int main()
{
    auto start = std::chrono::high_resolution_clock::now();
    std::thread producer_thread(&producer);
    std::vector<std::thread> consumer_threads(numConsumers);
    for (int i = 0; i < numConsumers; ++i)
    {
        int next_consumer_id = i == numConsumers - 1 ? -1 : i + 1;
        consumer_threads[i] = std::thread(&consumer, i, next_consumer_id);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    producer_running = false;
    producer_thread.join();
    consumer_running = false;
    for (int i = 0; i < numConsumers; ++i)
    {
        consumer_threads[i].join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() * 1e-9;
    std::cout << sequence.sequence << " produced\n \n";
    for (int i = 0; i < numConsumers; ++i)
    {
        std::cout << consumer_sequences[i].sequence << " consumed by consumer " << i << "\n";
    }
    std::cout << "time spent: " << dur << " secs\n";
    //
}
