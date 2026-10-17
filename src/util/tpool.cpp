/*
*
* wave tracer
* Copyright  Shlomi Steinberg
*
* LICENSE: Creative Commons Attribution-NonCommercial 4.0 International
*
*/

#include <wt/util/thread_pool/tpool.hpp>
#include <wt/util/thread_pool/utils.hpp>

#include <wt/util/logger/logger.hpp>

#if defined(__APPLE__) || defined(__linux__)
#include <pthread.h>
#endif

#include <functional>
#include <memory>

using namespace wt;
using namespace wt::thread_pool;

thread_local std::uint32_t wt::thread_pool::tpool_tids_t::tid = tpool_tids_t::default_tid;

namespace {
inline void* tpool_worker_trampoline(void* p) {
    std::unique_ptr<std::function<void()>> fn{(std::function<void()>*)p};
    (*fn)();
    return nullptr;
}
}

std::vector<std::thread> tpool_t::spawn_threads(std::size_t threads) {
    std::vector<std::thread> t;
    t.reserve(threads);

#if defined(__APPLE__) || defined(__linux__)
    // NOTE: wave_tracer's cone traversal recurses deeply (bvh8w_t::intersect).
    // macOS gives secondary threads a fixed 512 KiB stack (Linux follows
    // `ulimit -s`, typically 8 MiB), which overflows with SIGBUS on deep
    // traversals. Spawn workers through pthread with a large explicit stack;
    // each std::thread slot then merely joins its worker.
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 32ul << 20);
    bool all_spawned = true;
    for (auto i=0ul;i<threads;++i) {
        auto* pfn = new std::function<void()>([tid=i, d=&this->d]() {
            // write this worker's id in the global thread_local indicator
            tpool_tids_t::tid = (std::uint32_t)tid;

            std::size_t completed=0;
            while (true) {
                {
                    std::unique_lock l(d->m);
                    // update enqueued tasks counter
                    if (completed)
                        d->enqueued_tasks -= completed;
                    // wait on cv
                    d->cv.wait(l, [d]() {
                        return d->terminate_flag || d->enqueued_tasks>0;
                    });
                }
                // check terminate signal
                if (d->terminate_flag)
                    break;

                for (completed=0;;++completed) {
                    // process tasks
                    task_t task;
                    if (!d->task_queue.try_pop(task))
                        break;

                    std::visit([](auto&& f) noexcept {
                        using Task = std::decay_t<decltype(f)>;
                        if constexpr (std::is_same_v<Task,std::coroutine_handle<>>)
                            f.resume();
                        else
                            f();
                    }, task.f);
                }
            }
        });
        pthread_t h;
        if (pthread_create(&h, &attr, &tpool_worker_trampoline, pfn)==0) {
            t.emplace_back([h]() { pthread_join(h, nullptr); });
        } else {
            delete pfn;
            all_spawned = false;
            break;
        }
    }
    pthread_attr_destroy(&attr);
    if (all_spawned) {
        logger::cout(verbosity_e::info) << "(thread_pool) spawned " << std::format("{:L}", threads) << " threads.\n";
        return t;
    }
    // pthread_create failed midway: join what we have and fall back below.
    for (auto& th : t) th.join();
    t.clear();
#endif

    for (auto i=0ul;i<threads;++i) {
        t.emplace_back([tid=i,d=&this->d]() {
            // write this worker's id in the global thread_local indicator
            tpool_tids_t::tid = (std::uint32_t)tid;

            std::size_t completed=0;
            while (true) {
                {
                    std::unique_lock l(d->m);
                    // update enqueued tasks counter
                    if (completed)
                        d->enqueued_tasks -= completed;
                    // wait on cv
                    d->cv.wait(l, [d]() {
                        return d->terminate_flag || d->enqueued_tasks>0;
                    });
                }
                // check terminate signal
                if (d->terminate_flag)
                    break;

                for (completed=0;;++completed) {
                    // process tasks
                    task_t task;
                    if (!d->task_queue.try_pop(task))
                        break;

                    std::visit([](auto&& f) noexcept {
                        using Task = std::decay_t<decltype(f)>;
                        if constexpr (std::is_same_v<Task,std::coroutine_handle<>>)
                            f.resume();
                        else
                            f();
                    }, task.f);
                }
            }
        });
    }

    logger::cout(verbosity_e::info) << "(thread_pool) spawned " << std::format("{:L}", threads) << " threads.\n";

    return t;
}

